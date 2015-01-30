#!/usr/bin/env python
import os
import commands
import time
import stat
import sys
import re
import glob
import logging
import time
import traceback
exec_shell=commands.getstatusoutput


#Splits the file in the parent process and commnuicate the content through pipe

is_file_avail  = True
is_init = True

def split_and_load (pipe_path,no_of_lines,feed_dir_path,err_file_path,fifo_util_path )  :
    
    global is_init
    global is_file_avail  
    if not os.path.isfile (feed_dir_path) :
        print "file not available " 
        #logger.error('File is not available') 
        sys.exit(1) 
    pid = os.fork() 
    if (pid == 0)  :
        if (is_init == True) :
            time.sleep(2)
            is_init = False
        try :
            while ( is_file_avail and stat.S_ISFIFO(os.stat(pipe_path).st_mode) ) :
                exec_sql_load_process ()
        except OSError :
            is_file_avail  = False
            sys.exit(0)
    else :
    	msg = "Waiting child pid = %s"% (pid) 
        print msg 
    	cmd_ = ('perl %s %s --fifo %s --lines %s') % (fifo_util_path,feed_dir_path
    						   ,pipe_path,no_of_lines)
    	(status,op) = exec_shell (cmd_)	
        (ret_pid,ret_exit_status) = os.waitpid(pid,0) 
        #logger.info ( ("%s|%s") %  (ret_pid,ret_exit_status) )
        if (ret_exit_status == 0) :
            sys.exit(0)
        else :
            #logger.error('Chunk loading error', exc_info=True)
            print "Chunk loading error"
        sys.exit (1)


def exec_sql_load_process () :

    #fd = open ('/tmp/testpipe') 
    #for line in fd :
    #    print line
    err_file_path='/tmp/db2_extract_err'
    load_file_path = 'load.sql'
    db2_cmd = ("db_cli < %s 2>> %s")%(load_file_path,err_file_path)
    (status,output) = exec_shell(db2_cmd)
    print status
    print output

def main () :
    
    FIFO_UTIL_PATH = ('%s/%s') % (os.getcwd(),'mk-fifo-split') 
    number_of_lines = 10000
    feed_dir_path = '/tmp/input_file.csv'
    err_file_path=  '/tmp/err.log'
    pipe_path = '/tmp/testpipe'
    split_and_load (pipe_path,number_of_lines,feed_dir_path,err_file_path,FIFO_UTIL_PATH)


if __name__ == "__main__" :
    main()

