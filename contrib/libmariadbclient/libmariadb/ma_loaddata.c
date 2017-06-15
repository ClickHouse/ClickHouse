/************************************************************************************
    Copyright (C) 2000, 2011 MySQL AB & MySQL Finland AB & TCX DataKonsult AB,
                 Monty Program AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA

   Part of this code includes code from the PHP project which
   is freely available from http://www.php.net
*************************************************************************************/
/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 2006-2011 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Authors: Georg Richter <georg@mysql.com>                             |
  |          Andrey Hristov <andrey@mysql.com>                           |
  |          Ulf Wendel <uwendel@mysql.com>                              |
  +----------------------------------------------------------------------+
*/

#include "ma_global.h"
#include <ma_sys.h>
#include <ma_string.h>
#include "errmsg.h"
#include "mysql.h"
#include <mariadb/ma_io.h>
#include <string.h>
#ifdef _WIN32
#include <share.h>
#endif

typedef struct st_mysql_infile_info
{
  MA_FILE   *fp;
  int        error_no;
  char       error_msg[MYSQL_ERRMSG_SIZE + 1];
  const char *filename;
} MYSQL_INFILE_INFO;

/* {{{ mysql_local_infile_init */
static
int mysql_local_infile_init(void **ptr, const char *filename, void *userdata)
{
  MYSQL_INFILE_INFO *info;
  MYSQL *mysql= (MYSQL *)userdata;

  info = (MYSQL_INFILE_INFO *)malloc(sizeof(MYSQL_INFILE_INFO));
  if (!info) {
    return(1);
  }
  memset(info, 0, sizeof(MYSQL_INFILE_INFO));
  *ptr = info;

  info->filename = filename;

  info->fp= ma_open(filename, "rb", mysql);

  if (!info->fp)
  {
    /* error handling is done via mysql_local_infile_error function, so we
       need to copy error to info */
    if (mysql_errno(mysql) && !info->error_no)
    {
      info->error_no= mysql_errno(mysql);
      ma_strmake(info->error_msg, mysql_error(mysql), MYSQL_ERRMSG_SIZE);
    }
    else
    {
      info->error_no = errno;
      snprintf((char *)info->error_msg, sizeof(info->error_msg), 
                  CER(CR_FILE_NOT_FOUND), filename, info->error_no);
    }
    return(1);
  }

  return(0);
}
/* }}} */


/* {{{ mysql_local_infile_read */
static
int mysql_local_infile_read(void *ptr, char * buf, unsigned int buf_len)
{
  MYSQL_INFILE_INFO *info = (MYSQL_INFILE_INFO *)ptr;
  size_t count;

  count= ma_read((void *)buf, 1, (size_t)buf_len, info->fp);

  if (count == (size_t)-1)
  {
    info->error_no = errno;
    snprintf((char *)info->error_msg, sizeof(info->error_msg), 
              CER(CR_FILE_READ), info->filename, info->error_no);
  }
  return((int)count);
}
/* }}} */


/* {{{ mysql_local_infile_error */
static
int mysql_local_infile_error(void *ptr, char *error_buf, unsigned int error_buf_len)
{
  MYSQL_INFILE_INFO *info = (MYSQL_INFILE_INFO *)ptr;

  if (info) {
    ma_strmake(error_buf, info->error_msg, error_buf_len);
    return(info->error_no);
  }

  ma_strmake(error_buf, "Unknown error", error_buf_len);
  return(CR_UNKNOWN_ERROR);
}
/* }}} */


/* {{{ mysql_local_infile_end */
static
void mysql_local_infile_end(void *ptr)
{
  MYSQL_INFILE_INFO *info = (MYSQL_INFILE_INFO *)ptr;

  if (info)
  {
    if (info->fp)
      ma_close(info->fp);
    free(ptr);
  }		
  return;
}
/* }}} */


/* {{{ mysql_local_infile_default */
void mysql_set_local_infile_default(MYSQL *conn)
{
  conn->options.local_infile_init = mysql_local_infile_init;
  conn->options.local_infile_read = mysql_local_infile_read;
  conn->options.local_infile_error = mysql_local_infile_error;
  conn->options.local_infile_end = mysql_local_infile_end;
  return;
}
/* }}} */

/* {{{ mysql_set_local_infile_handler */
void STDCALL mysql_set_local_infile_handler(MYSQL *conn,
        int (*local_infile_init)(void **, const char *, void *),
        int (*local_infile_read)(void *, char *, uint),
        void (*local_infile_end)(void *),
        int (*local_infile_error)(void *, char *, uint),
        void *userdata)
{
  conn->options.local_infile_init=  local_infile_init;
  conn->options.local_infile_read=  local_infile_read;
  conn->options.local_infile_end=   local_infile_end;
  conn->options.local_infile_error= local_infile_error;
  conn->options.local_infile_userdata = userdata;
  return;
}
/* }}} */

/* {{{ mysql_handle_local_infile */
my_bool mysql_handle_local_infile(MYSQL *conn, const char *filename)
{
  unsigned int buflen= 4096;
  int bufread;
  unsigned char *buf= NULL;
  void *info= NULL;
  my_bool result= 1;

  /* check if all callback functions exist */
  if (!conn->options.local_infile_init || !conn->options.local_infile_end ||
      !conn->options.local_infile_read || !conn->options.local_infile_error)
  {
    conn->options.local_infile_userdata= conn;
    mysql_set_local_infile_default(conn);
  }

  if (!(conn->options.client_flag & CLIENT_LOCAL_FILES)) {
    my_set_error(conn, CR_UNKNOWN_ERROR, SQLSTATE_UNKNOWN, "Load data local infile forbidden");
    /* write empty packet to server */
    ma_net_write(&conn->net, (unsigned char *)"", 0);
    ma_net_flush(&conn->net);
    goto infile_error;
  }

  /* allocate buffer for reading data */
  buf = (uchar *)malloc(buflen);

  /* init handler: allocate read buffer and open file */
  if (conn->options.local_infile_init(&info, filename,
                                      conn->options.local_infile_userdata))
  {
    char tmp_buf[MYSQL_ERRMSG_SIZE];
    int tmp_errno;

    tmp_errno= conn->options.local_infile_error(info, tmp_buf, sizeof(tmp_buf));
    my_set_error(conn, tmp_errno, SQLSTATE_UNKNOWN, tmp_buf);
    ma_net_write(&conn->net, (unsigned char *)"", 0);
    ma_net_flush(&conn->net);
    goto infile_error;
  }

  /* read data */
  while ((bufread= conn->options.local_infile_read(info, (char *)buf, buflen)) > 0)
  {
    if (ma_net_write(&conn->net, (unsigned char *)buf, bufread))
    {
      my_set_error(conn, CR_SERVER_LOST, SQLSTATE_UNKNOWN, NULL);
      goto infile_error;
    }
  }

  /* send empty packet for eof */
  if (ma_net_write(&conn->net, (unsigned char *)"", 0) || 
      ma_net_flush(&conn->net))
  {
    my_set_error(conn, CR_SERVER_LOST, SQLSTATE_UNKNOWN, NULL);
    goto infile_error;
  }

  /* error during read occured */
  if (bufread < 0)
  {
    char tmp_buf[MYSQL_ERRMSG_SIZE];
    int tmp_errno= conn->options.local_infile_error(info, tmp_buf, sizeof(tmp_buf));
    my_set_error(conn, tmp_errno, SQLSTATE_UNKNOWN, tmp_buf);
    goto infile_error;
  }

  result = 0;

infile_error:
  conn->options.local_infile_end(info);
  free(buf);
  return(result);
}
/* }}} */

