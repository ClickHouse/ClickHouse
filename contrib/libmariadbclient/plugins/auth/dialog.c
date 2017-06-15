/************************************************************************************
   Copyright (C) 2014 MariaDB Corporation AB
   
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
*************************************************************************************/
#ifndef _WIN32
#define _GNU_SOURCE 1
#endif

#include <ma_global.h>
#include <mysql.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <memory.h>

#ifndef WIN32
#include <dlfcn.h>
#endif


/* function prototypes */
extern char *get_tty_password(char *opt_message, char *buff, int bufflen);
static int auth_dialog_open(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql);
static int auth_dialog_init(char *unused1, 
                            size_t unused2, 
                            int unused3, 
                            va_list);

mysql_authentication_dialog_ask_t auth_dialog_func;

#ifndef HAVE_DIALOG_DYNAMIC
struct st_mysql_client_plugin_AUTHENTICATION auth_dialog_plugin=
#else
struct st_mysql_client_plugin_AUTHENTICATION _mysql_client_plugin_declaration_ =
#endif
{
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN,
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN_INTERFACE_VERSION,
  "dialog",
  "Sergei Golubchik, Georg Richter",
  "Dialog Client Authentication Plugin",
  {0,1,0},
  "LGPL",
  NULL,
  auth_dialog_init,
  NULL,
  NULL,
  auth_dialog_open
};


/* {{{ static char *auth_dialog_native_prompt */
/*
   Native dialog prompt via stdin

   SYNOPSIS
     auth_dialog_native_prompt
     mysql            connection handle
     type             input type
     prompt           prompt
     buffer           Input buffer
     buffer_len       Input buffer length

  DESCRIPTION
    
  RETURNS
    Input buffer
*/
static char *auth_dialog_native_prompt(MYSQL *mysql __attribute__((unused)),
                                       int type,
                                       const char *prompt,
                                       char *buffer,
                                       int buffer_len)
{
  /* display prompt */
  fprintf(stdout, "%s", prompt);

  memset(buffer, 0, buffer_len);

  /* for type 2 (password) don't display input */
  if (type != 2)
  {
    if (fgets(buffer, buffer_len - 1, stdin))
    {
      /* remove trailing line break */
      size_t length= strlen(buffer);
      if (length && buffer[length - 1] == '\n')
        buffer[length - 1]= 0;
    }
  }
  else
  {
    get_tty_password((char *)"", buffer, buffer_len - 1);
  }
  return buffer;
}
/* }}} */

/* {{{ static int auth_dialog_open */
/*
   opens dialog

   SYNOPSIS
     vio           Vio
     mysql         connection handle

   DESCRIPTION
     reads prompt from server, waits for input and sends
     input to server.
     Note that first byte of prompt indicates if we have a 
     password which should not be echoed to stdout.

   RETURN
     CR_ERROR      if an error occurs
     CR_OK
     CR_OK_HANDSHAKE_COMPLETE
*/
static int auth_dialog_open(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql)
{
  uchar *packet;
  uchar type= 0;
  char dialog_buffer[1024];
  char *response;
  int packet_length;
  my_bool first_loop= TRUE;

  do {
    if ((packet_length= vio->read_packet(vio, &packet)) == -1)
      /* read error */
      return CR_ERROR;

    if (packet_length > 0)
    {
      type= *packet;
      packet++;

      /* check for protocol packet */
      if (!type || type == 254)
        return CR_OK_HANDSHAKE_COMPLETE;

      if ((type >> 1) == 2 &&
          first_loop &&
          mysql->passwd && mysql->passwd[0])
        response= mysql->passwd;
      else
        response= auth_dialog_func(mysql, type >> 1,
                                  (const char *)packet,
                                  dialog_buffer, 1024);
    }
    else
    {
      /* in case mysql_change_user was called the client needs
         to send packet first */
      response= mysql->passwd;
    }
    if (!response ||
        vio->write_packet(vio, (uchar *)response, (int)strlen(response) + 1))
      return CR_ERROR;

    first_loop= FALSE;

  } while((type & 1) != 1);
  return CR_OK;
}
/* }}} */

/* {{{ static int auth_dialog_init */
/* 
  Initialization routine

  SYNOPSIS
    auth_dialog_init
      unused1
      unused2
      unused3
      unused4

  DESCRIPTION
    Init function checks if the caller provides own dialog function.
    The function name must be mariadb_auth_dialog or
    mysql_authentication_dialog_ask. If the function cannot be found,
    we will use owr own simple command line input.

  RETURN
    0           success
*/
static int auth_dialog_init(char *unused1 __attribute__((unused)), 
                            size_t unused2  __attribute__((unused)), 
                            int unused3     __attribute__((unused)), 
                            va_list unused4 __attribute__((unused)))
{
  void *func;
#ifdef WIN32
  if (!(func= GetProcAddress(GetModuleHandle(NULL), "mariadb_auth_dialog")))
    /* for MySQL users */
    func= GetProcAddress(GetModuleHandle(NULL), "mysql_authentication_dialog_ask");
#else
  if (!(func= dlsym(RTLD_DEFAULT, "mariadb_auth_dialog")))
    /* for MySQL users */
    func= dlsym(RTLD_DEFAULT, "mysql_authentication_dialog_ask");
#endif
  if (func)
    auth_dialog_func= (mysql_authentication_dialog_ask_t)func;
  else
    auth_dialog_func= auth_dialog_native_prompt;

  return 0;
}
/* }}} */
