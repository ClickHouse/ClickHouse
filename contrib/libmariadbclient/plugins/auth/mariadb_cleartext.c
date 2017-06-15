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
#include <ma_global.h>
#include <mysql.h>
#include <mysql/client_plugin.h>
#include <string.h>

/* clear text plugin submits the password without opening a dialog.
   This will be the case if pam-use-cleartext-plugin option is
   enabled on server side */

/* {{{ auth_send_plain_password() */
/*
   sends an unencrypted password to server

   SYNOPSIS
     auth_send_plain_password()
     vio             pointer to vio structure
     mysql           connection handle

   DESCRIPTION
     sends an unencrypted password (which was specified either in
     mysql_real_connect or mysql_change_user) to server.

   RETURN
     CR_OK
     CR_ERROR        if an error occured
*/
static int clear_password_auth_client(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql)
{
  if (!vio || !mysql || !mysql->passwd)
    return CR_ERROR;

  /* write password including terminating zero character */
  return vio->write_packet(vio, (const unsigned char *) mysql->passwd, (int)strlen(mysql->passwd) + 1) ?
         CR_ERROR : CR_OK;
}
/* }}} */

#ifndef HAVE_DIALOG_DYNAMIC
struct st_mysql_client_plugin_AUTHENTICATION auth_cleartext_plugin=
#else
struct st_mysql_client_plugin_AUTHENTICATION _mysql_client_plugin_declaration_ =
#endif
{
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN,
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN_INTERFACE_VERSION,
  "mysql_clear_password",
  "Georg Richter",
  "MariaDB clear password authentication plugin",
  {0,1,0},
  "LGPL",
  NULL,
  NULL,
  NULL,
  NULL,
  clear_password_auth_client
};


