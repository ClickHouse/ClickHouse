/************************************************************************************
   Copyright (C) 2014,2015 MariaDB Corporation AB
   
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
#include <memory.h>
#include <errmsg.h>


/* function prototypes */
static int auth_old_password(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql);

typedef struct st_mysql_client_plugin_AUTHENTICATION auth_plugin_t;

typedef struct {
  int (*read_packet)(struct st_plugin_vio *vio, uchar **buf);
  int (*write_packet)(struct st_plugin_vio *vio, const uchar *pkt, size_t pkt_len);
  void (*info)(struct st_plugin_vio *vio, struct st_plugin_vio_info *info);
  /* -= end of MYSQL_PLUGIN_VIO =- */
  MYSQL *mysql;
  auth_plugin_t *plugin;             /**< what plugin we're under */
  const char *db;
  struct {
    uchar *pkt;                      /**< pointer into NET::buff */
    uint pkt_len;
  } cached_server_reply;
  uint packets_read, packets_written; /**< counters for send/received packets */
  my_bool mysql_change_user;          /**< if it's mysql_change_user() */
  int last_read_packet_len;           /**< the length of the last *read* packet */
} MCPVIO_EXT;

#ifndef HAVE_OLDPASSWORD_DYNAMIC
struct st_mysql_client_plugin_AUTHENTICATION old_password_client_plugin=
#else
struct st_mysql_client_plugin_AUTHENTICATION _mysql_client_plugin_declaration_ =
#endif
{
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN,
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN_INTERFACE_VERSION,
  "mysql_old_password",
  "Sergei Golubchik, R.J. Silk, Georg Richter",
  "Old (pre 4.1) authentication plugin",
  {1,0,0},
  "LGPL",
  NULL,
  NULL,
  NULL,
  NULL,
  auth_old_password
};

/**
  client authentication plugin that does old MySQL authentication
  using an 8-byte (4.0-) scramble
*/

static int auth_old_password(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql)
{
  uchar *pkt;
  int pkt_len;

  if (((MCPVIO_EXT *)vio)->mysql_change_user)
  {
    /*
      in mysql_change_user() the client sends the first packet.
      we use the old scramble.
    */
    pkt= (uchar*)mysql->scramble_buff;
    pkt_len= SCRAMBLE_LENGTH_323 + 1;
  }
  else
  {
    /* read the scramble */
    if ((pkt_len= vio->read_packet(vio, &pkt)) < 0)
      return CR_ERROR;

    if (pkt_len != SCRAMBLE_LENGTH_323 + 1 &&
        pkt_len != SCRAMBLE_LENGTH + 1)
        return CR_SERVER_HANDSHAKE_ERR;

    /* save it in MYSQL */
    memmove(mysql->scramble_buff, pkt, pkt_len);
    mysql->scramble_buff[pkt_len] = 0;
  }

  if (mysql && mysql->passwd[0])
  {
    char scrambled[SCRAMBLE_LENGTH_323 + 1];
    ma_scramble_323(scrambled, (char*)pkt, mysql->passwd);
    if (vio->write_packet(vio, (uchar*)scrambled, SCRAMBLE_LENGTH_323 + 1))
      return CR_ERROR;
  }
  else
    if (vio->write_packet(vio, 0, 0)) /* no password */
      return CR_ERROR;

  return CR_OK;
}



