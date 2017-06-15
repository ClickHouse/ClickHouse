#include <ma_global.h>
#include <ma_sys.h>
#include <errmsg.h>
#include <string.h>
#include <ma_common.h>
#include <mysql/client_plugin.h>

typedef struct st_mysql_client_plugin_AUTHENTICATION auth_plugin_t;
static int client_mpvio_write_packet(struct st_plugin_vio*, const uchar*, size_t);
static int native_password_auth_client(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql);
extern void read_user_name(char *name);
extern char *ma_send_connect_attr(MYSQL *mysql, unsigned char *buffer);

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
/*
#define compile_time_assert(A) \
do {\
  typedef char constraint[(A) ? 1 : -1];\
} while (0);
*/

auth_plugin_t native_password_client_plugin=
{
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN,
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN_INTERFACE_VERSION,
  native_password_plugin_name,
  "R.J.Silk, Sergei Golubchik",
  "Native MySQL authentication",
  {1, 0, 0},
  "LGPL",
  NULL,
  NULL,
  NULL,
  NULL,
  native_password_auth_client
};


static int native_password_auth_client(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql)
{
  int pkt_len;
  uchar *pkt;

  if (((MCPVIO_EXT *)vio)->mysql_change_user)
  {
    /*
      in mysql_change_user() the client sends the first packet.
      we use the old scramble.
    */
    pkt= (uchar*)mysql->scramble_buff;
    pkt_len= SCRAMBLE_LENGTH + 1;
  }
  else
  {
    /* read the scramble */
    if ((pkt_len= vio->read_packet(vio, &pkt)) < 0)
      return CR_ERROR;

    if (pkt_len != SCRAMBLE_LENGTH + 1)
      return CR_SERVER_HANDSHAKE_ERR;

    /* save it in MYSQL */
    memmove(mysql->scramble_buff, pkt, SCRAMBLE_LENGTH);
    mysql->scramble_buff[SCRAMBLE_LENGTH] = 0;
  }

  if (mysql && mysql->passwd[0])
  {
    char scrambled[SCRAMBLE_LENGTH + 1];
    ma_scramble_41((uchar *)scrambled, (char*)pkt, mysql->passwd);
    if (vio->write_packet(vio, (uchar*)scrambled, SCRAMBLE_LENGTH))
      return CR_ERROR;
  }
  else
    if (vio->write_packet(vio, 0, 0)) /* no password */
      return CR_ERROR;

  return CR_OK;
}



static int send_change_user_packet(MCPVIO_EXT *mpvio,
                                   const uchar *data, int data_len)
{
  MYSQL *mysql= mpvio->mysql;
  char *buff, *end;
  int res= 1;
  size_t conn_attr_len= (mysql->options.extension) ? 
                         mysql->options.extension->connect_attrs_len : 0;

  buff= malloc(USERNAME_LENGTH+1 + data_len+1 + NAME_LEN+1 + 2 + NAME_LEN+1 + 9 + conn_attr_len);

  end= ma_strmake(buff, mysql->user, USERNAME_LENGTH) + 1;

  if (!data_len)
    *end++= 0;
  else
  {
    if (mysql->client_flag & CLIENT_SECURE_CONNECTION)
    {
      DBUG_ASSERT(data_len <= 255);
      if (data_len > 255)
      {
        my_set_error(mysql, CR_MALFORMED_PACKET, SQLSTATE_UNKNOWN, 0);
        goto error;
      }
      *end++= data_len;
    }
    else
    {
      DBUG_ASSERT(data_len == SCRAMBLE_LENGTH_323 + 1);
      DBUG_ASSERT(data[SCRAMBLE_LENGTH_323] == 0);
    }
    memcpy(end, data, data_len);
    end+= data_len;
  }
  end= ma_strmake(end, mpvio->db ? mpvio->db : "", NAME_LEN) + 1;

  if (mysql->server_capabilities & CLIENT_PROTOCOL_41)
  {
    int2store(end, (ushort) mysql->charset->nr);
    end+= 2;
  }

  if (mysql->server_capabilities & CLIENT_PLUGIN_AUTH)
    end= ma_strmake(end, mpvio->plugin->name, NAME_LEN) + 1;

  end= ma_send_connect_attr(mysql, (unsigned char *)end);

  res= ma_simple_command(mysql, COM_CHANGE_USER,
                      buff, (ulong)(end-buff), 1, NULL);

error:
  free(buff);
  return res;
}



static int send_client_reply_packet(MCPVIO_EXT *mpvio,
                                    const uchar *data, int data_len)
{
  MYSQL *mysql= mpvio->mysql;
  NET *net= &mysql->net;
  char *buff, *end;
  size_t conn_attr_len= (mysql->options.extension) ? 
                         mysql->options.extension->connect_attrs_len : 0;

  /* see end= buff+32 below, fixed size of the packet is 32 bytes */
  buff= malloc(33 + USERNAME_LENGTH + data_len + NAME_LEN + NAME_LEN + conn_attr_len + 9);
  end= buff;

  mysql->client_flag|= mysql->options.client_flag;
  mysql->client_flag|= CLIENT_CAPABILITIES;

  if (mysql->client_flag & CLIENT_MULTI_STATEMENTS)
    mysql->client_flag|= CLIENT_MULTI_RESULTS;

#if defined(HAVE_TLS) && !defined(EMBEDDED_LIBRARY)
  if (mysql->options.ssl_key || mysql->options.ssl_cert ||
      mysql->options.ssl_ca || mysql->options.ssl_capath ||
      mysql->options.ssl_cipher || mysql->options.use_ssl ||
      (mysql->options.client_flag & CLIENT_SSL_VERIFY_SERVER_CERT))
    mysql->options.use_ssl= 1;
  if (mysql->options.use_ssl)
    mysql->client_flag|= CLIENT_SSL;
#endif /* HAVE_TLS && !EMBEDDED_LIBRARY*/
  if (mpvio->db)
    mysql->client_flag|= CLIENT_CONNECT_WITH_DB;

  /* if server doesn't support SSL and verification of server certificate
     was set to mandatory, we need to return an error */
  if (mysql->options.use_ssl && !(mysql->server_capabilities & CLIENT_SSL))
  {
    if ((mysql->client_flag & CLIENT_SSL_VERIFY_SERVER_CERT) ||
        (mysql->options.extension && (mysql->options.extension->tls_fp || 
                                      mysql->options.extension->tls_fp_list)))
    {
      my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                          ER(CR_SSL_CONNECTION_ERROR), 
                          "SSL is required, but the server does not support it");
      goto error;
    }
  }


  /* Remove options that server doesn't support */
  mysql->client_flag= mysql->client_flag &
                       (~(CLIENT_COMPRESS | CLIENT_SSL | CLIENT_PROTOCOL_41) 
                       | mysql->server_capabilities);

#ifndef HAVE_COMPRESS
  mysql->client_flag&= ~CLIENT_COMPRESS;
#endif

  if (mysql->client_flag & CLIENT_PROTOCOL_41)
  {
    /* 4.1 server and 4.1 client has a 32 byte option flag */
    if (!(mysql->server_capabilities & CLIENT_MYSQL))
      mysql->client_flag&= ~CLIENT_MYSQL;
    int4store(buff,mysql->client_flag);
    int4store(buff+4, net->max_packet_size);
    buff[8]= (char) mysql->charset->nr;
    memset(buff + 9, 0, 32-9);
    if (!(mysql->server_capabilities & CLIENT_MYSQL))
    {
      mysql->extension->mariadb_client_flag = MARIADB_CLIENT_SUPPORTED_FLAGS >> 32;
      int4store(buff + 28, mysql->extension->mariadb_client_flag);
    }
    end= buff+32;
  }
  else
  {
    int2store(buff, mysql->client_flag);
    int3store(buff+2, net->max_packet_size);
    end= buff+5;
  }
#ifdef HAVE_TLS
  if (mysql->options.ssl_key ||
      mysql->options.ssl_cert ||
      mysql->options.ssl_ca ||
      mysql->options.ssl_capath ||
      mysql->options.ssl_cipher
#ifdef CRL_IMPLEMENTED
      || (mysql->options.extension &&
       (mysql->options.extension->ssl_crl ||
        mysql->options.extension->ssl_crlpath))
#endif
      )
    mysql->options.use_ssl= 1;
  if (mysql->options.use_ssl &&
      (mysql->client_flag & CLIENT_SSL))
  {
    /*
      Send mysql->client_flag, max_packet_size - unencrypted otherwise
      the server does not know we want to do SSL
    */
    if (ma_net_write(net, (unsigned char *)buff, (size_t) (end-buff)) || ma_net_flush(net))
    {
      my_set_error(mysql, CR_SERVER_LOST, SQLSTATE_UNKNOWN,
                          ER(CR_SERVER_LOST_EXTENDED),
                          "sending connection information to server",
                          errno);
      goto error;
    }
    if (ma_pvio_start_ssl(mysql->net.pvio))
      goto error;
  }
#endif /* HAVE_TLS */

  /* This needs to be changed as it's not useful with big packets */
  if (mysql->user && mysql->user[0])
    ma_strmake(end, mysql->user, USERNAME_LENGTH);
  else
    read_user_name(end);

  /* We have to handle different version of handshake here */
  end+= strlen(end) + 1;
  if (data_len)
  {
    if (mysql->server_capabilities & CLIENT_SECURE_CONNECTION)
    {
      *end++= data_len;
      memcpy(end, data, data_len);
      end+= data_len;
    }
    else
    {
      DBUG_ASSERT(data_len == SCRAMBLE_LENGTH_323 + 1); /* incl. \0 at the end */
      memcpy(end, data, data_len);
      end+= data_len;
    }
  }
  else
    *end++= 0;

  /* Add database if needed */
  if (mpvio->db && (mysql->server_capabilities & CLIENT_CONNECT_WITH_DB))
  {
    end= ma_strmake(end, mpvio->db, NAME_LEN) + 1;
    mysql->db= strdup(mpvio->db);
  }

  if (mysql->server_capabilities & CLIENT_PLUGIN_AUTH)
    end= ma_strmake(end, mpvio->plugin->name, NAME_LEN) + 1;

  end= ma_send_connect_attr(mysql, (unsigned char *)end);

  /* Write authentication package */
  if (ma_net_write(net, (unsigned char *)buff, (size_t) (end-buff)) || ma_net_flush(net))
  {
    my_set_error(mysql, CR_SERVER_LOST, SQLSTATE_UNKNOWN,
                        ER(CR_SERVER_LOST_EXTENDED),
                        "sending authentication information",
                        errno);
    goto error;
  }
  free(buff);
  return 0;
  
error:
  free(buff);
  return 1;
}

/**
  vio->read_packet() callback method for client authentication plugins

  This function is called by a client authentication plugin, when it wants
  to read data from the server.
*/

static int client_mpvio_read_packet(struct st_plugin_vio *mpv, uchar **buf)
{
  MCPVIO_EXT *mpvio= (MCPVIO_EXT*)mpv;
  MYSQL *mysql= mpvio->mysql;
  ulong  pkt_len;

  /* there are cached data left, feed it to a plugin */
  if (mpvio->cached_server_reply.pkt)
  {
    *buf= mpvio->cached_server_reply.pkt;
    mpvio->cached_server_reply.pkt= 0;
    mpvio->packets_read++;
    return mpvio->cached_server_reply.pkt_len;
  }

  if (mpvio->packets_read == 0)
  {
    /*
      the server handshake packet came from the wrong plugin,
      or it's mysql_change_user(). Either way, there is no data
      for a plugin to read. send a dummy packet to the server
      to initiate a dialog.
    */
    if (client_mpvio_write_packet(mpv, 0, 0))
      return (int)packet_error;
  }

  /* otherwise read the data */
  pkt_len= ma_net_safe_read(mysql);
  mpvio->last_read_packet_len= pkt_len;
  *buf= mysql->net.read_pos;

  /* was it a request to change plugins ? */
  if (**buf == 254)
    return (int)packet_error; /* if yes, this plugin shan't continue */

  /*
    the server sends \1\255 or \1\254 instead of just \255 or \254 -
    for us to not confuse it with an error or "change plugin" packets.
    We remove this escaping \1 here.

    See also server_mpvio_write_packet() where the escaping is done.
  */
  if (pkt_len && **buf == 1)
  {
    (*buf)++;
    pkt_len--;
  }
  mpvio->packets_read++;
  return pkt_len;
}

/**
  vio->write_packet() callback method for client authentication plugins

  This function is called by a client authentication plugin, when it wants
  to send data to the server.

  It transparently wraps the data into a change user or authentication
  handshake packet, if neccessary.
*/

static int client_mpvio_write_packet(struct st_plugin_vio *mpv,
                                     const uchar *pkt, size_t pkt_len)
{
  int res;
  MCPVIO_EXT *mpvio= (MCPVIO_EXT*)mpv;

  if (mpvio->packets_written == 0)
  {
    if (mpvio->mysql_change_user)
      res= send_change_user_packet(mpvio, pkt, (int)pkt_len);
    else
      res= send_client_reply_packet(mpvio, pkt, (int)pkt_len);
  }
  else
  {
    NET *net= &mpvio->mysql->net;
    if (mpvio->mysql->thd)
      res= 1; /* no chit-chat in embedded */
    else
      res= ma_net_write(net, (unsigned char *)pkt, pkt_len) || ma_net_flush(net);
    if (res)
      my_set_error(mpvio->mysql, CR_SERVER_LOST, SQLSTATE_UNKNOWN,
                                 ER(CR_SERVER_LOST_EXTENDED),
                                 "sending authentication information",
                                 errno);
  }
  mpvio->packets_written++;
  return res;
}

/**
  fills MYSQL_PLUGIN_VIO_INFO structure with the information about the
  connection
*/

void mpvio_info(MARIADB_PVIO *pvio, MYSQL_PLUGIN_VIO_INFO *info)
{
  memset(info, 0, sizeof(*info));
  switch (pvio->type) {
  case PVIO_TYPE_SOCKET:
    info->protocol= MYSQL_VIO_TCP;
    ma_pvio_get_handle(pvio, &info->socket);
    return;
  case PVIO_TYPE_UNIXSOCKET:
    info->protocol= MYSQL_VIO_SOCKET;
    ma_pvio_get_handle(pvio, &info->socket);
    return;
    /*
  case VIO_TYPE_SSL:
    {
      struct sockaddr addr;
      SOCKET_SIZE_TYPE addrlen= sizeof(addr);
      if (getsockname(vio->sd, &addr, &addrlen))
        return;
      info->protocol= addr.sa_family == AF_UNIX ?
        MYSQL_VIO_SOCKET : MYSQL_VIO_TCP;
      info->socket= vio->sd;
      return;
    }
    */
#ifdef _WIN32
    /*
  case VIO_TYPE_NAMEDPIPE:
    info->protocol= MYSQL_VIO_PIPE;
    info->handle= vio->hPipe;
    return;
    */
/* not supported yet
  case VIO_TYPE_SHARED_MEMORY:
    info->protocol= MYSQL_VIO_MEMORY;
    info->handle= vio->handle_file_map; 
    return;
*/
#endif
  default: DBUG_ASSERT(0);
  }
}

static void client_mpvio_info(MYSQL_PLUGIN_VIO *vio,
                              MYSQL_PLUGIN_VIO_INFO *info)
{
  MCPVIO_EXT *mpvio= (MCPVIO_EXT*)vio;
  mpvio_info(mpvio->mysql->net.pvio, info);
}

/**
  Client side of the plugin driver authentication.

  @note this is used by both the mysql_real_connect and mysql_change_user

  @param mysql       mysql
  @param data        pointer to the plugin auth data (scramble) in the
                     handshake packet
  @param data_len    the length of the data
  @param data_plugin a plugin that data were prepared for
                     or 0 if it's mysql_change_user()
  @param db          initial db to use, can be 0

  @retval 0 ok
  @retval 1 error
*/

int run_plugin_auth(MYSQL *mysql, char *data, uint data_len,
                    const char *data_plugin, const char *db)
{
  const char    *auth_plugin_name;
  auth_plugin_t *auth_plugin;
  MCPVIO_EXT    mpvio;
  ulong		pkt_length;
  int           res;

  /* determine the default/initial plugin to use */
  if (mysql->options.extension && mysql->options.extension->default_auth &&
      mysql->server_capabilities & CLIENT_PLUGIN_AUTH)
  {
    auth_plugin_name= mysql->options.extension->default_auth;
    if (!(auth_plugin= (auth_plugin_t*) mysql_client_find_plugin(mysql,
                       auth_plugin_name, MYSQL_CLIENT_AUTHENTICATION_PLUGIN)))
      return 1; /* oops, not found */
  }
  else
  {
    if (mysql->server_capabilities & CLIENT_PROTOCOL_41)
      auth_plugin= &native_password_client_plugin;
    else
    {
      if (!(auth_plugin= (auth_plugin_t*)mysql_client_find_plugin(mysql,
                         "old_password", MYSQL_CLIENT_AUTHENTICATION_PLUGIN)))
        return 1; /* not found */
    }
    auth_plugin_name= auth_plugin->name;
  }

  mysql->net.last_errno= 0; /* just in case */

  if (data_plugin && strcmp(data_plugin, auth_plugin_name))
  {
    /* data was prepared for a different plugin, don't show it to this one */
    data= 0;
    data_len= 0;
  }

  mpvio.mysql_change_user= data_plugin == 0;
  mpvio.cached_server_reply.pkt= (uchar*)data;
  mpvio.cached_server_reply.pkt_len= data_len;
  mpvio.read_packet= client_mpvio_read_packet;
  mpvio.write_packet= client_mpvio_write_packet;
  mpvio.info= client_mpvio_info;
  mpvio.mysql= mysql;
  mpvio.packets_read= mpvio.packets_written= 0;
  mpvio.db= db;
  mpvio.plugin= auth_plugin;

  res= auth_plugin->authenticate_user((struct st_plugin_vio *)&mpvio, mysql);

  if (res > CR_OK && mysql->net.read_pos[0] != 254)
  {
    /*
      the plugin returned an error. write it down in mysql,
      unless the error code is CR_ERROR and mysql->net.last_errno
      is already set (the plugin has done it)
    */
    if (res > CR_ERROR)
      my_set_error(mysql, res, SQLSTATE_UNKNOWN, 0);
    else
      if (!mysql->net.last_errno) {
        my_set_error(mysql, CR_UNKNOWN_ERROR, SQLSTATE_UNKNOWN, 0);
      }
    return 1;
  }

  /* read the OK packet (or use the cached value in mysql->net.read_pos */
  if (res == CR_OK)
    pkt_length= ma_net_safe_read(mysql);
  else /* res == CR_OK_HANDSHAKE_COMPLETE */
    pkt_length= mpvio.last_read_packet_len;

  if (pkt_length == packet_error)
  {
    if (mysql->net.last_errno == CR_SERVER_LOST)
      my_set_error(mysql, CR_SERVER_LOST, SQLSTATE_UNKNOWN,
                          ER(CR_SERVER_LOST_EXTENDED),
                          "reading authorization packet",
                          errno);
    return 1;
  }

  if (mysql->net.read_pos[0] == 254)
  {
    /* The server asked to use a different authentication plugin */
    if (pkt_length == 1)
    {
      /* old "use short scramble" packet */
      auth_plugin_name= old_password_plugin_name;
      mpvio.cached_server_reply.pkt= (uchar*)mysql->scramble_buff;
      mpvio.cached_server_reply.pkt_len= SCRAMBLE_LENGTH + 1;
    }
    else
    {
      /* new "use different plugin" packet */
      uint len;
      auth_plugin_name= (char*)mysql->net.read_pos + 1;
      len= (uint)strlen(auth_plugin_name); /* safe as ma_net_read always appends \0 */
      mpvio.cached_server_reply.pkt_len= pkt_length - len - 2;
      mpvio.cached_server_reply.pkt= mysql->net.read_pos + len + 2;
    }
    if (!(auth_plugin= (auth_plugin_t *) mysql_client_find_plugin(mysql,
                         auth_plugin_name, MYSQL_CLIENT_AUTHENTICATION_PLUGIN)))
      return 1;

    mpvio.plugin= auth_plugin;
    res= auth_plugin->authenticate_user((struct st_plugin_vio *)&mpvio, mysql);

    if (res > CR_OK)
    {
      if (res > CR_ERROR)
        my_set_error(mysql, res, SQLSTATE_UNKNOWN, 0);
      else
        if (!mysql->net.last_errno)
          my_set_error(mysql, CR_UNKNOWN_ERROR, SQLSTATE_UNKNOWN, 0);
      return 1;
    }

    if (res != CR_OK_HANDSHAKE_COMPLETE)
    {
      /* Read what server thinks about out new auth message report */
      if (ma_net_safe_read(mysql) == packet_error)
      {
        if (mysql->net.last_errno == CR_SERVER_LOST)
          my_set_error(mysql, CR_SERVER_LOST, SQLSTATE_UNKNOWN,
                              ER(CR_SERVER_LOST_EXTENDED),
                              "reading final connect information",
                              errno);
        return 1;
      }
    }
  }
  /*
    net->read_pos[0] should always be 0 here if the server implements
    the protocol correctly
  */
  return mysql->net.read_pos[0] != 0;
}

