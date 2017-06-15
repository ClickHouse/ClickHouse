/************************************************************************************
    Copyright (C) 2015 MariaDB Corporation AB,
   
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

/* MariaDB Communication IO (PVIO) interface

   PVIO is the interface for client server communication and replaces former vio
   component of the client library.

   PVIO support various protcols like sockets, pipes and shared memory, which are 
   implemented as plugins and can be extended therfore easily.

   Interface function description:

   ma_pvio_init          allocates a new PVIO object which will be used
                        for the current connection

   ma_pvio_close         frees all resources of previously allocated PVIO object
                        and closes open connections

   ma_pvio_read          reads data from server

   ma_pvio_write         sends data to server

   ma_pvio_set_timeout   sets timeout for connection, read and write

   ma_pvio_register_callback
                        register callback functions for read and write
 */

#include <ma_global.h>
#include <ma_sys.h>
#include <mysql.h>
#include <errmsg.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <ma_common.h>
#include <ma_pvio.h>
#include <mariadb_async.h>
#include <ma_context.h>

/* callback functions for read/write */
LIST *pvio_callback= NULL;

#define IS_BLOCKING_ERROR()                   \
  IF_WIN(WSAGetLastError() != WSAEWOULDBLOCK, \
         (errno != EAGAIN && errno != EINTR))

/* {{{ MARIADB_PVIO *ma_pvio_init */
MARIADB_PVIO *ma_pvio_init(MA_PVIO_CINFO *cinfo)
{
  /* check connection type and load the required plugin.
   * Currently we support the following pvio types:
   *   pvio_socket
   *   pvio_namedpipe
   *   pvio_sharedmed
   */
  const char *pvio_plugins[] = {"pvio_socket", "pvio_npipe", "pvio_shmem"};
  int type;
  MARIADB_PVIO_PLUGIN *pvio_plugin;
  MARIADB_PVIO *pvio= NULL;

  switch (cinfo->type)
  {
    case PVIO_TYPE_UNIXSOCKET:
    case PVIO_TYPE_SOCKET:
      type= 0;
      break;
#ifdef _WIN32
    case PVIO_TYPE_NAMEDPIPE:
      type= 1;
      break;
    case PVIO_TYPE_SHAREDMEM:
      type= 2;
      break;
#endif
    default:
      return NULL;
  }

  if (!(pvio_plugin= (MARIADB_PVIO_PLUGIN *)
                 mysql_client_find_plugin(cinfo->mysql,
                                          pvio_plugins[type], 
                                          MARIADB_CLIENT_PVIO_PLUGIN)))
  {
    /* error already set in mysql_client_find_plugin */
    return NULL;
  }


  if (!(pvio= (MARIADB_PVIO *)calloc(1, sizeof(MARIADB_PVIO)))) 
  {
    PVIO_SET_ERROR(cinfo->mysql, CR_OUT_OF_MEMORY, unknown_sqlstate, 0);
    return NULL;
  }

  /* register error routine and methods */
  pvio->methods= pvio_plugin->methods;
  pvio->set_error= my_set_error;
  pvio->type= cinfo->type;

  /* set timeout to connect timeout - after successfull connect we will set 
   * correct values for read and write */
  if (pvio->methods->set_timeout)
  {
    pvio->methods->set_timeout(pvio, PVIO_CONNECT_TIMEOUT, cinfo->mysql->options.connect_timeout);
    pvio->methods->set_timeout(pvio, PVIO_READ_TIMEOUT, cinfo->mysql->options.connect_timeout);
    pvio->methods->set_timeout(pvio, PVIO_WRITE_TIMEOUT, cinfo->mysql->options.connect_timeout);
  }

  if (!(pvio->cache= calloc(1, PVIO_READ_AHEAD_CACHE_SIZE)))
  {
    PVIO_SET_ERROR(cinfo->mysql, CR_OUT_OF_MEMORY, unknown_sqlstate, 0);
    free(pvio);
    return NULL;
  }
  pvio->cache_size= 0;
  pvio->cache_pos= pvio->cache;

  return pvio;
}
/* }}} */

/* {{{ my_bool ma_pvio_is_alive */
my_bool ma_pvio_is_alive(MARIADB_PVIO *pvio)
{
  if (!pvio)
    return FALSE;
  if (pvio->methods->is_alive)
    return pvio->methods->is_alive(pvio);
  return TRUE;
}
/* }}} */

/* {{{ int ma_pvio_fast_send */
int ma_pvio_fast_send(MARIADB_PVIO *pvio)
{
  if (!pvio || !pvio->methods->fast_send)
    return 1;
  return pvio->methods->fast_send(pvio);
}
/* }}} */

/* {{{ int ma_pvio_keepalive */
int ma_pvio_keepalive(MARIADB_PVIO *pvio)
{
  if (!pvio || !pvio->methods->keepalive)
    return 1;
  return pvio->methods->keepalive(pvio);
}
/* }}} */

/* {{{ my_bool ma_pvio_set_timeout */
my_bool ma_pvio_set_timeout(MARIADB_PVIO *pvio, 
                           enum enum_pvio_timeout type,
                           int timeout)
{
  if (!pvio)
    return 1;

  if (pvio->methods->set_timeout)
    return pvio->methods->set_timeout(pvio, type, timeout);
  return 1;
}
/* }}} */

/* {{{ size_t ma_pvio_read_async */
static size_t ma_pvio_read_async(MARIADB_PVIO *pvio, uchar *buffer, size_t length)
{
  ssize_t res;
  struct mysql_async_context *b= pvio->mysql->options.extension->async_context;
  int timeout= pvio->timeout[PVIO_READ_TIMEOUT];

  if (!pvio->methods->async_read)
  {
    PVIO_SET_ERROR(pvio->mysql, CR_ASYNC_NOT_SUPPORTED, unknown_sqlstate, 0);
    return -1;
  }

  for (;;)
  {
    if (pvio->methods->async_read)
      res= pvio->methods->async_read(pvio, buffer, length);
    if (res >= 0 || IS_BLOCKING_ERROR())
      return res;
    b->events_to_wait_for= MYSQL_WAIT_READ;
    if (timeout >= 0)
    {
      b->events_to_wait_for|= MYSQL_WAIT_TIMEOUT;
      b->timeout_value= timeout;
    }
    if (b->suspend_resume_hook)
      (*b->suspend_resume_hook)(TRUE, b->suspend_resume_hook_user_data);
    my_context_yield(&b->async_context);
    if (b->suspend_resume_hook)
      (*b->suspend_resume_hook)(FALSE, b->suspend_resume_hook_user_data);
    if (b->events_occured & MYSQL_WAIT_TIMEOUT)
      return -1;
  }
}
/* }}} */

/* {{{ size_t ma_pvio_read */
ssize_t ma_pvio_read(MARIADB_PVIO *pvio, uchar *buffer, size_t length)
{
  ssize_t r= -1;
  if (!pvio)
    return -1;
  if (IS_PVIO_ASYNC_ACTIVE(pvio))
  {
    r= ma_pvio_read_async(pvio, buffer, length);
    goto end;
  }
  else
  {
    if (IS_PVIO_ASYNC(pvio))
    {
      /*
        If switching from non-blocking to blocking API usage, set the socket
        back to blocking mode.
      */
      my_bool old_mode;
      ma_pvio_blocking(pvio, TRUE, &old_mode);
    }
  }

  /* secure connection */
#ifdef HAVE_TLS
  if (pvio->ctls)
  {
    r= ma_pvio_tls_read(pvio->ctls, buffer, length);
    goto end;
  }
#endif
  if (pvio->methods->read)
    r= pvio->methods->read(pvio, buffer, length);
end:
  if (pvio_callback)
  {
    void (*callback)(int mode, MYSQL *mysql, const uchar *buffer, size_t length);
    LIST *p= pvio_callback;
    while (p)
    {
      callback= p->data;
      callback(0, pvio->mysql, buffer, r);
      p= p->next;
    }
  }
  return r;
}
/* }}} */

/* {{{  size_t ma_pvio_cache_read */
ssize_t ma_pvio_cache_read(MARIADB_PVIO *pvio, uchar *buffer, size_t length)
{
  ssize_t r;

  if (!pvio)
    return -1;

  if (!pvio->cache)
    return ma_pvio_read(pvio, buffer, length);

  if (pvio->cache + pvio->cache_size > pvio->cache_pos)
  {
    ssize_t remaining = pvio->cache + pvio->cache_size - pvio->cache_pos;
    assert(remaining > 0);
    r= MIN((ssize_t)length, remaining);
    memcpy(buffer, pvio->cache_pos, r);
    pvio->cache_pos+= r;
  }
  else if (length >= PVIO_READ_AHEAD_CACHE_MIN_SIZE)
  {
    r= ma_pvio_read(pvio, buffer, length); 
  }
  else
  {
    r= ma_pvio_read(pvio, pvio->cache, PVIO_READ_AHEAD_CACHE_SIZE);
    if (r > 0)
    {
      if (length < (size_t)r)
      {
        pvio->cache_size= r;
        pvio->cache_pos= pvio->cache + length;
        r= length;
      }
      memcpy(buffer, pvio->cache, r);
    }
  }
  return r;
}
/* }}} */

/* {{{ size_t ma_pvio_write_async */
static ssize_t ma_pvio_write_async(MARIADB_PVIO *pvio, const uchar *buffer, size_t length)
{
  ssize_t res;
  struct mysql_async_context *b= pvio->mysql->options.extension->async_context;
  int timeout= pvio->timeout[PVIO_WRITE_TIMEOUT];

  for (;;)
  {
    res= pvio->methods->async_write(pvio, buffer, length);
    if (res >= 0 || IS_BLOCKING_ERROR())
      return res;
    b->events_to_wait_for= MYSQL_WAIT_WRITE;
    if (timeout >= 0)
    {
      b->events_to_wait_for|= MYSQL_WAIT_TIMEOUT;
      b->timeout_value= timeout;
    }
    if (b->suspend_resume_hook)
      (*b->suspend_resume_hook)(TRUE, b->suspend_resume_hook_user_data);
    my_context_yield(&b->async_context);
    if (b->suspend_resume_hook)
      (*b->suspend_resume_hook)(FALSE, b->suspend_resume_hook_user_data);
    if (b->events_occured & MYSQL_WAIT_TIMEOUT)
      return -1;
  }
}
/* }}} */

/* {{{ size_t ma_pvio_write */
ssize_t ma_pvio_write(MARIADB_PVIO *pvio, const uchar *buffer, size_t length)
{
  ssize_t r;

  if (!pvio)
   return -1;

  /* secure connection */
#ifdef HAVE_TLS
  if (pvio->ctls)
  {
    r= ma_pvio_tls_write(pvio->ctls, buffer, length);
    goto end;
  }
  else
#endif
  if (IS_PVIO_ASYNC_ACTIVE(pvio))
  {
    r= ma_pvio_write_async(pvio, buffer, length);
    goto end;
  }
  else
  {
    if (IS_PVIO_ASYNC(pvio))
    {
      /*
        If switching from non-blocking to blocking API usage, set the socket
        back to blocking mode.
      */
      my_bool old_mode;
      ma_pvio_blocking(pvio, TRUE, &old_mode);
    }
  }

  if (pvio->methods->write)
    r= pvio->methods->write(pvio, buffer, length);
end:
  if (pvio_callback)
  {
    void (*callback)(int mode, MYSQL *mysql, const uchar *buffer, size_t length);
    LIST *p= pvio_callback;
    while (p)
    {
      callback= p->data;
      callback(1, pvio->mysql, buffer, r);
      p= p->next;
    }
  }
  return r;
}
/* }}} */

/* {{{ void ma_pvio_close */
void ma_pvio_close(MARIADB_PVIO *pvio)
{
  /* free internal structures and close connection */
#ifdef HAVE_TLS
  if (pvio && pvio->ctls)
  {
    ma_pvio_tls_close(pvio->ctls);
    free(pvio->ctls);
  }
#endif
  if (pvio && pvio->methods->close)
    pvio->methods->close(pvio);

  if (pvio->cache)
    free(pvio->cache);

  free(pvio);
}
/* }}} */

/* {{{ my_bool ma_pvio_get_handle */
my_bool ma_pvio_get_handle(MARIADB_PVIO *pvio, void *handle)
{
  if (pvio && pvio->methods->get_handle)
    return pvio->methods->get_handle(pvio, handle);
  return 1;
}
/* }}} */

/* {{{ ma_pvio_wait_async */
static my_bool
ma_pvio_wait_async(struct mysql_async_context *b, enum enum_pvio_io_event event,
                 int timeout)
{
  switch (event)
  {
  case VIO_IO_EVENT_READ:
    b->events_to_wait_for = MYSQL_WAIT_READ;
    break;
  case VIO_IO_EVENT_WRITE:
    b->events_to_wait_for = MYSQL_WAIT_WRITE;
    break;
  case VIO_IO_EVENT_CONNECT:
    b->events_to_wait_for = MYSQL_WAIT_WRITE | IF_WIN(0, MYSQL_WAIT_EXCEPT);
    break;
  }

  if (timeout >= 0)
  {
    b->events_to_wait_for |= MYSQL_WAIT_TIMEOUT;
    b->timeout_value= timeout;
  }
  if (b->suspend_resume_hook)
    (*b->suspend_resume_hook)(TRUE, b->suspend_resume_hook_user_data);
  my_context_yield(&b->async_context);
  if (b->suspend_resume_hook)
    (*b->suspend_resume_hook)(FALSE, b->suspend_resume_hook_user_data);
  return (b->events_occured & MYSQL_WAIT_TIMEOUT) ? 0 : 1;
}
/* }}} */

/* {{{ ma_pvio_wait_io_or_timeout */
int ma_pvio_wait_io_or_timeout(MARIADB_PVIO *pvio, my_bool is_read, int timeout)
{
  if (IS_PVIO_ASYNC_ACTIVE(pvio))
    return ma_pvio_wait_async(pvio->mysql->options.extension->async_context, 
                             (is_read) ? VIO_IO_EVENT_READ : VIO_IO_EVENT_WRITE,
                              timeout);

  if (pvio && pvio->methods->wait_io_or_timeout)
    return pvio->methods->wait_io_or_timeout(pvio, is_read, timeout);
  return 1;
}
/* }}} */

/* {{{ my_bool ma_pvio_connect */
my_bool ma_pvio_connect(MARIADB_PVIO *pvio,  MA_PVIO_CINFO *cinfo)
{
  if (pvio && pvio->methods->connect)
    return pvio->methods->connect(pvio, cinfo);
  return 1;
}
/* }}} */

/* {{{ my_bool ma_pvio_blocking */
my_bool ma_pvio_blocking(MARIADB_PVIO *pvio, my_bool block, my_bool *previous_mode)
{
  if (pvio && pvio->methods->blocking)
    return pvio->methods->blocking(pvio, block, previous_mode);
  return 1;
}
/* }}} */

/* {{{ my_bool ma_pvio_is_blocking */ 
my_bool ma_pvio_is_blocking(MARIADB_PVIO *pvio) 
{
  if (pvio && pvio->methods->is_blocking)
    return pvio->methods->is_blocking(pvio);
  return 1;
}
/* }}} */

/* {{{ ma_pvio_has_data */
my_bool ma_pvio_has_data(MARIADB_PVIO *pvio, ssize_t *data_len)
{
  /* check if we still have unread data in cache */
  if (pvio && pvio->cache)
    if (pvio->cache_pos > pvio->cache)
      return test(pvio->cache_pos - pvio->cache);
  if (pvio && pvio->methods->has_data)
    return pvio->methods->has_data(pvio, data_len);
  return 1;
}
/* }}} */

#ifdef HAVE_TLS
/* {{{ my_bool ma_pvio_start_ssl */
my_bool ma_pvio_start_ssl(MARIADB_PVIO *pvio)
{
  if (!pvio || !pvio->mysql)
    return 1;
  CLEAR_CLIENT_ERROR(pvio->mysql);
  if (!(pvio->ctls= ma_pvio_tls_init(pvio->mysql)))
  {
    return 1;
  }
  if (ma_pvio_tls_connect(pvio->ctls))
  {
    free(pvio->ctls);
    pvio->ctls= NULL;
    return 1;
  }

  /* default behaviour:
     1. peer certificate verification
     2. verify CN (requires option ssl_verify_check)
     3. verrify finger print
  */
  if ((pvio->mysql->options.ssl_ca || pvio->mysql->options.ssl_capath) &&
        (pvio->mysql->client_flag & CLIENT_SSL_VERIFY_SERVER_CERT) &&
         ma_pvio_tls_verify_server_cert(pvio->ctls))
    return 1;

  if (pvio->mysql->options.extension &&
      ((pvio->mysql->options.extension->tls_fp && pvio->mysql->options.extension->tls_fp[0]) ||
      (pvio->mysql->options.extension->tls_fp_list && pvio->mysql->options.extension->tls_fp_list[0])))
  {
    if (ma_pvio_tls_check_fp(pvio->ctls, 
          pvio->mysql->options.extension->tls_fp,
          pvio->mysql->options.extension->tls_fp_list))
      return 1;
  }

  return 0;
}
/* }}} */
#endif

/* {{{ ma_pvio_register_callback */
int ma_pvio_register_callback(my_bool register_callback,
                             void (*callback_function)(int mode, MYSQL *mysql, const uchar *buffer, size_t length))
{
  LIST *list;

  if (!callback_function)
    return 1;

  /* plugin will unregister in it's deinit function */
  if (register_callback)
  {
    list= (LIST *)malloc(sizeof(LIST));

    list->data= (void *)callback_function;
    pvio_callback= list_add(pvio_callback, list);
  }
  else /* unregister callback function */
  {  
    LIST *p= pvio_callback;
    while (p)
    {
      if (p->data == callback_function)
      {
        list_delete(pvio_callback, p);
        break;
      }
      p= p->next;
    }
  }
  return 0;
}
/* }}} */
