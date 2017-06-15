/* Copyright (C) 2012 MariaDB Services and Kristian Nielsen
                 2015 MariaDB Corporation

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
*/

/*
  MySQL non-blocking client library functions.
*/

#include "ma_global.h"
#include "ma_sys.h"
#include "mysql.h"
#include "errmsg.h"
#ifndef LIBMARIADB
#include "sql_common.h"
#else
#include "ma_common.h"
#endif
#include "ma_context.h"
#include "ma_pvio.h"
#include "mariadb_async.h"
#include <string.h>


#ifdef _WIN32
/*
  Windows does not support MSG_DONTWAIT for send()/recv(). So we need to ensure
  that the socket is non-blocking at the start of every operation.
*/
#define WIN_SET_NONBLOCKING(mysql) { \
    my_bool old_mode; \
    if ((mysql)->net.pvio) ma_pvio_blocking((mysql)->net.pvio, FALSE, &old_mode); \
  }
#else
#define WIN_SET_NONBLOCKING(mysql)
#endif

extern void mysql_close_slow_part(MYSQL *mysql);


void
my_context_install_suspend_resume_hook(struct mysql_async_context *b,
                                       void (*hook)(my_bool, void *),
                                       void *user_data)
{
  b->suspend_resume_hook= hook;
  b->suspend_resume_hook_user_data= user_data;
}


/* Asynchronous connect(); socket must already be set non-blocking. */
int
my_connect_async(MARIADB_PVIO *pvio,
                 const struct sockaddr *name, uint namelen, int vio_timeout)
{
  int res;
  size_socket s_err_size;
  struct mysql_async_context *b= pvio->mysql->options.extension->async_context;
  my_socket sock;

  ma_pvio_get_handle(pvio, &sock);

  /* Make the socket non-blocking. */
  ma_pvio_blocking(pvio, 0, 0);

  b->events_to_wait_for= 0;
  /*
    Start to connect asynchronously.
    If this will block, we suspend the call and return control to the
    application context. The application will then resume us when the socket
    polls ready for write, indicating that the connection attempt completed.
  */
  res= connect(sock, name, namelen);
  if (res != 0)
  {
#ifdef _WIN32
    int wsa_err= WSAGetLastError();
    if (wsa_err != WSAEWOULDBLOCK)
      return res;
    b->events_to_wait_for|= MYSQL_WAIT_EXCEPT;
#else
    int err= errno;
    if (err != EINPROGRESS && err != EALREADY && err != EAGAIN)
      return res;
#endif
    b->events_to_wait_for|= MYSQL_WAIT_WRITE;
    if (vio_timeout >= 0)
    {
      b->timeout_value= vio_timeout;
      b->events_to_wait_for|= MYSQL_WAIT_TIMEOUT;
    }
    else
      b->timeout_value= 0;
    if (b->suspend_resume_hook)
      (*b->suspend_resume_hook)(TRUE, b->suspend_resume_hook_user_data);
    my_context_yield(&b->async_context);
    if (b->suspend_resume_hook)
      (*b->suspend_resume_hook)(FALSE, b->suspend_resume_hook_user_data);
    if (b->events_occured & MYSQL_WAIT_TIMEOUT)
      return -1;

    s_err_size= sizeof(res);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, (char*) &res, &s_err_size) != 0)
      return -1;
    if (res)
    {
      errno= res;
      return -1;
    }
  }
  return res;
}

#define IS_BLOCKING_ERROR()                   \
  IF_WIN(WSAGetLastError() != WSAEWOULDBLOCK, \
         (errno != EAGAIN && errno != EINTR))

#ifdef _AIX
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif
#endif

#ifdef HAVE_TLS_FIXME
static my_bool
my_ssl_async_check_result(int res, struct mysql_async_context *b, MARIADB_SSL *cssl)
{
  int ssl_err;
  b->events_to_wait_for= 0;
  if (res >= 0)
    return 1;
  ssl_err= SSL_get_error(ssl, res);
  if (ssl_err == SSL_ERROR_WANT_READ)
    b->events_to_wait_for|= MYSQL_WAIT_READ;
  else if (ssl_err == SSL_ERROR_WANT_WRITE)
    b->events_to_wait_for|= MYSQL_WAIT_WRITE;
  else
    return 1;
  if (b->suspend_resume_hook)
    (*b->suspend_resume_hook)(TRUE, b->suspend_resume_hook_user_data);
  my_context_yield(&b->async_context);
  if (b->suspend_resume_hook)
    (*b->suspend_resume_hook)(FALSE, b->suspend_resume_hook_user_data);
  return 0;
}

int
my_ssl_read_async(struct mysql_async_context *b, SSL *ssl,
                  void *buf, int size)
{
  int res;

  for (;;)
  {
    res= SSL_read(ssl, buf, size);
    if (my_ssl_async_check_result(res, b, ssl))
      return res;
  }
}

int
my_ssl_write_async(struct mysql_async_context *b, SSL *ssl,
                   const void *buf, int size)
{
  int res;

  for (;;)
  {
    res= SSL_write(ssl, buf, size);
    if (my_ssl_async_check_result(res, b, ssl))
      return res;
  }
}
#endif  /* HAVE_OPENSSL */




/*
  Now create non-blocking definitions for all the calls that may block.

  Each call FOO gives rise to FOO_start() that prepares the MYSQL object for
  doing non-blocking calls that can suspend operation mid-way, and then starts
  the call itself. And a FOO_start_internal trampoline to assist with running
  the real call in a co-routine that can be suspended. And a FOO_cont() that
  can continue a suspended operation.
*/

#define MK_ASYNC_INTERNAL_BODY(call, invoke_args, mysql_val, ret_type, ok_val)\
  struct call ## _params *parms= (struct call ## _params *)d;                 \
  ret_type ret;                                                               \
  struct mysql_async_context *b=                                              \
    (mysql_val)->options.extension->async_context;                            \
                                                                              \
  ret= call invoke_args;                                                      \
  b->ret_result. ok_val = ret;                                                \
  b->events_to_wait_for= 0;

#define MK_ASYNC_START_BODY(call, mysql_val, parms_assign, err_val, ok_val, extra1) \
  int res;                                                                    \
  struct mysql_async_context *b;                                              \
  struct call ## _params parms;                                               \
                                                                              \
  extra1                                                                      \
  b= mysql_val->options.extension->async_context;                             \
  parms_assign                                                                \
                                                                              \
  b->active= 1;                                                               \
  res= my_context_spawn(&b->async_context, call ## _start_internal, &parms);  \
  b->active= b->suspended= 0;                                                 \
  if (res > 0)                                                                \
  {                                                                           \
    /* Suspended. */                                                          \
    b->suspended= 1;                                                          \
    return b->events_to_wait_for;                                             \
  }                                                                           \
  if (res < 0)                                                                \
  {                                                                           \
    set_mariadb_error((mysql_val), CR_OUT_OF_MEMORY, unknown_sqlstate);         \
    *ret= err_val;                                                            \
  }                                                                           \
  else                                                                        \
    *ret= b->ret_result. ok_val;                                              \
  return 0;

#define MK_ASYNC_CONT_BODY(mysql_val, err_val, ok_val) \
  int res;                                                                    \
  struct mysql_async_context *b=                                              \
    (mysql_val)->options.extension->async_context;                            \
  if (!b->suspended)                                                          \
  {                                                                           \
    set_mariadb_error((mysql_val), CR_COMMANDS_OUT_OF_SYNC, unknown_sqlstate);  \
    *ret= err_val;                                                            \
    return 0;                                                                 \
  }                                                                           \
                                                                              \
  b->active= 1;                                                               \
  b->events_occured= ready_status;                                            \
  res= my_context_continue(&b->async_context);                                \
  b->active= 0;                                                               \
  if (res > 0)                                                                \
    return b->events_to_wait_for;               /* (Still) suspended */       \
  b->suspended= 0;                                                            \
  if (res < 0)                                                                \
  {                                                                           \
    set_mariadb_error((mysql_val), CR_OUT_OF_MEMORY, unknown_sqlstate);         \
    *ret= err_val;                                                            \
  }                                                                           \
  else                                                                        \
    *ret= b->ret_result. ok_val;                /* Finished. */               \
  return 0;

#define MK_ASYNC_INTERNAL_BODY_VOID_RETURN(call, invoke_args, mysql_val)      \
  struct call ## _params *parms= (struct call ## _params *)d;                 \
  struct mysql_async_context *b=                                              \
    (mysql_val)->options.extension->async_context;                            \
                                                                              \
  call invoke_args;                                                           \
  b->events_to_wait_for= 0;

#define MK_ASYNC_START_BODY_VOID_RETURN(call, mysql_val, parms_assign, extra1)\
  int res;                                                                    \
  struct mysql_async_context *b;                                              \
  struct call ## _params parms;                                               \
                                                                              \
  extra1                                                                      \
  b= mysql_val->options.extension->async_context;                             \
  parms_assign                                                                \
                                                                              \
  b->active= 1;                                                               \
  res= my_context_spawn(&b->async_context, call ## _start_internal, &parms);  \
  b->active= b->suspended= 0;                                                 \
  if (res > 0)                                                                \
  {                                                                           \
    /* Suspended. */                                                          \
    b->suspended= 1;                                                          \
    return b->events_to_wait_for;                                             \
  }                                                                           \
  if (res < 0)                                                                \
    set_mariadb_error((mysql_val), CR_OUT_OF_MEMORY, unknown_sqlstate);         \
  return 0;

#define MK_ASYNC_CONT_BODY_VOID_RETURN(mysql_val)                             \
  int res;                                                                    \
  struct mysql_async_context *b=                                              \
    (mysql_val)->options.extension->async_context;                            \
  if (!b->suspended)                                                          \
  {                                                                           \
    set_mariadb_error((mysql_val), CR_COMMANDS_OUT_OF_SYNC, unknown_sqlstate);  \
    return 0;                                                                 \
  }                                                                           \
                                                                              \
  b->active= 1;                                                               \
  b->events_occured= ready_status;                                            \
  res= my_context_continue(&b->async_context);                                \
  b->active= 0;                                                               \
  if (res > 0)                                                                \
    return b->events_to_wait_for;               /* (Still) suspended */       \
  b->suspended= 0;                                                            \
  if (res < 0)                                                                \
    set_mariadb_error((mysql_val), CR_OUT_OF_MEMORY, unknown_sqlstate);         \
  return 0;


/* Structure used to pass parameters from mysql_real_connect_start(). */
struct mysql_real_connect_params {
  MYSQL *mysql;
  const char *host;
  const char *user;
  const char *passwd;
  const char *db;
  unsigned int port;
  const char *unix_socket;
  unsigned long client_flags;
};
static void
mysql_real_connect_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_real_connect,
  (parms->mysql, parms->host, parms->user, parms->passwd, parms->db,
   parms->port, parms->unix_socket, parms->client_flags),
  parms->mysql,
  MYSQL *,
  r_ptr)
}
int STDCALL
mysql_real_connect_start(MYSQL **ret, MYSQL *mysql, const char *host,
                         const char *user, const char *passwd, const char *db,
                         unsigned int port, const char *unix_socket,
                         unsigned long client_flags)
{
MK_ASYNC_START_BODY(
  mysql_real_connect,
  mysql,
  {
    parms.mysql= mysql;
    parms.host= host;
    parms.user= user;
    parms.passwd= passwd;
    parms.db= db;
    parms.port= port;
    parms.unix_socket= unix_socket;
    parms.client_flags= client_flags | CLIENT_REMEMBER_OPTIONS;
  },
  NULL,
  r_ptr,
  /* Nothing */)
}
int STDCALL
mysql_real_connect_cont(MYSQL **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_real_query_start(). */
struct mysql_real_query_params {
  MYSQL *mysql;
  const char *stmt_str;
  size_t length;
};
static void
mysql_real_query_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_real_query,
  (parms->mysql, parms->stmt_str, parms->length),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_real_query_start(int *ret, MYSQL *mysql, const char *stmt_str, size_t length)
{
  int res;
  struct mysql_async_context *b;
  struct mysql_real_query_params parms;

  b= mysql->options.extension->async_context;
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.stmt_str= stmt_str;
    parms.length= length;
  }

  b->active= 1;
  res= my_context_spawn(&b->async_context, mysql_real_query_start_internal, &parms);
  b->active= b->suspended= 0;
  if (res > 0)
  {
    /* Suspended. */
    b->suspended= 1;
    return b->events_to_wait_for;
  }
  if (res < 0)
  {
    set_mariadb_error((mysql), CR_OUT_OF_MEMORY, unknown_sqlstate);
    *ret= 1;
  }
  else
    *ret= b->ret_result.r_int;
  return 0;

}
int STDCALL
mysql_real_query_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_fetch_row_start(). */
struct mysql_fetch_row_params {
  MYSQL_RES *result;
};
static void
mysql_fetch_row_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_fetch_row,
  (parms->result),
  parms->result->handle,
  MYSQL_ROW,
  r_ptr)
}
int STDCALL
mysql_fetch_row_start(MYSQL_ROW *ret, MYSQL_RES *result)
{
MK_ASYNC_START_BODY(
  mysql_fetch_row,
  result->handle,
  {
    WIN_SET_NONBLOCKING(result->handle)
    parms.result= result;
  },
  NULL,
  r_ptr,
  /*
    If we already fetched all rows from server (eg. mysql_store_result()),
    then result->handle will be NULL and we cannot suspend. But that is fine,
    since in this case mysql_fetch_row cannot block anyway. Just return
    directly.
  */
  if (!result->handle)
  {
    *ret= mysql_fetch_row(result);
    return 0;
  })
}
int STDCALL
mysql_fetch_row_cont(MYSQL_ROW *ret, MYSQL_RES *result, int ready_status)
{
MK_ASYNC_CONT_BODY(
  result->handle,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_set_character_set_start(). */
struct mysql_set_character_set_params {
  MYSQL *mysql;
  const char *csname;
};
static void
mysql_set_character_set_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_set_character_set,
  (parms->mysql, parms->csname),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_set_character_set_start(int *ret, MYSQL *mysql, const char *csname)
{
MK_ASYNC_START_BODY(
  mysql_set_character_set,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.csname= csname;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_set_character_set_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_sekect_db_start(). */
struct mysql_select_db_params {
  MYSQL *mysql;
  const char *db;
};
static void
mysql_select_db_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_select_db,
  (parms->mysql, parms->db),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_select_db_start(int *ret, MYSQL *mysql, const char *db)
{
MK_ASYNC_START_BODY(
  mysql_select_db,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.db= db;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_select_db_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_send_query_start(). */
struct mysql_send_query_params {
  MYSQL *mysql;
  const char *q;
  size_t length;
};
static void
mysql_send_query_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_send_query,
  (parms->mysql, parms->q, parms->length),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_send_query_start(int *ret, MYSQL *mysql, const char *q, unsigned long length)
{
MK_ASYNC_START_BODY(
  mysql_send_query,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.q= q;
    parms.length= length;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_send_query_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_store_result_start(). */
struct mysql_store_result_params {
  MYSQL *mysql;
};
static void
mysql_store_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_store_result,
  (parms->mysql),
  parms->mysql,
  MYSQL_RES *,
  r_ptr)
}
int STDCALL
mysql_store_result_start(MYSQL_RES **ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_store_result,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  NULL,
  r_ptr,
  /* Nothing */)
}
int STDCALL
mysql_store_result_cont(MYSQL_RES **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_free_result_start(). */
struct mysql_free_result_params {
  MYSQL_RES *result;
};
static void
mysql_free_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY_VOID_RETURN(
  mysql_free_result,
  (parms->result),
  parms->result->handle)
}
int STDCALL
mysql_free_result_start(MYSQL_RES *result)
{
MK_ASYNC_START_BODY_VOID_RETURN(
  mysql_free_result,
  result->handle,
  {
    WIN_SET_NONBLOCKING(result->handle)
    parms.result= result;
  },
  /*
    mysql_free_result() can have NULL in result->handle (this happens when all
    rows have been fetched and mysql_fetch_row() returned NULL.)
    So we cannot suspend, but it does not matter, as in this case
    mysql_free_result() cannot block.
    It is also legitimate to have NULL result, which will do nothing.
  */
  if (!result || !result->handle)
  {
    mysql_free_result(result);
    return 0;
  })
}
int STDCALL
mysql_free_result_cont(MYSQL_RES *result, int ready_status)
{
MK_ASYNC_CONT_BODY_VOID_RETURN(result->handle)
}

/* Structure used to pass parameters from mysql_close_slow_part_start(). */
struct mysql_close_slow_part_params {
  MYSQL *sock;
};
/*
  We need special handling for mysql_close(), as the first part may block,
  while the last part needs to free our extra library context stack.

  So we do the first part (mysql_close_slow_part()) non-blocking, but the last
  part blocking.
*/
static void
mysql_close_slow_part_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY_VOID_RETURN(
  mysql_close_slow_part,
  (parms->sock),
  parms->sock)
}

int STDCALL
mysql_close_slow_part_start(MYSQL *sock)
{
MK_ASYNC_START_BODY_VOID_RETURN(
  mysql_close_slow_part,
  sock,
  {
    WIN_SET_NONBLOCKING(sock)
    parms.sock= sock;
  },
  /* Nothing */)
}
int STDCALL
mysql_close_slow_part_cont(MYSQL *sock, int ready_status)
{
MK_ASYNC_CONT_BODY_VOID_RETURN(sock)
}
int STDCALL
mysql_close_start(MYSQL *sock)
{
  int res;

  /* It is legitimate to have NULL sock argument, which will do nothing. */
  if (sock && sock->net.pvio)
  {
    res= mysql_close_slow_part_start(sock);
    /* If we need to block, return now and do the rest in mysql_close_cont(). */
    if (res)
      return res;
  }
  mysql_close(sock);
  return 0;
}
int STDCALL
mysql_close_cont(MYSQL *sock, int ready_status)
{
  int res;

  res= mysql_close_slow_part_cont(sock, ready_status);
  if (res)
    return res;
  mysql_close(sock);
  return 0;
}

/*
  These following are not available inside the server (neither blocking or
  non-blocking).
*/
#ifndef MYSQL_SERVER
/* Structure used to pass parameters from mysql_change_user_start(). */
struct mysql_change_user_params {
  MYSQL *mysql;
  const char *user;
  const char *passwd;
  const char *db;
};
static void
mysql_change_user_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_change_user,
  (parms->mysql, parms->user, parms->passwd, parms->db),
  parms->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_change_user_start(my_bool *ret, MYSQL *mysql, const char *user, const char *passwd, const char *db)
{
MK_ASYNC_START_BODY(
  mysql_change_user,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.user= user;
    parms.passwd= passwd;
    parms.db= db;
  },
  TRUE,
  r_my_bool,
  /* Nothing */)
}
int STDCALL
mysql_change_user_cont(my_bool *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_query_start(). */
struct mysql_query_params {
  MYSQL *mysql;
  const char *q;
};
static void
mysql_query_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_query,
  (parms->mysql, parms->q),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_query_start(int *ret, MYSQL *mysql, const char *q)
{
MK_ASYNC_START_BODY(
  mysql_query,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.q= q;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_query_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_shutdown_start(). */
struct mysql_shutdown_params {
  MYSQL *mysql;
  enum mysql_enum_shutdown_level shutdown_level;
};
static void
mysql_shutdown_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_shutdown,
  (parms->mysql, parms->shutdown_level),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_shutdown_start(int *ret, MYSQL *mysql, enum mysql_enum_shutdown_level shutdown_level)
{
MK_ASYNC_START_BODY(
  mysql_shutdown,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.shutdown_level= shutdown_level;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_shutdown_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_dump_debug_info_start(). */
struct mysql_dump_debug_info_params {
  MYSQL *mysql;
};
static void
mysql_dump_debug_info_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_dump_debug_info,
  (parms->mysql),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_dump_debug_info_start(int *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_dump_debug_info,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_dump_debug_info_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_refresh_start(). */
struct mysql_refresh_params {
  MYSQL *mysql;
  unsigned int refresh_options;
};
static void
mysql_refresh_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_refresh,
  (parms->mysql, parms->refresh_options),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_refresh_start(int *ret, MYSQL *mysql, unsigned int refresh_options)
{
MK_ASYNC_START_BODY(
  mysql_refresh,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.refresh_options= refresh_options;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_refresh_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_kill_start(). */
struct mysql_kill_params {
  MYSQL *mysql;
  unsigned long pid;
};
static void
mysql_kill_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_kill,
  (parms->mysql, parms->pid),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_kill_start(int *ret, MYSQL *mysql, unsigned long pid)
{
MK_ASYNC_START_BODY(
  mysql_kill,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.pid= pid;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_kill_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_set_server_option_start(). */
struct mysql_set_server_option_params {
  MYSQL *mysql;
  enum enum_mysql_set_option option;
};
static void
mysql_set_server_option_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_set_server_option,
  (parms->mysql, parms->option),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_set_server_option_start(int *ret, MYSQL *mysql,
                              enum enum_mysql_set_option option)
{
MK_ASYNC_START_BODY(
  mysql_set_server_option,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.option= option;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_set_server_option_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_ping_start(). */
struct mysql_ping_params {
  MYSQL *mysql;
};
static void
mysql_ping_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_ping,
  (parms->mysql),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_ping_start(int *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_ping,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_ping_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_reset_connection_start(). */
struct mysql_reset_connection_params {
  MYSQL *mysql;
};
static void
mysql_reset_connection_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_reset_connection,
  (parms->mysql),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_reset_connection_start(int *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_reset_connection,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_reset_connection_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_stat_start(). */
struct mysql_stat_params {
  MYSQL *mysql;
};
static void
mysql_stat_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stat,
  (parms->mysql),
  parms->mysql,
  const char *,
  r_const_ptr)
}
int STDCALL
mysql_stat_start(const char **ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_stat,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  NULL,
  r_const_ptr,
  /* Nothing */)
}
int STDCALL
mysql_stat_cont(const char **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_const_ptr)
}

/* Structure used to pass parameters from mysql_list_dbs_start(). */
struct mysql_list_dbs_params {
  MYSQL *mysql;
  const char *wild;
};
static void
mysql_list_dbs_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_list_dbs,
  (parms->mysql, parms->wild),
  parms->mysql,
  MYSQL_RES *,
  r_ptr)
}
int STDCALL
mysql_list_dbs_start(MYSQL_RES **ret, MYSQL *mysql, const char *wild)
{
MK_ASYNC_START_BODY(
  mysql_list_dbs,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.wild= wild;
  },
  NULL,
  r_ptr,
  /* Nothing */)
}
int STDCALL
mysql_list_dbs_cont(MYSQL_RES **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_list_tables_start(). */
struct mysql_list_tables_params {
  MYSQL *mysql;
  const char *wild;
};
static void
mysql_list_tables_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_list_tables,
  (parms->mysql, parms->wild),
  parms->mysql,
  MYSQL_RES *,
  r_ptr)
}
int STDCALL
mysql_list_tables_start(MYSQL_RES **ret, MYSQL *mysql, const char *wild)
{
MK_ASYNC_START_BODY(
  mysql_list_tables,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.wild= wild;
  },
  NULL,
  r_ptr,
  /* Nothing */)
}
int STDCALL
mysql_list_tables_cont(MYSQL_RES **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_list_processes_start(). */
struct mysql_list_processes_params {
  MYSQL *mysql;
};
static void
mysql_list_processes_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_list_processes,
  (parms->mysql),
  parms->mysql,
  MYSQL_RES *,
  r_ptr)
}
int STDCALL
mysql_list_processes_start(MYSQL_RES **ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_list_processes,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  NULL,
  r_ptr,
  /* Nothing */)
}
int STDCALL
mysql_list_processes_cont(MYSQL_RES **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_list_fields_start(). */
struct mysql_list_fields_params {
  MYSQL *mysql;
  const char *table;
  const char *wild;
};
static void
mysql_list_fields_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_list_fields,
  (parms->mysql, parms->table, parms->wild),
  parms->mysql,
  MYSQL_RES *,
  r_ptr)
}
int STDCALL
mysql_list_fields_start(MYSQL_RES **ret, MYSQL *mysql, const char *table,
                        const char *wild)
{
MK_ASYNC_START_BODY(
  mysql_list_fields,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.table= table;
    parms.wild= wild;
  },
  NULL,
  r_ptr,
  /* Nothing */)
}
int STDCALL
mysql_list_fields_cont(MYSQL_RES **ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  NULL,
  r_ptr)
}

/* Structure used to pass parameters from mysql_read_query_result_start(). */
struct mysql_read_query_result_params {
  MYSQL *mysql;
};
static void
mysql_read_query_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_read_query_result,
  (parms->mysql),
  parms->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_read_query_result_start(my_bool *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_read_query_result,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  TRUE,
  r_my_bool,
  /* Nothing */)
}
int STDCALL
mysql_read_query_result_cont(my_bool *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_stmt_prepare_start(). */
struct mysql_stmt_prepare_params {
  MYSQL_STMT *stmt;
  const char *query;
  size_t length;
};
static void
mysql_stmt_prepare_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_prepare,
  (parms->stmt, parms->query, parms->length),
  parms->stmt->mysql,
  int,
  r_int)
}
int STDCALL
mysql_stmt_prepare_start(int *ret, MYSQL_STMT *stmt, const char *query,
                         size_t length)
{
MK_ASYNC_START_BODY(
  mysql_stmt_prepare,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
    parms.query= query;
    parms.length= length;
  },
  1,
  r_int,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_prepare(stmt, query, length);
    return 0;
  })
}
int STDCALL
mysql_stmt_prepare_cont(int *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_stmt_execute_start(). */
struct mysql_stmt_execute_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_execute_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_execute,
  (parms->stmt),
  parms->stmt->mysql,
  int,
  r_int)
}
int STDCALL
mysql_stmt_execute_start(int *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_execute,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  1,
  r_int,
  /*
    If eg. mysql_change_user(), stmt->mysql will be NULL.
    In this case, we cannot block.
  */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_execute(stmt);
    return 0;
  })
}
int STDCALL
mysql_stmt_execute_cont(int *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_stmt_fetch_start(). */
struct mysql_stmt_fetch_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_fetch_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_fetch,
  (parms->stmt),
  parms->stmt->mysql,
  int,
  r_int)
}
int STDCALL
mysql_stmt_fetch_start(int *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_fetch,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  1,
  r_int,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_fetch(stmt);
    return 0;
  })
}
int STDCALL
mysql_stmt_fetch_cont(int *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_stmt_store_result_start(). */
struct mysql_stmt_store_result_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_store_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_store_result,
  (parms->stmt),
  parms->stmt->mysql,
  int,
  r_int)
}
int STDCALL
mysql_stmt_store_result_start(int *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_store_result,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  1,
  r_int,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_store_result(stmt);
    return 0;
  })
}
int STDCALL
mysql_stmt_store_result_cont(int *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_stmt_close_start(). */
struct mysql_stmt_close_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_close_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_close,
  (parms->stmt),
  parms->stmt->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_stmt_close_start(my_bool *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_close,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  TRUE,
  r_my_bool,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_close(stmt);
    return 0;
  })
}
int STDCALL
mysql_stmt_close_cont(my_bool *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_stmt_reset_start(). */
struct mysql_stmt_reset_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_reset_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_reset,
  (parms->stmt),
  parms->stmt->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_stmt_reset_start(my_bool *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_reset,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  TRUE,
  r_my_bool,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_reset(stmt);
    return 0;
  })
}
int STDCALL
mysql_stmt_reset_cont(my_bool *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_stmt_free_result_start(). */
struct mysql_stmt_free_result_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_free_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_free_result,
  (parms->stmt),
  parms->stmt->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_stmt_free_result_start(my_bool *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_free_result,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  TRUE,
  r_my_bool,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_free_result(stmt);
    return 0;
  })
}
int STDCALL
mysql_stmt_free_result_cont(my_bool *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_stmt_send_long_data_start(). */
struct mysql_stmt_send_long_data_params {
  MYSQL_STMT *stmt;
  unsigned int param_number;
  const char *data;
  size_t length;
};
static void
mysql_stmt_send_long_data_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_send_long_data,
  (parms->stmt, parms->param_number, parms->data, parms->length),
  parms->stmt->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_stmt_send_long_data_start(my_bool *ret, MYSQL_STMT *stmt,
                                unsigned int param_number,
                                const char *data, size_t length)
{
MK_ASYNC_START_BODY(
  mysql_stmt_send_long_data,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
    parms.param_number= param_number;
    parms.data= data;
    parms.length= length;
  },
  TRUE,
  r_my_bool,
  /* If stmt->mysql==NULL then we will not block so can call directly. */
  if (!stmt->mysql)
  {
    *ret= mysql_stmt_send_long_data(stmt, param_number, data, length);
    return 0;
  })
}
int STDCALL
mysql_stmt_send_long_data_cont(my_bool *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_commit_start(). */
struct mysql_commit_params {
  MYSQL *mysql;
};
static void
mysql_commit_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_commit,
  (parms->mysql),
  parms->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_commit_start(my_bool *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_commit,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  TRUE,
  r_my_bool,
  /* Nothing */)
}
int STDCALL
mysql_commit_cont(my_bool *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_rollback_start(). */
struct mysql_rollback_params {
  MYSQL *mysql;
};
static void
mysql_rollback_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_rollback,
  (parms->mysql),
  parms->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_rollback_start(my_bool *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_rollback,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  TRUE,
  r_my_bool,
  /* Nothing */)
}
int STDCALL
mysql_rollback_cont(my_bool *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_autocommit_start(). */
struct mysql_autocommit_params {
  MYSQL *mysql;
  my_bool auto_mode;
};
static void
mysql_autocommit_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_autocommit,
  (parms->mysql, parms->auto_mode),
  parms->mysql,
  my_bool,
  r_my_bool)
}
int STDCALL
mysql_autocommit_start(my_bool *ret, MYSQL *mysql, my_bool auto_mode)
{
MK_ASYNC_START_BODY(
  mysql_autocommit,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
    parms.auto_mode= auto_mode;
  },
  TRUE,
  r_my_bool,
  /* Nothing */)
}
int STDCALL
mysql_autocommit_cont(my_bool *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  TRUE,
  r_my_bool)
}

/* Structure used to pass parameters from mysql_next_result_start(). */
struct mysql_next_result_params {
  MYSQL *mysql;
};
static void
mysql_next_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_next_result,
  (parms->mysql),
  parms->mysql,
  int,
  r_int)
}
int STDCALL
mysql_next_result_start(int *ret, MYSQL *mysql)
{
MK_ASYNC_START_BODY(
  mysql_next_result,
  mysql,
  {
    WIN_SET_NONBLOCKING(mysql)
    parms.mysql= mysql;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_next_result_cont(int *ret, MYSQL *mysql, int ready_status)
{
MK_ASYNC_CONT_BODY(
  mysql,
  1,
  r_int)
}

/* Structure used to pass parameters from mysql_stmt_next_result_start(). */
struct mysql_stmt_next_result_params {
  MYSQL_STMT *stmt;
};
static void
mysql_stmt_next_result_start_internal(void *d)
{
MK_ASYNC_INTERNAL_BODY(
  mysql_stmt_next_result,
  (parms->stmt),
  parms->stmt->mysql,
  int,
  r_int)
}
int STDCALL
mysql_stmt_next_result_start(int *ret, MYSQL_STMT *stmt)
{
MK_ASYNC_START_BODY(
  mysql_stmt_next_result,
  stmt->mysql,
  {
    WIN_SET_NONBLOCKING(stmt->mysql)
    parms.stmt= stmt;
  },
  1,
  r_int,
  /* Nothing */)
}
int STDCALL
mysql_stmt_next_result_cont(int *ret, MYSQL_STMT *stmt, int ready_status)
{
MK_ASYNC_CONT_BODY(
  stmt->mysql,
  1,
  r_int)
}
#endif


/*
  The following functions are deprecated, and so have no non-blocking version:

    mysql_connect
    mysql_create_db
    mysql_drop_db
*/

/*
  The following functions can newer block, and so do not have special
  non-blocking versions:

    mysql_num_rows()
    mysql_num_fields()
    mysql_eof()
    mysql_fetch_field_direct()
    mysql_fetch_fields()
    mysql_row_tell()
    mysql_field_tell()
    mysql_field_count()
    mysql_affected_rows()
    mysql_insert_id()
    mysql_errno()
    mysql_error()
    mysql_sqlstate()
    mysql_warning_count()
    mysql_info()
    mysql_thread_id()
    mysql_character_set_name()
    mysql_init()
    mysql_ssl_set()
    mysql_get_ssl_cipher()
    mysql_use_result()
    mysql_get_character_set_info()
    mysql_set_local_infile_handler()
    mysql_set_local_infile_default()
    mysql_get_server_info()
    mysql_get_server_name()
    mysql_get_client_info()
    mysql_get_client_version()
    mysql_get_host_info()
    mysql_get_server_version()
    mysql_get_proto_info()
    mysql_options()
    mysql_data_seek()
    mysql_row_seek()
    mysql_field_seek()
    mysql_fetch_lengths()
    mysql_fetch_field()
    mysql_escape_string()
    mysql_hex_string()
    mysql_real_escape_string()
    mysql_debug()
    myodbc_remove_escape()
    mysql_thread_safe()
    mysql_embedded()
    mariadb_connection()
    mysql_stmt_init()
    mysql_stmt_fetch_column()
    mysql_stmt_param_count()
    mysql_stmt_attr_set()
    mysql_stmt_attr_get()
    mysql_stmt_bind_param()
    mysql_stmt_bind_result()
    mysql_stmt_result_metadata()
    mysql_stmt_param_metadata()
    mysql_stmt_errno()
    mysql_stmt_error()
    mysql_stmt_sqlstate()
    mysql_stmt_row_seek()
    mysql_stmt_row_tell()
    mysql_stmt_data_seek()
    mysql_stmt_num_rows()
    mysql_stmt_affected_rows()
    mysql_stmt_insert_id()
    mysql_stmt_field_count()
    mysql_more_results()
    mysql_get_socket()
    mysql_get_timeout_value()
*/
