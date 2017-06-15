/****************************************************************************
  Copyright (C) 2012 Monty Program AB

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
 *****************************************************************************/

/* The implementation for prepared statements was ported from PHP's mysqlnd
   extension, written by Andrey Hristov, Georg Richter and Ulf Wendel

   Original file header:
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
#include <mariadb_ctype.h>
#include "mysql.h"
#include "errmsg.h"
#include <ma_pvio.h>
#include <sys/stat.h>
#include <signal.h>
#include <time.h>
#include <mysql/client_plugin.h>
#include <ma_common.h>

#define STMT_NUM_OFS(type, a,r) ((type *)(a))[r]
#define MADB_RESET_ERROR     1
#define MADB_RESET_LONGDATA  2
#define MADB_RESET_SERVER    4
#define MADB_RESET_BUFFER    8
#define MADB_RESET_STORED   16

#define MAX_TIME_STR_LEN 13
#define MAX_DATE_STR_LEN 5
#define MAX_DATETIME_STR_LEN 12

typedef struct
{
  MA_MEM_ROOT fields_ma_alloc_root;
} MADB_STMT_EXTENSION;

MYSQL_DATA *read_rows(MYSQL *mysql,MYSQL_FIELD *mysql_fields, uint fields);
void free_rows(MYSQL_DATA *cur);
int ma_multi_command(MYSQL *mysql, enum enum_multi_status status);
MYSQL_FIELD * unpack_fields(MYSQL_DATA *data,MA_MEM_ROOT *alloc,uint fields, my_bool default_value, my_bool long_flag_protocol);
static my_bool net_stmt_close(MYSQL_STMT *stmt, my_bool remove);

static my_bool is_not_null= 0;
static my_bool is_null= 1;

void stmt_set_error(MYSQL_STMT *stmt,
                  unsigned int error_nr,
                  const char *sqlstate,
                  const char *format,
                  ...)
{
  va_list ap;

  stmt->last_errno= error_nr;
  ma_strmake(stmt->sqlstate, sqlstate, SQLSTATE_LENGTH);
  va_start(ap, format);
  vsnprintf(stmt->last_error, MYSQL_ERRMSG_SIZE,
            format ? format : ER(error_nr), ap);
  va_end(ap);
  return;
}

my_bool mthd_supported_buffer_type(enum enum_field_types type)
{
  switch (type) {
  case MYSQL_TYPE_BIT:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_NEWDATE:
  case MYSQL_TYPE_NEWDECIMAL:
  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_JSON:
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_YEAR:
    return 1;
    break;
  default:
    return 0;
    break;
  }
}

static my_bool madb_reset_stmt(MYSQL_STMT *stmt, unsigned int flags);
static my_bool mysql_stmt_internal_reset(MYSQL_STMT *stmt, my_bool is_close);
static int stmt_unbuffered_eof(MYSQL_STMT *stmt __attribute__((unused)),
                               uchar **row __attribute__((unused)))
{
  return MYSQL_NO_DATA;
}

static int stmt_unbuffered_fetch(MYSQL_STMT *stmt, uchar **row)
{
  ulong pkt_len;

  pkt_len= ma_net_safe_read(stmt->mysql);

  if (pkt_len == packet_error)
  {
    stmt->fetch_row_func= stmt_unbuffered_eof;
    return(1);
  }

  if (stmt->mysql->net.read_pos[0] == 254)
  {
    *row = NULL;
    stmt->fetch_row_func= stmt_unbuffered_eof;
    return(MYSQL_NO_DATA);
  }
  else
    *row = stmt->mysql->net.read_pos;
  stmt->result.rows++;
  return(0);
}

static int stmt_buffered_fetch(MYSQL_STMT *stmt, uchar **row)
{
  if (!stmt->result_cursor)
  {
    *row= NULL;
    stmt->state= MYSQL_STMT_FETCH_DONE;
    return MYSQL_NO_DATA;
  }
  stmt->state= MYSQL_STMT_USER_FETCHING;
  *row= (uchar *)stmt->result_cursor->data;

  stmt->result_cursor= stmt->result_cursor->next;
  return 0;
}

int mthd_stmt_read_all_rows(MYSQL_STMT *stmt)
{
  MYSQL_DATA *result= &stmt->result;
  MYSQL_ROWS *current, **pprevious;
  ulong packet_len;
  unsigned char *p;

  pprevious= &result->data;

  while ((packet_len = ma_net_safe_read(stmt->mysql)) != packet_error)
  {
    p= stmt->mysql->net.read_pos;
    if (packet_len > 7 || p[0] != 254)
    {
      /* allocate space for rows */
      if (!(current= (MYSQL_ROWS *)ma_alloc_root(&result->alloc, sizeof(MYSQL_ROWS) + packet_len)))
      {
        SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
        return(1);
      }
      current->data= (MYSQL_ROW)(current + 1);
      *pprevious= current;
      pprevious= &current->next;

      /* copy binary row, we will encode it during mysql_stmt_fetch */
      memcpy((char *)current->data, (char *)p, packet_len);

      if (stmt->update_max_length)
      {
        uchar *null_ptr, bit_offset= 4;
        uchar *cp= p;
        unsigned int i;

        cp++; /* skip first byte */
        null_ptr= cp;
        cp+= (stmt->field_count + 9) / 8;

        for (i=0; i < stmt->field_count; i++)
        {
          if (!(*null_ptr & bit_offset))
          {
            if (mysql_ps_fetch_functions[stmt->fields[i].type].pack_len < 0)
            {
              /* We need to calculate the sizes for date and time types */
              size_t len= net_field_length(&cp);
              switch(stmt->fields[i].type) {
              case MYSQL_TYPE_TIME:
              case MYSQL_TYPE_DATE:
              case MYSQL_TYPE_DATETIME:
              case MYSQL_TYPE_TIMESTAMP:
                stmt->fields[i].max_length= mysql_ps_fetch_functions[stmt->fields[i].type].max_len;
                break;
              default:
                if (len > stmt->fields[i].max_length)
                  stmt->fields[i].max_length= (ulong)len;
                break;
              }
              cp+= len;
            }
            else
            {
              if (!stmt->fields[i].max_length)
                stmt->fields[i].max_length= mysql_ps_fetch_functions[stmt->fields[i].type].max_len;
              cp+= mysql_ps_fetch_functions[stmt->fields[i].type].pack_len;
            }
          }
          if (!((bit_offset <<=1) & 255))
          {
            bit_offset= 1; /* To next byte */
            null_ptr++;
          }
        }
      }
      current->length= packet_len;
      result->rows++;
    } else  /* end of stream */
    {
      *pprevious= 0;
      /* sace status info */
      p++;
      stmt->upsert_status.warning_count= stmt->mysql->warning_count= uint2korr(p);
      p+=2;
      stmt->upsert_status.server_status= stmt->mysql->server_status= uint2korr(p);
      stmt->result_cursor= result->data;
      return(0);
    }
  }
  stmt->result_cursor= 0;
  SET_CLIENT_STMT_ERROR(stmt, stmt->mysql->net.last_errno, stmt->mysql->net.sqlstate,
      stmt->mysql->net.last_error);
  return(1);
}

static int stmt_cursor_fetch(MYSQL_STMT *stmt, uchar **row)
{
  uchar buf[STMT_ID_LENGTH + 4];
  MYSQL_DATA *result= &stmt->result;

  if (stmt->state < MYSQL_STMT_USE_OR_STORE_CALLED)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  /* do we have some prefetched rows available ? */
  if (stmt->result_cursor)
    return(stmt_buffered_fetch(stmt, row));
  if (stmt->upsert_status.server_status & SERVER_STATUS_LAST_ROW_SENT)
    stmt->upsert_status.server_status&=  ~SERVER_STATUS_LAST_ROW_SENT;
  else
  {
    int4store(buf, stmt->stmt_id);
    int4store(buf + STMT_ID_LENGTH, stmt->prefetch_rows);

    if (stmt->mysql->methods->db_command(stmt->mysql, COM_STMT_FETCH, (char *)buf, sizeof(buf), 1, stmt))
      return(1);

    /* free previously allocated buffer */
    ma_free_root(&result->alloc, MYF(MY_KEEP_PREALLOC));
    result->data= 0;
    result->rows= 0;

    if (stmt->mysql->methods->db_stmt_read_all_rows(stmt))
      return(1);

    return(stmt_buffered_fetch(stmt, row));
  }
  /* no more cursor data available */
  *row= NULL;
  return(MYSQL_NO_DATA);
}

/* flush one result set */
void mthd_stmt_flush_unbuffered(MYSQL_STMT *stmt)
{
  ulong packet_len;
  int in_resultset= stmt->state > MYSQL_STMT_EXECUTED &&
                    stmt->state < MYSQL_STMT_FETCH_DONE;
  while ((packet_len = ma_net_safe_read(stmt->mysql)) != packet_error)
  {
    uchar *pos= stmt->mysql->net.read_pos;
    if (!in_resultset && *pos == 0) /* OK */
    {
      pos++;
      net_field_length(&pos);
      net_field_length(&pos);
      stmt->mysql->server_status= uint2korr(pos);
      goto end;
    }
    if (packet_len < 8 && *pos == 254) /* EOF */
    {
      stmt->mysql->server_status= uint2korr(pos + 3);
      if (in_resultset)
        goto end;
      in_resultset= 1;
    }
  }
end:
  stmt->state= MYSQL_STMT_FETCH_DONE;
}

int mthd_stmt_fetch_to_bind(MYSQL_STMT *stmt, unsigned char *row)
{
  uint i;
  size_t truncations= 0;
  unsigned char *null_ptr, bit_offset= 4;

  if (!stmt->bind_result_done)  /* nothing to do */
    return(0);

  row++; /* skip status byte */
  null_ptr= row;
  row+= (stmt->field_count + 9) / 8;

  for (i=0; i < stmt->field_count; i++)
  {
    /* save row position for fetching values in pieces */
    if (*null_ptr & bit_offset)
    {
      *stmt->bind[i].is_null= 1;
      stmt->bind[i].u.row_ptr= NULL;
    } else
    {
      stmt->bind[i].u.row_ptr= row;
      if (stmt->bind[i].flags & MADB_BIND_DUMMY)
      {
        unsigned long length;

        if (mysql_ps_fetch_functions[stmt->fields[i].type].pack_len >= 0)
          length= mysql_ps_fetch_functions[stmt->fields[i].type].pack_len;
        else
          length= net_field_length(&row);
        row+= length;
      }
      else
      {
        if (!stmt->bind[i].length)
          stmt->bind[i].length= &stmt->bind[i].length_value;
        if (!stmt->bind[i].is_null)
          stmt->bind[i].is_null= &stmt->bind[i].is_null_value;
        *stmt->bind[i].is_null= 0;
        mysql_ps_fetch_functions[stmt->fields[i].type].func(&stmt->bind[i], &stmt->fields[i], &row);
        if (stmt->mysql->options.report_data_truncation)
          truncations+= *stmt->bind[i].error;
      }
    }

    if (!((bit_offset <<=1) & 255)) {
      bit_offset= 1; /* To next byte */
      null_ptr++;
    }
  }
  return((truncations) ? MYSQL_DATA_TRUNCATED : 0);
}

MYSQL_RES *_mysql_stmt_use_result(MYSQL_STMT *stmt)
{
  MYSQL *mysql= stmt->mysql;

  if (!stmt->field_count ||
      (!stmt->cursor_exists && mysql->status != MYSQL_STATUS_STMT_RESULT) ||
      (stmt->cursor_exists && mysql->status != MYSQL_STATUS_READY) ||
      (stmt->state != MYSQL_STMT_WAITING_USE_OR_STORE))
  {
    SET_CLIENT_ERROR(mysql, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(NULL);
  }

  CLEAR_CLIENT_STMT_ERROR(stmt);

  stmt->state = MYSQL_STMT_USE_OR_STORE_CALLED;
  if (!stmt->cursor_exists)
    stmt->fetch_row_func= stmt_unbuffered_fetch; //mysql_stmt_fetch_unbuffered_row;
  else
    stmt->fetch_row_func= stmt_cursor_fetch;

  return(NULL);
}

unsigned char *mysql_net_store_length(unsigned char *packet, size_t length)
{
  if (length < (unsigned long long) L64(251)) {
    *packet = (unsigned char) length;
    return packet + 1;
  }

  if (length < (unsigned long long) L64(65536)) {
    *packet++ = 252;
    int2store(packet,(uint) length);
    return packet + 2;
  }

  if (length < (unsigned long long) L64(16777216)) {
    *packet++ = 253;
    int3store(packet,(ulong) length);
    return packet + 3;
  }
  *packet++ = 254;
  int8store(packet, length);
  return packet + 8;
}

static long ma_get_length(MYSQL_STMT *stmt, unsigned int param_nr, unsigned long row_nr)
{
  if (!stmt->params[param_nr].length)
    return 0;
  if (stmt->row_size)
    return *(long *)((char *)stmt->params[param_nr].length + row_nr * stmt->row_size);
  else
    return stmt->params[param_nr].length[row_nr];
}

static char ma_get_indicator(MYSQL_STMT *stmt, unsigned int param_nr, unsigned long row_nr)
{
  if (!MARIADB_STMT_BULK_SUPPORTED(stmt) ||
      !stmt->array_size ||
      !stmt->params[param_nr].u.indicator)
    return 0;
  if (stmt->row_size)
    return *((char *)stmt->params[param_nr].u.indicator + (row_nr * stmt->row_size));
  return stmt->params[param_nr].u.indicator[row_nr];
}

static void *ma_get_buffer_offset(MYSQL_STMT *stmt, enum enum_field_types type,
                                  void *buffer, unsigned long row_nr)
{
  if (stmt->array_size)
  {
    int len;
    if (stmt->row_size)
      return (void *)((char *)buffer + stmt->row_size * row_nr);
    len= mysql_ps_fetch_functions[type].pack_len;
    if (len > 0)
      return (void *)((char *)buffer + len * row_nr);
    return ((void **)buffer)[row_nr];
  }
  return buffer;
}

int store_param(MYSQL_STMT *stmt, int column, unsigned char **p, unsigned long row_nr)
{
  void *buf= ma_get_buffer_offset(stmt, stmt->params[column].buffer_type,
                                  stmt->params[column].buffer, row_nr);
  char indicator= ma_get_indicator(stmt, column, row_nr);

  switch (stmt->params[column].buffer_type) {
  case MYSQL_TYPE_TINY:
    int1store(*p, (*(uchar *)buf));
    (*p) += 1;
    break;
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_YEAR:
    int2store(*p, (*(short *)buf));
    (*p) += 2;
    break;
  case MYSQL_TYPE_FLOAT:
    float4store(*p, (*(float *)buf));
    (*p) += 4;
    break;
  case MYSQL_TYPE_DOUBLE:
    float8store(*p, (*(double *)buf));
    (*p) += 8;
    break;
  case MYSQL_TYPE_LONGLONG:
    int8store(*p, (*(ulonglong *)buf));
    (*p) += 8;
    break;
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
    int4store(*p, (*(int32 *)buf));
    (*p)+= 4;
    break;
  case MYSQL_TYPE_TIME:
  {
    /* binary encoding:
       Offset     Length  Field
       0          1       Length
       1          1       negative
       2-5        4       day
       6          1       hour
       7          1       ninute
       8          1       second;
       9-13       4       second_part
       */
    MYSQL_TIME *t= (MYSQL_TIME *)ma_get_buffer_offset(stmt, stmt->params[column].buffer_type,
                                                      stmt->params[column].buffer, row_nr);
    char t_buffer[MAX_TIME_STR_LEN];
    uint len= 0;

    t_buffer[1]= t->neg ? 1 : 0;
    int4store(t_buffer + 2, t->day);
    t_buffer[6]= (uchar) t->hour;
    t_buffer[7]= (uchar) t->minute;
    t_buffer[8]= (uchar) t->second;
    if (t->second_part)
    {
      int4store(t_buffer + 9, t->second_part);
      len= 12;
    }
    else if (t->day || t->hour || t->minute || t->second)
      len= 8;
    t_buffer[0]= len++;
    memcpy(*p, t_buffer, len);
    (*p)+= len;
    break;
  }
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATETIME:
  {
    /* binary format for date, timestamp and datetime
       Offset     Length  Field
       0          1       Length
       1-2        2       Year
       3          1       Month
       4          1       Day
       5          1       Hour
       6          1       minute
       7          1       second
       8-11       4       secondpart
       */
    MYSQL_TIME *t= (MYSQL_TIME *)ma_get_buffer_offset(stmt, stmt->params[column].buffer_type,
                                                      stmt->params[column].buffer, row_nr);
    char t_buffer[MAX_DATETIME_STR_LEN];
    uint len= 0;

    int2store(t_buffer + 1, t->year);
    t_buffer[3]= (char) t->month;
    t_buffer[4]= (char) t->day;
    t_buffer[5]= (char) t->hour;
    t_buffer[6]= (char) t->minute;
    t_buffer[7]= (char) t->second;
    if (t->second_part)
    {
      int4store(t_buffer + 8, t->second_part);
      len= 11;
    }
    else if (t->hour || t->minute || t->second)
      len= 7;
    else if (t->year || t->month || t->day)
      len= 4;
    else
      len=0;
    t_buffer[0]= len++;
    memcpy(*p, t_buffer, len);
    (*p)+= len;
    break;
  }
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_JSON:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
  {
    ulong len;
    /* to is after p. The latter hasn't been moved */
    uchar *to;

    if (indicator == STMT_INDICATOR_NTS)
      len= -1;
    else
      len= ma_get_length(stmt, column, row_nr);

    if (len == (ulong)-1)
      len= (ulong)strlen((char *)buf);

    to = mysql_net_store_length(*p, len);

    if (len)
      memcpy(to, buf, len);
    (*p) = to + len;
    break;
  }

  default:
    /* unsupported parameter type */
    SET_CLIENT_STMT_ERROR(stmt, CR_UNSUPPORTED_PARAM_TYPE, SQLSTATE_UNKNOWN, 0);
    return 1;
  }
  return 0;
}

/* {{{ mysqlnd_stmt_execute_generate_simple_request */
unsigned char* mysql_stmt_execute_generate_simple_request(MYSQL_STMT *stmt, size_t *request_len)
{
  /* execute packet has the following format:
     Offset   Length      Description
     -----------------------------------------
     0             4      Statement id
     4             1      Flags (cursor type)
     5             4      Iteration count
     -----------------------------------------
     if (stmt->param_count):
     6  (paramcount+7)/8  null bitmap
     ------------------------------------------
     if (stmt->send_types_to_server):
     param_count*2    parameter types
     1st byte: parameter type
     2nd byte flag:
              unsigned flag (32768)
              indicator variable exists (16384)
     ------------------------------------------
     n      data from bind_buffer

     */

  size_t length= 1024;
  size_t free_bytes= 0;
  size_t null_byte_offset= 0;
  uint i;

  uchar *start= NULL, *p;

  /* preallocate length bytes */
  /* check: gr */
  if (!(start= p= (uchar *)malloc(length)))
    goto mem_error;

  int4store(p, stmt->stmt_id);
  p += STMT_ID_LENGTH;

  /* flags is 4 bytes, we store just 1 */
  int1store(p, (unsigned char) stmt->flags);
  p++;

  int4store(p, 1);
  p+= 4;

  if (stmt->param_count)
  {
    size_t null_count= (stmt->param_count + 7) / 8;

    free_bytes= length - (p - start);
    if (null_count + 20 > free_bytes)
    {
      size_t offset= p - start;
      length+= offset + null_count + 20;
      if (!(start= (uchar *)realloc(start, length)))
        goto mem_error;
      p= start + offset;
    }

    null_byte_offset= p - start;
    memset(p, 0, null_count);
    p += null_count;

    int1store(p, stmt->send_types_to_server);
    p++;

    free_bytes= length - (p - start);

    /* Store type information:
       2 bytes per type
       */
    if (stmt->send_types_to_server)
    {
      if (free_bytes < stmt->param_count * 2 + 20)
      {
        size_t offset= p - start;
        length= offset + stmt->param_count * 2 + 20;
        if (!(start= (uchar *)realloc(start, length)))
          goto mem_error;
        p= start + offset;
      }
      for (i = 0; i < stmt->param_count; i++)
      {
        /* this differs from mysqlnd, c api supports unsinged !! */
        uint buffer_type= stmt->params[i].buffer_type | (stmt->params[i].is_unsigned ? 32768 : 0);
        /* check if parameter requires indicator variable */
        int2store(p, buffer_type);
        p+= 2;
      }
    }

    /* calculate data size */
    for (i=0; i < stmt->param_count; i++)
    {
      size_t size= 0;
      my_bool has_data= TRUE;

      if (stmt->params[i].long_data_used)
      {
        has_data= FALSE;
        stmt->params[i].long_data_used= 0;
      }

      if (has_data)
      {
        switch (stmt->params[i].buffer_type) {
        case MYSQL_TYPE_NULL:
          has_data= FALSE;
          break;
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_SET:
          size+= 5; /* max 8 bytes for size */
          size+= (size_t)ma_get_length(stmt, i, 0);
          break;
        default:
          size+= mysql_ps_fetch_functions[stmt->params[i].buffer_type].pack_len;
          break;
        }
      }
      free_bytes= length - (p - start);
      if (free_bytes < size + 20)
      {
        size_t offset= p - start;
        length= MAX(2 * length, offset + size + 20);
        if (!(start= (uchar *)realloc(start, length)))
          goto mem_error;
        p= start + offset;
      }
      if (((stmt->params[i].is_null && *stmt->params[i].is_null) ||
             stmt->params[i].buffer_type == MYSQL_TYPE_NULL ||
             !stmt->params[i].buffer))
      {
        has_data= FALSE;
        (start + null_byte_offset)[i/8] |= (unsigned char) (1 << (i & 7));
      }

      if (has_data)
      {
        store_param(stmt, i, &p, 0);
      }
    }
  }
  stmt->send_types_to_server= 0;
  *request_len = (size_t)(p - start);
  return start;
mem_error:
  SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
  free(start);
  *request_len= 0;
  return NULL;
}
/* }}} */

/* {{{ mysqlnd_stmt_execute_generate_bulk_request */
unsigned char* mysql_stmt_execute_generate_bulk_request(MYSQL_STMT *stmt, size_t *request_len)
{
  /* execute packet has the following format:
     Offset   Length      Description
     -----------------------------------------
     0             4      Statement id
     4             2      Flags (cursor type):
                            STMT_BULK_FLAG_CLIENT_SEND_TYPES = 128
                            STMT_BULK_FLAG_INSERT_ID_REQUEST = 64
     -----------------------------------------
     if (stmt->send_types_to_server):
     for (i=0; i < param_count; i++)
       1st byte: parameter type
       2nd byte flag:
              unsigned flag (32768)
     ------------------------------------------
     for (i=0; i < param_count; i++)
                   1      indicator variable
                            STMT_INDICATOR_NONE 0
                            STMT_INDICATOR_NULL 1
                            STMT_INDICATOR_DEFAULT 2
                            STMT_INDICATOR_IGNORE 3
                   n      data from bind buffer

     */

  size_t length= 1024;
  size_t free_bytes= 0;
  ushort flags= 0;
  uint i, j;

  uchar *start= NULL, *p;

  if (!MARIADB_STMT_BULK_SUPPORTED(stmt))
  {
    stmt_set_error(stmt, CR_FUNCTION_NOT_SUPPORTED, "IM001",
                   CER(CR_FUNCTION_NOT_SUPPORTED), "Bulk operation");
    return NULL;
  }

  if (!stmt->param_count)
  {
    stmt_set_error(stmt, CR_BULK_WITHOUT_PARAMETERS, "IM001",
                   CER(CR_BULK_WITHOUT_PARAMETERS), "Bulk operation");
    return NULL;
  }

  /* preallocate length bytes */
  if (!(start= p= (uchar *)malloc(length)))
    goto mem_error;

  int4store(p, stmt->stmt_id);
  p += STMT_ID_LENGTH;

  /* todo: request to return auto generated ids */
  if (stmt->send_types_to_server)
    flags|= STMT_BULK_FLAG_CLIENT_SEND_TYPES;
  int2store(p, flags);
  p+=2;

  /* When using mariadb_stmt_execute_direct stmt->paran_count is
     not knowm, so we need to assign prebind_params, which was previously
     set by mysql_stmt_attr_set
  */
  if (!stmt->param_count && stmt->prebind_params)
    stmt->param_count= stmt->prebind_params;

  if (stmt->param_count)
  {
    free_bytes= length - (p - start);

    /* Store type information:
       2 bytes per type
       */
    if (stmt->send_types_to_server)
    {
      if (free_bytes < stmt->param_count * 2 + 20)
      {
        size_t offset= p - start;
        length= offset + stmt->param_count * 2 + 20;
        if (!(start= (uchar *)realloc(start, length)))
          goto mem_error;
        p= start + offset;
      }
      for (i = 0; i < stmt->param_count; i++)
      {
        /* this differs from mysqlnd, c api supports unsinged !! */
        uint buffer_type= stmt->params[i].buffer_type | (stmt->params[i].is_unsigned ? 32768 : 0);
        int2store(p, buffer_type);
        p+= 2;
      }
    }

    /* calculate data size */
    for (j=0; j < stmt->array_size; j++)
    {
      for (i=0; i < stmt->param_count; i++)
      {
        size_t size= 0;
        my_bool has_data= TRUE;
        char indicator= ma_get_indicator(stmt, i, j);
        /* check if we need to send data */
        if (indicator > 0)
          has_data= FALSE;
        size= 1;

        /* Please note that mysql_stmt_send_long_data is not supported
           current when performing bulk execute */

        if (has_data)
        {
          switch (stmt->params[i].buffer_type) {
          case MYSQL_TYPE_NULL:
            has_data= FALSE;
            indicator= STMT_INDICATOR_NULL;
            break;
          case MYSQL_TYPE_TINY_BLOB:
          case MYSQL_TYPE_MEDIUM_BLOB:
          case MYSQL_TYPE_LONG_BLOB:
          case MYSQL_TYPE_BLOB:
          case MYSQL_TYPE_VARCHAR:
          case MYSQL_TYPE_VAR_STRING:
          case MYSQL_TYPE_STRING:
          case MYSQL_TYPE_JSON:
          case MYSQL_TYPE_DECIMAL:
          case MYSQL_TYPE_NEWDECIMAL:
          case MYSQL_TYPE_GEOMETRY:
          case MYSQL_TYPE_NEWDATE:
          case MYSQL_TYPE_ENUM:
          case MYSQL_TYPE_BIT:
          case MYSQL_TYPE_SET:
            size+= 5; /* max 8 bytes for size */
            if (indicator == STMT_INDICATOR_NTS ||
              (!stmt->row_size && ma_get_length(stmt,i,j) == -1))
                size+= strlen(ma_get_buffer_offset(stmt,
                                                   stmt->params[i].buffer_type,
                                                   stmt->params[i].buffer,j));
            else
              size+= (size_t)ma_get_length(stmt, i, j);
            break;
          default:
            size+= mysql_ps_fetch_functions[stmt->params[i].buffer_type].pack_len;
            break;
          }
        }
        free_bytes= length - (p - start);
        if (free_bytes < size + 20)
        {
          size_t offset= p - start;
          length= MAX(2 * length, offset + size + 20);
          if (!(start= (uchar *)realloc(start, length)))
            goto mem_error;
          p= start + offset;
        }

        int1store(p, indicator > 0 ? indicator : 0);
        p++;
        if (has_data)
          store_param(stmt, i, &p, j);
      }
    }

  }
  stmt->send_types_to_server= 0;
  *request_len = (size_t)(p - start);
  return start;
mem_error:
  SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
  free(start);
  *request_len= 0;
  return NULL;
}
/* }}} */
/*!
 *******************************************************************************

 \fn        unsigned long long mysql_stmt_affected_rows
 \brief     returns the number of affected rows from last mysql_stmt_execute
 call

 \param[in]  stmt The statement handle
 *******************************************************************************
 */
unsigned long long STDCALL mysql_stmt_affected_rows(MYSQL_STMT *stmt)
{
  return stmt->upsert_status.affected_rows;
}

my_bool STDCALL mysql_stmt_attr_get(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, void *value)
{
  switch (attr_type) {
    case STMT_ATTR_UPDATE_MAX_LENGTH:
      *(my_bool *)value= stmt->update_max_length;
      break;
    case STMT_ATTR_CURSOR_TYPE:
      *(unsigned long *)value= stmt->flags;
      break;
    case STMT_ATTR_PREFETCH_ROWS:
      *(unsigned long *)value= stmt->prefetch_rows;
      break;
    case STMT_ATTR_PREBIND_PARAMS:
      *(unsigned int *)value= stmt->prebind_params;
      break;
    case STMT_ATTR_ARRAY_SIZE:
      *(unsigned int *)value= stmt->array_size;
      break;
    case STMT_ATTR_ROW_SIZE:
      *(size_t *)value= stmt->row_size;
      break;
    default:
      return(1);
  }
  return(0);
}

my_bool STDCALL mysql_stmt_attr_set(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, const void *value)
{
  switch (attr_type) {
  case STMT_ATTR_UPDATE_MAX_LENGTH:
    stmt->update_max_length= *(my_bool *)value;
    break;
  case STMT_ATTR_CURSOR_TYPE:
    if (*(ulong *)value > (unsigned long) CURSOR_TYPE_READ_ONLY)
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_NOT_IMPLEMENTED, SQLSTATE_UNKNOWN, 0);
      return(1);
    }
    stmt->flags = *(ulong *)value;
    break;
  case STMT_ATTR_PREFETCH_ROWS:
    if (*(ulong *)value == 0)
      *(long *)value= MYSQL_DEFAULT_PREFETCH_ROWS;
    else
      stmt->prefetch_rows= *(long *)value;
    break;
  case STMT_ATTR_PREBIND_PARAMS:
    if (stmt->state > MYSQL_STMT_INITTED)
    {
      mysql_stmt_internal_reset(stmt, 1);
      net_stmt_close(stmt, 0);
      stmt->state= MYSQL_STMT_INITTED;
      stmt->params= 0;
    }
    stmt->prebind_params= *(unsigned int *)value;
    break;
  case STMT_ATTR_ARRAY_SIZE:
    stmt->array_size= *(unsigned int *)value;
    break;
  case STMT_ATTR_ROW_SIZE:
    stmt->row_size= *(size_t *)value;
    break;
  default:
    SET_CLIENT_STMT_ERROR(stmt, CR_NOT_IMPLEMENTED, SQLSTATE_UNKNOWN, 0);
    return(1);
  }
  return(0);
}

my_bool STDCALL mysql_stmt_bind_param(MYSQL_STMT *stmt, MYSQL_BIND *bind)
{
  MYSQL *mysql= stmt->mysql;

  if (!mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  /* If number of parameters was specified via mysql_stmt_attr_set we need to realloc
     them, e.g. for mariadb_stmt_execute_direct()
     */
  if ((stmt->state < MYSQL_STMT_PREPARED || stmt->state >= MYSQL_STMT_EXECUTED) &&
       stmt->prebind_params > 0)
  {
    if (!stmt->params && stmt->prebind_params)
    {
      if (!(stmt->params= (MYSQL_BIND *)ma_alloc_root(&stmt->mem_root, stmt->prebind_params * sizeof(MYSQL_BIND))))
      {
        SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
        return(1);
      }
      memset(stmt->params, '\0', stmt->prebind_params * sizeof(MYSQL_BIND));
    }
    stmt->param_count= stmt->prebind_params;
  }
  else if (stmt->state < MYSQL_STMT_PREPARED) {
    SET_CLIENT_STMT_ERROR(stmt, CR_NO_PREPARE_STMT, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->param_count && bind)
  {
    uint i;

    memcpy(stmt->params, bind, sizeof(MYSQL_BIND) * stmt->param_count);
    stmt->send_types_to_server= 1;

    for (i=0; i < stmt->param_count; i++)
    {
      if (stmt->mysql->methods->db_supported_buffer_type &&
          !stmt->mysql->methods->db_supported_buffer_type(stmt->params[i].buffer_type))
      {
        SET_CLIENT_STMT_ERROR(stmt, CR_UNSUPPORTED_PARAM_TYPE, SQLSTATE_UNKNOWN, 0);
        return(1);
      }
      if (!stmt->params[i].is_null)
        stmt->params[i].is_null= &is_not_null;

      if (stmt->params[i].long_data_used)
        stmt->params[i].long_data_used= 0;

      if (!stmt->params[i].length)
        stmt->params[i].length= &stmt->params[i].buffer_length;

      switch(stmt->params[i].buffer_type) {
      case MYSQL_TYPE_NULL:
        stmt->params[i].is_null= &is_null;
        break;
      case MYSQL_TYPE_TINY:
        stmt->params[i].buffer_length= 1;
        break;
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_YEAR:
        stmt->params[i].buffer_length= 2;
        break;
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_FLOAT:
        stmt->params[i].buffer_length= 4;
        break;
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_DOUBLE:
        stmt->params[i].buffer_length= 8;
        break;
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
        stmt->params[i].buffer_length= 12;
        break;
      case MYSQL_TYPE_TIME:
        stmt->params[i].buffer_length= 13;
        break;
      case MYSQL_TYPE_DATE:
        stmt->params[i].buffer_length= 5;
        break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_JSON:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        break;
      default:
        SET_CLIENT_STMT_ERROR(stmt, CR_UNSUPPORTED_PARAM_TYPE, SQLSTATE_UNKNOWN, 0);
        return(1);
        break;
      }
    }
  }
  stmt->bind_param_done= stmt->send_types_to_server= 1;

  CLEAR_CLIENT_STMT_ERROR(stmt);
  return(0);
}

my_bool STDCALL mysql_stmt_bind_result(MYSQL_STMT *stmt, MYSQL_BIND *bind)
{
  uint i;

  if (stmt->state < MYSQL_STMT_PREPARED)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_NO_PREPARE_STMT, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (!stmt->field_count)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_NO_STMT_METADATA, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (!bind)
    return(1);

  /* In case of a stored procedure we don't allocate memory for bind
     in mysql_stmt_prepare
     */

  if (stmt->field_count && !stmt->bind)
  {
    MA_MEM_ROOT *fields_ma_alloc_root=
                &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;
    if (!(stmt->bind= (MYSQL_BIND *)ma_alloc_root(fields_ma_alloc_root, stmt->field_count * sizeof(MYSQL_BIND))))
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      return(1);
    }
  }

  memcpy(stmt->bind, bind, sizeof(MYSQL_BIND) * stmt->field_count);

  for (i=0; i < stmt->field_count; i++)
  {
    if (stmt->mysql->methods->db_supported_buffer_type &&
        !stmt->mysql->methods->db_supported_buffer_type(bind[i].buffer_type))
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_UNSUPPORTED_PARAM_TYPE, SQLSTATE_UNKNOWN, 0);
      return(1);
    }

    if (!stmt->bind[i].is_null)
      stmt->bind[i].is_null= &stmt->bind[i].is_null_value;
    if (!stmt->bind[i].length)
      stmt->bind[i].length= &stmt->bind[i].length_value;
    if (!stmt->bind[i].error)
      stmt->bind[i].error= &stmt->bind[i].error_value;

    /* set length values for numeric types */
    switch(bind[i].buffer_type) {
    case MYSQL_TYPE_NULL:
      *stmt->bind[i].length= stmt->bind[i].length_value= 0;
      break;
    case MYSQL_TYPE_TINY:
      *stmt->bind[i].length= stmt->bind[i].length_value= 1;
      break;
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_YEAR:
      *stmt->bind[i].length= stmt->bind[i].length_value= 2;
      break;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_FLOAT:
      *stmt->bind[i].length= stmt->bind[i].length_value= 4;
      break;
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_DOUBLE:
      *stmt->bind[i].length= stmt->bind[i].length_value= 8;
      break;
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
      *stmt->bind[i].length= stmt->bind[i].length_value= sizeof(MYSQL_TIME);
      break;
    default:
      break;
    }
  }
  stmt->bind_result_done= 1;
  CLEAR_CLIENT_STMT_ERROR(stmt);

  return(0);
}

static my_bool net_stmt_close(MYSQL_STMT *stmt, my_bool remove)
{
  char stmt_id[STMT_ID_LENGTH];
  MA_MEM_ROOT *fields_ma_alloc_root= &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;

  /* clear memory */
  ma_free_root(&stmt->result.alloc, MYF(0)); /* allocated in mysql_stmt_store_result */
  ma_free_root(&stmt->mem_root,MYF(0));
  ma_free_root(fields_ma_alloc_root, MYF(0));

  if (stmt->mysql)
  {
    CLEAR_CLIENT_ERROR(stmt->mysql);

    /* remove from stmt list */
    if (remove)
      stmt->mysql->stmts= list_delete(stmt->mysql->stmts, &stmt->list);

    /* check if all data are fetched */
    if (stmt->mysql->status != MYSQL_STATUS_READY)
    {
      do {
        stmt->mysql->methods->db_stmt_flush_unbuffered(stmt);
      } while(mysql_stmt_more_results(stmt));
      stmt->mysql->status= MYSQL_STATUS_READY;
    }
    if (stmt->state > MYSQL_STMT_INITTED)
    {
      int4store(stmt_id, stmt->stmt_id);
      if (stmt->mysql->methods->db_command(stmt->mysql,COM_STMT_CLOSE, stmt_id,
                                           sizeof(stmt_id), 1, stmt))
      {
        SET_CLIENT_STMT_ERROR(stmt, stmt->mysql->net.last_errno, stmt->mysql->net.sqlstate, stmt->mysql->net.last_error);
        return 1;
      }
    }
  }
  return 0;
}

my_bool STDCALL mysql_stmt_close(MYSQL_STMT *stmt)
{
  my_bool rc;
  if (stmt && stmt->mysql && stmt->mysql->net.pvio)
    mysql_stmt_internal_reset(stmt, 1);

  rc= net_stmt_close(stmt, 1);

  free(stmt->extension);
  free(stmt);

  return(rc);
}

void STDCALL mysql_stmt_data_seek(MYSQL_STMT *stmt, unsigned long long offset)
{
  unsigned long long i= offset;
  MYSQL_ROWS *ptr= stmt->result.data;

  while(i-- && ptr)
    ptr= ptr->next;

  stmt->result_cursor= ptr;
  stmt->state= MYSQL_STMT_USER_FETCHING;

  return;
}

unsigned int STDCALL mysql_stmt_errno(MYSQL_STMT *stmt)
{
  return stmt->last_errno;
}

const char * STDCALL mysql_stmt_error(MYSQL_STMT *stmt)
{
  return (const char *)stmt->last_error;
}

int mthd_stmt_fetch_row(MYSQL_STMT *stmt, unsigned char **row)
{
  return stmt->fetch_row_func(stmt, row);
}

int STDCALL mysql_stmt_fetch(MYSQL_STMT *stmt)
{
  unsigned char *row;
  int rc;

  if (stmt->state <= MYSQL_STMT_EXECUTED)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->state < MYSQL_STMT_WAITING_USE_OR_STORE || !stmt->field_count)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  } else if (stmt->state== MYSQL_STMT_WAITING_USE_OR_STORE)
  {
    stmt->default_rset_handler(stmt);
  }

  if (stmt->state == MYSQL_STMT_FETCH_DONE)
    return(MYSQL_NO_DATA);

  if ((rc= stmt->mysql->methods->db_stmt_fetch(stmt, &row)))
  {
    stmt->state= MYSQL_STMT_FETCH_DONE;
    stmt->mysql->status= MYSQL_STATUS_READY;
    /* to fetch data again, stmt must be executed again */
    return(rc);
  }

  if ((rc= stmt->mysql->methods->db_stmt_fetch_to_bind(stmt, row)))
  {
    return(rc);
  }

  stmt->state= MYSQL_STMT_USER_FETCHING;
  CLEAR_CLIENT_ERROR(stmt->mysql);
  CLEAR_CLIENT_STMT_ERROR(stmt);
  return(0);
}

int STDCALL mysql_stmt_fetch_column(MYSQL_STMT *stmt, MYSQL_BIND *bind, unsigned int column, unsigned long offset)
{
  if (stmt->state < MYSQL_STMT_USER_FETCHING || column >= stmt->field_count ||
      stmt->state == MYSQL_STMT_FETCH_DONE)  {
    SET_CLIENT_STMT_ERROR(stmt, CR_NO_DATA, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (!stmt->bind[column].u.row_ptr)
  {
    /* we set row_ptr only for columns which contain data, so this must be a NULL column */
    if (bind[0].is_null)
      *bind[0].is_null= 1;
  }
  else
  {
    unsigned char *save_ptr;
    if (bind[0].length)
      *bind[0].length= *stmt->bind[column].length;
    else
      bind[0].length= &stmt->bind[column].length_value;
    if (bind[0].is_null)
      *bind[0].is_null= 0;
    else
      bind[0].is_null= &bind[0].is_null_value;
    if (!bind[0].error)
      bind[0].error= &bind[0].error_value;
    *bind[0].error= 0;
    bind[0].offset= offset;
    save_ptr= stmt->bind[column].u.row_ptr;
    mysql_ps_fetch_functions[stmt->fields[column].type].func(&bind[0], &stmt->fields[column], &stmt->bind[column].u.row_ptr);
    stmt->bind[column].u.row_ptr= save_ptr;
  }
  return(0);
}

unsigned int STDCALL mysql_stmt_field_count(MYSQL_STMT *stmt)
{
  return stmt->field_count;
}

my_bool STDCALL mysql_stmt_free_result(MYSQL_STMT *stmt)
{
  return madb_reset_stmt(stmt, MADB_RESET_LONGDATA | MADB_RESET_STORED |
                               MADB_RESET_BUFFER | MADB_RESET_ERROR);
}

MYSQL_STMT * STDCALL mysql_stmt_init(MYSQL *mysql)
{

  MYSQL_STMT *stmt= NULL;

  if (!(stmt= (MYSQL_STMT *)calloc(1, sizeof(MYSQL_STMT))) ||
      !(stmt->extension= (MADB_STMT_EXTENSION *)calloc(1, sizeof(MADB_STMT_EXTENSION))))
  {
    free(stmt);
    SET_CLIENT_ERROR(mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
    return(NULL);
  }


  /* fill mysql's stmt list */
  stmt->list.data= stmt;
  stmt->mysql= mysql;
  stmt->stmt_id= 0;
  mysql->stmts= list_add(mysql->stmts, &stmt->list);


  /* clear flags */
  strcpy(stmt->sqlstate, "00000");

  stmt->state= MYSQL_STMT_INITTED;

  /* set default */
  stmt->prefetch_rows= 1;

  ma_init_alloc_root(&stmt->mem_root, 2048, 2048);
  ma_init_alloc_root(&stmt->result.alloc, 4096, 4096);
  ma_init_alloc_root(&((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root, 2048, 2048);

  return(stmt);
}

my_bool mthd_stmt_read_prepare_response(MYSQL_STMT *stmt)
{
  ulong packet_length;
  uchar *p;

  if ((packet_length= ma_net_safe_read(stmt->mysql)) == packet_error)
    return(1);

  p= (uchar *)stmt->mysql->net.read_pos;

  if (0xFF == p[0])  /* Error occured */
  {
    return(1);
  }

  p++;
  stmt->stmt_id= uint4korr(p);
  p+= 4;
  stmt->field_count= uint2korr(p);
  p+= 2;
  stmt->param_count= uint2korr(p);
  p+= 2;

  /* filler */
  p++;
  /* for backward compatibility we also update mysql->warning_count */
  stmt->mysql->warning_count= stmt->upsert_status.warning_count= uint2korr(p);
  return(0);
}

my_bool mthd_stmt_get_param_metadata(MYSQL_STMT *stmt)
{
  MYSQL_DATA *result;

  if (!(result= stmt->mysql->methods->db_read_rows(stmt->mysql, (MYSQL_FIELD *)0, 7)))
    return(1);

  free_rows(result);
  return(0);
}

my_bool mthd_stmt_get_result_metadata(MYSQL_STMT *stmt)
{
  MYSQL_DATA *result;
  MA_MEM_ROOT *fields_ma_alloc_root= &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;

  if (!(result= stmt->mysql->methods->db_read_rows(stmt->mysql, (MYSQL_FIELD *)0, 7)))
    return(1);
  if (!(stmt->fields= unpack_fields(result,fields_ma_alloc_root,
          stmt->field_count, 0,
          stmt->mysql->server_capabilities & CLIENT_LONG_FLAG)))
    return(1);
  return(0);
}

int STDCALL mysql_stmt_warning_count(MYSQL_STMT *stmt)
{
  return stmt->upsert_status.warning_count;
}

int STDCALL mysql_stmt_prepare(MYSQL_STMT *stmt, const char *query, unsigned long length)
{
  MYSQL *mysql= stmt->mysql;
  int rc= 1;
  my_bool is_multi= 0;

  if (!stmt->mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (length == (unsigned long) -1)
    length= (unsigned long)strlen(query);

  /* clear flags */
  CLEAR_CLIENT_STMT_ERROR(stmt);
  CLEAR_CLIENT_ERROR(stmt->mysql);
  stmt->upsert_status.affected_rows= mysql->affected_rows= (unsigned long long) ~0;

  /* check if we have to clear results */
  if (stmt->state > MYSQL_STMT_INITTED)
  {
    char stmt_id[STMT_ID_LENGTH];
    is_multi= (mysql->net.extension->multi_status > COM_MULTI_OFF);
    /* We need to semi-close the prepared statement:
       reset stmt and free all buffers and close the statement
       on server side. Statment handle will get a new stmt_id */

    if (!is_multi)
      ma_multi_command(mysql, COM_MULTI_ENABLED);

    if (mysql_stmt_internal_reset(stmt, 1))
      goto fail;

    ma_free_root(&stmt->mem_root, MYF(MY_KEEP_PREALLOC));
    ma_free_root(&((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root, MYF(0));

    stmt->param_count= 0;
    stmt->field_count= 0;
    stmt->params= 0;

    int4store(stmt_id, stmt->stmt_id);
    if (mysql->methods->db_command(mysql, COM_STMT_CLOSE, stmt_id,
                                         sizeof(stmt_id), 1, stmt))
      goto fail;
  }
  if (mysql->methods->db_command(mysql, COM_STMT_PREPARE, query, length, 1, stmt))
    goto fail;

  if (!is_multi && mysql->net.extension->multi_status == COM_MULTI_ENABLED)
    ma_multi_command(mysql, COM_MULTI_END);
  
  if (mysql->net.extension->multi_status > COM_MULTI_OFF)
    return 0;

  if (mysql->methods->db_read_prepare_response &&
      mysql->methods->db_read_prepare_response(stmt))
    goto fail;

  /* metadata not supported yet */

  if (stmt->param_count &&
      stmt->mysql->methods->db_stmt_get_param_metadata(stmt))
  {
    goto fail;
  }

  /* allocated bind buffer for parameters */
  if (stmt->field_count &&
      stmt->mysql->methods->db_stmt_get_result_metadata(stmt))
  {
    goto fail;
  }
  if (stmt->param_count)
  {
    if (stmt->prebind_params)
    {
      if (stmt->prebind_params != stmt->param_count)
      {
        SET_CLIENT_STMT_ERROR(stmt, CR_INVALID_PARAMETER_NO, SQLSTATE_UNKNOWN, 0);
        goto fail;
      }
    } else {
      if (!(stmt->params= (MYSQL_BIND *)ma_alloc_root(&stmt->mem_root, stmt->param_count * sizeof(MYSQL_BIND))))
      {
        SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
        goto fail;
      }
      memset(stmt->params, '\0', stmt->param_count * sizeof(MYSQL_BIND));
    }
  }
  /* allocated bind buffer for result */
  if (stmt->field_count)
  {
    MA_MEM_ROOT *fields_ma_alloc_root= &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;
    if (!(stmt->bind= (MYSQL_BIND *)ma_alloc_root(fields_ma_alloc_root, stmt->field_count * sizeof(MYSQL_BIND))))
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      goto fail;
    }
  }
  stmt->state = MYSQL_STMT_PREPARED;
  return(0);

fail:
  stmt->state= MYSQL_STMT_INITTED;
  SET_CLIENT_STMT_ERROR(stmt, mysql->net.last_errno, mysql->net.sqlstate,
      mysql->net.last_error);
  return(rc);
}

int STDCALL mysql_stmt_store_result(MYSQL_STMT *stmt)
{
  unsigned int last_server_status;

  if (!stmt->mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (!stmt->field_count)
    return(0);

  /* test_pure_coverage requires checking of error_no */
  if (stmt->last_errno)
    return(1);

  if (stmt->state < MYSQL_STMT_EXECUTED)
  {
    SET_CLIENT_ERROR(stmt->mysql, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  last_server_status= stmt->mysql->server_status;

  /* if stmt is a cursor, we need to tell server to send all rows */
  if (stmt->cursor_exists && stmt->mysql->status == MYSQL_STATUS_READY)
  {
    char buff[STMT_ID_LENGTH + 4];
    int4store(buff, stmt->stmt_id);
    int4store(buff + STMT_ID_LENGTH, (int)~0);

    if (stmt->mysql->methods->db_command(stmt->mysql, COM_STMT_FETCH,
                                         buff, sizeof(buff), 1, stmt))
      return(1);
    /* todo: cursor */
  }
  else if (stmt->mysql->status != MYSQL_STATUS_STMT_RESULT)
  {
    SET_CLIENT_ERROR(stmt->mysql, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->mysql->methods->db_stmt_read_all_rows(stmt))
  {
    /* error during read - reset stmt->data */
    ma_free_root(&stmt->result.alloc, 0);
    stmt->result.data= NULL;
    stmt->result.rows= 0;
    stmt->mysql->status= MYSQL_STATUS_READY;
    return(1);
  }

  /* workaround for MDEV 6304:
     more results not set if the resultset has
     SERVER_PS_OUT_PARAMS set
   */
  if (last_server_status & SERVER_PS_OUT_PARAMS &&
      !(stmt->mysql->server_status & SERVER_MORE_RESULTS_EXIST))
    stmt->mysql->server_status|= SERVER_MORE_RESULTS_EXIST;

  stmt->result_cursor= stmt->result.data;
  stmt->fetch_row_func= stmt_buffered_fetch;
  stmt->mysql->status= MYSQL_STATUS_READY;

  if (!stmt->result.rows)
    stmt->state= MYSQL_STMT_FETCH_DONE;
  else
    stmt->state= MYSQL_STMT_USE_OR_STORE_CALLED;

  /* set affected rows: see bug 2247 */
  stmt->upsert_status.affected_rows= stmt->result.rows;
  stmt->mysql->affected_rows= stmt->result.rows;

  return(0);
}

static int madb_alloc_stmt_fields(MYSQL_STMT *stmt)
{
  uint i;
  MA_MEM_ROOT *fields_ma_alloc_root= &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;

  if (stmt->mysql->field_count)
  {
    ma_free_root(fields_ma_alloc_root, MYF(0));
    if (!(stmt->fields= (MYSQL_FIELD *)ma_alloc_root(fields_ma_alloc_root,
            sizeof(MYSQL_FIELD) * stmt->mysql->field_count)))
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      return(1);
    }
    stmt->field_count= stmt->mysql->field_count;

    for (i=0; i < stmt->field_count; i++)
    {
      if (stmt->mysql->fields[i].db)
        stmt->fields[i].db= ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].db);
      if (stmt->mysql->fields[i].table)
        stmt->fields[i].table= ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].table);
      if (stmt->mysql->fields[i].org_table)
        stmt->fields[i].org_table= ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].org_table);
      if (stmt->mysql->fields[i].name)
        stmt->fields[i].name= ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].name);
      if (stmt->mysql->fields[i].org_name)
        stmt->fields[i].org_name= ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].org_name);
      if (stmt->mysql->fields[i].catalog)
        stmt->fields[i].catalog= ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].catalog);
      stmt->fields[i].def= stmt->mysql->fields[i].def ? ma_strdup_root(fields_ma_alloc_root, stmt->mysql->fields[i].def) : NULL;
      stmt->fields[i].type= stmt->mysql->fields[i].type;
      stmt->fields[i].length= stmt->mysql->fields[i].length;
      stmt->fields[i].flags= stmt->mysql->fields[i].flags;
      stmt->fields[i].decimals= stmt->mysql->fields[i].decimals;
      stmt->fields[i].charsetnr= stmt->mysql->fields[i].charsetnr;
      stmt->fields[i].max_length= stmt->mysql->fields[i].max_length;
    }
    if (!(stmt->bind= (MYSQL_BIND *)ma_alloc_root(fields_ma_alloc_root, stmt->field_count * sizeof(MYSQL_BIND))))
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      return(1);
    }
    memset(stmt->bind, 0, stmt->field_count * sizeof(MYSQL_BIND));
    stmt->bind_result_done= 0;
  }
  return(0);
}

int stmt_read_execute_response(MYSQL_STMT *stmt)
{
  MYSQL *mysql= stmt->mysql;
  int ret;

  if (!mysql)
    return(1);

  ret= test((mysql->methods->db_read_stmt_result &&
                 mysql->methods->db_read_stmt_result(mysql)));
  /* if a reconnect occured, our connection handle is invalid */
  if (!stmt->mysql)
    return(1);

  /* update affected rows, also if an error occured */
  stmt->upsert_status.affected_rows= stmt->mysql->affected_rows;

  if (ret)
  {
    SET_CLIENT_STMT_ERROR(stmt, mysql->net.last_errno, mysql->net.sqlstate,
       mysql->net.last_error);
    stmt->state= MYSQL_STMT_PREPARED;
    return(1);
  }
  stmt->upsert_status.last_insert_id= mysql->insert_id;
  stmt->upsert_status.server_status= mysql->server_status;
  stmt->upsert_status.warning_count= mysql->warning_count;

  CLEAR_CLIENT_ERROR(mysql);
  CLEAR_CLIENT_STMT_ERROR(stmt);

  stmt->execute_count++;
  stmt->send_types_to_server= 0;

  stmt->state= MYSQL_STMT_EXECUTED;

  if (mysql->field_count)
  {
    if (!stmt->field_count ||
        mysql->server_status & SERVER_MORE_RESULTS_EXIST) /* fix for ps_bug: test_misc */
    {
      MA_MEM_ROOT *fields_ma_alloc_root=
                  &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;
      uint i;

      ma_free_root(fields_ma_alloc_root, MYF(0));
      if (!(stmt->bind= (MYSQL_BIND *)ma_alloc_root(fields_ma_alloc_root,
              sizeof(MYSQL_BIND) * mysql->field_count)) ||
          !(stmt->fields= (MYSQL_FIELD *)ma_alloc_root(fields_ma_alloc_root,
              sizeof(MYSQL_FIELD) * mysql->field_count)))
      {
        SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
        return(1);
      }
      stmt->field_count= mysql->field_count;

      for (i=0; i < stmt->field_count; i++)
      {
        if (mysql->fields[i].db)
          stmt->fields[i].db= ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].db);
        if (mysql->fields[i].table)
          stmt->fields[i].table= ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].table);
        if (mysql->fields[i].org_table)
          stmt->fields[i].org_table= ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].org_table);
        if (mysql->fields[i].name)
          stmt->fields[i].name= ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].name);
        if (mysql->fields[i].org_name)
          stmt->fields[i].org_name= ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].org_name);
        if (mysql->fields[i].catalog)
          stmt->fields[i].catalog= ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].catalog);
        stmt->fields[i].def= mysql->fields[i].def ? ma_strdup_root(fields_ma_alloc_root, mysql->fields[i].def) : NULL;
      }
    }

    if (stmt->upsert_status.server_status & SERVER_STATUS_CURSOR_EXISTS)
    {
      stmt->cursor_exists = TRUE;
      mysql->status = MYSQL_STATUS_READY;

      /* Only cursor read */
      stmt->default_rset_handler = _mysql_stmt_use_result;

    } else if (stmt->flags & CURSOR_TYPE_READ_ONLY)
    {
      /*
         We have asked for CURSOR but got no cursor, because the condition
         above is not fulfilled. Then...
         This is a single-row result set, a result set with no rows, EXPLAIN,
         SHOW VARIABLES, or some other command which either a) bypasses the
         cursors framework in the server and writes rows directly to the
         network or b) is more efficient if all (few) result set rows are
         precached on client and server's resources are freed.
         */

      /* preferred is buffered read */
      mysql_stmt_store_result(stmt);
      stmt->mysql->status= MYSQL_STATUS_STMT_RESULT;
    } else
    {
      /* preferred is unbuffered read */
      stmt->default_rset_handler = _mysql_stmt_use_result;
      stmt->mysql->status= MYSQL_STATUS_STMT_RESULT;
    }
    stmt->state= MYSQL_STMT_WAITING_USE_OR_STORE;
    /* in certain cases parameter types can change: For example see bug
       4026 (SELECT ?), so we need to update field information */
    if (mysql->field_count == stmt->field_count)
    {
      uint i;
      for (i=0; i < stmt->field_count; i++)
      {
        stmt->fields[i].type= mysql->fields[i].type;
        stmt->fields[i].length= mysql->fields[i].length;
        stmt->fields[i].flags= mysql->fields[i].flags;
        stmt->fields[i].decimals= mysql->fields[i].decimals;
        stmt->fields[i].charsetnr= mysql->fields[i].charsetnr;
        stmt->fields[i].max_length= mysql->fields[i].max_length;
      }
    } else
    {
      /* table was altered, see test_wl4166_2  */
      SET_CLIENT_STMT_ERROR(stmt, CR_NEW_STMT_METADATA, SQLSTATE_UNKNOWN, 0);
      return(1);
    }
  }
  return(0);
}

int STDCALL mysql_stmt_execute(MYSQL_STMT *stmt)
{
  MYSQL *mysql= stmt->mysql;
  char *request;
  int ret;
  size_t request_len= 0;

  if (!stmt->mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->state < MYSQL_STMT_PREPARED)
  {
    SET_CLIENT_ERROR(mysql, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->param_count && !stmt->bind_param_done)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_PARAMS_NOT_BOUND, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->state == MYSQL_STMT_WAITING_USE_OR_STORE)
  {
    stmt->default_rset_handler = _mysql_stmt_use_result;
    stmt->default_rset_handler(stmt);
  }
  if (stmt->state > MYSQL_STMT_WAITING_USE_OR_STORE && stmt->state < MYSQL_STMT_FETCH_DONE && !stmt->result.data)
  {
    if (!stmt->cursor_exists)
      do {
        stmt->mysql->methods->db_stmt_flush_unbuffered(stmt);
      } while(mysql_stmt_more_results(stmt));
    stmt->state= MYSQL_STMT_PREPARED;
    stmt->mysql->status= MYSQL_STATUS_READY;
  }

  /* clear data, in case mysql_stmt_store_result was called */
  if (stmt->result.data)
  {
    ma_free_root(&stmt->result.alloc, MYF(MY_KEEP_PREALLOC));
    stmt->result_cursor= stmt->result.data= 0;
    stmt->result.rows= 0;
  }
  if (stmt->array_size > 0)
    request= (char *)mysql_stmt_execute_generate_bulk_request(stmt, &request_len);
  else
    request= (char *)mysql_stmt_execute_generate_simple_request(stmt, &request_len);

  if (!request)
    return 1;

  ret= stmt->mysql->methods->db_command(mysql, 
                                        stmt->array_size > 0 ? COM_STMT_BULK_EXECUTE : COM_STMT_EXECUTE,
                                        request, request_len, 1, stmt);
  if (request)
    free(request);

  if (ret)
  {
    SET_CLIENT_STMT_ERROR(stmt, mysql->net.last_errno, mysql->net.sqlstate,
    mysql->net.last_error);
    return(1);
  }

  if (mysql->net.extension->multi_status > COM_MULTI_OFF)
    return(0);

  return(stmt_read_execute_response(stmt));
}

static my_bool madb_reset_stmt(MYSQL_STMT *stmt, unsigned int flags)
{
  MYSQL *mysql= stmt->mysql;
  my_bool ret= 0;

  if (!stmt->mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  /* clear error */
  if (flags & MADB_RESET_ERROR)
  {
    CLEAR_CLIENT_ERROR(stmt->mysql);
    CLEAR_CLIENT_STMT_ERROR(stmt);
  }

  if (stmt->stmt_id)
  {
    /* free buffered resultset, previously allocated
     * by mysql_stmt_store_result
     */
    if (flags & MADB_RESET_STORED &&
        stmt->result_cursor)
    {
      ma_free_root(&stmt->result.alloc, MYF(MY_KEEP_PREALLOC));
      stmt->result.data= NULL;
      stmt->result.rows= 0;
      stmt->result_cursor= NULL;
      stmt->mysql->status= MYSQL_STATUS_READY;
      stmt->state= MYSQL_STMT_FETCH_DONE;
    }

    /* if there is a pending result set, we will flush it */
    if (flags & MADB_RESET_BUFFER)
    {
      if (stmt->state == MYSQL_STMT_WAITING_USE_OR_STORE)
      {
        stmt->default_rset_handler(stmt);
        stmt->state = MYSQL_STMT_USER_FETCHING;
      }

      if (stmt->mysql->status!= MYSQL_STATUS_READY && stmt->field_count)
      {
        mysql->methods->db_stmt_flush_unbuffered(stmt);
        mysql->status= MYSQL_STATUS_READY;
      }
    }

    if (flags & MADB_RESET_SERVER)
    {
      /* reset statement on server side */
      if (stmt->mysql && stmt->mysql->status == MYSQL_STATUS_READY &&
          stmt->mysql->net.pvio)
      {
        unsigned char cmd_buf[STMT_ID_LENGTH];
        int4store(cmd_buf, stmt->stmt_id);
        if ((ret= stmt->mysql->methods->db_command(mysql,COM_STMT_RESET, (char *)cmd_buf,
                                                   sizeof(cmd_buf), 0, stmt)))
        {
          SET_CLIENT_STMT_ERROR(stmt, mysql->net.last_errno, mysql->net.sqlstate,
              mysql->net.last_error);
          return(ret);
        }
      }
    }

    if (flags & MADB_RESET_LONGDATA)
    {
      if (stmt->params)
      {
        ulonglong i;
        for (i=0; i < stmt->param_count; i++)
          if (stmt->params[i].long_data_used)
            stmt->params[i].long_data_used= 0;
      }
    }

  }
  return(ret);
}

static my_bool mysql_stmt_internal_reset(MYSQL_STMT *stmt, my_bool is_close)
{
  MYSQL *mysql= stmt->mysql;
  my_bool ret= 1;
  unsigned int flags= MADB_RESET_LONGDATA | MADB_RESET_BUFFER | MADB_RESET_ERROR;

  if (!mysql)
  {
    /* connection could be invalid, e.g. after mysql_stmt_close or failed reconnect
       attempt (see bug CONC-97) */
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->state >= MYSQL_STMT_USER_FETCHING &&
      stmt->fetch_row_func == stmt_unbuffered_fetch)
    flags|= MADB_RESET_BUFFER;

  ret= madb_reset_stmt(stmt, flags);

  if (stmt->stmt_id)
  {
    if ((stmt->state > MYSQL_STMT_EXECUTED &&
        stmt->mysql->status != MYSQL_STATUS_READY) ||
        stmt->mysql->server_status & SERVER_MORE_RESULTS_EXIST)
    {
      /* flush any pending (multiple) result sets */
      if (stmt->state == MYSQL_STMT_WAITING_USE_OR_STORE)
      {
        stmt->default_rset_handler(stmt);
        stmt->state = MYSQL_STMT_USER_FETCHING;
      }

      if (stmt->field_count)
      {
        while (mysql_stmt_next_result(stmt) == 0);
        stmt->mysql->status= MYSQL_STATUS_READY;
      }
    }
    if (!is_close)
      ret= madb_reset_stmt(stmt, MADB_RESET_SERVER);
  }
  stmt->state= MYSQL_STMT_PREPARED;
  stmt->upsert_status.affected_rows= mysql->affected_rows;
  stmt->upsert_status.last_insert_id= mysql->insert_id;
  stmt->upsert_status.server_status= mysql->server_status;
  stmt->upsert_status.warning_count= mysql->warning_count;
  mysql->status= MYSQL_STATUS_READY;

  return(ret);
}

MYSQL_RES * STDCALL mysql_stmt_result_metadata(MYSQL_STMT *stmt)
{
  MYSQL_RES *res;

  if (!stmt->field_count)
    return(NULL);

  /* aloocate result set structutr and copy stmt information */
  if (!(res= (MYSQL_RES *)calloc(1, sizeof(MYSQL_RES))))
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
    return(NULL);
  }

  res->eof= 1;
  res->fields= stmt->fields;
  res->field_count= stmt->field_count;
  return(res);
}

my_bool STDCALL mysql_stmt_reset(MYSQL_STMT *stmt)
{
  return mysql_stmt_internal_reset(stmt, 0);
}

const char * STDCALL mysql_stmt_sqlstate(MYSQL_STMT *stmt)
{
  return stmt->sqlstate;
}

MYSQL_ROW_OFFSET STDCALL mysql_stmt_row_tell(MYSQL_STMT *stmt)
{
  return(stmt->result_cursor);
}

unsigned long STDCALL mysql_stmt_param_count(MYSQL_STMT *stmt)
{
  return stmt->param_count;
}

MYSQL_ROW_OFFSET STDCALL mysql_stmt_row_seek(MYSQL_STMT *stmt, MYSQL_ROW_OFFSET new_row)
{
  MYSQL_ROW_OFFSET old_row; /* for returning old position */

  old_row= stmt->result_cursor;
  stmt->result_cursor= new_row;

  return(old_row);
}

my_bool STDCALL mysql_stmt_send_long_data(MYSQL_STMT *stmt, uint param_number,
    const char *data, size_t length)
{
  CLEAR_CLIENT_ERROR(stmt->mysql);
  CLEAR_CLIENT_STMT_ERROR(stmt);

  if (stmt->state < MYSQL_STMT_PREPARED || !stmt->params)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_NO_PREPARE_STMT, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (param_number >= stmt->param_count)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_INVALID_PARAMETER_NO, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (length || !stmt->params[param_number].long_data_used)
  {
    int ret;
    size_t packet_len= STMT_ID_LENGTH + 2 + length;
    uchar *cmd_buff= (uchar *)calloc(1, packet_len);
    int4store(cmd_buff, stmt->stmt_id);
    int2store(cmd_buff + STMT_ID_LENGTH, param_number);
    memcpy(cmd_buff + STMT_ID_LENGTH + 2, data, length);
    stmt->params[param_number].long_data_used= 1;
    ret= stmt->mysql->methods->db_command(stmt->mysql, COM_STMT_SEND_LONG_DATA,
                                         (char *)cmd_buff, packet_len, 1, stmt);
    free(cmd_buff);
    return(ret);
  }
  return(0);
}

unsigned long long STDCALL mysql_stmt_insert_id(MYSQL_STMT *stmt)
{
  return stmt->upsert_status.last_insert_id;
}

unsigned long long STDCALL mysql_stmt_num_rows(MYSQL_STMT *stmt)
{
  return stmt->result.rows;
}

MYSQL_RES* STDCALL mysql_stmt_param_metadata(MYSQL_STMT *stmt __attribute__((unused)))
{
  /* server doesn't deliver any information yet,
     so we just return NULL
     */
  return(NULL);
}

my_bool STDCALL mysql_stmt_more_results(MYSQL_STMT *stmt)
{
  /* MDEV 4604: Server doesn't set MORE_RESULT flag for
                OutParam result set, so we need to check
                for SERVER_MORE_RESULTS_EXIST and for
                SERVER_PS_OUT_PARAMS)
  */
  return (stmt &&
          stmt->mysql &&
          ((stmt->mysql->server_status & SERVER_MORE_RESULTS_EXIST) ||
           (stmt->mysql->server_status & SERVER_PS_OUT_PARAMS)));
}

int STDCALL mysql_stmt_next_result(MYSQL_STMT *stmt)
{
  int rc= 0;

  if (!stmt->mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (stmt->state < MYSQL_STMT_EXECUTED)
  {
    SET_CLIENT_ERROR(stmt->mysql, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (!mysql_stmt_more_results(stmt))
    return(-1);

  if (stmt->state > MYSQL_STMT_EXECUTED &&
      stmt->state < MYSQL_STMT_FETCH_DONE)
    madb_reset_stmt(stmt, MADB_RESET_ERROR | MADB_RESET_BUFFER | MADB_RESET_LONGDATA);
  stmt->state= MYSQL_STMT_WAITING_USE_OR_STORE;

  if (mysql_next_result(stmt->mysql))
  {
    stmt->state= MYSQL_STMT_FETCH_DONE;
    SET_CLIENT_STMT_ERROR(stmt, stmt->mysql->net.last_errno, stmt->mysql->net.sqlstate,
        stmt->mysql->net.last_error);
    return(1);
  }

  if (stmt->mysql->status == MYSQL_STATUS_GET_RESULT)
    stmt->mysql->status= MYSQL_STATUS_STMT_RESULT;

  if (stmt->mysql->field_count)
    rc= madb_alloc_stmt_fields(stmt);
  else
  {
    stmt->upsert_status.affected_rows= stmt->mysql->affected_rows;
    stmt->upsert_status.last_insert_id= stmt->mysql->insert_id;
    stmt->upsert_status.server_status= stmt->mysql->server_status;
    stmt->upsert_status.warning_count= stmt->mysql->warning_count;
  }

  stmt->field_count= stmt->mysql->field_count;

  return(rc);
}

int STDCALL mariadb_stmt_execute_direct(MYSQL_STMT *stmt,
                                      const char *stmt_str,
                                      size_t length)
{
  MYSQL *mysql= stmt->mysql;
  my_bool emulate_cmd= !(!(stmt->mysql->server_capabilities & CLIENT_MYSQL) &&
      (stmt->mysql->extension->mariadb_server_capabilities &
      (MARIADB_CLIENT_STMT_BULK_OPERATIONS >> 32)));

  if (!mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    goto fail;
  }

  /* Server versions < 10.2 don't support execute_direct, so we need to 
     emulate it */
  if (emulate_cmd)
  {
    int rc;

    /* avoid sending close + prepare in 2 packets */

    if ((rc= mysql_stmt_prepare(stmt, stmt_str, length)))
      return rc;
    return mysql_stmt_execute(stmt);
  }

  if (ma_multi_command(mysql, COM_MULTI_ENABLED))
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_COMMANDS_OUT_OF_SYNC, SQLSTATE_UNKNOWN, 0);
    goto fail;
  }

  if (!stmt->mysql)
  {
    SET_CLIENT_STMT_ERROR(stmt, CR_SERVER_LOST, SQLSTATE_UNKNOWN, 0);
    return(1);
  }

  if (length == (size_t) -1)
    length= strlen(stmt_str);

  /* clear flags */
  CLEAR_CLIENT_STMT_ERROR(stmt);
  CLEAR_CLIENT_ERROR(stmt->mysql);
  stmt->upsert_status.affected_rows= mysql->affected_rows= (unsigned long long) ~0;

  /* check if we have to clear results */
  if (stmt->state > MYSQL_STMT_INITTED)
  {
    /* We need to semi-close the prepared statement:
       reset stmt and free all buffers and close the statement
       on server side. Statment handle will get a new stmt_id */
    char stmt_id[STMT_ID_LENGTH];

    if (mysql_stmt_internal_reset(stmt, 1))
      goto fail;

    ma_free_root(&stmt->mem_root, MYF(MY_KEEP_PREALLOC));
    ma_free_root(&((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root, MYF(0));
    stmt->field_count= 0;
    stmt->param_count= 0;
    stmt->params= 0;

    int4store(stmt_id, stmt->stmt_id);
    if (mysql->methods->db_command(mysql, COM_STMT_CLOSE, stmt_id,
                                         sizeof(stmt_id), 1, stmt))
      goto fail;
  }
  stmt->stmt_id= -1;
  if (mysql->methods->db_command(mysql, COM_STMT_PREPARE, stmt_str, length, 1, stmt))
    goto fail;

  stmt->state= MYSQL_STMT_PREPARED;
  /* Since we can't determine stmt_id here, we need to set it to -1, so server will know that the
   * execute command belongs to previous prepare */
  stmt->stmt_id= -1;
  if (mysql_stmt_execute(stmt))
    goto fail;

  /* flush multi buffer */
  if (ma_multi_command(mysql, COM_MULTI_END))
    goto fail;

  /* read prepare response */
  if (mysql->methods->db_read_prepare_response &&
    mysql->methods->db_read_prepare_response(stmt))
  goto fail;

  /* metadata not supported yet */

  if (stmt->param_count &&
      stmt->mysql->methods->db_stmt_get_param_metadata(stmt))
  {
    goto fail;
  }

  /* allocated bind buffer for parameters */
  if (stmt->field_count &&
      stmt->mysql->methods->db_stmt_get_result_metadata(stmt))
  {
    goto fail;
  }

  /* allocated bind buffer for result */
  if (stmt->field_count)
  {
    MA_MEM_ROOT *fields_ma_alloc_root= &((MADB_STMT_EXTENSION *)stmt->extension)->fields_ma_alloc_root;
    if (!(stmt->bind= (MYSQL_BIND *)ma_alloc_root(fields_ma_alloc_root, stmt->field_count * sizeof(MYSQL_BIND))))
    {
      SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      goto fail;
    }
  }
  stmt->state = MYSQL_STMT_PREPARED;

  /* read execute response packet */
  return stmt_read_execute_response(stmt);
fail:
  SET_CLIENT_STMT_ERROR(stmt, mysql->net.last_errno, mysql->net.sqlstate,
      mysql->net.last_error);
  do {
    stmt->mysql->methods->db_stmt_flush_unbuffered(stmt);
  } while(mysql_stmt_more_results(stmt));
  stmt->state= MYSQL_STMT_INITTED;
  return 1;
}
