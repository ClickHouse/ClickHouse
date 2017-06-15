/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
                 2016 MariaDB Corporation AB   
   
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
   MA 02111-1301, USA */

#include <ma_global.h>
#include <ma_sys.h>
// #include "mysys_err.h"
#include <mariadb_ctype.h>
#include <ma_string.h>

MARIADB_CHARSET_INFO *ma_default_charset_info = (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[5];
MARIADB_CHARSET_INFO *ma_charset_bin= (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[32];
MARIADB_CHARSET_INFO *ma_charset_latin1= (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[5];
MARIADB_CHARSET_INFO *ma_charset_utf8_general_ci= (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[21];
MARIADB_CHARSET_INFO *ma_charset_utf16le_general_ci= (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[68];

MARIADB_CHARSET_INFO * STDCALL mysql_get_charset_by_nr(uint cs_number)
{
  int i= 0;

  while (mariadb_compiled_charsets[i].nr && cs_number != mariadb_compiled_charsets[i].nr)
    i++;
  
  return (mariadb_compiled_charsets[i].nr) ? (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[i] : NULL;
}

my_bool set_default_charset(uint cs, myf flags __attribute__((unused)))
{
  MARIADB_CHARSET_INFO *new_charset;
  new_charset = mysql_get_charset_by_nr(cs);
  if (!new_charset)
  {
    return(TRUE);   /* error */
  }
  ma_default_charset_info = new_charset;
  return(FALSE);
}

MARIADB_CHARSET_INFO * STDCALL mysql_get_charset_by_name(const char *cs_name)
{
  int i= 0;

  while (mariadb_compiled_charsets[i].nr && strcmp(cs_name, mariadb_compiled_charsets[i].csname) != 0)
    i++;
 
  return (mariadb_compiled_charsets[i].nr) ? (MARIADB_CHARSET_INFO *)&mariadb_compiled_charsets[i] : NULL;
}

my_bool set_default_charset_by_name(const char *cs_name, myf flags __attribute__((unused)))
{
  MARIADB_CHARSET_INFO *new_charset;
  new_charset = mysql_get_charset_by_name(cs_name);
  if (!new_charset)
  {
    return(TRUE);   /* error */
  }

  ma_default_charset_info = new_charset;
  return(FALSE);
}
