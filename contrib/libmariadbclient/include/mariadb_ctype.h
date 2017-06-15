/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
   
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

/*
  A better inplementation of the UNIX ctype(3) library.
  Notes:   my_global.h should be included before ctype.h
*/

#ifndef _mariadb_ctype_h
#define _mariadb_ctype_h

#include <ctype.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define CHARSET_DIR	"charsets/"
#define MY_CS_NAME_SIZE 32

#define MADB_DEFAULT_CHARSET_NAME "latin1"
#define MADB_DEFAULT_COLLATION_NAME "latin1_swedish_ci"
#define MADB_AUTODETECT_CHARSET_NAME "auto"

/* we use the mysqlnd implementation */
typedef struct ma_charset_info_st
{
  unsigned int	nr; /* so far only 1 byte for charset */
  unsigned int  state;
  const char	*csname;
  const char	*name;
  const char  *dir;
  unsigned int codepage;
  const char  *encoding;
  unsigned int	char_minlen;
  unsigned int	char_maxlen;
  unsigned int 	(*mb_charlen)(unsigned int c);
  unsigned int 	(*mb_valid)(const char *start, const char *end);
} MARIADB_CHARSET_INFO;

extern const MARIADB_CHARSET_INFO  mariadb_compiled_charsets[];
extern MARIADB_CHARSET_INFO *ma_default_charset_info;
extern MARIADB_CHARSET_INFO *ma_charset_bin;
extern MARIADB_CHARSET_INFO *ma_charset_latin1;
extern MARIADB_CHARSET_INFO *ma_charset_utf8_general_ci;
extern MARIADB_CHARSET_INFO *ma_charset_utf16le_general_ci;

MARIADB_CHARSET_INFO *find_compiled_charset(unsigned int cs_number);
MARIADB_CHARSET_INFO *find_compiled_charset_by_name(const char *name);

size_t mysql_cset_escape_quotes(const MARIADB_CHARSET_INFO *cset, char *newstr,  const char *escapestr, size_t escapestr_len);
size_t mysql_cset_escape_slashes(const MARIADB_CHARSET_INFO *cset, char *newstr, const char *escapestr, size_t escapestr_len);
const char* madb_get_os_character_set(void);
#ifdef _WIN32
int madb_get_windows_cp(const char *charset);
#endif

#ifdef	__cplusplus
}
#endif

#endif
