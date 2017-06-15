/* Copyright (C) 2015 MariaDB Corporation AB

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

#ifndef _ma_io_h_
#define _ma_io_h_


#ifdef HAVE_CURL
#include <curl/curl.h>
#endif

enum enum_file_type {
  MA_FILE_NONE=0,
  MA_FILE_LOCAL=1,
  MA_FILE_REMOTE=2
};

typedef struct 
{
  enum enum_file_type type;
  void *ptr;
} MA_FILE;

#ifdef HAVE_REMOTEIO
struct st_rio_methods {
  MA_FILE *(*mopen)(const char *url, const char *mode);
  int (*mclose)(MA_FILE *ptr);
  int (*mfeof)(MA_FILE *file);
  size_t (*mread)(void *ptr, size_t size, size_t nmemb, MA_FILE *file);
  char * (*mgets)(char *ptr, size_t size, MA_FILE *file);
};
#endif

/* function prototypes */
MA_FILE *ma_open(const char *location, const char *mode, MYSQL *mysql);
int ma_close(MA_FILE *file);
int ma_feof(MA_FILE *file);
size_t ma_read(void *ptr, size_t size, size_t nmemb, MA_FILE *file);
char *ma_gets(char *ptr, size_t size, MA_FILE *file);

#endif
