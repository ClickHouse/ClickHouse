/*
   Copyright (C) 2015 MariaDB Corporation AB
   
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
   MA 02111-1301, USA 
*/

#include <ma_global.h>
#include <ma_sys.h>
#include <errmsg.h>
#include <mysql.h>
#include <mysql/client_plugin.h>
#include <mariadb/ma_io.h>
#include <stdio.h>
#include <string.h>

#ifdef HAVE_REMOTEIO
struct st_mysql_client_plugin_REMOTEIO *rio_plugin= NULL;
#endif

/* {{{ ma_open */
MA_FILE *ma_open(const char *location, const char *mode, MYSQL *mysql)
{
  int CodePage= -1;
  FILE *fp= NULL;
  MA_FILE *ma_file= NULL;

  if (!location || !location[0])
    return NULL;
#ifdef HAVE_REMOTEIO
  if (strstr(location, "://"))
    goto remote;
#endif

#ifdef _WIN32
  if (mysql && mysql->charset)
    CodePage= madb_get_windows_cp(mysql->charset->csname);
#endif
  if (CodePage == -1)
  {
    if (!(fp= fopen(location, mode)))
    {
      return NULL;
    }
  }
#ifdef _WIN32
  /* See CONC-44: we need to support non ascii filenames too, so we convert
     current character set to wchar_t and try to open the file via _wsopen */
  else
   {
    wchar_t *w_filename= NULL;
    wchar_t *w_mode= NULL;
    int len;
    DWORD Length;

    len= MultiByteToWideChar(CodePage, 0, location, (int)strlen(location), NULL, 0);
    if (!len)
      return NULL;
    if (!(w_filename= (wchar_t *)calloc(1, (len + 1) * sizeof(wchar_t))))
    {
      my_set_error(mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      return NULL;
    }
    Length= len;
    len= MultiByteToWideChar(CodePage, 0, location, (int)strlen(location), w_filename, (int)Length);
    if (!len)
    {
      /* todo: error handling */
      free(w_filename);
      return NULL;
    }
    len= (int)strlen(mode);
    if (!(w_mode= (wchar_t *)calloc(1, (len + 1) * sizeof(wchar_t))))
    {
      my_set_error(mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      free(w_filename);
      return NULL;
    }
    Length= len;
    len= MultiByteToWideChar(CodePage, 0, mode, (int)strlen(mode), w_mode, (int)Length);
    if (!len)
    {
      /* todo: error handling */
      free(w_filename);
      free(w_mode);
      return NULL;
    }
    fp= _wfopen(w_filename, w_mode);
    free(w_filename);
    free(w_mode);
  }

#endif
  if (fp)
  {
    ma_file= (MA_FILE *)malloc(sizeof(MA_FILE));
    if (!ma_file)
    {
      my_set_error(mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
      return NULL;
    }
    ma_file->type= MA_FILE_LOCAL;
    ma_file->ptr= (void *)fp;
  }
  return ma_file;
#ifdef HAVE_REMOTEIO
remote:
  /* check if plugin for remote io is available and try
   * to open location */
  {
    MYSQL mysql;
    if (rio_plugin ||(rio_plugin= (struct st_mysql_client_plugin_REMOTEIO *)
                      mysql_client_find_plugin(&mysql, NULL, MARIADB_CLIENT_REMOTEIO_PLUGIN)))
      return rio_plugin->methods->mopen(location, mode);
    return NULL;
  }
#endif
}
/* }}} */

/* {{{ ma_close */
int ma_close(MA_FILE *file)
{
  int rc;
  if (!file)
    return -1;

  switch (file->type) {
  case MA_FILE_LOCAL:
    rc= fclose((FILE *)file->ptr);
    free(file);
    break;
#ifdef HAVE_REMOTEIO
  case MA_FILE_REMOTE:
    rc= rio_plugin->methods->mclose(file);
    break;
#endif
  default:
    return -1;
  }
  return rc;
}
/* }}} */


/* {{{ ma_feof */
int ma_feof(MA_FILE *file)
{
  if (!file)
    return -1;

  switch (file->type) {
  case MA_FILE_LOCAL:
    return feof((FILE *)file->ptr);
    break;
#ifdef HAVE_REMOTEIO
  case MA_FILE_REMOTE:
    return rio_plugin->methods->mfeof(file);
    break;
#endif
  default:
    return -1;
  }
}
/* }}} */

/* {{{ ma_read */
size_t ma_read(void *ptr, size_t size, size_t nmemb, MA_FILE *file)
{
  size_t s= 0;
  if (!file)
    return -1;

  switch (file->type) {
  case MA_FILE_LOCAL:
    s= fread(ptr, size, nmemb, (FILE *)file->ptr);
    return s;
    break;
#ifdef HAVE_REMOTEIO
  case MA_FILE_REMOTE:
    return rio_plugin->methods->mread(ptr, size, nmemb, file);
    break;
#endif
  default:
    return -1;
  }
}
/* }}} */

/* {{{ ma_gets */
char *ma_gets(char *ptr, size_t size, MA_FILE *file)
{
  if (!file)
    return NULL;

  switch (file->type) {
  case MA_FILE_LOCAL:
    return fgets(ptr, (int)size, (FILE *)file->ptr);
    break;
#ifdef HAVE_REMOTEIO
  case MA_FILE_REMOTE:
    return rio_plugin->methods->mgets(ptr, size, file);
    break;
#endif
  default:
    return NULL;
  }
}
/* }}} */


