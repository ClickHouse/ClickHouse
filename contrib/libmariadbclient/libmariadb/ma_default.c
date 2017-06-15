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
#include "ma_string.h"
#include <ctype.h>
#include "mariadb_ctype.h"
#include <mysql.h>
#include <ma_common.h>
#include <mariadb/ma_io.h>

#ifdef _WIN32
#include <io.h>
static const char *ini_exts[]= {"ini", "cnf", 0};
static const char *ini_dirs[]= {"C:", ".", 0};
static const char *ini_env_dirs[]= {"WINDOWS", "HOMEPATH", 0};
#define ENV_HOME_DIR "HOMEPATH"
#define R_OK 4
#else
#include <unistd.h>
static const char *ini_exts[]= {"cnf", 0};
static const char *ini_dirs[]= {"/etc", "/etc/mysql", ".", 0};
static const char *ini_env_dirs[]= {"HOME", "SYSCONFDIR", 0};
#define ENV_HOME_DIR "HOME"
#endif

extern my_bool _mariadb_set_conf_option(MYSQL *mysql, const char *config_option, const char *config_value);

char *_mariadb_get_default_file(char *filename, size_t length)
{
  int dirs; int exts;
  char *env;

  for (dirs= 0; ini_dirs[dirs]; dirs++)
  {
    for (exts= 0; ini_exts[exts]; exts++)
    {
      snprintf(filename, length,
               "%s%cmy.%s", ini_dirs[dirs], FN_LIBCHAR, ini_exts[exts]);
      if (!access(filename, R_OK))
        return filename;
    }
  }
  for (dirs= 0; ini_env_dirs[dirs]; dirs++)
  {
    for (exts= 0; ini_exts[exts]; exts++)
    {
      env= getenv(ini_env_dirs[dirs]);
      snprintf(filename, length,
               "%s%cmy.%s", env, FN_LIBCHAR, ini_exts[exts]);
      if (!access(filename, R_OK))
        return filename;
    }
  }

  /* check for .my file in home directoy */
  env= getenv(ENV_HOME_DIR);
  for (exts= 0; ini_exts[exts]; exts++)
  {
    snprintf(filename, length,
             "%s%c.my.%s", env, FN_LIBCHAR, ini_exts[exts]);
    if (!access(filename, R_OK))
      return filename;
  }
  return NULL;
}

my_bool _mariadb_read_options(MYSQL *mysql, const char *config_file,
    const char *group)
{
  char buff[4096],*ptr,*end,*value, *key= 0, *optval;
  MA_FILE *file= NULL;
  char *filename;
  uint line=0;
  my_bool rc= 1;
  my_bool read_values= 0, found_group= 0, is_escaped= 0, is_quoted= 0;
  my_bool (*set_option)(MYSQL *mysql, const char *config_option, const char *config_value);

  /* if a plugin registered a hook we will call this hook, otherwise
   * default (_mariadb_set_conf_option) will be called */
  if (mysql->options.extension && mysql->options.extension->set_option)
    set_option= mysql->options.extension->set_option;
  else
    set_option= _mariadb_set_conf_option;

  if (config_file)
    filename= strdup(config_file);
  else
  {
    filename= (char *)malloc(FN_REFLEN + 10);
    if (!_mariadb_get_default_file(filename, FN_REFLEN + 10))
    {
      goto err;
    }
  }

  if (!(file = ma_open(filename, "r", NULL)))
    goto err;

  while (ma_gets(buff,sizeof(buff)-1,file))
  {
    line++;
    key= 0;
    /* Ignore comment and empty lines */
    for (ptr=buff ; isspace(*ptr) ; ptr++ );
    if (!is_escaped && (*ptr == '\"' || *ptr== '\''))
    {
      is_quoted= !is_quoted;
      continue;
    }
    if (*ptr == '#' || *ptr == ';' || !*ptr)
      continue;
    is_escaped= (*ptr == '\\');
    if (*ptr == '[')				/* Group name */
    {
      found_group=1;
      if (!(end=(char *) strchr(++ptr,']')))
      {
        /* todo: set error */
        goto err;
      }
      for ( ; isspace(end[-1]) ; end--) ;	/* Remove end space */
      end[0]=0;
      read_values= test(strcmp(ptr, group) == 0);
      continue;
    }
    if (!found_group)
    {
      /* todo: set error */
      goto err;
    }
    if (!read_values)
      continue;
    if (!(end=value=strchr(ptr,'=')))
    {
      end=strchr(ptr, '\0');				/* Option without argument */
      set_option(mysql, ptr, NULL);
    }
    if (!key)
      key= ptr;
    for ( ; isspace(end[-1]) ; end--) ;

    if (!value)
    {
      if (!key)
        key= ptr;
    }
    else
    {
      /* Remove pre- and end space */
      char *value_end;
      *value= 0;
      value++;
      ptr= value;
      for ( ; isspace(*value); value++) ;
      optval= value;
      value_end=strchr(value, '\0');
      for ( ; isspace(value_end[-1]) ; value_end--) ;
      /* remove possible quotes */
      if (*value == '\'' || *value == '\"')
      {
        value++;
        if (value_end[-1] == '\'' || value_end[-1] == '\"')
          value_end--;
      }
      if (value_end < value)			/* Empty string */
        value_end=value;
      for ( ; value != value_end; value++)
      {
        if (*value == '\\' && value != value_end-1)
        {
          switch(*++value) {
            case 'n':
              *ptr++='\n';
              break;
            case 't':
              *ptr++= '\t';
              break;
            case 'r':
              *ptr++ = '\r';
              break;
            case 'b':
              *ptr++ = '\b';
              break;
            case 's':
              *ptr++= ' ';			/* space */
              break;
            case '\"':
              *ptr++= '\"';
              break;
            case '\'':
              *ptr++= '\'';
              break;
            case '\\':
              *ptr++= '\\';
              break;
            default:				/* Unknown; Keep '\' */
              *ptr++= '\\';
              *ptr++= *value;
              break;
          }
        }
        else
          *ptr++= *value;
      }
      *ptr=0;
      set_option(mysql, key, optval);
      key= optval= 0;
    }
  }
  rc= 0;

err:
  free(filename);
  if (file)
    ma_close(file);
  return rc;
}


