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
#include "mariadb_ctype.h"
#include <ma_string.h>
#include <mariadb_ctype.h>
#ifdef HAVE_GETRUSAGE
#include <sys/resource.h>
/* extern int     getrusage(int, struct rusage *); */
#endif
#include <signal.h>
#ifdef _WIN32
#ifdef _MSC_VER
#include <locale.h>
#include <crtdbg.h>
#endif
static my_bool my_win_init(void);
#else
#define my_win_init()
#endif

my_bool ma_init_done=0;



/* Init ma_sys functions and ma_sys variabels */

void ma_init(void)
{
  if (ma_init_done)
    return;
  ma_init_done=1;
  {
#ifdef _WIN32
    my_win_init();
#endif
    return;
  }
} /* ma_init */



void ma_end(int infoflag __attribute__((unused)))
{
#ifdef _WIN32
  WSACleanup( );
#endif /* _WIN32 */
  ma_init_done=0;
} /* ma_end */

#ifdef _WIN32

/*
  This code is specially for running MySQL, but it should work in
  other cases too.

  Inizializzazione delle variabili d'ambiente per Win a 32 bit.

  Vengono inserite nelle variabili d'ambiente (utilizzando cosi'
  le funzioni getenv e putenv) i valori presenti nelle chiavi
  del file di registro:

  HKEY_LOCAL_MACHINE\software\MySQL

  Se la kiave non esiste nonn inserisce nessun valore
*/

/* Crea la stringa d'ambiente */

void setEnvString(char *ret, const char *name, const char *value)
{
  sprintf(ret, "%s=%s", name, value);
  return ;
}

static my_bool my_win_init()
{
  WORD VersionRequested;
  int err;
  WSADATA WsaData;
  const unsigned int MajorVersion=2,
                     MinorVersion=2;
  VersionRequested= MAKEWORD(MajorVersion, MinorVersion);
  /* Load WinSock library */
  if ((err= WSAStartup(VersionRequested, &WsaData)))
  {
    return 0;
  }
  /* make sure 2.2 or higher is supported */
  if ((LOBYTE(WsaData.wVersion) * 10 + HIBYTE(WsaData.wVersion)) < 22)
  {
    WSACleanup();
    return 1;
  }
  return 0;
}
#endif

