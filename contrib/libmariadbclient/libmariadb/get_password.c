/************************************************************************************
   Copyright (C) 2014 MariaDB Corporation AB
   
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
#include <ma_global.h>
#include <ma_sys.h>
#include "mysql.h"
#include <ma_string.h>
#include <mariadb_ctype.h>
#include <stdio.h>
#include <memory.h>

#ifndef _WIN32
#include <termios.h>
#else
#include <conio.h>
#endif /* _WIN32 */

/* {{{ static char *get_password() */
/*
   read password from device

   SYNOPSIS
     get_password
     Hdl/file          file handle
     buffer            input buffer
     length            buffer length

   RETURN
     buffer            zero terminated input buffer
*/   
#ifdef _WIN32
static char *get_password(HANDLE Hdl, char *buffer, DWORD length)
#else
static char *get_password(FILE *file, char *buffer, int length)
#endif
{
  char inChar;
  int  CharsProcessed= 1;
#ifdef _WIN32
  DWORD Offset= 0;
#else
  int  Offset= 0;
#endif
  memset(buffer, 0, length);

  do
  {
#ifdef _WIN32
    if (!ReadConsole(Hdl, &inChar, 1, &CharsProcessed, NULL) ||
        !CharsProcessed)
      break;
#else
    inChar= fgetc(file);
#endif

    switch(inChar) {
    case '\b': /* backslash */
      if (Offset)
      {
        /* cursor is always at the end */
        Offset--;
        buffer[Offset]= 0;
#ifdef _WIN32
        _cputs("\b \b");
#endif
      }
      break;
    case '\n':
    case '\r':
      break;
    default:
      buffer[Offset]= inChar;
      if (Offset < length - 2)
        Offset++;
#ifdef _WIN32
      _cputs("*");
#endif
      break;
    }  
  } while (CharsProcessed && inChar != '\n' && inChar != '\r');
  return buffer;
}
/* }}} */

/* {{{ static char* get_tty_password */
/*
   reads password from tty/console

   SYNOPSIS
     get_tty_password()
     buffer           input buffer
     length           length of input buffer

   DESCRIPTION
     reads a password from console (Windows) or tty without echoing
     it's characters. Input buffer must be allocated by calling function.

   RETURNS
     buffer           pointer to input buffer
*/
char* get_tty_password(char *prompt, char *buffer, int length)
{
#ifdef _WIN32
  DWORD  SaveState;
  HANDLE Hdl;
  int    Offset= 0;
  DWORD  CharsProcessed=  0;

  if (prompt)
    fprintf(stderr, "%s", prompt);

  if (!(Hdl= CreateFile("CONIN$", 
                        GENERIC_READ | GENERIC_WRITE,
                        FILE_SHARE_READ,
                        NULL,
                        OPEN_EXISTING, 0, NULL)))
  {
    /* todo: provide a graphical dialog */
    return buffer;
  }
  /* Save ConsoleMode and set ENABLE_PROCESSED_INPUT:
     CTRL+C is processed by the system and is not placed in the input buffer */
  GetConsoleMode(Hdl, &SaveState);
  SetConsoleMode(Hdl, ENABLE_PROCESSED_INPUT);

  buffer= get_password(Hdl, buffer, length);
  SetConsoleMode(Hdl, SaveState);
  CloseHandle(Hdl);
  return buffer;
#else
  struct termios term_old, 
                 term_new;
  FILE *readfrom;

  if (prompt && isatty(fileno(stderr)))
    fputs(prompt, stderr);

  if (!(readfrom= fopen("/dev/tty", "r")))
    readfrom= stdin;

  /* try to disable echo */
  tcgetattr(fileno(readfrom), &term_old);
  term_new= term_old;
  term_new.c_cc[VMIN] = 1;
  term_new.c_cc[VTIME]= 0;
  term_new.c_lflag&= ~(ECHO | ISIG | ICANON | ECHONL);
  tcsetattr(fileno(readfrom), TCSADRAIN, &term_new);

  buffer= get_password(readfrom, buffer, length);

  if (isatty(fileno(readfrom)))
    tcsetattr(fileno(readfrom), TCSADRAIN, &term_old);

  fclose(readfrom);

  return buffer;
#endif
}
/* }}} */
