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
#include "ma_string.h"
#include <ctype.h>

char NEAR _dig_vec[] =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

#define char_val(X) (X >= '0' && X <= '9' ? X-'0' :\
		     X >= 'A' && X <= 'Z' ? X-'A'+10 :\
		     X >= 'a' && X <= 'z' ? X-'a'+10 :\
		     '\177')

char *ma_ll2str(long long val,char *dst,int radix)
{
  char buffer[65];
  register char *p;
  long long_val;

  if (radix < 0)
  {
    if (radix < -36 || radix > -2) return (char*) 0;
    if (val < 0) {
      *dst++ = '-';
      val = 0ULL - val;
    }
    radix = -radix;
  }
  else
  {
    if (radix > 36 || radix < 2) return (char*) 0;
  }
  if (val == 0)
  {
    *dst++='0';
    *dst='\0';
    return dst;
  }
  p = &buffer[sizeof(buffer)-1];
  *p = '\0';

  while ((ulonglong) val > (ulonglong) LONG_MAX)
  {
    ulonglong quo=(ulonglong) val/(uint) radix;
    uint rem= (uint) (val- quo* (uint) radix);
    *--p = _dig_vec[rem];
    val= quo;
  }
  long_val= (long) val;
  while (long_val != 0)
  {
    long quo= long_val/radix;
    *--p = _dig_vec[(uchar) (long_val - quo*radix)];
    long_val= quo;
  }
  while ((*dst++ = *p++) != 0) ;
  return dst-1;
}
