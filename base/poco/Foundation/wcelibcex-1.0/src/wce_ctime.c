/*
 * $Id: wce_ctime.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines ctime() function.
 *
 * Created by Mateusz Loskot (mateusz@loskot.net)
 *
 * Copyright (c) 2006 Taxus SI Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom 
 * the Software is furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH
 * THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * MIT License:
 * http://opensource.org/licenses/mit-license.php
 *
 * Contact:
 * Taxus SI Ltd.
 * http://www.taxussi.com.pl
 *
 */

#include <wce_time.h>
#include <stdlib.h>

/*******************************************************************************
* wceex_ctime - Convert a time value to a date and time string
*
* Description:
*
*   The ctime function is similar to asctime, except that you specify
*   the calendar time argument as a time_t simple time value rather
*   than in broken-down local time format.
*   It shall be equivalent to: asctime(localtime(clock))
*
*   The ctime() is not required to be thread-safe.
*
*   Windows CE specific:
*       ctime does not set the variable tzname.
*
* Reference:
*
*   IEEE Std 1003.1-2001
*   The GNU C Library Manual
* 
*******************************************************************************/
char * wceex_ctime(const time_t *timer)
{
   return wceex_asctime(wceex_localtime(timer));
}
 

/*******************************************************************************
* wceex_ctime_r - Convert a time value to a date and time string
*
* Description:
*
*   This function is similar to ctime, but places the result in the string
*   pointed to by buffer.
*
*   The ctime() is not required to be thread-safe.
*
*   Windows CE specific:
*       ctime does not set the variable tzname.
*
* Return value:
*
*   Upon success the function returns a pointer to the string the result was written into.
*   Otherwise return NULL.
*
* Reference:
*
*   IEEE Std 1003.1-2001
*   The GNU C Library Manual
* 
*******************************************************************************/
char * wceex_ctime_r(const time_t *timer, char *buf)
{
  /*
  XXX - mloskot - it's waiting for localtime_r function.

  ({ struct tm tm; asctime_r (localtime_r (time, &tm), buf); })
  */
  return NULL;
}

