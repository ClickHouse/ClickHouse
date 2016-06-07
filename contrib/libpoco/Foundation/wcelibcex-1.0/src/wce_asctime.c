/*
 * $Id: wce_asctime.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines asctime() function.
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
* Internal definitions
*******************************************************************************/

/* Buffer to store string representation of tm struct. */
#define TIME_STRING_SIZE    26

/* Buffer to store string representation of tm struct. */
static char __wce_asc_result[TIME_STRING_SIZE];

/* Format of string returned. */
static const char __wce_asc_format[] = "%.3s %.3s %02d %.2d:%.2d:%.2d %d\n"; 


/*******************************************************************************
* wceex_asctime - Convert date and time to a string
*
* Description:
*
*   The asctime function converts the broken-down time value that
*   brokentime points to into a string in a standard format:
*       "Mon Jan 30 13:46:22 2006\n"
*
*   The abbreviations for the days of week are:
*   Sun, Mon, Tue, Wed, Thu, Fri, and Sat.
*
*   The abbreviations for the months are:
*   Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, and Dec. 
*
*   The asctime() is not required to be thread-safe.
*
* Reference:
*
*   IEEE Std 1003.1-2001
*   The GNU C Library Manual
* 
*******************************************************************************/
char * wceex_asctime(const struct tm *tmbuff)
{
    return wceex_asctime_r(tmbuff, __wce_asc_result);
}


/*******************************************************************************
* wceex_asctime_r - Convert date and time to a string
*
* Description:
*
*   This function is similar to asctime but instead of placing the result
*   in a static buffer it writes the string in the buffer pointed to by
*   the parameter buffer. This buffer should have room for at least 26 bytes,
*   including the terminating null.
*
*   The asctime_r() is not required to be thread-safe.
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
char * wceex_asctime_r(const struct tm *tmbuff, char *buff) 
{
    int res;
    static char wday_short[7][3] = {
        "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
    };
    static char mon_short[12][3] = {
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    };
 
    if (tmbuff == NULL)
    {
        // XXX - mloskot - set errno with EINVAL
        return NULL;
    }

    /* Format: "%.3s %.3s%3d %.2d:%.2d:%.2d %d\n" */
    res = sprintf(buff, __wce_asc_format,
                        wday_short[tmbuff->tm_wday],
                        mon_short[tmbuff->tm_mon],
                        tmbuff->tm_mday,
                        tmbuff->tm_hour,
                        tmbuff->tm_min,
                        tmbuff->tm_sec,
                        1900 + tmbuff->tm_year);

    if (res < 0)
        return NULL;
    else
        return buff;
}

