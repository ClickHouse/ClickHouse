/*
 * $Id: wce_timesys.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 *  Defines function to convert time between formats SYSTEMTIME,
 *  FILETIME and time_t value.
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
#include <wce_timesys.h>
#include <windows.h>

/*******************************************************************************
* wceex_filetime_to_time - Convert FILETIME to time as time_t value
*
* Description:
*
*
* Return:
*
*   This function shall return the specified time since the Epoch
*   encoded as a value of type time_t. 
*       
*******************************************************************************/
time_t wceex_filetime_to_time(const FILETIME * pft)
{
    SYSTEMTIME st;
    FILETIME lft;

    /* File time as 0 value cannot be represented as Epoch time. */

    if (!pft->dwLowDateTime && !pft->dwHighDateTime)
    {
        return (time_t)-1;
    }

    /* Convert to a broken down local time value */
    if (!FileTimeToLocalFileTime(pft, &lft) ||
        !FileTimeToSystemTime(&lft, &st))
    {
        return (time_t)-1;
    }

    return wceex_local_to_time(&st);
}


/*******************************************************************************
* wceex_local_to_time - Convert broken-down local time to value of type time_t
*
* Description:
*
*
* Return:
*
*   This function shall return the specified time since the Epoch
*   encoded as a value of type time_t. 
*       
*******************************************************************************/
time_t wceex_local_to_time(const SYSTEMTIME *st)
{
    if (st == NULL)
    {
        return (time_t)-1;
    }
 
    return wceex_local_to_time_r(st->wYear - TM_YEAR_BASE,
                               st->wMonth - 1,
                               st->wDay, st->wHour,
                               st->wMinute,
                               st->wSecond);
}

/*******************************************************************************
* wceex_local_to_time - Convert broken-down local time to value of type time_t
*
* Description:
*
*   Date and time are given as a set of separate values.
*   Parameters:
*   - year is Epoch-based, year - 1900
*   - mon is 0 based number of current month
*   - day is 1 based number of current day
*   - hour, min and sec represent current local time.
*
* Return:
*
*   This function shall return the specified time since the Epoch
*   encoded as a value of type time_t. 
*       
*******************************************************************************/
time_t wceex_local_to_time_r(int year, int mon, int day, int hour, int min, int sec)
{
    struct tm tmbuff = { 0 };

    tmbuff.tm_year = year;
    tmbuff.tm_mon = mon;
    tmbuff.tm_mday = day;
    tmbuff.tm_hour = hour;
    tmbuff.tm_min = min;
    tmbuff.tm_sec = sec;
    tmbuff.tm_isdst = 0;
    tmbuff.tm_wday = 0;
    tmbuff.tm_yday = 0;

    /* Convert tm struct to time_tUTC */
    return wceex_mktime(&tmbuff);
}

