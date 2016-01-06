/*
 * $Id: wce_mktime.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines functions to convert struct tm to time_t value.
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
#include <windows.h>

/* Function used intenally to convert struct tm to a time_t value. */
static time_t __wceex_mktime_internal(struct tm *tmbuff, time_t _loctime_offset);

/*******************************************************************************
* wceex_mktime - Convert local time to calendar value in seconds since epoch.
*******************************************************************************/
time_t wceex_mktime(struct tm *tmbuff)
{
    time_t offset;
    TIME_ZONE_INFORMATION tzi;
    
    offset = 0;

    // Retrive timezone offset in seconds
	if (GetTimeZoneInformation(&tzi) != 0xFFFFFFFF)
	{
		offset += (tzi.Bias * 60);
		if (tzi.StandardDate.wMonth != 0)
        {
			offset += (tzi.StandardBias * 60);
        }
	}

    return __wceex_mktime_internal(tmbuff, offset);
}


/*******************************************************************************
* wceex_gmmktime - Get Unix timestamp for a GMT date
*
* Description:
*   Given a struct tm representing a calendar time in UTC, convert it to
*   seconds since epoch.
*   Note that this function does not canonicalize the provided
*   struct tm, nor does it allow out of range values or years before 1970.
*   The tm struct values of tm_wday and tm_yday are ignored.
*
* Return:
*   Value of time if success, otherwise (time_t)-1 is returned.
*       
*******************************************************************************/
time_t wceex_gmmktime(struct tm *tmbuff)
{
    return __wceex_mktime_internal(tmbuff, 0);
}

/*******************************************************************************
* __wceex_mktime_internal - Convert struct tm to a time_t value.
*
* Description:
*   Use offset as difference in seconds between local time and UTC time.
*
/*******************************************************************************/

/* The number of days in each month. */
#define MONTHS_NUMBER 12

static const int MONTHDAYS[] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

static time_t __wceex_mktime_internal(struct tm *tmbuff, time_t _loctime_offset)
{
    time_t tres;
    int doy;
    int i;

    /* We do allow some ill-formed dates, but we don't do anything special
    with them and our callers really shouldn't pass them to us.  Do
    explicitly disallow the ones that would cause invalid array accesses
    or other algorithm problems. */
    if (tmbuff->tm_mon < 0 || tmbuff->tm_mon > 11 || tmbuff->tm_year < (EPOCH_YEAR - TM_YEAR_BASE))
    {
        return (time_t) -1;
    }

    /* Convert calender time to a time_t value. */
    tres = 0;

    /* Sum total amount of days from the Epoch with respect to leap years. */
    for (i = EPOCH_YEAR; i < tmbuff->tm_year + TM_YEAR_BASE; i++)
    {
        tres += 365 + IS_LEAP_YEAR(i);
    }

    /* Add days of months before current month. */
    doy = 0;
    for (i = 0; i < tmbuff->tm_mon; i++)
    {
        doy += MONTHDAYS[i];
    }
    tres += doy;
    
    /* Day of year */
    tmbuff->tm_yday = doy + tmbuff->tm_mday;

    if (tmbuff->tm_mon > 1 && IS_LEAP_YEAR(tmbuff->tm_year + TM_YEAR_BASE))
    {
        tres++;
    }
    
    /* Add days of current month and convert to total to hours. */
    tres = 24 * (tres + tmbuff->tm_mday - 1) + tmbuff->tm_hour;

    /* Add minutes part and convert total to minutes. */
    tres = 60 * tres + tmbuff->tm_min;

    /* Add seconds part and convert total to seconds. */
    tres = 60 * tres + tmbuff->tm_sec;
    
    /* For offset > 0 adjust time value for timezone
    given as local to UTC time difference in seconds). */
    tres += _loctime_offset;
    
    return tres;
}


