/*
 * $Id: wce_time.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines time() function.
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

/*******************************************************************************
* wceex_time - Return the value of time in seconds since the Epoch.
*
* Description:
*   The timer argument points to an area where the return value is also stored.
*   If timer is a null pointer, no value is stored.
*
* Return:
*   Value of time if success, otherwise (time_t)-1 is returned.
*       
* Reference:
*   IEEE Standard and an Open Group Technical Standard 1003.1, 2004 Edition
*******************************************************************************/
time_t wceex_time(time_t *timer)
{
    time_t t;
    struct tm tmbuff;
    SYSTEMTIME st;

    /* Retrive current system date time as UTC */
    GetSystemTime(&st);

    /* Build tm struct based on SYSTEMTIME values */

    /* Date values */
    tmbuff.tm_year = st.wYear - TM_YEAR_BASE;
    tmbuff.tm_mon = st.wMonth - 1;      /* wMonth value 1-12 */
    tmbuff.tm_mday = st.wDay;

    /* Time values */
    tmbuff.tm_hour = st.wHour;
    tmbuff.tm_min = st.wMinute;
    tmbuff.tm_sec = st.wSecond;
    tmbuff.tm_isdst = 0;    /* Always 0 for UTC time. */
    tmbuff.tm_wday = st.wDayOfWeek;
    tmbuff.tm_yday = 0;     /* Value is set by wceex_gmmktime */

    /* Convert tm struct to time_tUTC */
    t = wceex_gmmktime(&tmbuff);

    /* Assign time value. */
    if (timer != NULL)
    {
        *timer = t;
    }

	return t;
}
/*******************************************************************************
* _wceex_tm_to_time_t - Convert time from tm struct format to time_t value.
*
* Description:
*   The tmbuff points to structure that contains broken-down time.
*   Input and output times are encoded as UTC.
*   
* Return:
*   Specified time since the Epoch encoded as a value of type time_t.
*   (time_t)-1 is returned if time can not be represented by time_t.
*       
* Reference:
*   IEEE Standard and an Open Group Technical Standard 1003.1, 2004 Edition
*******************************************************************************/
time_t __wceex_tm_to_time_t(const struct tm *tmbuff)
{
    time_t timer;

    /* If the year is <1970 or the value is negative, the relationship is undefined */ 
    if (tmbuff->tm_year < 70)
    {
        return (time_t) -1;
    }
    
    /* If the year is >=1970 */
    /* Each and every day shall be accounted for by exactly 86400 seconds */

    timer = tmbuff->tm_sec
        + tmbuff->tm_min * 60                     /* convert minutes to seconds */
        + tmbuff->tm_hour * 3600                  /* convert hours to seconds */
        + tmbuff->tm_yday * 86400                 /* convert day of year to seconds */
        + (tmbuff->tm_year - 70) * 31536000       /* convert year to seconds */
        + ((tmbuff->tm_year - 69) / 4) * 86400        /* add a day (seconds) every 4 years starting in 1973 */
        - ((tmbuff->tm_year - 1) / 100) * 86400       /* subtract a day back out every 100 years starting in 2001 */
        + ((tmbuff->tm_year + 299) / 400) * 86400;    /* add a day back in every 400 years starting in 2001 */

    return timer;
}

