/*
 * $Id: wce_localtime.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines localtime() function.
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
#include <windows.h>

/*******************************************************************************
  Constants and macros used internally
*******************************************************************************/

#define SECS_PER_MIN	60
#define MINS_PER_HOUR	60
#define HOURS_PER_DAY	24
#define DAYS_PER_WEEK	7
#define DAYS_PER_NYEAR	365
#define DAYS_PER_LYEAR	366
#define SECS_PER_HOUR	(SECS_PER_MIN * MINS_PER_HOUR)
#define SECS_PER_DAY	((long) SECS_PER_HOUR * HOURS_PER_DAY)
#define MONS_PER_YEAR	12

#define TM_SUNDAY	0
#define TM_MONDAY	1
#define TM_TUESDAY	2
#define TM_WEDNESDAY	3
#define TM_THURSDAY	4
#define TM_FRIDAY	5
#define TM_SATURDAY	6

#define TM_JANUARY	0
#define TM_FEBRUARY	1
#define TM_MARCH	2
#define TM_APRIL	3
#define TM_MAY		4
#define TM_JUNE		5
#define TM_JULY		6
#define TM_AUGUST	7
#define TM_SEPTEMBER	8
#define TM_OCTOBER	9
#define TM_NOVEBER	10
#define TM_DECEMBER	11
#define TM_SUNDAY	0

#define TM_YEAR_BASE	1900

#define EPOCH_YEAR	1970
#define EPOCH_WDAY	TM_THURSDAY

#define isleap(y) (((y) % 4) == 0 && ((y) % 100) != 0 || ((y) % 400) == 0)

/*******************************************************************************
  Local time functions
*******************************************************************************/

struct tm * __wceex_offtime(const time_t *timer, long tzoffset);

/*******************************************************************************
* wceex_localtime - Convert time_t value to tm struct.
*
* Description:
*   Use offset as difference in seconds between local time and UTC time.
*
*******************************************************************************/
struct tm * wceex_localtime(const time_t *timer)
{
    register struct tm *tmp;

    long tzoffset;
    TIME_ZONE_INFORMATION tzi;
    
    // Retrive timezone offset in seconds
    tzoffset = 0;
	if (GetTimeZoneInformation(&tzi) != 0xFFFFFFFF)
	{
		tzoffset += (tzi.Bias * 60);
		if (tzi.StandardDate.wMonth != 0)
        {
			tzoffset += (tzi.StandardBias * 60);
        }
	}
    
    tzoffset *= -1;
    tmp = __wceex_offtime(timer, tzoffset);

    return tmp;
}

/*******************************************************************************
* wceex_gmtime - Convert time_t value to tm struct.
*
* Description:
*   This function is similar to localtime, except that the broken-down
*   time is expressed as Coordinated Universal Time (UTC) 
*   rather than relative to a local time zone.
*
*******************************************************************************/
struct tm * wceex_gmtime(const time_t *timer)
{
    register struct tm *tmp;

    tmp = __wceex_offtime(timer, 0L);

    return tmp;
}

/*******************************************************************************
* __wceex_offtime - Convert time_t value to tm struct.
*
* Description:
*   Use offset as difference in seconds between local time and UTC time.
*
*******************************************************************************/
static int	mon_lengths[2][MONS_PER_YEAR] =
{
    31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};

static int	year_lengths[2] = { DAYS_PER_NYEAR, DAYS_PER_LYEAR };


struct tm * __wceex_offtime(const time_t *timer, long tzoffset)
{
    register struct tm *tmp;
    register long		days;
    register long		rem;
    register int		y;
    register int		yleap;
    register int       *ip;
    static struct tm    tm;

    tmp = &tm;
    days = *timer / SECS_PER_DAY;
    rem = *timer % SECS_PER_DAY;
    rem += tzoffset;
    while (rem < 0)
    {
        rem += SECS_PER_DAY;
        --days;
    }
    
    while (rem >= SECS_PER_DAY)
    {
        rem -= SECS_PER_DAY;
        ++days;
    }
    tmp->tm_hour = (int) (rem / SECS_PER_HOUR);

    rem = rem % SECS_PER_HOUR;
    tmp->tm_min = (int) (rem / SECS_PER_MIN);
    tmp->tm_sec = (int) (rem % SECS_PER_MIN);
    tmp->tm_wday = (int) ((EPOCH_WDAY + days) % DAYS_PER_WEEK);
    
    if (tmp->tm_wday < 0)
        tmp->tm_wday += DAYS_PER_WEEK;
    
    y = EPOCH_YEAR;
    
    if (days >= 0)
    {
        for ( ; ; )
        {
            yleap = isleap(y);
            if (days < (long) year_lengths[yleap])
                break;

            ++y;
            days = days - (long) year_lengths[yleap];
        }
    }
    else
    {
        do
        {
            --y;
            yleap = isleap(y);
            days = days + (long) year_lengths[yleap];
        } while (days < 0);
    }

    tmp->tm_year = y - TM_YEAR_BASE;
    tmp->tm_yday = (int) days;
    ip = mon_lengths[yleap];

    for (tmp->tm_mon = 0; days >= (long) ip[tmp->tm_mon]; ++(tmp->tm_mon))
    {
        days = days - (long) ip[tmp->tm_mon];
    }

    tmp->tm_mday = (int) (days + 1);
    tmp->tm_isdst = 0;

    return tmp;
}

