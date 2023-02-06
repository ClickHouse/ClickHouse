/*
 * $Id$
 *
 * Defines gettimeofday function.
 *
 * Author of first version (timeval.h): by Wu Yongwei
 * Author of Windows CE version: Mateusz Loskot (mateusz@loskot.net)
 *
 * All code here is considered in the public domain though we do wish our names
 * could be retained if anyone uses them.
 */

#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <time.h>
#include <wce_time.h>

/*******************************************************************************
* wceex_gettimeofday - get the date and time
*
* Description:
*
*   The gettimeofday() function shall obtain the current time,
*   expressed as seconds and microseconds since the Epoch,
*   and store it in the timeval structure pointed to by tp.
*   The resolution of the system clock is unspecified.
*
* Return value:
*
*   The gettimeofday() function shall return 0 and
*   no value shall be reserved to indicate an error.
*
* Reference:
*
*   IEEE Standard and an Open Group Technical Standard 1003.1, 2004 Edition
* 
*******************************************************************************/

int wceex_gettimeofday(struct timeval *tp, struct timezone *tzp)
{
    SYSTEMTIME      st;
    FILETIME        ft;
    LARGE_INTEGER   li;
    TIME_ZONE_INFORMATION tzi;
    __int64         t;
    static int      tzflag;

    if (NULL != tp)
    {
        GetSystemTime(&st);
        SystemTimeToFileTime(&st, &ft);
        li.LowPart  = ft.dwLowDateTime;
        li.HighPart = ft.dwHighDateTime;
        t  = li.QuadPart;       /* In 100-nanosecond intervals */
        t -= EPOCHFILETIME;     /* Offset to the Epoch time */
        t /= 10;                /* In microseconds */
        tp->tv_sec  = (long)(t / 1000000);
        tp->tv_usec = (long)(t % 1000000);
    }

    if (NULL != tzp)
    {   
        GetTimeZoneInformation(&tzi);

        tzp->tz_minuteswest = tzi.Bias;
        if (tzi.StandardDate.wMonth != 0)
        {
            tzp->tz_minuteswest += tzi.StandardBias * 60;
        }

        if (tzi.DaylightDate.wMonth != 0)
        {
            tzp->tz_dsttime = 1;
        }
        else
        {
            tzp->tz_dsttime = 0;
        }
    }

    return 0;
}

