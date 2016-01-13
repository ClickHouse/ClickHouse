/*
 * $Id: wce_clock.c 20 2006-11-18 17:00:30Z mloskot $
 *
 * Defines clock() function.
 *
 * Created by hav (TODO: Full name of hav)
 *
 * Copyright (c) 2006 (TODO: Full name of hav)
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
 */

#include <time.h>
#include <winbase.h>

/*******************************************************************************
* wceex_clock - report CPU time used
*
* Description:
*
*   The clock() function shall return the implementation's best approximation to
*   the processor time used by the process since the beginning of
*   an implementation-defined era related only to the process invocation.
*
*   Windows CE specific:
*      CLOCKS_PER_SEC is defined in <time.h> available in Windows CE SDK.
*
* Return value:
*
*   To determine the time in seconds, the value returned by clock() should be
*   divided by the value of the macro CLOCKS_PER_SEC.
*   CLOCKS_PER_SEC is defined to be one million in <time.h>.
*   If the processor time used is not available or its value cannot be represented,
*   the function shall return the value ( clock_t)-1.
* 
* Reference:
*
*   IEEE Std 1003.1-2001
*   The GNU C Library Manual
* 
*******************************************************************************/



clock_t wceex_clock()
{
    __int64 ticks;
    SYSTEMTIME stCurrent;
    FILETIME   ftCurrent;
    
    GetSystemTime(&stCurrent);
    
    if (SystemTimeToFileTime(&stCurrent, &ftCurrent))
    {
        ticks = *(__int64*)&ftCurrent;
    }
    else
    {
        /* The processor time used is not available or
         * its value cannot be represented.
         */
        ticks = -1;
    }
 
   return (clock_t)ticks;
}
 
