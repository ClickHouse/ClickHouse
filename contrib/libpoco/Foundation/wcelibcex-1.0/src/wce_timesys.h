/*
 * $Id: wce_timesys.h,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * wce_timesys.h - SYSTEMTIME and FILETIME conversion utilities.
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
 */
#ifndef WCEEX_TIMESYS_H
#define WCEEX_TIMESYS_H  1

#ifndef _WIN32_WCE
# error "Only Windows CE target compilation supported"
#endif

#include <windows.h>
#include <wce_time.h>

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

/* Function declarations. */

time_t wceex_filetime_to_time(const FILETIME * filetime);
time_t wceex_local_to_time(const SYSTEMTIME *systemtime);
time_t wceex_local_to_time_r(int year, int mon, int day, int hour, int min, int sec);

#ifdef __cplusplus
}
#endif  /* __cplusplus */

#endif /* #ifndef WCEEX_TIMESYS_H */
