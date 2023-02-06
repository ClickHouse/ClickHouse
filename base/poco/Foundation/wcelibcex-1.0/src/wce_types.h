/*
 * $Id: wce_types.h,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * sys/types.h - data types
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
#ifndef WCEEX_SYS_TYPES_H
#define WCEEX_SYS_TYPES_H 1

#if !defined(_WIN32_WCE)
# error "Only Winddows CE target is supported!"
#endif

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */


#ifndef _TIME_T_DEFINED
typedef long    time_t;             /* Used for time in seconds. */
# define _TIME_T_DEFINED
#endif

#ifndef _SIZE_T_DEFINED
typedef unsigned int    size_t;     /* Used for sizes of objects. */
# define _SIZE_T_DEFINED
#endif


#ifdef __cplusplus
}
#endif  /* __cplusplus */

#endif /* #ifndef WCEEX_SYS_TYPES_H */