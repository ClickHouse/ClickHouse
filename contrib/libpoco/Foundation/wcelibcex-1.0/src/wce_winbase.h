/*
 * $Id: wce_winbase.h,v 1.0 2006/04/12 15:11:36 sdunand Exp $
 *
 * Created by Stéphane Dunand (sdunand@sirap.fr)
 *
 * Copyright (c) 2006 Stéphane Dunand
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
#ifndef WCEEX_WINBASE_H
#define WCEEX_WINBASE_H	1

#if !defined(_WIN32_WCE)
# error "Only Windows CE target is supported!"
#endif

#include <winbase.h>

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

#define STD_INPUT_HANDLE    (DWORD)-10
#define STD_OUTPUT_HANDLE   (DWORD)-11
#define STD_ERROR_HANDLE    (DWORD)-12

HANDLE GetStdHandle( DWORD nStdHandle );

#ifdef __cplusplus
}
#endif  /* __cplusplus */

#endif /* #ifndef WCEEX_WINBASE_H */

