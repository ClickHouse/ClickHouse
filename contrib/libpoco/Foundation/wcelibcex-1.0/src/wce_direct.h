/*
 * $Id: wce_direct.h,v 1.0 2006/11/29 16:38:05 stephaned Exp $
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
#ifndef WCEEX_DIRECT_H
#define WCEEX_DIRECT_H	1

#if !defined(_WIN32_WCE)
# error "Only Windows CE target is supported!"
#endif

#include <winbase.h>

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

int wceex_wmkdir( const wchar_t* dirname );

char*    wceex_getcwd( char *buffer, int maxlen );
wchar_t* wceex_wgetcwd( wchar_t *buffer, int maxlen );

int wceex_chdir( const char *dirname );
int wceex_wchdir( const wchar_t *dirname );

DWORD wceex_GetCurrentDirectoryW( DWORD nBufferLength, LPWSTR lpBuffer );

#ifdef __cplusplus
}
#endif  /* __cplusplus */

#endif /* #ifndef WCEEX_DIRECT_H */

