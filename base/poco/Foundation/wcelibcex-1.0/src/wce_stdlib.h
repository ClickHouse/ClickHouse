/*
 * $Id: wce_stdlib.h,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * stdlib.h - standard library definitions
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
#ifndef WCEEX_STDLIB_H
#define WCEEX_STDLIB_H 1

#if !defined(_WIN32_WCE)
# error "Only Windows CE target is supported!"
#endif

/*
 * Minimal set of required declarations to avoid inclusion of <windows.h>,
 * because it defines 'LP' type and other which conflict with some libraries,
 * like PROJ.4.
 */
typedef unsigned long DWORD;
typedef wchar_t *LPWSTR, *PWSTR;
typedef const wchar_t *LPCWSTR, *PCWSTR;

#define _MAX_DIR   256
#define _MAX_FNAME 256
#define _MAX_EXT   256

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */


/* Generate an abnormal process abort. */

void wceex_abort(void);

/* Searching and sorting utilities. */

void* wceex_bsearch(const void *key, const void *base, size_t nmemb, size_t size,
                    int (*compar)(const void *, const void *));

void* wceex_lfind(const void *key, const void *base, size_t *nmemb, size_t size,
                 int(*compar)(const void *, const void *));

/* File Management Functions */

void wceex_splitpath( const char *path, 
                      char *drive, char *dir, char *name, char *ext );
void wceex_wsplitpath( const wchar_t *path, 
                       wchar_t *drive, wchar_t *dir, wchar_t *name, wchar_t *ext );

void wceex_makepath( char *path,
                     const char *drive, const char *dir,
                     const char *name, const char *ext );
void wceex_wmakepath( wchar_t *path,
                      const wchar_t *drive, const wchar_t *dir,
                      const wchar_t *name, const wchar_t *ext );

char*    wceex_fullpath( char *absPath, const char *relPath, size_t maxLength );
wchar_t* wceex_wfullpath( wchar_t *absPath, const wchar_t *relPath, size_t maxLength );

DWORD wceex_GetFullPathNameW( LPCWSTR lpFileName, DWORD nBufferLength, 
                              LPWSTR lpBuffer, LPWSTR *lpFilePart );

/* Dummy compilation enablers - functions that do not provide any implementation. */

char* wceex_getenv(const char* varname); 

#ifdef __cplusplus
}
#endif  /* __cplusplus */


#endif /* #ifndef WCEEX_STDLIB_H */
