/*
 * $Id: wce_path.c,v 1.0 2006/11/29 16:56:01 sdunand Exp $
 *
 * Defines _splitpath, _wsplitpath, _makepath, _wmakepath,
 *         wceex_GetFullPathNameW, _fullpath, _wfullpath functions
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

#include <string.h>
#include <wce_stdlib.h>
#include <wce_direct.h>
#include <wce_errno.h>

/*******************************************************************************
* wceex_splitpath
*
* Description:
*    Break a path name into components.
*
* Return:
*
* Reference:
*
*******************************************************************************/

void wceex_splitpath( const char *path, 
                      char *drive, char *dir, char *name, char *ext )
{
    char *slash, *bslash;
    if( drive )
        *drive = 0;
    if( dir )
        *dir = 0;
    if( name )
        *name = 0;
    if( ext )
        *ext = 0;
    if( !path || *path == 0 )
        return;
    slash = strrchr( path, '/' );
    bslash = strrchr( path, '\\' );
    if( slash > bslash )
        bslash = slash;
    if( bslash )
    {
        if( dir )
        {
            size_t count = (bslash - path);
            if( count >= _MAX_DIR )
                count = _MAX_DIR - 1;
            strncat( dir, path, count );
        }
        bslash++;
    }
    else
        bslash = (char*)path;
    if( name )
    {
        char* dot;
        strncat( name, bslash, _MAX_FNAME - 1 );
        dot = strrchr( name, '.' );
        if( dot )
            *dot = 0;
    }
    if( ext )
    {
        char* dot = strrchr( bslash, '.' );
        if( dot )
            strncat( ext, dot, _MAX_EXT - 1 );
    }
}

/*******************************************************************************
* wceex_wsplitpath
*
* Description:
*    Break a path name into components.
*
* Return:
*
* Reference:
*
*******************************************************************************/

void wceex_wsplitpath( const wchar_t *path, 
                       wchar_t *drive, wchar_t *dir, wchar_t *name, wchar_t *ext )
{
    wchar_t *slash, *bslash;
    if( drive )
        *drive = 0;
    if( dir )
        *dir = 0;
    if( name )
        *name = 0;
    if( ext )
        *ext = 0;
    if( !path || *path == 0 )
        return;
    slash = wcsrchr( path, '/' );
    bslash = wcsrchr( path, '\\' );
    if( slash > bslash )
        bslash = slash;
    if( bslash )
    {
        if( dir )
        {
            size_t count = (bslash - path) / sizeof(wchar_t);
            if( count >= _MAX_DIR )
                count = _MAX_DIR - 1;
            wcsncat( dir, path, count );
        }
        bslash++;
    }
    else
        bslash = (wchar_t*)path;
    if( name )
    {
        wchar_t* dot;
        wcsncat( name, bslash, _MAX_FNAME - 1 );
        dot = wcsrchr( name, '.' );
        if( dot )
            *dot = 0;
    }
    if( ext )
    {
        wchar_t* dot = wcsrchr( bslash, '.' );
        if( dot )
            wcsncat( ext, dot, _MAX_EXT - 1 );
    }
}

/*******************************************************************************
* wceex_makepath
*
* Description:
*    Create a path name from components
*
* Return:
*
* Reference:
*
*******************************************************************************/

void wceex_makepath( char *path,
                     const char *drive, const char *dir,
                     const char *name, const char *ext )
{
    char* ptr = path;
    size_t slen, sbuf = _MAX_PATH - 1;
    *path = 0;
    if( drive && *drive )
    {
        strncat( ptr, drive, sbuf );
        slen = strlen( ptr );
        ptr += slen;
        sbuf -= slen;
    }
    if( dir && *dir && sbuf )
    {
        strncat( ptr, dir, sbuf );
        slen = strlen( ptr );
        ptr += slen - 1;
        sbuf -= slen;
        // backslash ?
        if( sbuf && *ptr != '\\' && *ptr != '/' )
        {
            char* slash = strchr( path, '/' );
            if( !slash )
                slash = strchr( path, '\\' );
            ptr++;
            if( slash )
                *ptr = *slash;
            else
                *ptr = '\\';
            ptr++;
            *ptr = 0;
            sbuf--;
        }
        ptr++;
    }
    if( name && *name && sbuf )
    {
        strncat( ptr, name, sbuf );
        slen = strlen( ptr );
        ptr += slen;
        sbuf -= slen;
    }
    if( ext && *ext && sbuf )
    {
        if( *ext != '.' )
        {
            *ptr = '.';
            ptr++;
            *ptr = 0;
            sbuf--;
        }
        if( sbuf )
            strncat( ptr, ext, sbuf );
    }
}

/*******************************************************************************
* wceex_wmakepath
*
* Description:
*    Create a path name from components
*
* Return:
*
* Reference:
*
*******************************************************************************/

void wceex_wmakepath( wchar_t *path,
                      const wchar_t *drive, const wchar_t *dir,
                      const wchar_t *name, const wchar_t *ext )
{
    wchar_t* ptr = path;
    size_t slen, sbuf = _MAX_PATH - 1;
    *path = 0;
    if( drive && *drive )
    {
        wcsncat( ptr, drive, sbuf );
        slen = wcslen( ptr );
        ptr += slen;
        sbuf -= slen;
    }
    if( dir && *dir && sbuf )
    {
        wcsncat( ptr, dir, sbuf );
        slen = wcslen( ptr );
        ptr += slen - 1;
        sbuf -= slen;
        // backslash ?
        if( sbuf && *ptr != '\\' && *ptr != '/' )
        {
            wchar_t* slash = wcschr( path, '/' );
            if( !slash )
                slash = wcschr( path, '\\' );
            ptr++;
            if( slash )
                *ptr = *slash;
            else
                *ptr = '\\';
            ptr++;
            *ptr = 0;
            sbuf--;
        }
        ptr++;
    }
    if( name && *name && sbuf )
    {
        wcsncat( ptr, name, sbuf );
        slen = wcslen( ptr );
        ptr += slen;
        sbuf -= slen;
    }
    if( ext && *ext && sbuf )
    {
        if( *ext != '.' )
        {
            *ptr = '.';
            ptr++;
            *ptr = 0;
            sbuf--;
        }
        if( sbuf )
            wcsncat( ptr, ext, sbuf );
    }
}

/*******************************************************************************
* wceex_GetFullPathNameW
*
* Description:
*    retrieves the full path and file name of a specified file.
*
* Return:
*
* Reference:
*
*******************************************************************************/

DWORD wceex_GetFullPathNameW( LPCWSTR lpFileName, DWORD nBufferLength, 
                              LPWSTR lpBuffer, LPWSTR *lpFilePart )
{
    int up = 0, down = 0;
    size_t len_tot, len_buf = 0;
    LPWSTR file;

    // reference to current working directory ?
    if( wcsncmp( lpFileName, L".\\", 2 ) == 0 )
        down = 1;
    else if( wcsncmp( lpFileName, L"./", 2 ) == 0 )
        down = 2;
    if( wcsncmp( lpFileName, L"..\\", 3 ) == 0 )
        up = 1;
    else if( wcsncmp( lpFileName, L"../", 3 ) == 0 )
        up = 2;
    if( down || up )
    {
        LPWSTR last;
        len_buf = wceex_GetCurrentDirectoryW( nBufferLength, lpBuffer );
        if( !len_buf )
            return 0;
        // backslash at the end ?
        last = lpBuffer + len_buf - 1;
        if( *last != '\\' && *last != '/' )
        {
            // test sufficient buffer before add
            len_buf++;
            if( len_buf >= nBufferLength )
                return len_buf + wcslen( lpFileName ) + 1;
            last++;
            if( down == 1 || up == 1 )
                *last = '\\';
            else
                *last = '/';
            *(last + 1) = 0;
        }
        if( down )
        {
            lpBuffer = last + 1;
            lpFileName += 2;
        }
        else if( up )
        {
            LPWSTR fname = (LPWSTR)lpFileName;
            for(;;)
            {
                // root ?
                if( last == lpBuffer )
                {
                    errno = ERROR_BAD_PATHNAME;
                    return 0;
                }
                // erase last backslash
                *last = 0;
                // parent directory
                if( up == 1 )
                    last = wcsrchr( lpBuffer, '\\' );
                else
                    last = wcsrchr( lpBuffer, '/' );
                if( !last )
                {
                    errno = ERROR_BAD_PATHNAME;
                    return 0;
                }
                *(last + 1) = 0;
                // next parent directory ?
                fname += 3;
                if( up == 1 )
                {
                    if( wcsncmp( fname, L"..\\", 3 ) )
                        break;
                }
                else
                {
                    if( wcsncmp( fname, L"../", 3 ) )
                        break;
                }
            }
            len_buf = wcslen( lpBuffer );
            lpBuffer = last + 1;
            lpFileName = fname;
        }
    }
    len_tot = len_buf + wcslen( lpFileName );
    if( len_tot >= nBufferLength )
        return len_tot + 1;
    wcscpy( lpBuffer, lpFileName );
    // delimiter of file name ?
    file = wcsrchr( lpBuffer, '\\' );
    if( !file )
        file = wcsrchr( lpBuffer, '/' );
    if( file )
    {
        file++;
        if( *file == 0 )
            *lpFilePart = NULL;
        else
            *lpFilePart = file;
    }
    else
        *lpFilePart = lpBuffer;
    return len_tot;
}

/*******************************************************************************
* wceex_wfullpath
*
* Description:
*    Create an absolute or full path name for the specified relative path name.
*
* Return:
*
* Reference:
*
*******************************************************************************/

wchar_t* wceex_wfullpath( wchar_t *absPath, const wchar_t *relPath, size_t maxLength )
{
    wchar_t* lpFilePart;
    DWORD ret = wceex_GetFullPathNameW( relPath, maxLength, absPath, &lpFilePart );
    if( !ret || ret > maxLength )
    {
        *absPath = 0;
        return NULL;
    }
    return absPath;
}

/*******************************************************************************
* wceex_fullpath
*
* Description:
*    Create an absolute or full path name for the specified relative path name.
*
* Return:
*
* Reference:
*
*******************************************************************************/

char* wceex_fullpath( char *absPath, const char *relPath, size_t maxLength )
{
    wchar_t wrelPath[_MAX_PATH*2], *wabsPath, *wret;
	if( !MultiByteToWideChar( CP_ACP, 0, relPath, -1, wrelPath, _MAX_PATH*2 ) )
    {
        errno = ENOMEM;
        *absPath = 0;
        return NULL;
    }
    if( (wabsPath = (wchar_t*)malloc( maxLength * sizeof(wchar_t) )) == NULL )
    {
        errno = ENOMEM;
        *absPath = 0;
        return NULL;
    }
    wret = wceex_wfullpath( wabsPath, wrelPath, maxLength ); 
    if( wret && !WideCharToMultiByte( CP_ACP, 0, wabsPath, -1, absPath, 
                                      maxLength, NULL, NULL ) )
    {
        errno = GetLastError();
        wret = NULL;
    }
    free( wabsPath );
    if( !wret )
    {
        *absPath = 0;
        return NULL;
    }
    return absPath;
}
