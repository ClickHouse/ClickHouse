/*
 * $Id: wce_directorymanagement.c,v 1.0 2006/11/29 17:00:28 stephaned Exp $
 *
 * Defines _getcwd(), GetCurrentDirectoryW() _wgetcwd(), 
 *         _chdir(), _wchdir functions.
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

#include <winbase.h>
#include <shellapi.h>
#include <wce_direct.h>
#include <wce_errno.h>

/*******************************************************************************
* InitCwd - Init the current directory with the path for the file used to create 
*           the calling process 
*
* Description:
*
* Return:
*
* Reference:
*
*******************************************************************************/

static wchar_t Cwd[_MAX_PATH] = { '\0' };

static int InitCwd()
{
    if( Cwd[0] )
        return 0;
    if( !GetModuleFileName( NULL, Cwd, _MAX_PATH ) )
        return errno = GetLastError();
    else
    {
        wchar_t* slash = wcsrchr( Cwd, '\\' );
        if( !slash )
            slash = wcsrchr( Cwd, '/' );
        if( slash )
        {
            if( slash == Cwd )
                slash++;
            *slash = 0;
        }
        return 0;
    }
}

/*******************************************************************************
* wceex_getcwd - Get the current working directory
*
* Description:
*
* Return:
*
* Reference:
*
*******************************************************************************/

char* wceex_getcwd( char *buffer, int maxlen )
{
    if( !buffer && (buffer = (char*)malloc(maxlen)) == NULL )
    {
        errno = ENOMEM;
        return NULL;
    }
    if( InitCwd() )
        return NULL;    
	if( !WideCharToMultiByte( CP_ACP, 0, Cwd, -1, buffer, maxlen, NULL, NULL ) )
    {
        errno = GetLastError();
        return NULL;
    }
    return buffer;
}

/*******************************************************************************
* wceex_GetCurrentDirectoryW - Get the current working directory
*
* Description:
*
* Return:
*
* Reference:
*
*******************************************************************************/

DWORD wceex_GetCurrentDirectoryW( DWORD nBufferLength, LPWSTR lpBuffer )
{
    *lpBuffer = 0;
    if( InitCwd() )
    {
        SetLastError( errno );
        return 0;
    }
    else
    {
        size_t slen = wcslen( Cwd );
        if( slen >= (size_t)nBufferLength )
            return slen + 1;
        wcscpy( lpBuffer, Cwd );
        return slen;
    }
}

/*******************************************************************************
* wceex_wgetcwd - Get the current working directory
*
* Description:
*
* Return:
*
* Reference:
*
*******************************************************************************/

wchar_t* wceex_wgetcwd( wchar_t *buffer, int maxlen )
{
    if( !buffer && (buffer = (wchar_t*)malloc(maxlen * sizeof(wchar_t))) == NULL )
    {
        errno = ENOMEM;
        return NULL;
    }
    else
    {
        DWORD slen = wceex_GetCurrentDirectoryW( maxlen, buffer );
        if( !slen )
            return NULL;
        if( slen >= (DWORD)maxlen )
        {
            errno = ERANGE;
            return NULL;
        }
        return buffer;
    }
}

/*******************************************************************************
* wceex_wchdir - Change the current working directory
*
* Description:
*
* Return:
*
* Reference:
*
*******************************************************************************/

int wceex_wchdir( const wchar_t *dirname )
{
    if( !dirname || *dirname == 0 )
    {
        errno = ENOENT;
        return -1;
    }
    else
    {
        SHFILEINFO fi;
        if( !SHGetFileInfo( dirname, 0, &fi, sizeof(fi), SHGFI_ATTRIBUTES ) )
        {
            errno = ENOENT;
            return -1;
        }
        if( !(fi.dwAttributes & SFGAO_FOLDER) )
        {
            errno = ENOENT;
            return -1;
        }
        wcscpy( Cwd, dirname );
        return 0;
    }
}

/*******************************************************************************
* wceex_chdir - Change the current working directory
*
* Description:
*
* Return:
*
* Reference:
*
*******************************************************************************/

int wceex_chdir( const char *dirname )
{
    if( !dirname || *dirname == 0 )
    {
        errno = ENOENT;
        return -1;
    }
    else
    {
        wchar_t wdirname[_MAX_PATH];
	    if( !MultiByteToWideChar( CP_ACP, 0, dirname, -1, wdirname, _MAX_PATH ) )
        {
            errno = ENOENT;
            return -1;
        }
        return wceex_wchdir( wdirname );
    }
}
