/*
 * $Id: wce_access.c,v 1.0 2006/11/29 16:37:06 stephaned Exp $
 *
 * Defines _access(), _waccess() functions.
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

#include <wce_io.h>
#include <wce_errno.h>
#include <shellapi.h>

/*******************************************************************************
* wceex_waccess - Determine file-access permission
*
* Description:
*
* Return:
*   0 if the file has the given mode
*   –1 if the named file does not exist or is not accessible in the given mode 
*   and errno set as :
*       EACCES file's permission setting does not allow specified access
*       ENOENT filename or path not found
*
* Reference:
*
*******************************************************************************/

int wceex_waccess( const wchar_t *path, int mode )
{
    SHFILEINFO fi;
    if( !SHGetFileInfo( path, 0, &fi, sizeof(fi), SHGFI_ATTRIBUTES ) )
    {
        errno = ENOENT;
        return -1;
    }
    // existence ?
    if( mode == 0 )
        return 0;
    // write permission ?
    if( mode & 2 )
    {
        if( fi.dwAttributes & SFGAO_READONLY )
        {
            errno = EACCES;
            return -1;
        }
    }
    return 0;
}

/*******************************************************************************
* wceex_access - Determine file-access permission
*
* Description:
*
* Return: 
*   0 if the file has the given mode
*   –1 if the named file does not exist or is not accessible in the given mode 
*   and errno set as :
*       EACCES file's permission setting does not allow specified access
*       ENOENT filename or path not found
*       
* Reference:
* 
*******************************************************************************/

int wceex_access( const char *path, int mode )
{
    wchar_t wpath[_MAX_PATH];
	if( !MultiByteToWideChar( CP_ACP, 0, path, -1, wpath, _MAX_PATH ) )
    {
        errno = ENOENT;
        return -1;
    }
    return wceex_waccess( wpath, mode );
}
