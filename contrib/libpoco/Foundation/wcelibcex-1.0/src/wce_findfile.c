/*
 * $Id: wce_findfile.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines functions to find files.
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

#include <wce_io.h>
#include <wce_timesys.h>
#include <windows.h>

/*******************************************************************************
* wceex_findclose - XXX
*
* Description:
*
*   XXX
*
* Return:
*
*   XXX
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*   
*******************************************************************************/
int wceex_findclose(intptr_t hFile)
{
    if(!FindClose((HANDLE)hFile))
	{
        //errno = EINVAL;
        return (-1);
    }
    return (0);

}

/*******************************************************************************
* wceex_findfirst - XXX
*
* Description:
*
*   XXX
*
* Return:
*
*   XXX
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
intptr_t wceex_findfirst(const char *filespec, struct _finddata_t *fileinfo)
{
    WIN32_FIND_DATA wfd;
    HANDLE          hFile;
    DWORD           err;
    wchar_t         wfilename[MAX_PATH];
    
    mbstowcs(wfilename, filespec, strlen(filespec) + 1);
    
    /* XXX - mloskot - set errno values! */
    
    hFile = FindFirstFile(wfilename, &wfd);
    if(hFile == INVALID_HANDLE_VALUE)
    {
        err = GetLastError();
        switch (err)
        {
        case ERROR_NO_MORE_FILES:
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND:
            //errno = ENOENT;
            break;
            
        case ERROR_NOT_ENOUGH_MEMORY:
            //errno = ENOMEM;
            break;
            
        default:
            //errno = EINVAL;
            break;
        }
        return (-1);
    }
    
    fileinfo->attrib = (wfd.dwFileAttributes == FILE_ATTRIBUTE_NORMAL) ? 0 : wfd.dwFileAttributes;
    fileinfo->time_create  = wceex_filetime_to_time(&wfd.ftCreationTime);
    fileinfo->time_access  = wceex_filetime_to_time(&wfd.ftLastAccessTime);
    fileinfo->time_write   = wceex_filetime_to_time(&wfd.ftLastWriteTime);
    
    fileinfo->size = wfd.nFileSizeLow;
    wcstombs(fileinfo->name, wfd.cFileName, wcslen(wfd.cFileName) + 1);
    
    return (intptr_t)hFile;
}

/*******************************************************************************
* wceex_findnext - XXX
*
* Description:
*
*   XXX
*
* Return:
*
*   XXX
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
int wceex_findnext(intptr_t handle, struct _finddata_t *fileinfo)
{
    WIN32_FIND_DATA wfd;
    DWORD           err;

    /* XXX - mloskot - set errno values! */

    if (!FindNextFile((HANDLE)handle, &wfd))
    {
        err = GetLastError();
        switch (err) {
        case ERROR_NO_MORE_FILES:
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND:
            //errno = ENOENT;
            break;
            
        case ERROR_NOT_ENOUGH_MEMORY:
            //errno = ENOMEM;
            break;
            
        default:
            //errno = EINVAL;
            break;
        }
        return (-1);
    }
    
    fileinfo->attrib = (wfd.dwFileAttributes == FILE_ATTRIBUTE_NORMAL)? 0 : wfd.dwFileAttributes;
    fileinfo->time_create  = wceex_filetime_to_time(&wfd.ftCreationTime);
    fileinfo->time_access  = wceex_filetime_to_time(&wfd.ftLastAccessTime);
    fileinfo->time_write   = wceex_filetime_to_time(&wfd.ftLastWriteTime);
    
    fileinfo->size = wfd.nFileSizeLow;
    wcstombs(fileinfo->name, wfd.cFileName, wcslen(wfd.cFileName)+1);
    
    return 0;
}

