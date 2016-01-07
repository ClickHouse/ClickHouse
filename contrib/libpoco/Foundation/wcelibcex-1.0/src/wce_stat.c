/*
 * $Id: wce_stat.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines stat() function.
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

#include <wce_stat.h>
#include <wce_time.h>
#include <wce_timesys.h>
#include <windows.h>

/*******************************************************************************
* Forward declarations.
********************************************************************************/

/* Return mode of file. */
static unsigned short __wceex_get_file_mode(const char* name, int attr);


/*******************************************************************************
* wceex_stat - Get file attributes for file and store them in buffer.
*
* Description:
*
*   File times on Windows CE: Windows CE object store keeps track of only
*   one time, the time the file was last written to.
*
* Return value:
*
*   Upon successful completion, 0 shall be returned.
*   Otherwise, -1 shall be returned and errno set to indicate the error.
*
*   XXX - mloskot - errno is not yet implemented
*
* Reference:
*   IEEE Std 1003.1, 2004 Edition
*
*******************************************************************************/
int wceex_stat(const char* filename, struct stat *buffer)
{
    HANDLE findhandle;
    WIN32_FIND_DATA findbuf;
    wchar_t pathWCE[MAX_PATH];

    //Don't allow wildcards to be interpreted by system
    if(strpbrk(filename, "?*"))
        //if(wcspbrk(path, L"?*"))
    {
        //errno = ENOENT;
        return(-1);
    }

    //search file/dir
    mbstowcs(pathWCE, filename, strlen(filename) + 1);
    findhandle = FindFirstFile(pathWCE, &findbuf);
    if(findhandle == INVALID_HANDLE_VALUE)
    {
        //is root
        if(_stricmp(filename, ".\\")==0)
        {
            findbuf.dwFileAttributes = FILE_ATTRIBUTE_DIRECTORY;

            //dummy values
            findbuf.nFileSizeHigh = 0;
            findbuf.nFileSizeLow = 0;
            findbuf.cFileName[0] = '\0';

            buffer->st_mtime = wceex_local_to_time_r(1980 - TM_YEAR_BASE, 0, 1, 0, 0, 0);
            buffer->st_atime = buffer->st_mtime;
            buffer->st_ctime = buffer->st_mtime;
        }

        //treat as an error
        else
        {
            //errno = ENOENT;
            return(-1);
        }
    }
    else
    {
        /* File is found*/

        SYSTEMTIME SystemTime;
        FILETIME LocalFTime;

        //Time of last modification
        if(!FileTimeToLocalFileTime( &findbuf.ftLastWriteTime, &LocalFTime) ||
            !FileTimeToSystemTime(&LocalFTime, &SystemTime))
        {
            //errno = ::GetLastError();
            FindClose( findhandle );
            return( -1 );
        }

        buffer->st_mtime = wceex_local_to_time(&SystemTime);

        //Time od last access of file
        if(findbuf.ftLastAccessTime.dwLowDateTime || findbuf.ftLastAccessTime.dwHighDateTime)
        {
            if(!FileTimeToLocalFileTime(&findbuf.ftLastAccessTime, &LocalFTime) ||
                !FileTimeToSystemTime(&LocalFTime, &SystemTime))
            {
                //errno = ::GetLastError();
                FindClose( findhandle );
                return( -1 );
            }
            buffer->st_atime = wceex_local_to_time(&SystemTime);
        }
        else
        {
            buffer->st_atime = buffer->st_mtime;
        }


        //Time of creation of file
        if(findbuf.ftCreationTime.dwLowDateTime || findbuf.ftCreationTime.dwHighDateTime)
        {
            if(!FileTimeToLocalFileTime(&findbuf.ftCreationTime, &LocalFTime) ||
                !FileTimeToSystemTime(&LocalFTime, &SystemTime))
            {
                //errno = ::GetLastError();
                FindClose( findhandle );
                return( -1 );
            }
            buffer->st_ctime = wceex_local_to_time(&SystemTime);
        }
        else
        {
            buffer->st_ctime = buffer->st_mtime;
        }

        //close handle
        FindClose(findhandle);
    }

    //file mode
    buffer->st_mode = __wceex_get_file_mode(filename, findbuf.dwFileAttributes);

    //file size
    buffer->st_size = findbuf.nFileSizeLow;

    //drive letter 0
    buffer->st_rdev = buffer->st_dev = 0;

    //set the common fields
    buffer->st_gid = 0;
    buffer->st_ino = 0;
    buffer->st_uid = 0;

    //1 dla nlink
    buffer->st_nlink = 1;


    return 0;
}

/*******************************************************************************
* Return mode of file.
********************************************************************************/

/* Test path for presence of slach at the end. */
#define IS_SLASH(a)  ((a) =='\\' || (a) == '/')

#define __DOSMODE_MASK  0xff

static unsigned short __wceex_get_file_mode(const char* filename, int attr)
{
    unsigned short file_mode;
    unsigned mode;
    const char *p;

    mode = attr & __DOSMODE_MASK;

    /* XXX - mloskot - remove it */
    if ((p = filename)[1] == ':')
        p += 2;

    /* Check to see if this is a directory. */
    file_mode = (unsigned short)
        (((IS_SLASH(*p) && !p[1]) || (mode & FILE_ATTRIBUTE_DIRECTORY) || !*p)
        ? S_IFDIR | S_IEXEC : S_IFREG);

    /* Check if attribute byte does have read-only bit, otherwise it is read-write. */
    file_mode |= (mode & FILE_ATTRIBUTE_READONLY) ? S_IREAD : (S_IREAD | S_IWRITE);

    /* See if file appears to be executable by the extension. */
    if (p = strrchr(filename, '.'))
    {
        if (!_stricmp(p, ".exe") ||
            !_stricmp(p, ".cmd") ||
            !_stricmp(p, ".bat") ||
            !_stricmp(p, ".com"))
            file_mode |= S_IEXEC;
    }

    /* Propagate user read/write/execute bits to group/other fields. */
    file_mode |= (file_mode & 0700) >> 3;
    file_mode |= (file_mode & 0700) >> 6;

    return(file_mode);
}

