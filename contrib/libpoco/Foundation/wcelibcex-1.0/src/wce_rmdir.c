/*
 * $Id: wce_rmdir.c,v 1.3 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines rmdir() function.
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
#include <stdlib.h>
#include <windows.h>

/*******************************************************************************
* wceex_rmdir - Remove empty directory from filesystem.
*
* Description:
*
*   The rmdir() function shall remove a directory whose name is given by path.
*   The directory shall be removed only if it is an empty directory.
*   Internally, mkdir() function wraps RemoveDirectory call from 
*   Windows CE API.
*
* Return:
*
*   Upon successful completion, rmdir() shall return 0.
*   Otherwise, -1 shall be returned. If -1 is returned, the named directory
*   shall not be changed.
*
*   XXX - mloskot - errno is not set - todo.
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
int wceex_rmdir(const char *filename)
{
    int res;    
    size_t len;
    wchar_t *widestr;

    /* Covert filename buffer to Unicode. */
	len = MultiByteToWideChar (CP_ACP, 0, filename, -1, NULL, 0) ;
	widestr = (wchar_t*)malloc(sizeof(wchar_t) * len);
	MultiByteToWideChar( CP_ACP, 0, filename, -1, widestr, len);
	
	/* Delete file using Win32 CE API call */
	res = RemoveDirectory(widestr);
	
	/* Free wide-char string */
	free(widestr);

    /* XXX - Consider following recommendations: */
    /* XXX - mloskot - update the st_ctime and st_mtime fields of the parent directory. */
    /* XXX - mloskot - set errno to [EEXIST] or [ENOTEMPTY] if function failed. */

    if (res)
	    return 0; /* success */
    else
        return -1;

}


