/*
 * $Id: wce_mkdir.c,v 1.3 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines wmkdir, mkdir() functions.
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

#include <windows.h>
#include <wce_errno.h>

/*******************************************************************************
* wceex_mkdir - Make a directory.
*
* Description:
*
*   The mkdir() function shall create a new directory with name path. 
*   Internally, mkdir() function wraps CreateDirectory call from 
*   Windows CE API.
*
* Return:
*
*   Upon successful completion, mkdir() shall return 0.
*   Otherwise, -1 shall be returned, no directory shall be created,
*   and errno shall be set with the error returned by CreateDirectory.
*
* Reference:
*   IEEE 1003.1, 2004 Edition
*******************************************************************************/
int wceex_mkdir(const char *filename)
{
    int res;    
    size_t len;
    wchar_t *widestr;

    /* Covert filename buffer to Unicode. */
    len = MultiByteToWideChar (CP_ACP, 0, filename, -1, NULL, 0) ;
	widestr  = (wchar_t*)malloc(sizeof(wchar_t) * len);

    MultiByteToWideChar( CP_ACP, 0, filename, -1, widestr, len);
	
    /* Delete file using Win32 CE API call */
    res = CreateDirectory(widestr, NULL);
	
    /* Free wide-char string */
    free(widestr);

    if (res)
	    return 0; /* success */
    else
    {
        errno = GetLastError();
        return -1;
    }
}

/*******************************************************************************
* wceex_mkdir - Make a directory.
*
* Description:
*
*   The wmkdir() function shall create a new directory with name path. 
*   Internally, wmkdir() function wraps CreateDirectory call from 
*   Windows CE API.
*
* Return:
*
*   Upon successful completion, wmkdir() shall return 0.
*   Otherwise, -1 shall be returned, no directory shall be created,
*   and errno shall be set with the error returned by CreateDirectory.
*
* Reference:
*   IEEE 1003.1, 2004 Edition
*******************************************************************************/

int wceex_wmkdir( const wchar_t* dirname )
{
    if( !CreateDirectory( dirname, NULL ) )
    {
        errno = GetLastError();
        return -1;
    }
    return 0;
}
