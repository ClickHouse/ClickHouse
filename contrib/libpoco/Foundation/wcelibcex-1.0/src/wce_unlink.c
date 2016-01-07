/*
 * $Id: wce_unlink.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines unlink() function.
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
* wceex_unlink -remove a directory entry.
*
* Return:
*
*   Upon successful completion, 0 shall be returned. Otherwise, -1.
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
int wceex_unlink(const char *filename)
{
    int res;
    int len;
    wchar_t* pWideStr;

    /* Covert filename buffer to Unicode. */
    len = MultiByteToWideChar(CP_ACP, 0, filename, -1, NULL, 0) ;
    pWideStr = (wchar_t*)malloc(sizeof(wchar_t) * len);
	
    MultiByteToWideChar(CP_ACP, 0, filename, -1, pWideStr, len);
	
    /* Delete file using Win32 CE API call */
    res = DeleteFile(pWideStr);
	
    /* Free wide-char string */
    free(pWideStr);

    if (res)
        return 0; /* success */
    else
    {
        errno = GetLastError();
        return -1;
    }
}

/*******************************************************************************
* wceex_wunlink -remove a directory entry.
*
* Return:
*
*   Upon successful completion, 0 shall be returned. Otherwise, -1.
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
int wceex_wunlink(const wchar_t *filename)
{
    if( DeleteFile(filename) )
        return 0;
    else
    {
        errno = GetLastError();
        return -1;
    }
}