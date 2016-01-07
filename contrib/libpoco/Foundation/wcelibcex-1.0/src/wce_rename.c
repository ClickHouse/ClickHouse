/*
 * $Id: wce_rename.c,v 1.3 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines rename() function.
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

/*******************************************************************************
* wceex_rename - rename a file
*
* Description:
*
*   The rename() function changes the name of a file.
*   The oldfile argument points to the pathname of the file to be renamed.
*   The newfile argument points to the new pathname of the file.
*
*   XXX - mloskot - function does not set errno value yet.
*
* Return:
*
* Upon successful completion, rename() returns 0. Otherwise, -1 is returned.
* 
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
int wceex_rename(const char *oldfile, const char *newfile)
{
    int res;    
    size_t lenold;
    size_t lennew;
    wchar_t *wsold;
    wchar_t *wsnew;
    
    /* Covert filename buffer to Unicode. */

    /* Old filename */
    lenold = MultiByteToWideChar (CP_ACP, 0, oldfile, -1, NULL, 0) ;
    wsold = (wchar_t*)malloc(sizeof(wchar_t) * lenold);
    MultiByteToWideChar( CP_ACP, 0, oldfile, -1, wsold, lenold);
    
    /* New filename */
    lennew = MultiByteToWideChar (CP_ACP, 0, newfile, -1, NULL, 0) ;
    wsnew = (wchar_t*)malloc(sizeof(wchar_t) * lennew);
    MultiByteToWideChar(CP_ACP, 0, newfile, -1, wsnew, lennew);

    /* Delete file using Win32 CE API call */
    res = MoveFile(wsold, wsnew);
    
    /* Free wide-char string */
    free(wsold);
    free(wsnew);
    
    if (res)
        return 0; /* success */
    else
        return -1;
}

