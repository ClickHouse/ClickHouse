/*
 * $Id: wce_rewind.c,v 1.2 2006/04/09 16:48:18 mloskot Exp $
 *
 * Defines rewind() function.
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
#include <assert.h>

/*******************************************************************************
* wceex_rewind - Reset the file position indicator in a stream
*
* Description:
*
*   The call rewind(stream) shall be equivalent to:
*   (void) fseek(stream, 0L, SEEK_SET)
*
*   Internally, rewind() function uses SetFilePointer call from 
*   Windows CE API.
*
*   Windows CE specific:
*   On Windows CE HANDLE type is defined as typedef void *HANDLE
*   and FILE type is declared as typedef void FILE.
*
* Return:
*
*   No return value.
*       
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/
void wceex_rewind(FILE * fp)
{
    int ret;

    /* TODO - mloskot: WARNING!
     * fseek() does not clear error and end-of-file indicators for the stream
     * So, that's why dirty asserts are used to get informed about potential problems.
     */
    ret = fseek(fp, 0L, SEEK_SET);
    
    assert(0 == ret);
    assert(0 == ferror(fp));
    assert(!feof(fp));

    /*

    // XXX - mloskot:
    // FILE* to HANDLE conversion needs hacks like _get_osfhandle()
    // which are not available on Windows CE.
    // Simple cast does not work.
    //
    // TODO: Does anyone know how to convert FILE* to HANDLE?

    DWORD dwError;
    HANDLE hFile;

    hFile = (void*)fp;

    if (0xFFFFFFFF == SetFilePointer(hFile, 0, NULL, FILE_BEGIN))
    {
    	dwError = GetLastError();
        assert(NO_ERROR == dwError);
    }

    */
}

