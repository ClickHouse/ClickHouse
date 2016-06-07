/*
 * $Id: wce_setlocale.c 20 2006-11-18 17:00:30Z mloskot $
 *
 * Defines setlocale() function with dummy implementation.
 *
 * Created by Mateusz Loskot (mloskot@loskot.net)
 *
 * Copyright (c) 2006 Mateusz Loskot (mloskot@loskot.net)
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


/*******************************************************************************
* wceex_setlocale - dummy setlocale() function
*
* Description:
*
*   C library on Windows CE includes <locale.h> file with prototype of
*   setlocale() function but no implementation is available.
*
*   Currently, wceex_setlocale() function does not provide any implementation.
*   It does ALWAYS return empty string.
*
*   TODO: Consider to implement working version of setlocale() based on
*         GetLocaleInfo and SetLocaleInfo() calls from Windows CE API.
* Return:
*
*   The wceex_setlocale() function ALWAYS returns empty string.*
*       
*******************************************************************************/

char* wceex_setlocale(int category, const char* locale)
{
    return "";
}
