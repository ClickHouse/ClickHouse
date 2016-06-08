/*
 * $Id: wce_bsearch.c 20 2006-11-18 17:00:30Z mloskot $
 *
 * Defines bsearch() function.
 *
 * Created by Mateusz Loskot (mateusz@loskot.net)
 *
 * Copyright (c) 2006 Mateusz Loskot
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

#include <stdlib.h>
#include <assert.h>
#include <wce_types.h>

/*******************************************************************************
* wceex_bsearch - TODO
*
* Description:
*
* Return:
*
*       
* Reference:
*   IEEE 1003.1, 2004 Edition
*******************************************************************************/

void* wceex_bsearch(const void *key, const void *base, size_t num, size_t width,
                    int (*compare)(const void *, const void *))
{
    size_t left;
    size_t middle;
    size_t right;
    int res;

    /* input parameters validation */
    assert(key != NULL);
    assert(base != NULL);
    assert(compare != NULL);

    res = 0;
    left = 0;
    right = num - 1;

    while (left <= right)
    {
        middle = (left + right) / 2;

        res = compare(((char*) base + (width * middle)), key);
        if (res > 0)
        {
            /* search from middle to left */
            right = middle - 1;
        }
        else if (res < 0)
        {
            /* search from middle to right */
            left = middle + 1;
        }
        else if (res == 0)
        {
            /* middle points to the key element. */
            return ((char*) base + (width * middle));
        }
    }

    /* key not found */
    return NULL;
}