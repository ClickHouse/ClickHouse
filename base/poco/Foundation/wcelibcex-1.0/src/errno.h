/*
 * $Id: errno.h 20 2006-11-18 17:00:30Z mloskot $
 *
 * errno.h - system error numbers
 *
 * Created by Mateusz Loskot (mateusz@loskot.net)
 *
 * Copyright (c) 2006 Mateusz Loskot (mateusz@loskot.net)
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
#ifndef WCEEX_ERRNO_WRAPPER_H
#define WCEEX_ERRNO_WRAPPER_H	1

/*
 * Windows CE SDK does not provide errno.h file.
 * In order to simplify usage of wce_errno.h extension, this header is provided.
 * It directly includes wce_errno.h file.
 */

#if !defined(_WIN32_WCE)
# error "Only Winddows CE target is supported!"
#else
# include "wce_errno.h"
#endif


#endif /* #ifndef WCEEX_ERRNO_WRAPPER_H */

