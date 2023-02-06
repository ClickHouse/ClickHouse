/* 
 * $Id: wce_fcntl.h 20 2006-11-18 17:00:30Z mloskot $
 *
 * fcntl.h - file control options
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
#ifndef WCEEX_FCNTL_H
#define WCEEX_FCNTL_H	1

#if !defined(_WIN32_WCE)
# error "Only Windows CE target is supported!"
#endif

/* File control flags and functions
 *
 * Implementation based on the Open Group Base Specifications Issue 6,
 * IEEE Std 1003.1, 2004 Edition
 *
 * Windows CE Notes:
 *
 * Mostly, these flags are not used, but defined to feed compiler.
 * Also, some programs (ie. libtiff) translate textual file flags (r, rw, etc.)
 * file control flags.
 * Functions fcntl() and open() are not implemented.
 */

/* File creation flags */

#define O_CREAT        0x0100  /* Open or create file if it does not exist. */
#define O_EXCL         0x0200  /* Exclusive file use. */
#define O_NOCTTY	   0x0400  /* Do not assign controlling terminal. */
#define O_TRUNC        0x1000  /* Open and truncate file. */

/* File access and status flags */

#define O_RDONLY       0x0000  /* Open for read only. */
#define O_WRONLY       0x0001  /* Open for write only. */
#define O_RDWR         0x0002  /* Open for reading and writing. */
#define O_APPEND       0x2000  /* Set append mode. */


#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */





#ifdef __cplusplus
}
#endif  /* __cplusplus */

#endif /* #ifndef WCEEX_FCNTL_H */