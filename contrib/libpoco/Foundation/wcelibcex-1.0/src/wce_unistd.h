/*
 * $Id$
 *
 * unistd.h - defines symbolic constants, types, and declares miscellaneous functions
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
#ifndef WCEEX_UNISTD_H
#define WCEEX_UNISTD_H 1

#if !defined(_WIN32_WCE)
# error "Only Winddows CE target is supported!"
#endif

/* Defines NULL pointer value */
#include <stddef.h>


/*
 * Symbolic constants for second argument to access() function.
 * All constants and following expressions R_OK|W_OK, R_OK|X_OK
 * and R_OK|W_OK|X_OK have distinct values.
 */

#define	R_OK	4		/* Test for read permission. */
#define	W_OK	2		/* Test for write permission. */
#define	X_OK	1		/* Test for execute (search) permission. */
#define	F_OK	0		/* Test for existence of file. */


/*
 * Variables used for communication with getopt() function 
 * to the caller.
 */

/* Argument associated with option */
extern char *optarg;    

/* Index into parent argv vector */
extern int optind;

/* If error message should be printed */
extern int opterr;

/* Character checked for validity */
extern int optopt;

#endif /* #ifndef WCEEX_UNISTD_H */