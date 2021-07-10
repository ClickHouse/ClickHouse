/*
 * UFC-crypt: ultra fast crypt(3) implementation
 *
 * Copyright (C) 1991-2018 Free Software Foundation, Inc.
 *
 * The GNU C Library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The GNU C Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the GNU C Library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * @(#)crypt.h	1.5 12/20/96
 *
 */

#ifndef _CRYPT_H
#define _CRYPT_H	1

#include <features.h>

__BEGIN_DECLS

/* One-way hash PHRASE, returning a string suitable for storage in the
   user database.  SALT selects the one-way function to use, and
   ensures that no two users' hashes are the same, even if they use
   the same passphrase.  The return value points to static storage
   which will be overwritten by the next call to crypt.  */
extern char *crypt (const char *__phrase, const char *__salt)
     __THROW __nonnull ((1, 2));

#ifdef __USE_GNU

/* This structure provides scratch and output buffers for 'crypt_r'.
   Its contents should not be accessed directly.  */
struct crypt_data
  {
    char keysched[16 * 8];
    char sb0[32768];
    char sb1[32768];
    char sb2[32768];
    char sb3[32768];
    /* end-of-aligment-critical-data */
    char crypt_3_buf[14];
    char current_salt[2];
    long int current_saltbits;
    int  direction, initialized;
  };

/* Thread-safe version of 'crypt'.
   DATA must point to a 'struct crypt_data' allocated by the caller.
   Before the first call to 'crypt_r' with a new 'struct crypt_data',
   that object must be initialized to all zeroes.  The pointer
   returned, if not NULL, will point within DATA.  (It will still be
   overwritten by the next call to 'crypt_r' with the same DATA.)  */
extern char *crypt_r (const char *__phrase, const char *__salt,
		      struct crypt_data * __restrict __data)
     __THROW __nonnull ((1, 2, 3));
#endif

__END_DECLS

#endif	/* crypt.h */
