/* OpenACC Runtime Library User-facing Declarations

   Copyright (C) 2013-2018 Free Software Foundation, Inc.

   Contributed by Mentor Embedded.

   This file is part of the GNU Offloading and Multi Processing Library
   (libgomp).

   Libgomp is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   Libgomp is distributed in the hope that it will be useful, but WITHOUT ANY
   WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
   FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
   more details.

   Under Section 7 of GPL version 3, you are granted additional
   permissions described in the GCC Runtime Library Exception, version
   3.1, as published by the Free Software Foundation.

   You should have received a copy of the GNU General Public License and
   a copy of the GCC Runtime Library Exception along with this program;
   see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _OPENACC_H
#define _OPENACC_H 1

/* The OpenACC standard is silent on whether or not including <openacc.h>
   might or must not include other header files.  We chose to include
   some.  */
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#if __cplusplus >= 201103
# define __GOACC_NOTHROW noexcept
#elif __cplusplus
# define __GOACC_NOTHROW throw ()
#else /* Not C++ */
# define __GOACC_NOTHROW __attribute__ ((__nothrow__))
#endif

/* Types */
typedef enum acc_device_t {
  /* Keep in sync with include/gomp-constants.h.  */
  acc_device_none = 0,
  acc_device_default = 1,
  acc_device_host = 2,
  /* acc_device_host_nonshm = 3 removed.  */
  acc_device_not_host = 4,
  acc_device_nvidia = 5,
  _ACC_device_hwm,
  /* Ensure enumeration is layout compatible with int.  */
  _ACC_highest = __INT_MAX__,
  _ACC_neg = -1
} acc_device_t;

typedef enum acc_async_t {
  /* Keep in sync with include/gomp-constants.h.  */
  acc_async_noval = -1,
  acc_async_sync  = -2
} acc_async_t;

int acc_get_num_devices (acc_device_t) __GOACC_NOTHROW;
void acc_set_device_type (acc_device_t) __GOACC_NOTHROW;
acc_device_t acc_get_device_type (void) __GOACC_NOTHROW;
void acc_set_device_num (int, acc_device_t) __GOACC_NOTHROW;
int acc_get_device_num (acc_device_t) __GOACC_NOTHROW;
int acc_async_test (int) __GOACC_NOTHROW;
int acc_async_test_all (void) __GOACC_NOTHROW;
void acc_wait (int) __GOACC_NOTHROW;
void acc_async_wait (int) __GOACC_NOTHROW;
void acc_wait_async (int, int) __GOACC_NOTHROW;
void acc_wait_all (void) __GOACC_NOTHROW;
void acc_async_wait_all (void) __GOACC_NOTHROW;
void acc_wait_all_async (int) __GOACC_NOTHROW;
void acc_init (acc_device_t) __GOACC_NOTHROW;
void acc_shutdown (acc_device_t) __GOACC_NOTHROW;
#ifdef __cplusplus
int acc_on_device (int __arg) __GOACC_NOTHROW;
#else
int acc_on_device (acc_device_t __arg) __GOACC_NOTHROW;
#endif
void *acc_malloc (size_t) __GOACC_NOTHROW;
void acc_free (void *) __GOACC_NOTHROW;
/* Some of these would be more correct with const qualifiers, but
   the standard specifies otherwise.  */
void *acc_copyin (void *, size_t) __GOACC_NOTHROW;
void *acc_present_or_copyin (void *, size_t) __GOACC_NOTHROW;
void *acc_pcopyin (void *, size_t) __GOACC_NOTHROW;
void *acc_create (void *, size_t) __GOACC_NOTHROW;
void *acc_present_or_create (void *, size_t) __GOACC_NOTHROW;
void *acc_pcreate (void *, size_t) __GOACC_NOTHROW;
void acc_copyout (void *, size_t) __GOACC_NOTHROW;
void acc_delete (void *, size_t) __GOACC_NOTHROW;
void acc_update_device (void *, size_t) __GOACC_NOTHROW;
void acc_update_self (void *, size_t) __GOACC_NOTHROW;
void acc_map_data (void *, void *, size_t) __GOACC_NOTHROW;
void acc_unmap_data (void *) __GOACC_NOTHROW;
void *acc_deviceptr (void *) __GOACC_NOTHROW;
void *acc_hostptr (void *) __GOACC_NOTHROW;
int acc_is_present (void *, size_t) __GOACC_NOTHROW;
void acc_memcpy_to_device (void *, void *, size_t) __GOACC_NOTHROW;
void acc_memcpy_from_device (void *, void *, size_t) __GOACC_NOTHROW;

/* CUDA-specific routines.  */
void *acc_get_current_cuda_device (void) __GOACC_NOTHROW;
void *acc_get_current_cuda_context (void) __GOACC_NOTHROW;
void *acc_get_cuda_stream (int) __GOACC_NOTHROW;
int acc_set_cuda_stream (int, void *) __GOACC_NOTHROW;

#ifdef __cplusplus
}

/* Forwarding function with correctly typed arg.  */

#pragma acc routine seq
inline int acc_on_device (acc_device_t __arg) __GOACC_NOTHROW
{
  return acc_on_device ((int) __arg);
}
#endif

#endif /* _OPENACC_H */
