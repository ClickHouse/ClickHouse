/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright (C) 2013 Linaro Limited

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#ifndef unwind_i_h
#define unwind_i_h

#include <stdint.h>

#include <libunwind-aarch64.h>

#include "libunwind_i.h"

/* DWARF column numbers for AArch64: */
#define X29     29
#define FP      29
#define X30     30
#define LR      30
#define SP      31

#define aarch64_lock                    UNW_OBJ(lock)
#define aarch64_local_resume            UNW_OBJ(local_resume)
#define aarch64_local_addr_space_init   UNW_OBJ(local_addr_space_init)

extern void aarch64_local_addr_space_init (void);
extern int aarch64_local_resume (unw_addr_space_t as, unw_cursor_t *cursor,
                             void *arg);

/* By-pass calls to access_mem() when known to be safe. */
#ifdef UNW_LOCAL_ONLY
# undef ACCESS_MEM_FAST
# define ACCESS_MEM_FAST(ret,validate,cur,addr,to)                     \
  do {                                                                 \
    if (unlikely(validate))                                            \
      (ret) = dwarf_get ((cur), DWARF_MEM_LOC ((cur), (addr)), &(to)); \
    else                                                               \
      (ret) = 0, (to) = *(unw_word_t *)(addr);                         \
  } while (0)
#endif

#define GET_FPCTX(uc) ((struct fpsimd_context *)(&uc->uc_mcontext.__reserved))

#endif /* unwind_i_h */
