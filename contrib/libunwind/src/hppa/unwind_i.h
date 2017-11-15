/* libunwind - a platform-independent unwind library
   Copyright (C) 2004-2005 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

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

#include <libunwind-hppa.h>

#include "libunwind_i.h"

#define hppa_lock                       UNW_OBJ(lock)
#define hppa_local_resume               UNW_OBJ(local_resume)
#define hppa_local_addr_space_init      UNW_OBJ(local_addr_space_init)
#define hppa_scratch_loc                UNW_OBJ(scratch_loc)
#define setcontext                      UNW_ARCH_OBJ (setcontext)

extern void hppa_local_addr_space_init (void);
extern int hppa_local_resume (unw_addr_space_t as, unw_cursor_t *cursor,
                              void *arg);
extern dwarf_loc_t hppa_scratch_loc (struct cursor *c, unw_regnum_t reg);
extern int setcontext (const ucontext_t *ucp);

#endif /* unwind_i_h */
