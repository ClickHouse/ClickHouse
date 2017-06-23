/* libunwind - a platform-independent unwind library
   Copyright (C) 2002 Hewlett-Packard Co
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

#include <libunwind-x86.h>

#include "libunwind_i.h"

/* DWARF column numbers: */
#define EAX     0
#define ECX     1
#define EDX     2
#define EBX     3
#define ESP     4
#define EBP     5
#define ESI     6
#define EDI     7
#define EIP     8
#define EFLAGS  9
#define TRAPNO  10
#define ST0     11

#define x86_lock                        UNW_OBJ(lock)
#define x86_local_resume                UNW_OBJ(local_resume)
#define x86_local_addr_space_init       UNW_OBJ(local_addr_space_init)
#define x86_scratch_loc                 UNW_OBJ(scratch_loc)
#define x86_get_scratch_loc             UNW_OBJ(get_scratch_loc)
#define x86_r_uc_addr                   UNW_OBJ(r_uc_addr)

extern void x86_local_addr_space_init (void);
extern int x86_local_resume (unw_addr_space_t as, unw_cursor_t *cursor,
                             void *arg);
extern dwarf_loc_t x86_scratch_loc (struct cursor *c, unw_regnum_t reg);
extern dwarf_loc_t x86_get_scratch_loc (struct cursor *c, unw_regnum_t reg);
extern void *x86_r_uc_addr (ucontext_t *uc, int reg);

#endif /* unwind_i_h */
