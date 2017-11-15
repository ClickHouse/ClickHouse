/* libunwind - a platform-independent unwind library
   Copyright (c) 2004 Hewlett-Packard Development Company, L.P.
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

#ifndef dwarf_config_h
#define dwarf_config_h

/* See DWARF_FRAME_REGNUM() macro in gcc/config/pa/pa32-regs.h: */
#define dwarf_to_unw_regnum(reg)                        \
  (((reg) < DWARF_NUM_PRESERVED_REGS) ? (reg) : 0)

/* This matches the value used by GCC (see
   gcc/config/pa/pa32-regs.h:FIRST_PSEUDO_REGISTER), which leaves
   plenty of room for expansion.  */
#define DWARF_NUM_PRESERVED_REGS        89

/* Return TRUE if the ADDR_SPACE uses big-endian byte-order.  */
#define dwarf_is_big_endian(addr_space) 1

/* Convert a pointer to a dwarf_cursor structure to a pointer to
   unw_cursor_t.  */
#define dwarf_to_cursor(c)      ((unw_cursor_t *) (c))

typedef struct dwarf_loc
  {
    unw_word_t val;
#ifndef UNW_LOCAL_ONLY
    unw_word_t type;            /* see X86_LOC_TYPE_* macros.  */
#endif
  }
dwarf_loc_t;

#endif /* dwarf_config_h */
