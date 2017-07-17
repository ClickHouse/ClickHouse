/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 Hewlett-Packard Co
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

#ifndef libunwind_ptrace_h
#define libunwind_ptrace_h

#include <libunwind.h>

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

/* Helper routines which make it easy to use libunwind via ptrace().
   They're available only if UNW_REMOTE_ONLY is _not_ defined and they
   aren't really part of the libunwind API.  They are implemented in a
   archive library called libunwind-ptrace.a.  */

extern void *_UPT_create (pid_t);
extern void _UPT_destroy (void *);
extern int _UPT_find_proc_info (unw_addr_space_t, unw_word_t,
                                unw_proc_info_t *, int, void *);
extern void _UPT_put_unwind_info (unw_addr_space_t, unw_proc_info_t *, void *);
extern int _UPT_get_dyn_info_list_addr (unw_addr_space_t, unw_word_t *,
                                        void *);
extern int _UPT_access_mem (unw_addr_space_t, unw_word_t, unw_word_t *, int,
                            void *);
extern int _UPT_access_reg (unw_addr_space_t, unw_regnum_t, unw_word_t *,
                            int, void *);
extern int _UPT_access_fpreg (unw_addr_space_t, unw_regnum_t, unw_fpreg_t *,
                              int, void *);
extern int _UPT_get_proc_name (unw_addr_space_t, unw_word_t, char *, size_t,
                               unw_word_t *, void *);
extern int _UPT_resume (unw_addr_space_t, unw_cursor_t *, void *);
extern unw_accessors_t _UPT_accessors;


#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* libunwind_ptrace_h */
