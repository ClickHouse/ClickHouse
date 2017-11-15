/* libunwind - a platform-independent unwind library

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

#ifndef libunwind_coredump_h
#define libunwind_coredump_h

#include <libunwind.h>

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

/* Helper routines which make it easy to use libunwind on a coredump.
   They're available only if UNW_REMOTE_ONLY is _not_ defined and they
   aren't really part of the libunwind API.  They are implemented in a
   archive library called libunwind-coredump.a.  */

struct UCD_info;

extern struct UCD_info *_UCD_create(const char *filename);
extern void _UCD_destroy(struct UCD_info *);

extern int _UCD_get_num_threads(struct UCD_info *);
extern void _UCD_select_thread(struct UCD_info *, int);
extern pid_t _UCD_get_pid(struct UCD_info *);
extern int _UCD_get_cursig(struct UCD_info *);
extern int _UCD_add_backing_file_at_segment(struct UCD_info *, int phdr_no, const char *filename);
extern int _UCD_add_backing_file_at_vaddr(struct UCD_info *,
                                          unsigned long vaddr,
                                          const char *filename);

extern int _UCD_find_proc_info (unw_addr_space_t, unw_word_t,
                                unw_proc_info_t *, int, void *);
extern void _UCD_put_unwind_info (unw_addr_space_t, unw_proc_info_t *, void *);
extern int _UCD_get_dyn_info_list_addr (unw_addr_space_t, unw_word_t *,
                                        void *);
extern int _UCD_access_mem (unw_addr_space_t, unw_word_t, unw_word_t *, int,
                            void *);
extern int _UCD_access_reg (unw_addr_space_t, unw_regnum_t, unw_word_t *,
                            int, void *);
extern int _UCD_access_fpreg (unw_addr_space_t, unw_regnum_t, unw_fpreg_t *,
                              int, void *);
extern int _UCD_get_proc_name (unw_addr_space_t, unw_word_t, char *, size_t,
                               unw_word_t *, void *);
extern int _UCD_resume (unw_addr_space_t, unw_cursor_t *, void *);
extern unw_accessors_t _UCD_accessors;


#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* libunwind_coredump_h */
