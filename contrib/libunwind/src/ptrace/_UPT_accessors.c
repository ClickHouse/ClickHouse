/* libunwind - a platform-independent unwind library
   Copyright (C) 2003 Hewlett-Packard Co
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

#include "_UPT_internal.h"

PROTECTED unw_accessors_t _UPT_accessors =
  {
    .find_proc_info             = _UPT_find_proc_info,
    .put_unwind_info            = _UPT_put_unwind_info,
    .get_dyn_info_list_addr     = _UPT_get_dyn_info_list_addr,
    .access_mem                 = _UPT_access_mem,
    .access_reg                 = _UPT_access_reg,
    .access_fpreg               = _UPT_access_fpreg,
    .resume                     = _UPT_resume,
    .get_proc_name              = _UPT_get_proc_name
  };
