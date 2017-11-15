/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2002, 2005 Hewlett-Packard Co
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

#include "libunwind_i.h"

HIDDEN int
unwi_extract_dynamic_proc_info (unw_addr_space_t as, unw_word_t ip,
                                unw_proc_info_t *pi, unw_dyn_info_t *di,
                                int need_unwind_info, void *arg)
{
  pi->start_ip = di->start_ip;
  pi->end_ip = di->end_ip;
  pi->gp = di->gp;
  pi->format = di->format;
  switch (di->format)
    {
    case UNW_INFO_FORMAT_DYNAMIC:
      pi->handler = di->u.pi.handler;
      pi->lsda = 0;
      pi->flags = di->u.pi.flags;
      pi->unwind_info_size = 0;
      if (need_unwind_info)
        pi->unwind_info = di;
      else
        pi->unwind_info = NULL;
      return 0;

    case UNW_INFO_FORMAT_TABLE:
    case UNW_INFO_FORMAT_REMOTE_TABLE:
    case UNW_INFO_FORMAT_ARM_EXIDX:
    case UNW_INFO_FORMAT_IP_OFFSET:
#ifdef tdep_search_unwind_table
      /* call platform-specific search routine: */
      return tdep_search_unwind_table (as, ip, di, pi, need_unwind_info, arg);
#else
      /* fall through */
#endif
    default:
      break;
    }
  return -UNW_EINVAL;
}
