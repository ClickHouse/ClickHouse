/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2005 Hewlett-Packard Co
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

PROTECTED void
unw_flush_cache (unw_addr_space_t as, unw_word_t lo, unw_word_t hi)
{
#if !UNW_TARGET_IA64
  struct unw_debug_frame_list *w = as->debug_frames;
#endif

  /* clear dyn_info_list_addr cache: */
  as->dyn_info_list_addr = 0;

#if !UNW_TARGET_IA64
  for (; w; w = w->next)
    {
      if (w->index)
        free (w->index);
      free (w->debug_frame);
    }
  as->debug_frames = NULL;
#endif

  /* This lets us flush caches lazily.  The implementation currently
     ignores the flush range arguments (lo-hi).  This is OK because
     unw_flush_cache() is allowed to flush more than the requested
     range. */

#ifdef HAVE_FETCH_AND_ADD
  fetch_and_add1 (&as->cache_generation);
#else
# warning unw_flush_cache(): need a way to atomically increment an integer.
  ++as->cache_generation;
#endif
}
