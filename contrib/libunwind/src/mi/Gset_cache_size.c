/* libunwind - a platform-independent unwind library
   Copyright (C) 2014
        Contributed by Milian Wolff <address@hidden>
                   and Dave Watson <dade.watson@gmail.com>

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

PROTECTED int
unw_set_cache_size (unw_addr_space_t as, size_t size, int flag)
{
  size_t power = 1;
  unsigned short log_size = 0;

  if (!tdep_init_done)
    tdep_init ();

  if (flag != 0)
    return -1;

  /* Currently not supported for per-thread cache due to memory leak */
  /* A pthread-key destructor would work, but is not signal safe */
#if defined(HAVE___THREAD) && HAVE___THREAD
  return -1;
#endif

  /* Round up to next power of two, slowly but portably */
  while(power < size)
    {
      power *= 2;
      log_size++;
      /* Largest size currently supported by rs_cache */
      if (log_size >= 15)
        break;
    }

#if !defined(__ia64__)
  if (log_size == as->global_cache.log_size)
    return 0;   /* no change */

  as->global_cache.log_size = log_size;
#endif

  /* Ensure caches are empty (and initialized).  */
  unw_flush_cache (as, 0, 0);
#ifdef __ia64__
  return 0;
#else
  /* Synchronously purge cache, to ensure memory is allocated */
  return dwarf_flush_rs_cache(&as->global_cache);
#endif
}
