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

HIDDEN intrmask_t unwi_full_mask;

static const char rcsid[] UNUSED =
  "$Id: " PACKAGE_STRING " --- report bugs to " PACKAGE_BUGREPORT " $";

#if UNW_DEBUG

/* Must not be declared HIDDEN/PROTECTED because libunwind.so and
   libunwind-PLATFORM.so will both define their own copies of this
   variable and we want to use only one or the other when both
   libraries are loaded.  */
long unwi_debug_level;

#endif /* UNW_DEBUG */

HIDDEN void
mi_init (void)
{
#if UNW_DEBUG
  const char *str = getenv ("UNW_DEBUG_LEVEL");

  if (str)
    unwi_debug_level = atoi (str);

  if (unwi_debug_level > 0)
    {
      setbuf (stdout, NULL);
      setbuf (stderr, NULL);
    }
#endif

  assert (sizeof (struct cursor) <= sizeof (unw_cursor_t));
}
