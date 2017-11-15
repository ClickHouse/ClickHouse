/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
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

#include <libunwind.h>
#include <setjmp.h>

#include "jmpbuf.h"

/* Why use K&R syntax here?  setjmp() is often a macro and that
   expands into a call to, say, __setjmp() and we need to define the
   libunwind-version of setjmp() with the name of the actual function.
   Using K&R syntax lets us keep the setjmp() macro while keeping the
   syntax valid...  This trick works provided setjmp() doesn't do
   anything other than a function call.  */

int
setjmp (env)
     jmp_buf env;
{
  void **wp = (void **) env;

  /* this should work on most platforms, but may not be
     performance-optimal; check the code! */
  wp[JB_SP] = __builtin_frame_address (0);
  wp[JB_RP] = (void *) __builtin_return_address (0);
  return 0;
}
