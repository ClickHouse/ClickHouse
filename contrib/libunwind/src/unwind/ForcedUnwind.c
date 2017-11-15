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

#include "unwind-internal.h"

PROTECTED _Unwind_Reason_Code
_Unwind_ForcedUnwind (struct _Unwind_Exception *exception_object,
                      _Unwind_Stop_Fn stop, void *stop_parameter)
{
  struct _Unwind_Context context;
  unw_context_t uc;

  /* We check "stop" here to tell the compiler's inliner that
     exception_object->private_1 isn't NULL when calling
     _Unwind_Phase2().  */
  if (!stop)
    return _URC_FATAL_PHASE2_ERROR;

  if (_Unwind_InitContext (&context, &uc) < 0)
    return _URC_FATAL_PHASE2_ERROR;

  exception_object->private_1 = (unsigned long) stop;
  exception_object->private_2 = (unsigned long) stop_parameter;

  return _Unwind_Phase2 (exception_object, &context);
}

_Unwind_Reason_Code __libunwind_Unwind_ForcedUnwind (struct _Unwind_Exception*,
                                                     _Unwind_Stop_Fn, void *)
     ALIAS (_Unwind_ForcedUnwind);
