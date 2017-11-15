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
_Unwind_RaiseException (struct _Unwind_Exception *exception_object)
{
  uint64_t exception_class = exception_object->exception_class;
  _Unwind_Personality_Fn personality;
  struct _Unwind_Context context;
  _Unwind_Reason_Code reason;
  unw_proc_info_t pi;
  unw_context_t uc;
  unw_word_t ip;
  int ret;

  Debug (1, "(exception_object=%p)\n", exception_object);

  if (_Unwind_InitContext (&context, &uc) < 0)
    return _URC_FATAL_PHASE1_ERROR;

  /* Phase 1 (search phase) */

  while (1)
    {
      if ((ret = unw_step (&context.cursor)) <= 0)
        {
          if (ret == 0)
            {
              Debug (1, "no handler found\n");
              return _URC_END_OF_STACK;
            }
          else
            return _URC_FATAL_PHASE1_ERROR;
        }

      if (unw_get_proc_info (&context.cursor, &pi) < 0)
        return _URC_FATAL_PHASE1_ERROR;

      personality = (_Unwind_Personality_Fn) (uintptr_t) pi.handler;
      if (personality)
        {
          reason = (*personality) (_U_VERSION, _UA_SEARCH_PHASE,
                                   exception_class, exception_object,
                                   &context);
          if (reason != _URC_CONTINUE_UNWIND)
            {
              if (reason == _URC_HANDLER_FOUND)
                break;
              else
                {
                  Debug (1, "personality returned %d\n", reason);
                  return _URC_FATAL_PHASE1_ERROR;
                }
            }
        }
    }

  /* Exceptions are associated with IP-ranges.  If a given exception
     is handled at a particular IP, it will _always_ be handled at
     that IP.  If this weren't true, we'd have to track the tuple
     (IP,SP,BSP) to uniquely identify the stack frame that's handling
     the exception.  */
  if (unw_get_reg (&context.cursor, UNW_REG_IP, &ip) < 0)
    return _URC_FATAL_PHASE1_ERROR;
  exception_object->private_1 = 0;      /* clear "stop" pointer */
  exception_object->private_2 = ip;     /* save frame marker */

  Debug (1, "found handler for IP=%lx; entering cleanup phase\n", (long) ip);

  /* Reset the cursor to the first frame: */
  if (unw_init_local (&context.cursor, &uc) < 0)
    return _URC_FATAL_PHASE1_ERROR;

  return _Unwind_Phase2 (exception_object, &context);
}

_Unwind_Reason_Code
__libunwind_Unwind_RaiseException (struct _Unwind_Exception *)
     ALIAS (_Unwind_RaiseException);
