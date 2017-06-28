/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 BEA Systems
        Contributed by Thomas Hallgren <thallgre@bea.com>

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

/* Returns the text corresponding to the given err_code or the
   text "invalid error code" if the err_code is invalid.  */
const char *
unw_strerror (int err_code)
{
  const char *cp;
  unw_error_t error = (unw_error_t)-err_code;
  switch (error)
    {
    case UNW_ESUCCESS:     cp = "no error"; break;
    case UNW_EUNSPEC:      cp = "unspecified (general) error"; break;
    case UNW_ENOMEM:       cp = "out of memory"; break;
    case UNW_EBADREG:      cp = "bad register number"; break;
    case UNW_EREADONLYREG: cp = "attempt to write read-only register"; break;
    case UNW_ESTOPUNWIND:  cp = "stop unwinding"; break;
    case UNW_EINVALIDIP:   cp = "invalid IP"; break;
    case UNW_EBADFRAME:    cp = "bad frame"; break;
    case UNW_EINVAL:       cp = "unsupported operation or bad value"; break;
    case UNW_EBADVERSION:  cp = "unwind info has unsupported version"; break;
    case UNW_ENOINFO:      cp = "no unwind info found"; break;
    default:               cp = "invalid error code";
    }
  return cp;
}
