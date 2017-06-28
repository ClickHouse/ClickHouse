/* libunwind - a platform-independent unwind library
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>
   Copyright (C) 2013 Linaro Limited

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

#include "unwind_i.h"

static const char *const regname[] =
  {
    [UNW_AARCH64_X0] = "x0",
    [UNW_AARCH64_X1] = "x1",
    [UNW_AARCH64_X2] = "x2",
    [UNW_AARCH64_X3] = "x3",
    [UNW_AARCH64_X4] = "x4",
    [UNW_AARCH64_X5] = "x5",
    [UNW_AARCH64_X6] = "x6",
    [UNW_AARCH64_X7] = "x7",
    [UNW_AARCH64_X8] = "x8",
    [UNW_AARCH64_X9] = "x9",
    [UNW_AARCH64_X10] = "x10",
    [UNW_AARCH64_X11] = "x11",
    [UNW_AARCH64_X12] = "x12",
    [UNW_AARCH64_X13] = "x13",
    [UNW_AARCH64_X14] = "x14",
    [UNW_AARCH64_X15] = "x15",
    [UNW_AARCH64_X16] = "ip0",
    [UNW_AARCH64_X17] = "ip1",
    [UNW_AARCH64_X18] = "x18",
    [UNW_AARCH64_X19] = "x19",
    [UNW_AARCH64_X20] = "x20",
    [UNW_AARCH64_X21] = "x21",
    [UNW_AARCH64_X22] = "x22",
    [UNW_AARCH64_X23] = "x23",
    [UNW_AARCH64_X24] = "x24",
    [UNW_AARCH64_X25] = "x25",
    [UNW_AARCH64_X26] = "x26",
    [UNW_AARCH64_X27] = "x27",
    [UNW_AARCH64_X28] = "x28",
    [UNW_AARCH64_X29] = "fp",
    [UNW_AARCH64_X30] = "lr",
    [UNW_AARCH64_SP] = "sp",
    [UNW_AARCH64_PC] = "pc",
    [UNW_AARCH64_V0] = "v0",
    [UNW_AARCH64_V1] = "v1",
    [UNW_AARCH64_V2] = "v2",
    [UNW_AARCH64_V3] = "v3",
    [UNW_AARCH64_V4] = "v4",
    [UNW_AARCH64_V5] = "v5",
    [UNW_AARCH64_V6] = "v6",
    [UNW_AARCH64_V7] = "v7",
    [UNW_AARCH64_V8] = "v8",
    [UNW_AARCH64_V9] = "v9",
    [UNW_AARCH64_V10] = "v10",
    [UNW_AARCH64_V11] = "v11",
    [UNW_AARCH64_V12] = "v12",
    [UNW_AARCH64_V13] = "v13",
    [UNW_AARCH64_V14] = "v14",
    [UNW_AARCH64_V15] = "v15",
    [UNW_AARCH64_V16] = "v16",
    [UNW_AARCH64_V17] = "v17",
    [UNW_AARCH64_V18] = "v18",
    [UNW_AARCH64_V19] = "v19",
    [UNW_AARCH64_V20] = "v20",
    [UNW_AARCH64_V21] = "v21",
    [UNW_AARCH64_V22] = "v22",
    [UNW_AARCH64_V23] = "v23",
    [UNW_AARCH64_V24] = "v24",
    [UNW_AARCH64_V25] = "v25",
    [UNW_AARCH64_V26] = "v26",
    [UNW_AARCH64_V27] = "v27",
    [UNW_AARCH64_V28] = "v28",
    [UNW_AARCH64_V29] = "v29",
    [UNW_AARCH64_V30] = "v30",
    [UNW_AARCH64_V31] = "v31",
    [UNW_AARCH64_FPSR] = "fpsr",
    [UNW_AARCH64_FPCR] = "fpcr",
  };

PROTECTED const char *
unw_regname (unw_regnum_t reg)
{
  if (reg < (unw_regnum_t) ARRAY_SIZE (regname) && regname[reg] != NULL)
    return regname[reg];
  else
    return "???";
}
