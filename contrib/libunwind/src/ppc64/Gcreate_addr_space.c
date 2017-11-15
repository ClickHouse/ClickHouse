/* libunwind - a platform-independent unwind library
   Copyright (C) 2006-2007 IBM
   Contributed by
     Corey Ashford <cjashfor@us.ibm.com>
     Jose Flavio Aguilar Paulino <jflavio@br.ibm.com> <joseflavio@gmail.com>

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

#include <stdlib.h>

#include <libunwind_i.h>

PROTECTED unw_addr_space_t
unw_create_addr_space (unw_accessors_t *a, int byte_order)
{
#ifdef UNW_LOCAL_ONLY
  return NULL;
#else
  unw_addr_space_t as;

  /*
   * We support both big- and little-endian on Linux ppc64.
   */
  if (byte_order != 0
      && byte_order != __LITTLE_ENDIAN
      && byte_order != __BIG_ENDIAN)
    return NULL;

  as = malloc (sizeof (*as));
  if (!as)
    return NULL;

  memset (as, 0, sizeof (*as));

  as->acc = *a;

  if (byte_order == 0)
    /* use host default: */
    as->big_endian = (__BYTE_ORDER == __BIG_ENDIAN);
  else
    as->big_endian = (byte_order == __BIG_ENDIAN);

  /* FIXME!  There is no way to specify the ABI.
     Default to ELFv1 on big-endian and ELFv2 on little-endian.  */
  if (as->big_endian)
    as->abi = UNW_PPC64_ABI_ELFv1;
  else
    as->abi = UNW_PPC64_ABI_ELFv2;

  return as;
#endif
}
