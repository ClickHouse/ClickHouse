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

#include "unwind_i.h"

/* Apply rotation to a general register.  REG must be in the range 0-127.  */

static inline int
rotate_gr (struct cursor *c, int reg)
{
  unsigned int rrb_gr, sor;
  int preg;

  sor = 8 * ((c->cfm >> 14) & 0xf);
  rrb_gr = (c->cfm >> 18) & 0x7f;

  if ((unsigned) (reg - 32) >= sor)
    preg = reg;
  else
    {
      preg = reg + rrb_gr;      /* apply rotation */
      if ((unsigned) (preg - 32) >= sor)
        preg -= sor;            /* wrap around */
    }
  if (sor)
    Debug (15, "sor=%u rrb.gr=%u, r%d -> r%d\n", sor, rrb_gr, reg, preg);
  return preg;
}

/* Apply rotation to a floating-point register.  The number REG must
   be in the range of 0-127.  */

static inline int
rotate_fr (struct cursor *c, int reg)
{
  unsigned int rrb_fr;
  int preg;

  rrb_fr = (c->cfm >> 25) & 0x7f;
  if (reg < 32)
    preg = reg;         /* register not part of the rotating partition */
  else
    {
      preg = reg + rrb_fr;      /* apply rotation */
      if (preg > 127)
        preg -= 96;             /* wrap around */
    }
  if (rrb_fr)
    Debug (15, "rrb.fr=%u, f%d -> f%d\n", rrb_fr, reg, preg);
  return preg;
}
