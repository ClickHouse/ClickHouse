/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>
   Copyright (C) 2010 Konstantin Belousov <kib@freebsd.org>

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

#include "_UPT_internal.h"

#if HAVE_DECL_PTRACE_POKEDATA || HAVE_TTRACE
int
_UPT_access_mem (unw_addr_space_t as, unw_word_t addr, unw_word_t *val,
                 int write, void *arg)
{
  struct UPT_info *ui = arg;
  int    i, end;
  unw_word_t tmp_val;

  if (!ui)
        return -UNW_EINVAL;

  pid_t pid = ui->pid;

  // Some 32-bit archs have to define a 64-bit unw_word_t.
  // Callers of this function therefore expect a 64-bit
  // return value, but ptrace only returns a 32-bit value
  // in such cases.
  if (sizeof(long) == 4 && sizeof(unw_word_t) == 8)
    end = 2;
  else
    end = 1;

  for (i = 0; i < end; i++)
    {
      unw_word_t tmp_addr = i == 0 ? addr : addr + 4;

      errno = 0;
      if (write)
        {
#if __BYTE_ORDER == __LITTLE_ENDIAN
          tmp_val = i == 0 ? *val : *val >> 32;
#else
          tmp_val = i == 0 && end == 2 ? *val >> 32 : *val;
#endif

          Debug (16, "mem[%lx] <- %lx\n", (long) tmp_addr, (long) tmp_val);
#ifdef HAVE_TTRACE
#         warning No support for ttrace() yet.
#else
          ptrace (PTRACE_POKEDATA, pid, tmp_addr, tmp_val);
          if (errno)
            return -UNW_EINVAL;
#endif
        }
      else
        {
#ifdef HAVE_TTRACE
#         warning No support for ttrace() yet.
#else
          tmp_val = (unsigned long) ptrace (PTRACE_PEEKDATA, pid, tmp_addr, 0);

          if (i == 0)
              *val = 0;

#if __BYTE_ORDER == __LITTLE_ENDIAN
          *val |= tmp_val << (i * 32);
#else
          *val |= i == 0 && end == 2 ? tmp_val << 32 : tmp_val;
#endif

          if (errno)
            return -UNW_EINVAL;
#endif
          Debug (16, "mem[%lx] -> %lx\n", (long) tmp_addr, (long) tmp_val);
        }
    }
  return 0;
}
#elif HAVE_DECL_PT_IO
int
_UPT_access_mem (unw_addr_space_t as, unw_word_t addr, unw_word_t *val,
                 int write, void *arg)
{
  struct UPT_info *ui = arg;
  if (!ui)
        return -UNW_EINVAL;
  pid_t pid = ui->pid;
  struct ptrace_io_desc iod;

  iod.piod_offs = (void *)addr;
  iod.piod_addr = val;
  iod.piod_len = sizeof(*val);
  iod.piod_op = write ? PIOD_WRITE_D : PIOD_READ_D;
  if (write)
    Debug (16, "mem[%lx] <- %lx\n", (long) addr, (long) *val);
  if (ptrace(PT_IO, pid, (caddr_t)&iod, 0) == -1)
    return -UNW_EINVAL;
  if (!write)
     Debug (16, "mem[%lx] -> %lx\n", (long) addr, (long) *val);
  return 0;
}
#else
#error Fix me
#endif
