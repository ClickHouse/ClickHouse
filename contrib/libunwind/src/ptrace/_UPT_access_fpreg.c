/* libunwind - a platform-independent unwind library
   Copyright (C) 2003 Hewlett-Packard Co
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

#if HAVE_DECL_PTRACE_POKEUSER || HAVE_TTRACE
int
_UPT_access_fpreg (unw_addr_space_t as, unw_regnum_t reg, unw_fpreg_t *val,
                   int write, void *arg)
{
  unw_word_t *wp = (unw_word_t *) val;
  struct UPT_info *ui = arg;
  pid_t pid = ui->pid;
  int i;

  if ((unsigned) reg >= ARRAY_SIZE (_UPT_reg_offset))
    return -UNW_EBADREG;

  errno = 0;
  if (write)
    for (i = 0; i < (int) (sizeof (*val) / sizeof (wp[i])); ++i)
      {
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
        ptrace (PTRACE_POKEUSER, pid, _UPT_reg_offset[reg] + i * sizeof(wp[i]),
                wp[i]);
#endif
        if (errno)
          return -UNW_EBADREG;
      }
  else
    for (i = 0; i < (int) (sizeof (*val) / sizeof (wp[i])); ++i)
      {
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
        wp[i] = ptrace (PTRACE_PEEKUSER, pid,
                        _UPT_reg_offset[reg] + i * sizeof(wp[i]), 0);
#endif
        if (errno)
          return -UNW_EBADREG;
      }
  return 0;
}
#elif HAVE_DECL_PT_GETFPREGS
int
_UPT_access_fpreg (unw_addr_space_t as, unw_regnum_t reg, unw_fpreg_t *val,
                   int write, void *arg)
{
  struct UPT_info *ui = arg;
  pid_t pid = ui->pid;
  fpregset_t fpreg;

  if ((unsigned) reg >= ARRAY_SIZE (_UPT_reg_offset))
    return -UNW_EBADREG;

  if (ptrace(PT_GETFPREGS, pid, (caddr_t)&fpreg, 0) == -1)
          return -UNW_EBADREG;
  if (write) {
#if defined(__amd64__)
          memcpy(&fpreg.fpr_xacc[reg], val, sizeof(unw_fpreg_t));
#elif defined(__i386__)
          memcpy(&fpreg.fpr_acc[reg], val, sizeof(unw_fpreg_t));
#else
#error Fix me
#endif
          if (ptrace(PT_SETFPREGS, pid, (caddr_t)&fpreg, 0) == -1)
                  return -UNW_EBADREG;
  } else
#if defined(__amd64__)
          memcpy(val, &fpreg.fpr_xacc[reg], sizeof(unw_fpreg_t));
#elif defined(__i386__)
          memcpy(val, &fpreg.fpr_acc[reg], sizeof(unw_fpreg_t));
#else
#error Fix me
#endif
  return 0;
}
#else
#error Fix me
#endif
