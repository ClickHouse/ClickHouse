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

#include <elf.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <sys/mman.h>

#include "_UPT_internal.h"

static int
get_unwind_info (struct elf_dyn_info *edi, pid_t pid, unw_addr_space_t as, unw_word_t ip)
{
  unsigned long segbase, mapoff;
  char path[PATH_MAX];

#if UNW_TARGET_IA64 && defined(__linux)
  if (!edi->ktab.start_ip && _Uia64_get_kernel_table (&edi->ktab) < 0)
    return -UNW_ENOINFO;

  if (edi->ktab.format != -1 && ip >= edi->ktab.start_ip && ip < edi->ktab.end_ip)
    return 0;
#endif

  if ((edi->di_cache.format != -1
       && ip >= edi->di_cache.start_ip && ip < edi->di_cache.end_ip)
#if UNW_TARGET_ARM
      || (edi->di_debug.format != -1
       && ip >= edi->di_arm.start_ip && ip < edi->di_arm.end_ip)
#endif
      || (edi->di_debug.format != -1
       && ip >= edi->di_debug.start_ip && ip < edi->di_debug.end_ip))
    return 0;

  invalidate_edi(edi);

  if (tdep_get_elf_image (&edi->ei, pid, ip, &segbase, &mapoff, path,
                          sizeof(path)) < 0)
    return -UNW_ENOINFO;

  /* Here, SEGBASE is the starting-address of the (mmap'ped) segment
     which covers the IP we're looking for.  */
  if (tdep_find_unwind_table (edi, as, path, segbase, mapoff, ip) < 0)
    return -UNW_ENOINFO;

  /* This can happen in corner cases where dynamically generated
     code falls into the same page that contains the data-segment
     and the page-offset of the code is within the first page of
     the executable.  */
  if (edi->di_cache.format != -1
      && (ip < edi->di_cache.start_ip || ip >= edi->di_cache.end_ip))
     edi->di_cache.format = -1;

  if (edi->di_debug.format != -1
      && (ip < edi->di_debug.start_ip || ip >= edi->di_debug.end_ip))
     edi->di_debug.format = -1;

  if (edi->di_cache.format == -1
#if UNW_TARGET_ARM
      && edi->di_arm.format == -1
#endif
      && edi->di_debug.format == -1)
    return -UNW_ENOINFO;

  return 0;
}

int
_UPT_find_proc_info (unw_addr_space_t as, unw_word_t ip, unw_proc_info_t *pi,
                     int need_unwind_info, void *arg)
{
  struct UPT_info *ui = arg;
  int ret = -UNW_ENOINFO;

  if (get_unwind_info (&ui->edi, ui->pid, as, ip) < 0)
    return -UNW_ENOINFO;

#if UNW_TARGET_IA64
  if (ui->edi.ktab.format != -1)
    {
      /* The kernel unwind table resides in local memory, so we have
         to use the local address space to search it.  Since
         _UPT_put_unwind_info() has no easy way of detecting this
         case, we simply make a copy of the unwind-info, so
         _UPT_put_unwind_info() can always free() the unwind-info
         without ill effects.  */
      ret = tdep_search_unwind_table (unw_local_addr_space, ip, &ui->edi.ktab, pi,
                                      need_unwind_info, arg);
      if (ret >= 0)
        {
          if (!need_unwind_info)
            pi->unwind_info = NULL;
          else
            {
              void *mem = malloc (pi->unwind_info_size);

              if (!mem)
                return -UNW_ENOMEM;
              memcpy (mem, pi->unwind_info, pi->unwind_info_size);
              pi->unwind_info = mem;
            }
        }
    }
#endif

  if (ret == -UNW_ENOINFO && ui->edi.di_cache.format != -1)
    ret = tdep_search_unwind_table (as, ip, &ui->edi.di_cache,
                                    pi, need_unwind_info, arg);

  if (ret == -UNW_ENOINFO && ui->edi.di_debug.format != -1)
    ret = tdep_search_unwind_table (as, ip, &ui->edi.di_debug, pi,
                                    need_unwind_info, arg);

#if UNW_TARGET_ARM
  if (ret == -UNW_ENOINFO && ui->edi.di_arm.format != -1)
    ret = tdep_search_unwind_table (as, ip, &ui->edi.di_arm, pi,
                                    need_unwind_info, arg);
#endif

  return ret;
}
