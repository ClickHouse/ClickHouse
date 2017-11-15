/* libunwind - a platform-independent unwind library
   Copyright (C) 2013 Garmin International
        Contributed by Matt Fischer <matt.fischer@garmin.com>

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

#include <string.h>

#include "libunwind_i.h"

struct cb_info
{
    unw_word_t ip;
    unsigned long segbase;
    unsigned long offset;
    const char *path;
};

static int callback(const struct dl_phdr_info *info, size_t size, void *data)
{
  int i;
  struct cb_info *cbi = (struct cb_info*)data;
  for(i=0; i<info->dlpi_phnum; i++) {
    int segbase = info->dlpi_addr + info->dlpi_phdr[i].p_vaddr;
    if(cbi->ip >= segbase && cbi->ip < segbase + info->dlpi_phdr[i].p_memsz)
    {
      cbi->path = info->dlpi_name;
      cbi->offset = info->dlpi_phdr[i].p_offset;
      cbi->segbase = segbase;
      return 1;
    }
  }

  return 0;
}

PROTECTED int
tdep_get_elf_image (struct elf_image *ei, pid_t pid, unw_word_t ip,
                    unsigned long *segbase, unsigned long *mapoff,
                    char *path, size_t pathlen)
{
  struct cb_info cbi;
  int ret = -1;
  cbi.ip = ip;
  cbi.segbase = 0;
  cbi.offset = 0;
  cbi.path = NULL;

  /* QNX's support for accessing symbol maps is severely broken.  There is
     a devctl() call that can be made on a proc node (DCMD_PROC_MAPDEBUG)
     which returns information similar to Linux's /proc/<pid>/maps
     node, however the filename that is returned by this call is not an
     absolute path, and there is no foolproof way to map the filename
     back to the file that it came from.

     Therefore, the normal approach for implementing this function,
     which works equally well for both local and remote unwinding,
     will not work here.  The only type of image lookup which works
     reliably is locally, using dl_iterate_phdr().  However, the only
     time that this function is required to look up a remote image is for
     ptrace support, which doesn't work on QNX anyway.  Local unwinding,
     which is the main case that makes use of this function, will work
     fine with dl_iterate_phdr().  Therefore, in lieu of any better
     platform support for remote image lookup, this function has just
     been implemented in terms of dl_iterate_phdr().
  */

  if (pid != getpid())
  {
    /* Return an error if an attempt is made to perform remote image lookup */
    return -1;
  }

  if (dl_iterate_phdr (callback, &cbi) != 0)
  {
    if (path)
    {
      strncpy (path, cbi.path, pathlen);
    }

    *mapoff = cbi.offset;
    *segbase = cbi.segbase;

    ret = elf_map_image (ei, cbi.path);
  }

  return ret;
}

#ifndef UNW_REMOTE_ONLY

PROTECTED void
tdep_get_exe_image_path (char *path)
{
  path[0] = 0; /* XXX */
}

#endif
