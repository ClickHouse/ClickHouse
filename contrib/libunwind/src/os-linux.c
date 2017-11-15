/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2005 Hewlett-Packard Co
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

#include <limits.h>
#include <stdio.h>

#include "libunwind_i.h"
#include "os-linux.h"

PROTECTED int
tdep_get_elf_image (struct elf_image *ei, pid_t pid, unw_word_t ip,
                    unsigned long *segbase, unsigned long *mapoff,
                    char *path, size_t pathlen)
{
  struct map_iterator mi;
  int found = 0, rc;
  unsigned long hi;

  if (maps_init (&mi, pid) < 0)
    return -1;

  while (maps_next (&mi, segbase, &hi, mapoff))
    if (ip >= *segbase && ip < hi)
      {
        found = 1;
        break;
      }

  if (!found)
    {
      maps_close (&mi);
      return -1;
    }
  if (path)
    {
      strncpy(path, mi.path, pathlen);
    }
  rc = elf_map_image (ei, mi.path);
  maps_close (&mi);
  return rc;
}

#ifndef UNW_REMOTE_ONLY

PROTECTED void
tdep_get_exe_image_path (char *path)
{
  strcpy(path, "/proc/self/exe");
}

#endif /* !UNW_REMOTE_ONLY */
