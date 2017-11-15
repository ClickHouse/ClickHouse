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

#include "libunwind_i.h"
#include "elf64.h"

static unw_word_t
find_gp (struct elf_dyn_info *edi, Elf64_Phdr *pdyn, Elf64_Addr load_base)
{
  Elf64_Off soff, str_soff;
  Elf64_Ehdr *ehdr = edi->ei.image;
  Elf64_Shdr *shdr;
  Elf64_Shdr *str_shdr;
  Elf64_Addr gp = 0;
  char *strtab;
  int i;

  if (pdyn)
    {
      /* If we have a PT_DYNAMIC program header, fetch the gp-value
         from the DT_PLTGOT entry.  */
      Elf64_Dyn *dyn = (Elf64_Dyn *) (pdyn->p_offset + (char *) edi->ei.image);
      for (; dyn->d_tag != DT_NULL; ++dyn)
        if (dyn->d_tag == DT_PLTGOT)
          {
            gp = (Elf64_Addr) dyn->d_un.d_ptr + load_base;
            goto done;
          }
    }

  /* Without a PT_DYAMIC header, lets try to look for a non-empty .opd
     section.  If there is such a section, we know it's full of
     function descriptors, and we can simply pick up the gp from the
     second word of the first entry in this table.  */

  soff = ehdr->e_shoff;
  str_soff = soff + (ehdr->e_shstrndx * ehdr->e_shentsize);

  if (soff + ehdr->e_shnum * ehdr->e_shentsize > edi->ei.size)
    {
      Debug (1, "section table outside of image? (%lu > %lu)",
             soff + ehdr->e_shnum * ehdr->e_shentsize,
             edi->ei.size);
      goto done;
    }

  shdr = (Elf64_Shdr *) ((char *) edi->ei.image + soff);
  str_shdr = (Elf64_Shdr *) ((char *) edi->ei.image + str_soff);
  strtab = (char *) edi->ei.image + str_shdr->sh_offset;
  for (i = 0; i < ehdr->e_shnum; ++i)
    {
      if (strcmp (strtab + shdr->sh_name, ".opd") == 0
          && shdr->sh_size >= 16)
        {
          gp = ((Elf64_Addr *) ((char *) edi->ei.image + shdr->sh_offset))[1];
          goto done;
        }
      shdr = (Elf64_Shdr *) (((char *) shdr) + ehdr->e_shentsize);
    }

 done:
  Debug (16, "image at %p, gp = %lx\n", edi->ei.image, gp);
  return gp;
}

int
ia64_find_unwind_table (struct elf_dyn_info *edi, unw_addr_space_t as,
                         char *path, unw_word_t segbase, unw_word_t mapoff,
                         unw_word_t ip)
{
  Elf64_Phdr *phdr, *ptxt = NULL, *punw = NULL, *pdyn = NULL;
  Elf64_Ehdr *ehdr;
  int i;

  if (!_Uelf64_valid_object (&edi->ei))
    return -UNW_ENOINFO;

  ehdr = edi->ei.image;
  phdr = (Elf64_Phdr *) ((char *) edi->ei.image + ehdr->e_phoff);

  for (i = 0; i < ehdr->e_phnum; ++i)
    {
      switch (phdr[i].p_type)
        {
        case PT_LOAD:
          if (phdr[i].p_offset == mapoff)
            ptxt = phdr + i;
          break;

        case PT_IA_64_UNWIND:
          punw = phdr + i;
          break;

        case PT_DYNAMIC:
          pdyn = phdr + i;
          break;

        default:
          break;
        }
    }
  if (!ptxt || !punw)
    return 0;

  edi->di_cache.start_ip = segbase;
  edi->di_cache.end_ip = edi->di_cache.start_ip + ptxt->p_memsz;
  edi->di_cache.gp = find_gp (edi, pdyn, segbase - ptxt->p_vaddr);
  edi->di_cache.format = UNW_INFO_FORMAT_TABLE;
  edi->di_cache.u.ti.name_ptr = 0;
  edi->di_cache.u.ti.segbase = segbase;
  edi->di_cache.u.ti.table_len = punw->p_memsz / sizeof (unw_word_t);
  edi->di_cache.u.ti.table_data = (unw_word_t *)
    ((char *) edi->ei.image + (punw->p_vaddr - ptxt->p_vaddr));
  return 1;
}
