/* libunwind - a platform-independent unwind library

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

#include "_UCD_lib.h"
#include "_UCD_internal.h"

static coredump_phdr_t *
CD_elf_map_image(struct UCD_info *ui, coredump_phdr_t *phdr)
{
  struct elf_image *ei = &ui->edi.ei;

  if (phdr->backing_fd < 0)
    {
      /* Note: coredump file contains only phdr->p_filesz bytes.
       * We want to map bigger area (phdr->p_memsz bytes) to make sure
       * these pages are allocated, but non-accessible.
       */
      /* addr, length, prot, flags, fd, fd_offset */
      ei->image = mmap(NULL, phdr->p_memsz, PROT_READ, MAP_PRIVATE, ui->coredump_fd, phdr->p_offset);
      if (ei->image == MAP_FAILED)
        {
          ei->image = NULL;
          return NULL;
        }
      ei->size = phdr->p_filesz;
      size_t remainder_len = phdr->p_memsz - phdr->p_filesz;
      if (remainder_len > 0)
        {
          void *remainder_base = (char*) ei->image + phdr->p_filesz;
          munmap(remainder_base, remainder_len);
        }
    } else {
      /* We have a backing file for this segment.
       * This file is always longer than phdr->p_memsz,
       * and if phdr->p_filesz !=0, first phdr->p_filesz bytes in coredump
       * are the same as first bytes in the file. (Thus no need to map coredump)
       * We map the entire file:
       * unwinding may need data which is past phdr->p_memsz bytes.
       */
      /* addr, length, prot, flags, fd, fd_offset */
      ei->image = mmap(NULL, phdr->backing_filesize, PROT_READ, MAP_PRIVATE, phdr->backing_fd, 0);
      if (ei->image == MAP_FAILED)
        {
          ei->image = NULL;
          return NULL;
        }
      ei->size = phdr->backing_filesize;
    }

  /* Check ELF header for sanity */
  if (!elf_w(valid_object)(ei))
    {
      munmap(ei->image, ei->size);
      ei->image = NULL;
      ei->size = 0;
      return NULL;
    }

  return phdr;
}

HIDDEN coredump_phdr_t *
_UCD_get_elf_image(struct UCD_info *ui, unw_word_t ip)
{
  unsigned i;
  for (i = 0; i < ui->phdrs_count; i++)
    {
      coredump_phdr_t *phdr = &ui->phdrs[i];
      if (phdr->p_vaddr <= ip && ip < phdr->p_vaddr + phdr->p_memsz)
        {
          phdr = CD_elf_map_image(ui, phdr);
          return phdr;
        }
    }
  return NULL;
}
