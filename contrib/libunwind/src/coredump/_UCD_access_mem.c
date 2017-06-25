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

#include "_UCD_lib.h"
#include "_UCD_internal.h"

int
_UCD_access_mem(unw_addr_space_t as, unw_word_t addr, unw_word_t *val,
                 int write, void *arg)
{
  if (write)
    {
      Debug(0, "write is not supported\n");
      return -UNW_EINVAL;
    }

  struct UCD_info *ui = arg;

  unw_word_t addr_last = addr + sizeof(*val)-1;
  coredump_phdr_t *phdr;
  unsigned i;
  for (i = 0; i < ui->phdrs_count; i++)
    {
      phdr = &ui->phdrs[i];
      if (phdr->p_vaddr <= addr && addr_last < phdr->p_vaddr + phdr->p_memsz)
        {
          goto found;
        }
    }
  Debug(1, "addr 0x%llx is unmapped\n", (unsigned long long)addr);
  return -UNW_EINVAL;

 found: ;

  const char *filename UNUSED;
  off_t fileofs;
  int fd;
  if (addr_last >= phdr->p_vaddr + phdr->p_filesz)
    {
      /* This part of mapped address space is not present in coredump file */
      /* Do we have it in the backup file? */
      if (phdr->backing_fd < 0)
        {
          Debug(1, "access to not-present data in phdr[%d]: addr:0x%llx\n",
                                i, (unsigned long long)addr
                        );
          return -UNW_EINVAL;
        }
      filename = phdr->backing_filename;
      fileofs = addr - phdr->p_vaddr;
      fd = phdr->backing_fd;
      goto read;
    }

  filename = ui->coredump_filename;
  fileofs = phdr->p_offset + (addr - phdr->p_vaddr);
  fd = ui->coredump_fd;
 read:
  if (lseek(fd, fileofs, SEEK_SET) != fileofs)
    goto read_error;
  if (read(fd, val, sizeof(*val)) != sizeof(*val))
    goto read_error;

  Debug(1, "0x%llx <- [addr:0x%llx fileofs:0x%llx]\n",
        (unsigned long long)(*val),
        (unsigned long long)addr,
        (unsigned long long)fileofs
  );
  return 0;

 read_error:
  Debug(1, "access out of file: addr:0x%llx fileofs:%llx file:'%s'\n",
        (unsigned long long)addr,
        (unsigned long long)fileofs,
        filename
  );
  return -UNW_EINVAL;
}
