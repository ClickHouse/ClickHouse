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


/* Find the ELF image that contains IP and return the "closest"
   procedure name, if there is one.  With some caching, this could be
   sped up greatly, but until an application materializes that's
   sensitive to the performance of this routine, why bother...  */
static int
elf_w (CD_get_proc_name) (struct UCD_info *ui, unw_addr_space_t as, unw_word_t ip,
                       char *buf, size_t buf_len, unw_word_t *offp)
{
  unsigned long segbase, mapoff;
  int ret;

  /* Used to be tdep_get_elf_image() in ptrace unwinding code */
  coredump_phdr_t *cphdr = _UCD_get_elf_image(ui, ip);
  if (!cphdr)
    {
      Debug(1, "returns error: _UCD_get_elf_image failed\n");
      return -UNW_ENOINFO;
    }
  /* segbase: where it is mapped in virtual memory */
  /* mapoff: offset in the file */
  segbase = cphdr->p_vaddr;
  /*mapoff  = phdr->p_offset; WRONG! phdr->p_offset is the offset in COREDUMP file */
  mapoff  = 0;

  ret = elf_w (get_proc_name_in_image) (as, &ui->edi.ei, segbase, mapoff, ip, buf, buf_len, offp);

  return ret;
}

int
_UCD_get_proc_name (unw_addr_space_t as, unw_word_t ip,
                    char *buf, size_t buf_len, unw_word_t *offp, void *arg)
{
  struct UCD_info *ui = arg;

#if ELF_CLASS == ELFCLASS64
  return _Uelf64_CD_get_proc_name (ui, as, ip, buf, buf_len, offp);
#elif ELF_CLASS == ELFCLASS32
  return _Uelf32_CD_get_proc_name (ui, as, ip, buf, buf_len, offp);
#else
  return -UNW_ENOINFO;
#endif
}
