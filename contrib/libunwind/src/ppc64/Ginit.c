/* libunwind - a platform-independent unwind library
   Copyright (C) 2006-2007 IBM
   Contributed by
     Corey Ashford <cjashfor@us.ibm.com>
     Jose Flavio Aguilar Paulino <jflavio@br.ibm.com> <joseflavio@gmail.com>

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

#include <stdlib.h>
#include <string.h>

#include "ucontext_i.h"
#include "unwind_i.h"

#ifdef UNW_REMOTE_ONLY

/* unw_local_addr_space is a NULL pointer in this case.  */
PROTECTED unw_addr_space_t unw_local_addr_space;

#else /* !UNW_REMOTE_ONLY */

static struct unw_addr_space local_addr_space;

PROTECTED unw_addr_space_t unw_local_addr_space = &local_addr_space;

static void *
uc_addr (ucontext_t *uc, int reg)
{
  void *addr;

  if ((unsigned) (reg - UNW_PPC64_R0) < 32)
    addr = &uc->uc_mcontext.gp_regs[reg - UNW_PPC64_R0];

  else if ((unsigned) (reg - UNW_PPC64_F0) < 32)
    addr = &uc->uc_mcontext.fp_regs[reg - UNW_PPC64_F0];

  else if ((unsigned) (reg - UNW_PPC64_V0) < 32)
    addr = (uc->uc_mcontext.v_regs == 0) ? NULL : &uc->uc_mcontext.v_regs->vrregs[reg - UNW_PPC64_V0][0];

  else
    {
      unsigned gregs_idx;

      switch (reg)
        {
        case UNW_PPC64_NIP:
          gregs_idx = NIP_IDX;
          break;
        case UNW_PPC64_CTR:
          gregs_idx = CTR_IDX;
          break;
        case UNW_PPC64_LR:
          gregs_idx = LINK_IDX;
          break;
        case UNW_PPC64_XER:
          gregs_idx = XER_IDX;
          break;
        case UNW_PPC64_CR0:
          gregs_idx = CCR_IDX;
          break;
        default:
          return NULL;
        }
      addr = &uc->uc_mcontext.gp_regs[gregs_idx];
    }
  return addr;
}

# ifdef UNW_LOCAL_ONLY

HIDDEN void *
tdep_uc_addr (ucontext_t *uc, int reg)
{
  return uc_addr (uc, reg);
}

# endif /* UNW_LOCAL_ONLY */

HIDDEN unw_dyn_info_list_t _U_dyn_info_list;


static void
put_unwind_info (unw_addr_space_t as, unw_proc_info_t *proc_info, void *arg)
{
  /* it's a no-op */
}

static int
get_dyn_info_list_addr (unw_addr_space_t as, unw_word_t *dyn_info_list_addr,
                        void *arg)
{
  *dyn_info_list_addr = (unw_word_t) &_U_dyn_info_list;
  return 0;
}

static int
access_mem (unw_addr_space_t as, unw_word_t addr, unw_word_t *val, int write,
            void *arg)
{
  if (write)
    {
      Debug (12, "mem[%lx] <- %lx\n", addr, *val);
      *(unw_word_t *) addr = *val;
    }
  else
    {
      *val = *(unw_word_t *) addr;
      Debug (12, "mem[%lx] -> %lx\n", addr, *val);
    }
  return 0;
}

static int
access_reg (unw_addr_space_t as, unw_regnum_t reg, unw_word_t *val,
            int write, void *arg)
{
  unw_word_t *addr;
  ucontext_t *uc = arg;

  if (UNW_PPC64_F0 <= reg && reg <= UNW_PPC64_F31)
    goto badreg;
  if (UNW_PPC64_V0 <= reg && reg <= UNW_PPC64_V31)
    goto badreg;

  addr = uc_addr (uc, reg);
  if (!addr)
    goto badreg;

  if (write)
    {
      *(unw_word_t *) addr = *val;
      Debug (12, "%s <- %lx\n", unw_regname (reg), *val);
    }
  else
    {
      *val = *(unw_word_t *) addr;
      Debug (12, "%s -> %lx\n", unw_regname (reg), *val);
    }
  return 0;

badreg:
  Debug (1, "bad register number %u\n", reg);
  return -UNW_EBADREG;
}

static int
access_fpreg (unw_addr_space_t as, unw_regnum_t reg, unw_fpreg_t *val,
              int write, void *arg)
{
  ucontext_t *uc = arg;
  unw_fpreg_t *addr;

  /* Allow only 32 fregs and 32 vregs */
  if (!(((unsigned) (reg - UNW_PPC64_F0) < 32)
	||((unsigned) (reg - UNW_PPC64_V0) < 32)))
    goto badreg;

  addr = uc_addr (uc, reg);
  if (!addr)
    goto badreg;

  if (write)
    {
      Debug (12, "%s <- %016Lf\n", unw_regname (reg), *val);
      *(unw_fpreg_t *) addr = *val;
    }
  else
    {
      *val = *(unw_fpreg_t *) addr;
      Debug (12, "%s -> %016Lf\n", unw_regname (reg), *val);
    }
  return 0;

badreg:
  Debug (1, "bad register number %u\n", reg);
  /* attempt to access a non-preserved register */
  return -UNW_EBADREG;
}

static int
get_static_proc_name (unw_addr_space_t as, unw_word_t ip,
                      char *buf, size_t buf_len, unw_word_t *offp,
                      void *arg)
{
  return _Uelf64_get_proc_name (as, getpid (), ip, buf, buf_len, offp);
}

HIDDEN void
ppc64_local_addr_space_init (void)
{
  memset (&local_addr_space, 0, sizeof (local_addr_space));
  local_addr_space.big_endian = (__BYTE_ORDER == __BIG_ENDIAN);
#if _CALL_ELF == 2
  local_addr_space.abi = UNW_PPC64_ABI_ELFv2;
#else
  local_addr_space.abi = UNW_PPC64_ABI_ELFv1;
#endif
  local_addr_space.caching_policy = UNWI_DEFAULT_CACHING_POLICY;
  local_addr_space.acc.find_proc_info = dwarf_find_proc_info;
  local_addr_space.acc.put_unwind_info = put_unwind_info;
  local_addr_space.acc.get_dyn_info_list_addr = get_dyn_info_list_addr;
  local_addr_space.acc.access_mem = access_mem;
  local_addr_space.acc.access_reg = access_reg;
  local_addr_space.acc.access_fpreg = access_fpreg;
  local_addr_space.acc.resume = ppc64_local_resume;
  local_addr_space.acc.get_proc_name = get_static_proc_name;
  unw_flush_cache (&local_addr_space, 0, 0);
}

#endif /* !UNW_REMOTE_ONLY */
