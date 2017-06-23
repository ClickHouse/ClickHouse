/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2005 Hewlett-Packard Co
   Copyright (C) 2007 David Mosberger-Tang
        Contributed by David Mosberger-Tang <dmosberger@gmail.com>

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

#include "unwind_i.h"

#ifdef HAVE_SYS_UC_ACCESS_H
# include <sys/uc_access.h>
#endif

#ifdef UNW_REMOTE_ONLY

/* unw_local_addr_space is a NULL pointer in this case.  */
PROTECTED unw_addr_space_t unw_local_addr_space;

#else /* !UNW_REMOTE_ONLY */

static struct unw_addr_space local_addr_space;

PROTECTED unw_addr_space_t unw_local_addr_space = &local_addr_space;

#ifdef HAVE_SYS_UC_ACCESS_H

#else /* !HAVE_SYS_UC_ACCESS_H */

HIDDEN void *
tdep_uc_addr (ucontext_t *uc, int reg, uint8_t *nat_bitnr)
{
  return inlined_uc_addr (uc, reg, nat_bitnr);
}

#endif /* !HAVE_SYS_UC_ACCESS_H */

static void
put_unwind_info (unw_addr_space_t as, unw_proc_info_t *proc_info, void *arg)
{
  /* it's a no-op */
}

static int
get_dyn_info_list_addr (unw_addr_space_t as, unw_word_t *dyn_info_list_addr,
                        void *arg)
{
#ifndef UNW_LOCAL_ONLY
# pragma weak _U_dyn_info_list_addr
  if (!_U_dyn_info_list_addr)
    return -UNW_ENOINFO;
#endif
  *dyn_info_list_addr = _U_dyn_info_list_addr ();
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

#ifdef HAVE_SYS_UC_ACCESS_H

#define SYSCALL_CFM_SAVE_REG    11 /* on a syscall, ar.pfs is saved in r11 */
#define REASON_SYSCALL          0

static int
access_reg (unw_addr_space_t as, unw_regnum_t reg, unw_word_t *val, int write,
            void *arg)
{
  ucontext_t *uc = arg;
  unsigned int nat, mask;
  uint64_t value;
  uint16_t reason;
  int ret;

  __uc_get_reason (uc, &reason);

  switch (reg)
    {
    case UNW_IA64_GR  ... UNW_IA64_GR + 31:
      if ((ret = __uc_get_grs (uc, (reg - UNW_IA64_GR), 1, &value, &nat)))
        break;

      if (write)
        ret = __uc_set_grs (uc, (reg - UNW_IA64_GR), 1, val, nat);
      else
        *val = value;
      break;

    case UNW_IA64_NAT ... UNW_IA64_NAT + 31:
      if ((ret = __uc_get_grs (uc, (reg - UNW_IA64_GR), 1, &value, &nat)))
        break;

      mask = 1 << (reg - UNW_IA64_GR);

      if (write)
        {
          if (*val)
            nat |= mask;
          else
            nat &= ~mask;
          ret = __uc_set_grs (uc, (reg - UNW_IA64_GR), 1, &value, nat);
        }
      else
        *val = (nat & mask) != 0;
      break;

    case UNW_IA64_AR  ... UNW_IA64_AR + 127:
      if (reg == UNW_IA64_AR_BSP)
        {
          if (write)
            ret = __uc_set_ar (uc, (reg - UNW_IA64_AR), *val);
          else
            ret = __uc_get_ar (uc, (reg - UNW_IA64_AR), val);
        }
      else if (reg == UNW_IA64_AR_PFS && reason == REASON_SYSCALL)
        {
          /* As of HP-UX 11.22, getcontext() does not have unwind info
             and because of that, we need to hack thins manually here.
             Hopefully, this is OK because the HP-UX kernel also needs
             to know where AR.PFS has been saved, so the use of
             register r11 for this purpose is pretty much nailed
             down.  */
          if (write)
            ret = __uc_set_grs (uc, SYSCALL_CFM_SAVE_REG, 1, val, 0);
          else
            ret = __uc_get_grs (uc, SYSCALL_CFM_SAVE_REG, 1, val, &nat);
        }
      else
        {
          if (write)
            ret = __uc_set_ar (uc, (reg - UNW_IA64_AR), *val);
          else
            ret = __uc_get_ar (uc, (reg - UNW_IA64_AR), val);
        }
      break;

    case UNW_IA64_BR  ... UNW_IA64_BR + 7:
      if (write)
        ret = __uc_set_brs (uc, (reg - UNW_IA64_BR), 1, val);
      else
        ret = __uc_get_brs (uc, (reg - UNW_IA64_BR), 1, val);
      break;

    case UNW_IA64_PR:
      if (write)
        ret = __uc_set_prs (uc, *val);
      else
        ret = __uc_get_prs (uc, val);
      break;

    case UNW_IA64_IP:
      if (write)
        ret = __uc_set_ip (uc, *val);
      else
        ret = __uc_get_ip (uc, val);
      break;

    case UNW_IA64_CFM:
      if (write)
        ret = __uc_set_cfm (uc, *val);
      else
        ret = __uc_get_cfm (uc, val);
      break;

    case UNW_IA64_FR  ... UNW_IA64_FR + 127:
    default:
      ret = EINVAL;
      break;
    }

  if (ret != 0)
    {
      Debug (1, "failed to %s %s (ret = %d)\n",
             write ? "write" : "read", unw_regname (reg), ret);
      return -UNW_EBADREG;
    }

  if (write)
    Debug (12, "%s <- %lx\n", unw_regname (reg), *val);
  else
    Debug (12, "%s -> %lx\n", unw_regname (reg), *val);
  return 0;
}

static int
access_fpreg (unw_addr_space_t as, unw_regnum_t reg, unw_fpreg_t *val,
              int write, void *arg)
{
  ucontext_t *uc = arg;
  fp_regval_t fp_regval;
  int ret;

  switch (reg)
    {
    case UNW_IA64_FR  ... UNW_IA64_FR + 127:
      if (write)
        {
          memcpy (&fp_regval, val, sizeof (fp_regval));
          ret = __uc_set_frs (uc, (reg - UNW_IA64_FR), 1, &fp_regval);
        }
      else
        {
          ret = __uc_get_frs (uc, (reg - UNW_IA64_FR), 1, &fp_regval);
          memcpy (val, &fp_regval, sizeof (*val));
        }
      break;

    default:
      ret = EINVAL;
      break;
    }
  if (ret != 0)
    return -UNW_EBADREG;

  return 0;
}

#else /* !HAVE_SYS_UC_ACCESS_H */

static int
access_reg (unw_addr_space_t as, unw_regnum_t reg, unw_word_t *val, int write,
            void *arg)
{
  unw_word_t *addr, mask;
  ucontext_t *uc = arg;

  if (reg >= UNW_IA64_NAT + 4 && reg <= UNW_IA64_NAT + 7)
    {
      mask = ((unw_word_t) 1) << (reg - UNW_IA64_NAT);
      if (write)
        {
          if (*val)
            uc->uc_mcontext.sc_nat |= mask;
          else
            uc->uc_mcontext.sc_nat &= ~mask;
        }
      else
        *val = (uc->uc_mcontext.sc_nat & mask) != 0;

      if (write)
        Debug (12, "%s <- %lx\n", unw_regname (reg), *val);
      else
        Debug (12, "%s -> %lx\n", unw_regname (reg), *val);
      return 0;
    }

  addr = tdep_uc_addr (uc, reg, NULL);
  if (!addr)
    goto badreg;

  if (write)
    {
      if (ia64_read_only_reg (addr))
        {
          Debug (16, "attempt to write read-only register\n");
          return -UNW_EREADONLYREG;
        }
      *addr = *val;
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

  if (reg < UNW_IA64_FR || reg >= UNW_IA64_FR + 128)
    goto badreg;

  addr = tdep_uc_addr (uc, reg, NULL);
  if (!addr)
    goto badreg;

  if (write)
    {
      if (ia64_read_only_reg (addr))
        {
          Debug (16, "attempt to write read-only register\n");
          return -UNW_EREADONLYREG;
        }
      *addr = *val;
      Debug (12, "%s <- %016lx.%016lx\n",
             unw_regname (reg), val->raw.bits[1], val->raw.bits[0]);
    }
  else
    {
      *val = *(unw_fpreg_t *) addr;
      Debug (12, "%s -> %016lx.%016lx\n",
             unw_regname (reg), val->raw.bits[1], val->raw.bits[0]);
    }
  return 0;

 badreg:
  Debug (1, "bad register number %u\n", reg);
  /* attempt to access a non-preserved register */
  return -UNW_EBADREG;
}

#endif /* !HAVE_SYS_UC_ACCESS_H */

static int
get_static_proc_name (unw_addr_space_t as, unw_word_t ip,
                      char *buf, size_t buf_len, unw_word_t *offp,
                      void *arg)
{
  return _Uelf64_get_proc_name (as, getpid (), ip, buf, buf_len, offp);
}

HIDDEN void
ia64_local_addr_space_init (void)
{
  memset (&local_addr_space, 0, sizeof (local_addr_space));
  local_addr_space.big_endian = (__BYTE_ORDER == __BIG_ENDIAN);
#if defined(__linux)
  local_addr_space.abi = ABI_LINUX;
#elif defined(__hpux)
  local_addr_space.abi = ABI_HPUX;
#endif
  local_addr_space.caching_policy = UNW_CACHE_GLOBAL;
  local_addr_space.acc.find_proc_info = tdep_find_proc_info;
  local_addr_space.acc.put_unwind_info = put_unwind_info;
  local_addr_space.acc.get_dyn_info_list_addr = get_dyn_info_list_addr;
  local_addr_space.acc.access_mem = access_mem;
  local_addr_space.acc.access_reg = access_reg;
  local_addr_space.acc.access_fpreg = access_fpreg;
  local_addr_space.acc.resume = ia64_local_resume;
  local_addr_space.acc.get_proc_name = get_static_proc_name;
  unw_flush_cache (&local_addr_space, 0, 0);
}

#endif /* !UNW_REMOTE_ONLY */

#ifndef UNW_LOCAL_ONLY

HIDDEN int
ia64_uc_access_reg (struct cursor *c, ia64_loc_t loc, unw_word_t *valp,
                    int write)
{
#ifdef HAVE_SYS_UC_ACCESS_H
  unw_word_t uc_addr = IA64_GET_AUX_ADDR (loc);
  ucontext_t *ucp;
  int ret;

  Debug (16, "%s location %s\n",
         write ? "writing" : "reading", ia64_strloc (loc));

  if (c->as == unw_local_addr_space)
    ucp = (ucontext_t *) uc_addr;
  else
    {
      unw_word_t *dst, src;

      /* Need to copy-in ucontext_t first.  */
      ucp = alloca (sizeof (ucontext_t));
      if (!ucp)
        return -UNW_ENOMEM;

      /* For now, there is no non-HP-UX implementation of the
         uc_access(3) interface.  Because of that, we cannot, e.g.,
         unwind an HP-UX program from a Linux program.  Should that
         become possible at some point in the future, the
         copy-in/copy-out needs to be adjusted to do byte-swapping if
         necessary. */
      assert (c->as->big_endian == (__BYTE_ORDER == __BIG_ENDIAN));

      dst = (unw_word_t *) ucp;
      for (src = uc_addr; src < uc_addr + sizeof (ucontext_t); src += 8)
        if ((ret = (*c->as->acc.access_mem) (c->as, src, dst++, 0, c->as_arg))
            < 0)
          return ret;
    }

  if (IA64_IS_REG_LOC (loc))
    ret = access_reg (unw_local_addr_space, IA64_GET_REG (loc), valp, write,
                      ucp);
  else
    {
      /* Must be an access to the RSE backing store in ucontext_t.  */
      unw_word_t addr = IA64_GET_ADDR (loc);

      if (write)
        ret = __uc_set_rsebs (ucp, (uint64_t *) addr, 1, valp);
      else
        ret = __uc_get_rsebs (ucp, (uint64_t *) addr, 1, valp);
      if (ret != 0)
        ret = -UNW_EBADREG;
    }
  if (ret < 0)
    return ret;

  if (write && c->as != unw_local_addr_space)
    {
      /* need to copy-out ucontext_t: */
      unw_word_t dst, *src = (unw_word_t *) ucp;
      for (dst = uc_addr; dst < uc_addr + sizeof (ucontext_t); dst += 8)
        if ((ret = (*c->as->acc.access_mem) (c->as, dst, src++, 1, c->as_arg))
            < 0)
          return ret;
    }
  return 0;
#else /* !HAVE_SYS_UC_ACCESS_H */
  return -UNW_EINVAL;
#endif /* !HAVE_SYS_UC_ACCESS_H */
}

HIDDEN int
ia64_uc_access_fpreg (struct cursor *c, ia64_loc_t loc, unw_fpreg_t *valp,
                      int write)
{
#ifdef HAVE_SYS_UC_ACCESS_H
  unw_word_t uc_addr = IA64_GET_AUX_ADDR (loc);
  ucontext_t *ucp;
  int ret;

  if (c->as == unw_local_addr_space)
    ucp = (ucontext_t *) uc_addr;
  else
    {
      unw_word_t *dst, src;

      /* Need to copy-in ucontext_t first.  */
      ucp = alloca (sizeof (ucontext_t));
      if (!ucp)
        return -UNW_ENOMEM;

      /* For now, there is no non-HP-UX implementation of the
         uc_access(3) interface.  Because of that, we cannot, e.g.,
         unwind an HP-UX program from a Linux program.  Should that
         become possible at some point in the future, the
         copy-in/copy-out needs to be adjusted to do byte-swapping if
         necessary. */
      assert (c->as->big_endian == (__BYTE_ORDER == __BIG_ENDIAN));

      dst = (unw_word_t *) ucp;
      for (src = uc_addr; src < uc_addr + sizeof (ucontext_t); src += 8)
        if ((ret = (*c->as->acc.access_mem) (c->as, src, dst++, 0, c->as_arg))
            < 0)
          return ret;
    }

  if ((ret = access_fpreg (unw_local_addr_space, IA64_GET_REG (loc), valp,
                           write, ucp)) < 0)
    return ret;

  if (write && c->as != unw_local_addr_space)
    {
      /* need to copy-out ucontext_t: */
      unw_word_t dst, *src = (unw_word_t *) ucp;
      for (dst = uc_addr; dst < uc_addr + sizeof (ucontext_t); dst += 8)
        if ((ret = (*c->as->acc.access_mem) (c->as, dst, src++, 1, c->as_arg))
            < 0)
          return ret;
    }
  return 0;
#else /* !HAVE_SYS_UC_ACCESS_H */
  return -UNW_EINVAL;
#endif /* !HAVE_SYS_UC_ACCESS_H */
}

#endif /* UNW_LOCAL_ONLY */
