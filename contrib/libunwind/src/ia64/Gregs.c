/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2005 Hewlett-Packard Co
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

#include "offsets.h"
#include "regs.h"
#include "unwind_i.h"

static inline ia64_loc_t
linux_scratch_loc (struct cursor *c, unw_regnum_t reg, uint8_t *nat_bitnr)
{
#if !defined(UNW_LOCAL_ONLY) || defined(__linux)
  unw_word_t addr = c->sigcontext_addr, flags, tmp_addr;
  int i;

  if (ia64_get_abi_marker (c) == ABI_MARKER_LINUX_SIGTRAMP
      || ia64_get_abi_marker (c) == ABI_MARKER_OLD_LINUX_SIGTRAMP)
    {
      switch (reg)
        {
        case UNW_IA64_NAT + 2 ... UNW_IA64_NAT + 3:
        case UNW_IA64_NAT + 8 ... UNW_IA64_NAT + 31:
          /* Linux sigcontext contains the NaT bit of scratch register
             N in bit position N of the sc_nat member. */
          *nat_bitnr = (reg - UNW_IA64_NAT);
          addr += LINUX_SC_NAT_OFF;
          break;

        case UNW_IA64_GR +  2 ... UNW_IA64_GR + 3:
        case UNW_IA64_GR +  8 ... UNW_IA64_GR + 31:
          addr += LINUX_SC_GR_OFF + 8 * (reg - UNW_IA64_GR);
          break;

        case UNW_IA64_FR + 6 ... UNW_IA64_FR + 15:
          addr += LINUX_SC_FR_OFF + 16 * (reg - UNW_IA64_FR);
          return IA64_LOC_ADDR (addr, IA64_LOC_TYPE_FP);

        case UNW_IA64_FR + 32 ... UNW_IA64_FR + 127:
          if (ia64_get (c, IA64_LOC_ADDR (addr + LINUX_SC_FLAGS_OFF, 0),
                        &flags) < 0)
            return IA64_NULL_LOC;

          if (!(flags & IA64_SC_FLAG_FPH_VALID))
            {
              /* initialize fph partition: */
              tmp_addr = addr + LINUX_SC_FR_OFF + 32*16;
              for (i = 32; i < 128; ++i, tmp_addr += 16)
                if (ia64_putfp (c, IA64_LOC_ADDR (tmp_addr, 0),
                                unw.read_only.f0) < 0)
                  return IA64_NULL_LOC;
              /* mark fph partition as valid: */
              if (ia64_put (c, IA64_LOC_ADDR (addr + LINUX_SC_FLAGS_OFF, 0),
                            flags | IA64_SC_FLAG_FPH_VALID) < 0)
                return IA64_NULL_LOC;
            }

          addr += LINUX_SC_FR_OFF + 16 * (reg - UNW_IA64_FR);
          return IA64_LOC_ADDR (addr, IA64_LOC_TYPE_FP);

        case UNW_IA64_BR + 0: addr += LINUX_SC_BR_OFF + 0; break;
        case UNW_IA64_BR + 6: addr += LINUX_SC_BR_OFF + 6*8; break;
        case UNW_IA64_BR + 7: addr += LINUX_SC_BR_OFF + 7*8; break;
        case UNW_IA64_AR_RSC: addr += LINUX_SC_AR_RSC_OFF; break;
        case UNW_IA64_AR_CSD: addr += LINUX_SC_AR_CSD_OFF; break;
        case UNW_IA64_AR_SSD: addr += LINUX_SC_AR_SSD_OFF; break;
        case UNW_IA64_AR_CCV: addr += LINUX_SC_AR_CCV; break;

        default:
          if (unw_is_fpreg (reg))
            return IA64_FPREG_LOC (c, reg);
          else
            return IA64_REG_LOC (c, reg);
        }
      return IA64_LOC_ADDR (addr, 0);
    }
  else
    {
      int is_nat = 0;

      if ((unsigned) (reg - UNW_IA64_NAT) < 128)
        {
          is_nat = 1;
          reg -= (UNW_IA64_NAT - UNW_IA64_GR);
        }
      if (ia64_get_abi_marker (c) == ABI_MARKER_LINUX_INTERRUPT)
        {
          switch (reg)
            {
            case UNW_IA64_BR + 6 ... UNW_IA64_BR + 7:
              addr += LINUX_PT_B6_OFF + 8 * (reg - (UNW_IA64_BR + 6));
              break;

            case UNW_IA64_AR_CSD: addr += LINUX_PT_CSD_OFF; break;
            case UNW_IA64_AR_SSD: addr += LINUX_PT_SSD_OFF; break;

            case UNW_IA64_GR +  8 ... UNW_IA64_GR + 11:
              addr += LINUX_PT_R8_OFF + 8 * (reg - (UNW_IA64_GR + 8));
              break;

            case UNW_IA64_IP: addr += LINUX_PT_IIP_OFF; break;
            case UNW_IA64_CFM: addr += LINUX_PT_IFS_OFF; break;
            case UNW_IA64_AR_UNAT: addr += LINUX_PT_UNAT_OFF; break;
            case UNW_IA64_AR_PFS: addr += LINUX_PT_PFS_OFF; break;
            case UNW_IA64_AR_RSC: addr += LINUX_PT_RSC_OFF; break;
            case UNW_IA64_AR_RNAT: addr += LINUX_PT_RNAT_OFF; break;
            case UNW_IA64_AR_BSPSTORE: addr += LINUX_PT_BSPSTORE_OFF; break;
            case UNW_IA64_PR: addr += LINUX_PT_PR_OFF; break;
            case UNW_IA64_BR + 0: addr += LINUX_PT_B0_OFF; break;

            case UNW_IA64_GR + 1:
              /* The saved r1 value is valid only in the frame in which
                 it was saved; for everything else we need to look up
                 the appropriate gp value.  */
              if (c->sigcontext_addr != c->sp + 0x10)
                return IA64_NULL_LOC;
              addr += LINUX_PT_R1_OFF;
              break;

            case UNW_IA64_GR + 12: addr += LINUX_PT_R12_OFF; break;
            case UNW_IA64_GR + 13: addr += LINUX_PT_R13_OFF; break;
            case UNW_IA64_AR_FPSR: addr += LINUX_PT_FPSR_OFF; break;
            case UNW_IA64_GR + 15: addr += LINUX_PT_R15_OFF; break;
            case UNW_IA64_GR + 14: addr += LINUX_PT_R14_OFF; break;
            case UNW_IA64_GR + 2: addr += LINUX_PT_R2_OFF; break;
            case UNW_IA64_GR + 3: addr += LINUX_PT_R3_OFF; break;

            case UNW_IA64_GR + 16 ... UNW_IA64_GR + 31:
              addr += LINUX_PT_R16_OFF + 8 * (reg - (UNW_IA64_GR + 16));
              break;

            case UNW_IA64_AR_CCV: addr += LINUX_PT_CCV_OFF; break;

            case UNW_IA64_FR + 6 ... UNW_IA64_FR + 11:
              addr += LINUX_PT_F6_OFF + 16 * (reg - (UNW_IA64_FR + 6));
              return IA64_LOC_ADDR (addr, IA64_LOC_TYPE_FP);

            default:
              if (unw_is_fpreg (reg))
                return IA64_FPREG_LOC (c, reg);
              else
                return IA64_REG_LOC (c, reg);
            }
        }
      else if (ia64_get_abi_marker (c) == ABI_MARKER_OLD_LINUX_INTERRUPT)
        {
          switch (reg)
            {
            case UNW_IA64_GR +  1:
              /* The saved r1 value is valid only in the frame in which
                 it was saved; for everything else we need to look up
                 the appropriate gp value.  */
              if (c->sigcontext_addr != c->sp + 0x10)
                return IA64_NULL_LOC;
              addr += LINUX_OLD_PT_R1_OFF;
              break;

            case UNW_IA64_GR +  2 ... UNW_IA64_GR + 3:
              addr += LINUX_OLD_PT_R2_OFF + 8 * (reg - (UNW_IA64_GR + 2));
              break;

            case UNW_IA64_GR +  8 ... UNW_IA64_GR + 11:
              addr += LINUX_OLD_PT_R8_OFF + 8 * (reg - (UNW_IA64_GR + 8));
              break;

            case UNW_IA64_GR + 16 ... UNW_IA64_GR + 31:
              addr += LINUX_OLD_PT_R16_OFF + 8 * (reg - (UNW_IA64_GR + 16));
              break;

            case UNW_IA64_FR + 6 ... UNW_IA64_FR + 9:
              addr += LINUX_OLD_PT_F6_OFF + 16 * (reg - (UNW_IA64_FR + 6));
              return IA64_LOC_ADDR (addr, IA64_LOC_TYPE_FP);

            case UNW_IA64_BR + 0: addr += LINUX_OLD_PT_B0_OFF; break;
            case UNW_IA64_BR + 6: addr += LINUX_OLD_PT_B6_OFF; break;
            case UNW_IA64_BR + 7: addr += LINUX_OLD_PT_B7_OFF; break;

            case UNW_IA64_AR_RSC: addr += LINUX_OLD_PT_RSC_OFF; break;
            case UNW_IA64_AR_CCV: addr += LINUX_OLD_PT_CCV_OFF; break;

            default:
              if (unw_is_fpreg (reg))
                return IA64_FPREG_LOC (c, reg);
              else
                return IA64_REG_LOC (c, reg);
            }
        }
      if (is_nat)
        {
          /* For Linux pt-regs structure, bit number is determined by
             the UNaT slot number (as determined by st8.spill) and the
             bits are saved wherever the (primary) UNaT was saved.  */
          *nat_bitnr = ia64_unat_slot_num (addr);
          return c->loc[IA64_REG_PRI_UNAT_MEM];
        }
      return IA64_LOC_ADDR (addr, 0);
    }
#endif
  return IA64_NULL_LOC;
}

static inline ia64_loc_t
hpux_scratch_loc (struct cursor *c, unw_regnum_t reg, uint8_t *nat_bitnr)
{
#if !defined(UNW_LOCAL_ONLY) || defined(__hpux)
  return IA64_LOC_UC_REG (reg, c->sigcontext_addr);
#else
  return IA64_NULL_LOC;
#endif
}

HIDDEN ia64_loc_t
ia64_scratch_loc (struct cursor *c, unw_regnum_t reg, uint8_t *nat_bitnr)
{
  if (c->sigcontext_addr)
    {
      if (ia64_get_abi (c) == ABI_LINUX)
        return linux_scratch_loc (c, reg, nat_bitnr);
      else if (ia64_get_abi (c) ==  ABI_HPUX)
        return hpux_scratch_loc (c, reg, nat_bitnr);
      else
        return IA64_NULL_LOC;
    }
  else
    return IA64_REG_LOC (c, reg);
}

static inline int
update_nat (struct cursor *c, ia64_loc_t nat_loc, unw_word_t mask,
            unw_word_t *valp, int write)
{
  unw_word_t nat_word;
  int ret;

  ret = ia64_get (c, nat_loc, &nat_word);
  if (ret < 0)
    return ret;

  if (write)
    {
      if (*valp)
        nat_word |= mask;
      else
        nat_word &= ~mask;
      ret = ia64_put (c, nat_loc, nat_word);
    }
  else
    *valp = (nat_word & mask) != 0;
  return ret;
}

static int
access_nat (struct cursor *c,
            ia64_loc_t nat_loc, ia64_loc_t reg_loc, uint8_t nat_bitnr,
            unw_word_t *valp, int write)
{
  unw_word_t mask = 0;
  unw_fpreg_t tmp;
  int ret;

  if (IA64_IS_FP_LOC (reg_loc))
    {
      /* NaT bit is saved as a NaTVal.  This happens when a general
         register is saved to a floating-point register.  */
      if (write)
        {
          if (*valp)
            {
              if (ia64_is_big_endian (c))
                ret = ia64_putfp (c, reg_loc, unw.nat_val_be);
              else
                ret = ia64_putfp (c, reg_loc, unw.nat_val_le);
            }
          else
            {
              unw_word_t *src, *dst;
              unw_fpreg_t tmp;

              ret = ia64_getfp (c, reg_loc, &tmp);
              if (ret < 0)
                return ret;

              /* Reset the exponent to 0x1003e so that the significand
                 will be interpreted as an integer value.  */
              src = (unw_word_t *) &unw.int_val_be;
              dst = (unw_word_t *) &tmp;
              if (!ia64_is_big_endian (c))
                ++src, ++dst;
              *dst = *src;

              ret = ia64_putfp (c, reg_loc, tmp);
            }
        }
      else
        {
          ret = ia64_getfp (c, reg_loc, &tmp);
          if (ret < 0)
            return ret;

          if (ia64_is_big_endian (c))
            *valp = (memcmp (&tmp, &unw.nat_val_be, sizeof (tmp)) == 0);
          else
            *valp = (memcmp (&tmp, &unw.nat_val_le, sizeof (tmp)) == 0);
        }
      return ret;
    }

  if ((IA64_IS_REG_LOC (nat_loc)
       && (unsigned) (IA64_GET_REG (nat_loc) - UNW_IA64_NAT) < 128)
      || IA64_IS_UC_LOC (reg_loc))
    {
      if (write)
        return ia64_put (c, nat_loc, *valp);
      else
        return ia64_get (c, nat_loc, valp);
    }

  if (IA64_IS_NULL_LOC (nat_loc))
    {
      /* NaT bit is not saved. This happens if a general register is
         saved to a branch register.  Since the NaT bit gets lost, we
         need to drop it here, too.  Note that if the NaT bit had been
         set when the save occurred, it would have caused a NaT
         consumption fault.  */
      if (write)
        {
          if (*valp)
            return -UNW_EBADREG;        /* can't set NaT bit */
        }
      else
        *valp = 0;
      return 0;
    }

  mask = (unw_word_t) 1 << nat_bitnr;
  return update_nat (c, nat_loc, mask, valp, write);
}

HIDDEN int
tdep_access_reg (struct cursor *c, unw_regnum_t reg, unw_word_t *valp,
                 int write)
{
  ia64_loc_t loc, reg_loc, nat_loc;
  unw_word_t mask, val;
  uint8_t nat_bitnr;
  int ret;

  switch (reg)
    {
      /* frame registers: */

    case UNW_IA64_BSP:
      if (write)
        c->bsp = *valp;
      else
        *valp = c->bsp;
      return 0;

    case UNW_REG_SP:
      if (write)
        c->sp = *valp;
      else
        *valp = c->sp;
      return 0;

    case UNW_REG_IP:
      if (write)
        {
          c->ip = *valp;        /* also update the IP cache */
          if (c->pi_valid && (*valp < c->pi.start_ip || *valp >= c->pi.end_ip))
            c->pi_valid = 0;    /* new IP outside of current proc */
        }
      loc = c->loc[IA64_REG_IP];
      break;

      /* preserved registers: */

    case UNW_IA64_GR + 4 ... UNW_IA64_GR + 7:
      loc = c->loc[IA64_REG_R4 + (reg - (UNW_IA64_GR + 4))];
      break;

    case UNW_IA64_NAT + 4 ... UNW_IA64_NAT + 7:
      loc = c->loc[IA64_REG_NAT4 + (reg - (UNW_IA64_NAT + 4))];
      reg_loc = c->loc[IA64_REG_R4 + (reg - (UNW_IA64_NAT + 4))];
      nat_bitnr = c->nat_bitnr[reg - (UNW_IA64_NAT + 4)];
      return access_nat (c, loc, reg_loc, nat_bitnr, valp, write);

    case UNW_IA64_AR_BSP:       loc = c->loc[IA64_REG_BSP]; break;
    case UNW_IA64_AR_BSPSTORE:  loc = c->loc[IA64_REG_BSPSTORE]; break;
    case UNW_IA64_AR_PFS:       loc = c->loc[IA64_REG_PFS]; break;
    case UNW_IA64_AR_RNAT:      loc = c->loc[IA64_REG_RNAT]; break;
    case UNW_IA64_AR_UNAT:      loc = c->loc[IA64_REG_UNAT]; break;
    case UNW_IA64_AR_LC:        loc = c->loc[IA64_REG_LC]; break;
    case UNW_IA64_AR_FPSR:      loc = c->loc[IA64_REG_FPSR]; break;
    case UNW_IA64_BR + 1:       loc = c->loc[IA64_REG_B1]; break;
    case UNW_IA64_BR + 2:       loc = c->loc[IA64_REG_B2]; break;
    case UNW_IA64_BR + 3:       loc = c->loc[IA64_REG_B3]; break;
    case UNW_IA64_BR + 4:       loc = c->loc[IA64_REG_B4]; break;
    case UNW_IA64_BR + 5:       loc = c->loc[IA64_REG_B5]; break;

    case UNW_IA64_CFM:
      if (write)
        c->cfm = *valp; /* also update the CFM cache */
      loc = c->cfm_loc;
      break;

    case UNW_IA64_PR:
      /*
       * Note: broad-side access to the predicates is NOT rotated
       * (i.e., it is done as if CFM.rrb.pr == 0.
       */
      if (write)
        {
          c->pr = *valp;                /* update the predicate cache */
          return ia64_put (c, c->loc[IA64_REG_PR], *valp);
        }
      else
        return ia64_get (c, c->loc[IA64_REG_PR], valp);

    case UNW_IA64_GR + 32 ... UNW_IA64_GR + 127:        /* stacked reg */
      reg = rotate_gr (c, reg - UNW_IA64_GR);
      if (reg < 0)
        return -UNW_EBADREG;
      ret = ia64_get_stacked (c, reg, &loc, NULL);
      if (ret < 0)
        return ret;
      break;

    case UNW_IA64_NAT + 32 ... UNW_IA64_NAT + 127:      /* stacked reg */
      reg = rotate_gr (c, reg - UNW_IA64_NAT);
      if (reg < 0)
        return -UNW_EBADREG;
      ret = ia64_get_stacked (c, reg, &loc, &nat_loc);
      if (ret < 0)
        return ret;
      assert (!IA64_IS_REG_LOC (loc));
      mask = (unw_word_t) 1 << rse_slot_num (IA64_GET_ADDR (loc));
      return update_nat (c, nat_loc, mask, valp, write);

    case UNW_IA64_AR_EC:
      if ((ret = ia64_get (c, c->ec_loc, &val)) < 0)
        return ret;

      if (write)
        {
          val = ((val & ~((unw_word_t) 0x3f << 52)) | ((*valp & 0x3f) << 52));
          return ia64_put (c, c->ec_loc, val);
        }
      else
        {
          *valp = (val >> 52) & 0x3f;
          return 0;
        }

      /* scratch & special registers: */

    case UNW_IA64_GR + 0:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = 0;
      return 0;

    case UNW_IA64_NAT + 0:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = 0;
      return 0;

    case UNW_IA64_NAT + 1:
    case UNW_IA64_NAT + 2 ... UNW_IA64_NAT + 3:
    case UNW_IA64_NAT + 8 ... UNW_IA64_NAT + 31:
      loc = ia64_scratch_loc (c, reg, &nat_bitnr);
      if (IA64_IS_NULL_LOC (loc) && reg == UNW_IA64_NAT + 1)
        {
          /* access to GP */
          if (write)
            return -UNW_EREADONLYREG;
          *valp = 0;
          return 0;
        }
      if (!(IA64_IS_REG_LOC (loc) || IA64_IS_UC_LOC (loc)
            || IA64_IS_FP_LOC (loc)))
        /* We're dealing with a NaT bit stored in memory.  */
        return update_nat(c, loc, (unw_word_t) 1 << nat_bitnr, valp, write);
      break;

    case UNW_IA64_GR + 15 ... UNW_IA64_GR + 18:
      mask = 1 << (reg - (UNW_IA64_GR + 15));
      if (write)
        {
          c->eh_args[reg - (UNW_IA64_GR + 15)] = *valp;
          c->eh_valid_mask |= mask;
          return 0;
        }
      else if ((c->eh_valid_mask & mask) != 0)
        {
          *valp = c->eh_args[reg - (UNW_IA64_GR + 15)];
          return 0;
        }
      else
        loc = ia64_scratch_loc (c, reg, NULL);
      break;

    case UNW_IA64_GR +  1:                              /* global pointer */
    case UNW_IA64_GR +  2 ... UNW_IA64_GR + 3:
    case UNW_IA64_GR +  8 ... UNW_IA64_GR + 14:
    case UNW_IA64_GR + 19 ... UNW_IA64_GR + 31:
    case UNW_IA64_BR + 0:
    case UNW_IA64_BR + 6:
    case UNW_IA64_BR + 7:
    case UNW_IA64_AR_RSC:
    case UNW_IA64_AR_CSD:
    case UNW_IA64_AR_SSD:
    case UNW_IA64_AR_CCV:
      loc = ia64_scratch_loc (c, reg, NULL);
      if (IA64_IS_NULL_LOC (loc) && reg == UNW_IA64_GR + 1)
        {
          /* access to GP */
          if (write)
            return -UNW_EREADONLYREG;

          /* ensure c->pi is up-to-date: */
          if ((ret = ia64_make_proc_info (c)) < 0)
            return ret;
          *valp = c->pi.gp;
          return 0;
        }
      break;

    default:
      Debug (1, "bad register number %d\n", reg);
      return -UNW_EBADREG;
    }

  if (write)
    return ia64_put (c, loc, *valp);
  else
    return ia64_get (c, loc, valp);
}

HIDDEN int
tdep_access_fpreg (struct cursor *c, int reg, unw_fpreg_t *valp,
                   int write)
{
  ia64_loc_t loc;

  switch (reg)
    {
    case UNW_IA64_FR + 0:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = unw.read_only.f0;
      return 0;

    case UNW_IA64_FR + 1:
      if (write)
        return -UNW_EREADONLYREG;

      if (ia64_is_big_endian (c))
        *valp = unw.read_only.f1_be;
      else
        *valp = unw.read_only.f1_le;
      return 0;

    case UNW_IA64_FR + 2: loc = c->loc[IA64_REG_F2]; break;
    case UNW_IA64_FR + 3: loc = c->loc[IA64_REG_F3]; break;
    case UNW_IA64_FR + 4: loc = c->loc[IA64_REG_F4]; break;
    case UNW_IA64_FR + 5: loc = c->loc[IA64_REG_F5]; break;

    case UNW_IA64_FR + 16 ... UNW_IA64_FR + 31:
      loc = c->loc[IA64_REG_F16 + (reg - (UNW_IA64_FR + 16))];
      break;

    case UNW_IA64_FR + 6 ... UNW_IA64_FR + 15:
      loc = ia64_scratch_loc (c, reg, NULL);
      break;

    case UNW_IA64_FR + 32 ... UNW_IA64_FR + 127:
      reg = rotate_fr (c, reg - UNW_IA64_FR) + UNW_IA64_FR;
      loc = ia64_scratch_loc (c, reg, NULL);
      break;

    default:
      Debug (1, "bad register number %d\n", reg);
      return -UNW_EBADREG;
    }

  if (write)
    return ia64_putfp (c, loc, *valp);
  else
    return ia64_getfp (c, loc, valp);
}
