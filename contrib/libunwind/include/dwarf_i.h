#ifndef DWARF_I_H
#define DWARF_I_H

/* This file contains definitions that cannot be used in code outside
   of libunwind.  In particular, most inline functions are here
   because otherwise they'd generate unresolved references when the
   files are compiled with inlining disabled.  */

#include "dwarf.h"
#include "libunwind_i.h"

/* Unless we are told otherwise, assume that a "machine address" is
   the size of an unw_word_t.  */
#ifndef dwarf_addr_size
# define dwarf_addr_size(as) (sizeof (unw_word_t))
#endif

#ifndef dwarf_to_unw_regnum
# define dwarf_to_unw_regnum_map                UNW_OBJ (dwarf_to_unw_regnum_map)
extern const uint8_t dwarf_to_unw_regnum_map[DWARF_REGNUM_MAP_LENGTH];
/* REG is evaluated multiple times; it better be side-effects free!  */
# define dwarf_to_unw_regnum(reg)                                         \
  (((reg) < DWARF_REGNUM_MAP_LENGTH) ? dwarf_to_unw_regnum_map[reg] : 0)
#endif

#ifdef UNW_LOCAL_ONLY

/* In the local-only case, we can let the compiler directly access
   memory and don't need to worry about differing byte-order.  */

typedef union __attribute__ ((packed))
  {
    int8_t s8;
    int16_t s16;
    int32_t s32;
    int64_t s64;
    uint8_t u8;
    uint16_t u16;
    uint32_t u32;
    uint64_t u64;
    void *ptr;
  }
dwarf_misaligned_value_t;

static inline int
dwarf_reads8 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
              int8_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->s8;
  *addr += sizeof (mvp->s8);
  return 0;
}

static inline int
dwarf_reads16 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               int16_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->s16;
  *addr += sizeof (mvp->s16);
  return 0;
}

static inline int
dwarf_reads32 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               int32_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->s32;
  *addr += sizeof (mvp->s32);
  return 0;
}

static inline int
dwarf_reads64 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               int64_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->s64;
  *addr += sizeof (mvp->s64);
  return 0;
}

static inline int
dwarf_readu8 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
              uint8_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->u8;
  *addr += sizeof (mvp->u8);
  return 0;
}

static inline int
dwarf_readu16 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               uint16_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->u16;
  *addr += sizeof (mvp->u16);
  return 0;
}

static inline int
dwarf_readu32 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               uint32_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->u32;
  *addr += sizeof (mvp->u32);
  return 0;
}

static inline int
dwarf_readu64 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               uint64_t *val, void *arg)
{
  dwarf_misaligned_value_t *mvp = (void *) (uintptr_t) *addr;

  *val = mvp->u64;
  *addr += sizeof (mvp->u64);
  return 0;
}

#else /* !UNW_LOCAL_ONLY */

static inline int
dwarf_readu8 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
              uint8_t *valp, void *arg)
{
  unw_word_t val, aligned_addr = *addr & -sizeof (unw_word_t);
  unw_word_t off = *addr - aligned_addr;
  int ret;

  *addr += 1;
  ret = (*a->access_mem) (as, aligned_addr, &val, 0, arg);
#if __BYTE_ORDER == __LITTLE_ENDIAN
  val >>= 8*off;
#else
  val >>= 8*(sizeof (unw_word_t) - 1 - off);
#endif
  *valp = (uint8_t) val;
  return ret;
}

static inline int
dwarf_readu16 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               uint16_t *val, void *arg)
{
  uint8_t v0, v1;
  int ret;

  if ((ret = dwarf_readu8 (as, a, addr, &v0, arg)) < 0
      || (ret = dwarf_readu8 (as, a, addr, &v1, arg)) < 0)
    return ret;

  if (tdep_big_endian (as))
    *val = (uint16_t) v0 << 8 | v1;
  else
    *val = (uint16_t) v1 << 8 | v0;
  return 0;
}

static inline int
dwarf_readu32 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               uint32_t *val, void *arg)
{
  uint16_t v0, v1;
  int ret;

  if ((ret = dwarf_readu16 (as, a, addr, &v0, arg)) < 0
      || (ret = dwarf_readu16 (as, a, addr, &v1, arg)) < 0)
    return ret;

  if (tdep_big_endian (as))
    *val = (uint32_t) v0 << 16 | v1;
  else
    *val = (uint32_t) v1 << 16 | v0;
  return 0;
}

static inline int
dwarf_readu64 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               uint64_t *val, void *arg)
{
  uint32_t v0, v1;
  int ret;

  if ((ret = dwarf_readu32 (as, a, addr, &v0, arg)) < 0
      || (ret = dwarf_readu32 (as, a, addr, &v1, arg)) < 0)
    return ret;

  if (tdep_big_endian (as))
    *val = (uint64_t) v0 << 32 | v1;
  else
    *val = (uint64_t) v1 << 32 | v0;
  return 0;
}

static inline int
dwarf_reads8 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
              int8_t *val, void *arg)
{
  uint8_t uval;
  int ret;

  if ((ret = dwarf_readu8 (as, a, addr, &uval, arg)) < 0)
    return ret;
  *val = (int8_t) uval;
  return 0;
}

static inline int
dwarf_reads16 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               int16_t *val, void *arg)
{
  uint16_t uval;
  int ret;

  if ((ret = dwarf_readu16 (as, a, addr, &uval, arg)) < 0)
    return ret;
  *val = (int16_t) uval;
  return 0;
}

static inline int
dwarf_reads32 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               int32_t *val, void *arg)
{
  uint32_t uval;
  int ret;

  if ((ret = dwarf_readu32 (as, a, addr, &uval, arg)) < 0)
    return ret;
  *val = (int32_t) uval;
  return 0;
}

static inline int
dwarf_reads64 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
               int64_t *val, void *arg)
{
  uint64_t uval;
  int ret;

  if ((ret = dwarf_readu64 (as, a, addr, &uval, arg)) < 0)
    return ret;
  *val = (int64_t) uval;
  return 0;
}

#endif /* !UNW_LOCAL_ONLY */

static inline int
dwarf_readw (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
             unw_word_t *val, void *arg)
{
  uint32_t u32;
  uint64_t u64;
  int ret;

  switch (dwarf_addr_size (as))
    {
    case 4:
      ret = dwarf_readu32 (as, a, addr, &u32, arg);
      if (ret < 0)
        return ret;
      *val = u32;
      return ret;

    case 8:
      ret = dwarf_readu64 (as, a, addr, &u64, arg);
      if (ret < 0)
        return ret;
      *val = u64;
      return ret;

    default:
      abort ();
    }
}

/* Read an unsigned "little-endian base 128" value.  See Chapter 7.6
   of DWARF spec v3.  */

static inline int
dwarf_read_uleb128 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
                    unw_word_t *valp, void *arg)
{
  unw_word_t val = 0, shift = 0;
  unsigned char byte;
  int ret;

  do
    {
      if ((ret = dwarf_readu8 (as, a, addr, &byte, arg)) < 0)
        return ret;

      val |= ((unw_word_t) byte & 0x7f) << shift;
      shift += 7;
    }
  while (byte & 0x80);

  *valp = val;
  return 0;
}

/* Read a signed "little-endian base 128" value.  See Chapter 7.6 of
   DWARF spec v3.  */

static inline int
dwarf_read_sleb128 (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
                    unw_word_t *valp, void *arg)
{
  unw_word_t val = 0, shift = 0;
  unsigned char byte;
  int ret;

  do
    {
      if ((ret = dwarf_readu8 (as, a, addr, &byte, arg)) < 0)
        return ret;

      val |= ((unw_word_t) byte & 0x7f) << shift;
      shift += 7;
    }
  while (byte & 0x80);

  if (shift < 8 * sizeof (unw_word_t) && (byte & 0x40) != 0)
    /* sign-extend negative value */
    val |= ((unw_word_t) -1) << shift;

  *valp = val;
  return 0;
}

static ALWAYS_INLINE int
dwarf_read_encoded_pointer_inlined (unw_addr_space_t as, unw_accessors_t *a,
                                    unw_word_t *addr, unsigned char encoding,
                                    const unw_proc_info_t *pi,
                                    unw_word_t *valp, void *arg)
{
  unw_word_t val, initial_addr = *addr;
  uint16_t uval16;
  uint32_t uval32;
  uint64_t uval64;
  int16_t sval16 = 0;
  int32_t sval32 = 0;
  int64_t sval64 = 0;
  int ret;

  /* DW_EH_PE_omit and DW_EH_PE_aligned don't follow the normal
     format/application encoding.  Handle them first.  */
  if (encoding == DW_EH_PE_omit)
    {
      *valp = 0;
      return 0;
    }
  else if (encoding == DW_EH_PE_aligned)
    {
      int size = dwarf_addr_size (as);
      *addr = (initial_addr + size - 1) & -size;
      return dwarf_readw (as, a, addr, valp, arg);
    }

  switch (encoding & DW_EH_PE_FORMAT_MASK)
    {
    case DW_EH_PE_ptr:
      if ((ret = dwarf_readw (as, a, addr, &val, arg)) < 0)
        return ret;
      break;

    case DW_EH_PE_uleb128:
      if ((ret = dwarf_read_uleb128 (as, a, addr, &val, arg)) < 0)
        return ret;
      break;

    case DW_EH_PE_udata2:
      if ((ret = dwarf_readu16 (as, a, addr, &uval16, arg)) < 0)
        return ret;
      val = uval16;
      break;

    case DW_EH_PE_udata4:
      if ((ret = dwarf_readu32 (as, a, addr, &uval32, arg)) < 0)
        return ret;
      val = uval32;
      break;

    case DW_EH_PE_udata8:
      if ((ret = dwarf_readu64 (as, a, addr, &uval64, arg)) < 0)
        return ret;
      val = uval64;
      break;

    case DW_EH_PE_sleb128:
      if ((ret = dwarf_read_uleb128 (as, a, addr, &val, arg)) < 0)
        return ret;
      break;

    case DW_EH_PE_sdata2:
      if ((ret = dwarf_reads16 (as, a, addr, &sval16, arg)) < 0)
        return ret;
      val = sval16;
      break;

    case DW_EH_PE_sdata4:
      if ((ret = dwarf_reads32 (as, a, addr, &sval32, arg)) < 0)
        return ret;
      val = sval32;
      break;

    case DW_EH_PE_sdata8:
      if ((ret = dwarf_reads64 (as, a, addr, &sval64, arg)) < 0)
        return ret;
      val = sval64;
      break;

    default:
      Debug (1, "unexpected encoding format 0x%x\n",
             encoding & DW_EH_PE_FORMAT_MASK);
      return -UNW_EINVAL;
    }

  if (val == 0)
    {
      /* 0 is a special value and always absolute.  */
      *valp = 0;
      return 0;
    }

  switch (encoding & DW_EH_PE_APPL_MASK)
    {
    case DW_EH_PE_absptr:
      break;

    case DW_EH_PE_pcrel:
      val += initial_addr;
      break;

    case DW_EH_PE_datarel:
      /* XXX For now, assume that data-relative addresses are relative
         to the global pointer.  */
      val += pi->gp;
      break;

    case DW_EH_PE_funcrel:
      val += pi->start_ip;
      break;

    case DW_EH_PE_textrel:
      /* XXX For now we don't support text-rel values.  If there is a
         platform which needs this, we probably would have to add a
         "segbase" member to unw_proc_info_t.  */
    default:
      Debug (1, "unexpected application type 0x%x\n",
             encoding & DW_EH_PE_APPL_MASK);
      return -UNW_EINVAL;
    }

  /* Trim off any extra bits.  Assume that sign extension isn't
     required; the only place it is needed is MIPS kernel space
     addresses.  */
  if (sizeof (val) > dwarf_addr_size (as))
    {
      assert (dwarf_addr_size (as) == 4);
      val = (uint32_t) val;
    }

  if (encoding & DW_EH_PE_indirect)
    {
      unw_word_t indirect_addr = val;

      if ((ret = dwarf_readw (as, a, &indirect_addr, &val, arg)) < 0)
        return ret;
    }

  *valp = val;
  return 0;
}

#endif /* DWARF_I_H */
