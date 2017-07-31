#ifndef REMOTE_H
#define REMOTE_H

/* Helper functions for accessing (remote) memory.  These functions
   assume that all addresses are naturally aligned (e.g., 32-bit
   quantity is stored at a 32-bit-aligned address.  */

#ifdef UNW_LOCAL_ONLY

static inline int
fetch8 (unw_addr_space_t as, unw_accessors_t *a,
        unw_word_t *addr, int8_t *valp, void *arg)
{
  *valp = *(int8_t *) (uintptr_t) *addr;
  *addr += 1;
  return 0;
}

static inline int
fetch16 (unw_addr_space_t as, unw_accessors_t *a,
         unw_word_t *addr, int16_t *valp, void *arg)
{
  *valp = *(int16_t *) (uintptr_t) *addr;
  *addr += 2;
  return 0;
}

static inline int
fetch32 (unw_addr_space_t as, unw_accessors_t *a,
         unw_word_t *addr, int32_t *valp, void *arg)
{
  *valp = *(int32_t *) (uintptr_t) *addr;
  *addr += 4;
  return 0;
}

static inline int
fetchw (unw_addr_space_t as, unw_accessors_t *a,
        unw_word_t *addr, unw_word_t *valp, void *arg)
{
  *valp = *(unw_word_t *) (uintptr_t) *addr;
  *addr += sizeof (unw_word_t);
  return 0;
}

#else /* !UNW_LOCAL_ONLY */

#define WSIZE   (sizeof (unw_word_t))

static inline int
fetch8 (unw_addr_space_t as, unw_accessors_t *a,
        unw_word_t *addr, int8_t *valp, void *arg)
{
  unw_word_t val, aligned_addr = *addr & -WSIZE, off = *addr - aligned_addr;
  int ret;

  *addr += 1;

  ret = (*a->access_mem) (as, aligned_addr, &val, 0, arg);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  val >>= 8*off;
#else
  val >>= 8*(WSIZE - 1 - off);
#endif
  *valp = val & 0xff;
  return ret;
}

static inline int
fetch16 (unw_addr_space_t as, unw_accessors_t *a,
         unw_word_t *addr, int16_t *valp, void *arg)
{
  unw_word_t val, aligned_addr = *addr & -WSIZE, off = *addr - aligned_addr;
  int ret;

  if ((off & 0x1) != 0)
    return -UNW_EINVAL;

  *addr += 2;

  ret = (*a->access_mem) (as, aligned_addr, &val, 0, arg);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  val >>= 8*off;
#else
  val >>= 8*(WSIZE - 2 - off);
#endif
  *valp = val & 0xffff;
  return ret;
}

static inline int
fetch32 (unw_addr_space_t as, unw_accessors_t *a,
         unw_word_t *addr, int32_t *valp, void *arg)
{
  unw_word_t val, aligned_addr = *addr & -WSIZE, off = *addr - aligned_addr;
  int ret;

  if ((off & 0x3) != 0)
    return -UNW_EINVAL;

  *addr += 4;

  ret = (*a->access_mem) (as, aligned_addr, &val, 0, arg);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  val >>= 8*off;
#else
  val >>= 8*(WSIZE - 4 - off);
#endif
  *valp = val & 0xffffffff;
  return ret;
}

static inline int
fetchw (unw_addr_space_t as, unw_accessors_t *a,
        unw_word_t *addr, unw_word_t *valp, void *arg)
{
  int ret;

  ret = (*a->access_mem) (as, *addr, valp, 0, arg);
  *addr += WSIZE;
  return ret;
}

#endif /* !UNW_LOCAL_ONLY */

#endif /* REMOTE_H */
