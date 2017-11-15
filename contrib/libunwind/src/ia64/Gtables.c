/* libunwind - a platform-independent unwind library
   Copyright (c) 2001-2005 Hewlett-Packard Development Company, L.P.
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

#include <assert.h>
#include <stdlib.h>
#include <stddef.h>

#include "unwind_i.h"

#ifdef HAVE_IA64INTRIN_H
# include <ia64intrin.h>
#endif

extern unw_addr_space_t _ULia64_local_addr_space;

struct ia64_table_entry
  {
    uint64_t start_offset;
    uint64_t end_offset;
    uint64_t info_offset;
  };

#ifdef UNW_LOCAL_ONLY

static inline int
is_local_addr_space (unw_addr_space_t as)
{
  return 1;
}

static inline int
read_mem (unw_addr_space_t as, unw_word_t addr, unw_word_t *valp, void *arg)
{
  *valp = *(unw_word_t *) addr;
  return 0;
}

#else /* !UNW_LOCAL_ONLY */

static inline int
is_local_addr_space (unw_addr_space_t as)
{
  return as == unw_local_addr_space;
}

static inline int
read_mem (unw_addr_space_t as, unw_word_t addr, unw_word_t *valp, void *arg)
{
  unw_accessors_t *a = unw_get_accessors (as);

  return (*a->access_mem) (as, addr, valp, 0, arg);
}

/* Helper macro for reading an ia64_table_entry from remote memory.  */
#define remote_read(addr, member)                                            \
        (*a->access_mem) (as, (addr) + offsetof (struct ia64_table_entry,    \
                                                 member), &member, 0, arg)

/* Lookup an unwind-table entry in remote memory.  Returns 1 if an
   entry is found, 0 if no entry is found, negative if an error
   occurred reading remote memory.  */
static int
remote_lookup (unw_addr_space_t as,
               unw_word_t table, size_t table_size, unw_word_t rel_ip,
               struct ia64_table_entry *e, void *arg)
{
  unw_word_t e_addr = 0, start_offset, end_offset, info_offset;
  unw_accessors_t *a = unw_get_accessors (as);
  unsigned long lo, hi, mid;
  int ret;

  /* do a binary search for right entry: */
  for (lo = 0, hi = table_size / sizeof (struct ia64_table_entry); lo < hi;)
    {
      mid = (lo + hi) / 2;
      e_addr = table + mid * sizeof (struct ia64_table_entry);
      if ((ret = remote_read (e_addr, start_offset)) < 0)
        return ret;

      if (rel_ip < start_offset)
        hi = mid;
      else
        {
          if ((ret = remote_read (e_addr, end_offset)) < 0)
            return ret;

          if (rel_ip >= end_offset)
            lo = mid + 1;
          else
            break;
        }
    }
  if (rel_ip < start_offset || rel_ip >= end_offset)
    return 0;
  e->start_offset = start_offset;
  e->end_offset = end_offset;

  if ((ret = remote_read (e_addr, info_offset)) < 0)
    return ret;
  e->info_offset = info_offset;
  return 1;
}

HIDDEN void
tdep_put_unwind_info (unw_addr_space_t as, unw_proc_info_t *pi, void *arg)
{
  if (!pi->unwind_info)
    return;

  if (is_local_addr_space (as))
    {
      free (pi->unwind_info);
      pi->unwind_info = NULL;
    }
}

PROTECTED unw_word_t
_Uia64_find_dyn_list (unw_addr_space_t as, unw_dyn_info_t *di, void *arg)
{
  unw_word_t hdr_addr, info_addr, hdr, directives, pers, cookie, off;
  unw_word_t start_offset, end_offset, info_offset, segbase;
  struct ia64_table_entry *e;
  size_t table_size;
  unw_word_t gp = di->gp;
  int ret;

  switch (di->format)
    {
    case UNW_INFO_FORMAT_DYNAMIC:
    default:
      return 0;

    case UNW_INFO_FORMAT_TABLE:
      e = (struct ia64_table_entry *) di->u.ti.table_data;
      table_size = di->u.ti.table_len * sizeof (di->u.ti.table_data[0]);
      segbase = di->u.ti.segbase;
      if (table_size < sizeof (struct ia64_table_entry))
        return 0;
      start_offset = e[0].start_offset;
      end_offset = e[0].end_offset;
      info_offset = e[0].info_offset;
      break;

    case UNW_INFO_FORMAT_REMOTE_TABLE:
      {
        unw_accessors_t *a = unw_get_accessors (as);
        unw_word_t e_addr = di->u.rti.table_data;

        table_size = di->u.rti.table_len * sizeof (unw_word_t);
        segbase = di->u.rti.segbase;
        if (table_size < sizeof (struct ia64_table_entry))
          return 0;

        if (   (ret = remote_read (e_addr, start_offset) < 0)
            || (ret = remote_read (e_addr, end_offset) < 0)
            || (ret = remote_read (e_addr, info_offset) < 0))
          return ret;
      }
      break;
    }

  if (start_offset != end_offset)
    /* dyn-list entry cover a zero-length "procedure" and should be
       first entry (note: technically a binary could contain code
       below the segment base, but this doesn't happen for normal
       binaries and certainly doesn't happen when libunwind is a
       separate shared object.  For weird cases, the application may
       have to provide its own (slower) version of this routine.  */
    return 0;

  hdr_addr = info_offset + segbase;
  info_addr = hdr_addr + 8;

  /* read the header word: */
  if ((ret = read_mem (as, hdr_addr, &hdr, arg)) < 0)
    return ret;

  if (IA64_UNW_VER (hdr) != 1
      || IA64_UNW_FLAG_EHANDLER (hdr) || IA64_UNW_FLAG_UHANDLER (hdr))
    /* dyn-list entry must be version 1 and doesn't have ehandler
       or uhandler */
    return 0;

  if (IA64_UNW_LENGTH (hdr) != 1)
    /* dyn-list entry must consist of a single word of NOP directives */
    return 0;

  if (   ((ret = read_mem (as, info_addr, &directives, arg)) < 0)
      || ((ret = read_mem (as, info_addr + 0x08, &pers, arg)) < 0)
      || ((ret = read_mem (as, info_addr + 0x10, &cookie, arg)) < 0)
      || ((ret = read_mem (as, info_addr + 0x18, &off, arg)) < 0))
    return 0;

  if (directives != 0 || pers != 0
      || (!as->big_endian && cookie != 0x7473696c2d6e7964ULL)
      || ( as->big_endian && cookie != 0x64796e2d6c697374ULL))
    return 0;

  /* OK, we ran the gauntlet and found it: */
  return off + gp;
}

#endif /* !UNW_LOCAL_ONLY */

static inline const struct ia64_table_entry *
lookup (struct ia64_table_entry *table, size_t table_size, unw_word_t rel_ip)
{
  const struct ia64_table_entry *e = 0;
  unsigned long lo, hi, mid;

  /* do a binary search for right entry: */
  for (lo = 0, hi = table_size / sizeof (struct ia64_table_entry); lo < hi;)
    {
      mid = (lo + hi) / 2;
      e = table + mid;
      if (rel_ip < e->start_offset)
        hi = mid;
      else if (rel_ip >= e->end_offset)
        lo = mid + 1;
      else
        break;
    }
  if (rel_ip < e->start_offset || rel_ip >= e->end_offset)
    return NULL;
  return e;
}

PROTECTED int
unw_search_ia64_unwind_table (unw_addr_space_t as, unw_word_t ip,
                              unw_dyn_info_t *di, unw_proc_info_t *pi,
                              int need_unwind_info, void *arg)
{
  unw_word_t addr, hdr_addr, info_addr, info_end_addr, hdr, *wp;
  const struct ia64_table_entry *e = NULL;
  unw_word_t handler_offset, segbase = 0;
  int ret, is_local;
#ifndef UNW_LOCAL_ONLY
  struct ia64_table_entry ent;
#endif

  assert ((di->format == UNW_INFO_FORMAT_TABLE
           || di->format == UNW_INFO_FORMAT_REMOTE_TABLE)
          && (ip >= di->start_ip && ip < di->end_ip));

  pi->flags = 0;
  pi->unwind_info = 0;
  pi->handler = 0;

  if (likely (di->format == UNW_INFO_FORMAT_TABLE))
    {
      segbase = di->u.ti.segbase;
      e = lookup ((struct ia64_table_entry *) di->u.ti.table_data,
                  di->u.ti.table_len * sizeof (unw_word_t),
                  ip - segbase);
    }
#ifndef UNW_LOCAL_ONLY
  else
    {
      segbase = di->u.rti.segbase;
      if ((ret = remote_lookup (as, di->u.rti.table_data,
                                di->u.rti.table_len * sizeof (unw_word_t),
                                ip - segbase, &ent, arg)) < 0)
        return ret;
      if (ret)
        e = &ent;
    }
#endif
  if (!e)
    {
      /* IP is inside this table's range, but there is no explicit
         unwind info => use default conventions (i.e., this is NOT an
         error).  */
      memset (pi, 0, sizeof (*pi));
      pi->start_ip = 0;
      pi->end_ip = 0;
      pi->gp = di->gp;
      pi->lsda = 0;
      return 0;
    }

  pi->start_ip = e->start_offset + segbase;
  pi->end_ip = e->end_offset + segbase;

  hdr_addr = e->info_offset + segbase;
  info_addr = hdr_addr + 8;

  /* Read the header word.  Note: the actual unwind-info is always
     assumed to reside in memory, independent of whether di->format is
     UNW_INFO_FORMAT_TABLE or UNW_INFO_FORMAT_REMOTE_TABLE.  */

  if ((ret = read_mem (as, hdr_addr, &hdr, arg)) < 0)
    return ret;

  if (IA64_UNW_VER (hdr) != 1)
    {
      Debug (1, "Unknown header version %ld (hdr word=0x%lx @ 0x%lx)\n",
             IA64_UNW_VER (hdr), (unsigned long) hdr,
             (unsigned long) hdr_addr);
      return -UNW_EBADVERSION;
    }

  info_end_addr = info_addr + 8 * IA64_UNW_LENGTH (hdr);

  is_local = is_local_addr_space (as);

  /* If we must have the unwind-info, return it.  Also, if we are in
     the local address-space, return the unwind-info because it's so
     cheap to do so and it may come in handy later on.  */
  if (need_unwind_info || is_local)
    {
      pi->unwind_info_size = 8 * IA64_UNW_LENGTH (hdr);

      if (is_local)
        pi->unwind_info = (void *) (uintptr_t) info_addr;
      else
        {
          /* Internalize unwind info.  Note: since we're doing this
             only for non-local address spaces, there is no
             signal-safety issue and it is OK to use malloc()/free().  */
          pi->unwind_info = malloc (8 * IA64_UNW_LENGTH (hdr));
          if (!pi->unwind_info)
            return -UNW_ENOMEM;

          wp = (unw_word_t *) pi->unwind_info;
          for (addr = info_addr; addr < info_end_addr; addr += 8, ++wp)
            {
              if ((ret = read_mem (as, addr, wp, arg)) < 0)
                {
                  free (pi->unwind_info);
                  return ret;
                }
            }
        }
    }

  if (IA64_UNW_FLAG_EHANDLER (hdr) || IA64_UNW_FLAG_UHANDLER (hdr))
    {
      /* read the personality routine address (address is gp-relative): */
      if ((ret = read_mem (as, info_end_addr, &handler_offset, arg)) < 0)
        return ret;
      Debug (4, "handler ptr @ offset=%lx, gp=%lx\n", handler_offset, di->gp);
      if ((read_mem (as, handler_offset + di->gp, &pi->handler, arg)) < 0)
        return ret;
    }
  pi->lsda = info_end_addr + 8;
  pi->gp = di->gp;
  pi->format = di->format;
  return 0;
}

#ifndef UNW_REMOTE_ONLY

# if defined(HAVE_DL_ITERATE_PHDR)
#  include <link.h>
#  include <stdlib.h>

#  if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 2) \
      || (__GLIBC__ == 2 && __GLIBC_MINOR__ == 2 && !defined(DT_CONFIG))
#    error You need GLIBC 2.2.4 or later on IA-64 Linux
#  endif

#  if defined(HAVE_GETUNWIND)
     extern unsigned long getunwind (void *buf, size_t len);
#  else /* HAVE_GETUNWIND */
#   include <unistd.h>
#   include <sys/syscall.h>
#   ifndef __NR_getunwind
#     define __NR_getunwind     1215
#   endif

static unsigned long
getunwind (void *buf, size_t len)
{
  return syscall (SYS_getunwind, buf, len);
}

#  endif /* HAVE_GETUNWIND */

static unw_dyn_info_t kernel_table;

static int
get_kernel_table (unw_dyn_info_t *di)
{
  struct ia64_table_entry *ktab, *etab;
  size_t size;

  Debug (16, "getting kernel table");

  size = getunwind (NULL, 0);
  ktab = sos_alloc (size);
  if (!ktab)
    {
      Dprintf (__FILE__".%s: failed to allocate %zu bytes",
               __FUNCTION__, size);
      return -UNW_ENOMEM;
    }
  getunwind (ktab, size);

  /* Determine length of kernel's unwind table & relocate its entries.  */
  for (etab = ktab; etab->start_offset; ++etab)
    etab->info_offset += (uint64_t) ktab;

  di->format = UNW_INFO_FORMAT_TABLE;
  di->gp = 0;
  di->start_ip = ktab[0].start_offset;
  di->end_ip = etab[-1].end_offset;
  di->u.ti.name_ptr = (unw_word_t) "<kernel>";
  di->u.ti.segbase = 0;
  di->u.ti.table_len = ((char *) etab - (char *) ktab) / sizeof (unw_word_t);
  di->u.ti.table_data = (unw_word_t *) ktab;

  Debug (16, "found table `%s': [%lx-%lx) segbase=%lx len=%lu\n",
         (char *) di->u.ti.name_ptr, di->start_ip, di->end_ip,
         di->u.ti.segbase, di->u.ti.table_len);
  return 0;
}

#  ifndef UNW_LOCAL_ONLY

/* This is exported for the benefit of libunwind-ptrace.a.  */
PROTECTED int
_Uia64_get_kernel_table (unw_dyn_info_t *di)
{
  int ret;

  if (!kernel_table.u.ti.table_data)
    if ((ret = get_kernel_table (&kernel_table)) < 0)
      return ret;

  memcpy (di, &kernel_table, sizeof (*di));
  return 0;
}

#  endif /* !UNW_LOCAL_ONLY */

static inline unsigned long
current_gp (void)
{
#  if defined(__GNUC__) && !defined(__INTEL_COMPILER)
      register unsigned long gp __asm__("gp");
      return gp;
#  elif HAVE_IA64INTRIN_H
      return __getReg (_IA64_REG_GP);
#  else
#    error Implement me.
#  endif
}

static int
callback (struct dl_phdr_info *info, size_t size, void *ptr)
{
  unw_dyn_info_t *di = ptr;
  const Elf64_Phdr *phdr, *p_unwind, *p_dynamic, *p_text;
  long n;
  Elf64_Addr load_base, segbase = 0;

  /* Make sure struct dl_phdr_info is at least as big as we need.  */
  if (size < offsetof (struct dl_phdr_info, dlpi_phnum)
             + sizeof (info->dlpi_phnum))
    return -1;

  Debug (16, "checking `%s' (load_base=%lx)\n",
         info->dlpi_name, info->dlpi_addr);

  phdr = info->dlpi_phdr;
  load_base = info->dlpi_addr;
  p_text = NULL;
  p_unwind = NULL;
  p_dynamic = NULL;

  /* See if PC falls into one of the loaded segments.  Find the unwind
     segment at the same time.  */
  for (n = info->dlpi_phnum; --n >= 0; phdr++)
    {
      if (phdr->p_type == PT_LOAD)
        {
          Elf64_Addr vaddr = phdr->p_vaddr + load_base;
          if (di->u.ti.segbase >= vaddr
              && di->u.ti.segbase < vaddr + phdr->p_memsz)
            p_text = phdr;
        }
      else if (phdr->p_type == PT_IA_64_UNWIND)
        p_unwind = phdr;
      else if (phdr->p_type == PT_DYNAMIC)
        p_dynamic = phdr;
    }
  if (!p_text || !p_unwind)
    return 0;

  if (likely (p_unwind->p_vaddr >= p_text->p_vaddr
              && p_unwind->p_vaddr < p_text->p_vaddr + p_text->p_memsz))
    /* normal case: unwind table is inside text segment */
    segbase = p_text->p_vaddr + load_base;
  else
    {
      /* Special case: unwind table is in some other segment; this
         happens for the Linux kernel's gate DSO, for example.  */
      phdr = info->dlpi_phdr;
      for (n = info->dlpi_phnum; --n >= 0; phdr++)
        {
          if (phdr->p_type == PT_LOAD && p_unwind->p_vaddr >= phdr->p_vaddr
              && p_unwind->p_vaddr < phdr->p_vaddr + phdr->p_memsz)
            {
              segbase = phdr->p_vaddr + load_base;
              break;
            }
        }
    }

  if (p_dynamic)
    {
      /* For dynamicly linked executables and shared libraries,
         DT_PLTGOT is the gp value for that object.  */
      Elf64_Dyn *dyn = (Elf64_Dyn *)(p_dynamic->p_vaddr + load_base);
      for (; dyn->d_tag != DT_NULL; ++dyn)
        if (dyn->d_tag == DT_PLTGOT)
          {
            /* On IA-64, _DYNAMIC is writable and GLIBC has relocated it.  */
            di->gp = dyn->d_un.d_ptr;
            break;
          }
    }
  else
    /* Otherwise this is a static executable with no _DYNAMIC.
       The gp is constant program-wide.  */
    di->gp = current_gp();
  di->format = UNW_INFO_FORMAT_TABLE;
  di->start_ip = p_text->p_vaddr + load_base;
  di->end_ip = p_text->p_vaddr + load_base + p_text->p_memsz;
  di->u.ti.name_ptr = (unw_word_t) info->dlpi_name;
  di->u.ti.table_data = (void *) (p_unwind->p_vaddr + load_base);
  di->u.ti.table_len = p_unwind->p_memsz / sizeof (unw_word_t);
  di->u.ti.segbase = segbase;

  Debug (16, "found table `%s': segbase=%lx, len=%lu, gp=%lx, "
         "table_data=%p\n", (char *) di->u.ti.name_ptr, di->u.ti.segbase,
         di->u.ti.table_len, di->gp, di->u.ti.table_data);
  return 1;
}

#  ifdef HAVE_DL_PHDR_REMOVALS_COUNTER

static inline int
validate_cache (unw_addr_space_t as)
{
  /* Note: we don't need to serialize here with respect to
     dl_iterate_phdr() because if somebody were to remove an object
     that is required to complete the unwind on whose behalf we're
     validating the cache here, we'd be hosed anyhow.  What we're
     guarding against here is the case where library FOO gets mapped,
     unwind info for FOO gets cached, FOO gets unmapped, BAR gets
     mapped in the place where FOO was and then we unwind across a
     function in FOO.  Since no thread can execute in BAR before FOO
     has been removed, we are guaranteed that
     dl_phdr_removals_counter() would have been incremented before we
     get here.  */
  unsigned long long removals = dl_phdr_removals_counter ();

  if (removals == as->shared_object_removals)
    return 1;

  as->shared_object_removals = removals;
  unw_flush_cache (as, 0, 0);
  return -1;
}

#  else /* !HAVE_DL_PHDR_REMOVALS_COUNTER */

/* Check whether any phdrs have been removed since we last flushed the
   cache.  If so we flush the cache and return -1, if not, we do
   nothing and return 1.  */

static int
check_callback (struct dl_phdr_info *info, size_t size, void *ptr)
{
#   ifdef HAVE_STRUCT_DL_PHDR_INFO_DLPI_SUBS
  unw_addr_space_t as = ptr;

  if (size <
      offsetof (struct dl_phdr_info, dlpi_subs) + sizeof (info->dlpi_subs))
    /* It would be safer to flush the cache here, but that would
       disable caching for older libc's which would be incompatible
       with the behavior of older versions of libunwind so we return 1
       instead and hope nobody runs into stale cache info...  */
    return 1;

  if (info->dlpi_subs == as->shared_object_removals)
    return 1;

  as->shared_object_removals = info->dlpi_subs;
  unw_flush_cache (as, 0, 0);
  return -1;            /* indicate that there were removals */
#   else
  return 1;
#   endif
}

static inline int
validate_cache (unw_addr_space_t as)
{
  intrmask_t saved_mask;
  int ret;

  SIGPROCMASK (SIG_SETMASK, &unwi_full_mask, &saved_mask);
  ret = dl_iterate_phdr (check_callback, as);
  SIGPROCMASK (SIG_SETMASK, &saved_mask, NULL);
  return ret;
}

#  endif /* HAVE_DL_PHDR_REMOVALS_COUNTER */

# elif defined(HAVE_DLMODINFO)
  /* Support for HP-UX-style dlmodinfo() */
#  include <dlfcn.h>

static inline int
validate_cache (unw_addr_space_t as)
{
  return 1;
}

# endif /* !HAVE_DLMODINFO */

HIDDEN int
tdep_find_proc_info (unw_addr_space_t as, unw_word_t ip,
                     unw_proc_info_t *pi, int need_unwind_info, void *arg)
{
# if defined(HAVE_DL_ITERATE_PHDR)
  unw_dyn_info_t di, *dip = &di;
  intrmask_t saved_mask;
  int ret;

  di.u.ti.segbase = ip; /* this is cheap... */

  SIGPROCMASK (SIG_SETMASK, &unwi_full_mask, &saved_mask);
  ret = dl_iterate_phdr (callback, &di);
  SIGPROCMASK (SIG_SETMASK, &saved_mask, NULL);

  if (ret <= 0)
    {
      if (!kernel_table.u.ti.table_data)
        {
          if ((ret = get_kernel_table (&kernel_table)) < 0)
            return ret;
        }
      if (ip < kernel_table.start_ip || ip >= kernel_table.end_ip)
        return -UNW_ENOINFO;
      dip = &kernel_table;
    }
# elif defined(HAVE_DLMODINFO)
# define UNWIND_TBL_32BIT       0x8000000000000000
  struct load_module_desc lmd;
  unw_dyn_info_t di, *dip = &di;
  struct unwind_header
    {
      uint64_t header_version;
      uint64_t start_offset;
      uint64_t end_offset;
    }
  *uhdr;

  if (!dlmodinfo (ip, &lmd, sizeof (lmd), NULL, 0, 0))
    return -UNW_ENOINFO;

  di.format = UNW_INFO_FORMAT_TABLE;
  di.start_ip = lmd.text_base;
  di.end_ip = lmd.text_base + lmd.text_size;
  di.gp = lmd.linkage_ptr;
  di.u.ti.name_ptr = 0; /* no obvious table-name available */
  di.u.ti.segbase = lmd.text_base;

  uhdr = (struct unwind_header *) lmd.unwind_base;

  if ((uhdr->header_version & ~UNWIND_TBL_32BIT) != 1
      && (uhdr->header_version & ~UNWIND_TBL_32BIT) != 2)
    {
      Debug (1, "encountered unknown unwind header version %ld\n",
             (long) (uhdr->header_version & ~UNWIND_TBL_32BIT));
      return -UNW_EBADVERSION;
    }
  if (uhdr->header_version & UNWIND_TBL_32BIT)
    {
      Debug (1, "32-bit unwind tables are not supported yet\n");
      return -UNW_EINVAL;
    }

  di.u.ti.table_data = (unw_word_t *) (di.u.ti.segbase + uhdr->start_offset);
  di.u.ti.table_len = ((uhdr->end_offset - uhdr->start_offset)
                       / sizeof (unw_word_t));

  Debug (16, "found table `%s': segbase=%lx, len=%lu, gp=%lx, "
         "table_data=%p\n", (char *) di.u.ti.name_ptr, di.u.ti.segbase,
         di.u.ti.table_len, di.gp, di.u.ti.table_data);
# endif

  /* now search the table: */
  return tdep_search_unwind_table (as, ip, dip, pi, need_unwind_info, arg);
}

/* Returns 1 if the cache is up-to-date or -1 if the cache contained
   stale data and had to be flushed.  */

HIDDEN int
ia64_local_validate_cache (unw_addr_space_t as, void *arg)
{
  return validate_cache (as);
}

#endif /* !UNW_REMOTE_ONLY */
