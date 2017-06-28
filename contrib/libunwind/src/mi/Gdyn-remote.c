/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2002, 2005 Hewlett-Packard Co
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

#include <stdlib.h>

#include "libunwind_i.h"
#include "remote.h"

static void
free_regions (unw_dyn_region_info_t *region)
{
  if (region->next)
    free_regions (region->next);
  free (region);
}

static int
intern_op (unw_addr_space_t as, unw_accessors_t *a, unw_word_t *addr,
           unw_dyn_op_t *op, void *arg)
{
  int ret;

  if ((ret = fetch8 (as, a, addr, &op->tag, arg)) < 0
      || (ret = fetch8 (as, a, addr, &op->qp, arg)) < 0
      || (ret = fetch16 (as, a, addr, &op->reg, arg)) < 0
      || (ret = fetch32 (as, a, addr, &op->when, arg)) < 0
      || (ret = fetchw  (as, a, addr, &op->val, arg)) < 0)
    return ret;
  return 0;
}

static int
intern_regions (unw_addr_space_t as, unw_accessors_t *a,
                unw_word_t *addr, unw_dyn_region_info_t **regionp, void *arg)
{
  uint32_t insn_count, op_count, i;
  unw_dyn_region_info_t *region;
  unw_word_t next_addr;
  int ret;

  *regionp = NULL;

  if (!*addr)
    return 0;   /* NULL region-list */

  if ((ret = fetchw (as, a, addr, &next_addr, arg)) < 0
      || (ret = fetch32 (as, a, addr, (int32_t *) &insn_count, arg)) < 0
      || (ret = fetch32 (as, a, addr, (int32_t *) &op_count, arg)) < 0)
    return ret;

  region = calloc (1, _U_dyn_region_info_size (op_count));
  if (!region)
    {
      ret = -UNW_ENOMEM;
      goto out;
    }

  region->insn_count = insn_count;
  region->op_count = op_count;
  for (i = 0; i < op_count; ++i)
    if ((ret = intern_op (as, a, addr, region->op + i, arg)) < 0)
      goto out;

  if (next_addr)
    if ((ret = intern_regions (as, a, &next_addr, &region->next, arg)) < 0)
      goto out;

  *regionp = region;
  return 0;

 out:
  if (region)
    free_regions (region);
  return ret;
}

static int
intern_array (unw_addr_space_t as, unw_accessors_t *a,
              unw_word_t *addr, unw_word_t table_len, unw_word_t **table_data,
              void *arg)
{
  unw_word_t i, *data = calloc (table_len, WSIZE);
  int ret = 0;

  if (!data)
    {
      ret = -UNW_ENOMEM;
      goto out;
    }

  for (i = 0; i < table_len; ++i)
    if (fetchw (as, a, addr, data + i, arg) < 0)
      goto out;

  *table_data = data;
  return 0;

 out:
  if (data)
    free (data);
  return ret;
}

static void
free_dyn_info (unw_dyn_info_t *di)
{
  switch (di->format)
    {
    case UNW_INFO_FORMAT_DYNAMIC:
      if (di->u.pi.regions)
        {
          free_regions (di->u.pi.regions);
          di->u.pi.regions = NULL;
        }
      break;

    case UNW_INFO_FORMAT_TABLE:
      if (di->u.ti.table_data)
        {
          free (di->u.ti.table_data);
          di->u.ti.table_data = NULL;
        }
      break;

    case UNW_INFO_FORMAT_REMOTE_TABLE:
    default:
      break;
    }
}

static int
intern_dyn_info (unw_addr_space_t as, unw_accessors_t *a,
                 unw_word_t *addr, unw_dyn_info_t *di, void *arg)
{
  unw_word_t first_region;
  int ret;

  switch (di->format)
    {
    case UNW_INFO_FORMAT_DYNAMIC:
      if ((ret = fetchw (as, a, addr, &di->u.pi.name_ptr, arg)) < 0
          || (ret = fetchw (as, a, addr, &di->u.pi.handler, arg)) < 0
          || (ret = fetch32 (as, a, addr,
                             (int32_t *) &di->u.pi.flags, arg)) < 0)
        goto out;
      *addr += 4;       /* skip over pad0 */
      if ((ret = fetchw (as, a, addr, &first_region, arg)) < 0
          || (ret = intern_regions (as, a, &first_region, &di->u.pi.regions,
                                    arg)) < 0)
        goto out;
      break;

    case UNW_INFO_FORMAT_TABLE:
      if ((ret = fetchw (as, a, addr, &di->u.ti.name_ptr, arg)) < 0
          || (ret = fetchw (as, a, addr, &di->u.ti.segbase, arg)) < 0
          || (ret = fetchw (as, a, addr, &di->u.ti.table_len, arg)) < 0
          || (ret = intern_array (as, a, addr, di->u.ti.table_len,
                                  &di->u.ti.table_data, arg)) < 0)
        goto out;
      break;

    case UNW_INFO_FORMAT_REMOTE_TABLE:
      if ((ret = fetchw (as, a, addr, &di->u.rti.name_ptr, arg)) < 0
          || (ret = fetchw (as, a, addr, &di->u.rti.segbase, arg)) < 0
          || (ret = fetchw (as, a, addr, &di->u.rti.table_len, arg)) < 0
          || (ret = fetchw (as, a, addr, &di->u.rti.table_data, arg)) < 0)
        goto out;
      break;

    default:
      ret = -UNW_ENOINFO;
      goto out;
    }
  return 0;

 out:
  free_dyn_info (di);
  return ret;
}

HIDDEN int
unwi_dyn_remote_find_proc_info (unw_addr_space_t as, unw_word_t ip,
                                unw_proc_info_t *pi,
                                int need_unwind_info, void *arg)
{
  unw_accessors_t *a = unw_get_accessors (as);
  unw_word_t dyn_list_addr, addr, next_addr, gen1, gen2, start_ip, end_ip;
  unw_dyn_info_t *di = NULL;
  int ret;

  if (as->dyn_info_list_addr)
    dyn_list_addr = as->dyn_info_list_addr;
  else
    {
      if ((*a->get_dyn_info_list_addr) (as, &dyn_list_addr, arg) < 0)
        return -UNW_ENOINFO;
      if (as->caching_policy != UNW_CACHE_NONE)
        as->dyn_info_list_addr = dyn_list_addr;
    }

  do
    {
      addr = dyn_list_addr;

      ret = -UNW_ENOINFO;

      if (fetchw (as, a, &addr, &gen1, arg) < 0
          || fetchw (as, a, &addr, &next_addr, arg) < 0)
        return ret;

      for (addr = next_addr; addr != 0; addr = next_addr)
        {
          if (fetchw (as, a, &addr, &next_addr, arg) < 0)
            goto recheck;       /* only fail if generation # didn't change */

          addr += WSIZE;        /* skip over prev_addr */

          if (fetchw (as, a, &addr, &start_ip, arg) < 0
              || fetchw (as, a, &addr, &end_ip, arg) < 0)
            goto recheck;       /* only fail if generation # didn't change */

          if (ip >= start_ip && ip < end_ip)
            {
              if (!di)
                di = calloc (1, sizeof (*di));

              di->start_ip = start_ip;
              di->end_ip = end_ip;

              if (fetchw (as, a, &addr, &di->gp, arg) < 0
                  || fetch32 (as, a, &addr, &di->format, arg) < 0)
                goto recheck;   /* only fail if generation # didn't change */

              addr += 4;        /* skip over padding */

              if (need_unwind_info
                  && intern_dyn_info (as, a, &addr, di, arg) < 0)
                goto recheck;   /* only fail if generation # didn't change */

              if (unwi_extract_dynamic_proc_info (as, ip, pi, di,
                                                  need_unwind_info, arg) < 0)
                {
                  free_dyn_info (di);
                  goto recheck; /* only fail if generation # didn't change */
                }
              ret = 0;  /* OK, found it */
              break;
            }
        }

      /* Re-check generation number to ensure the data we have is
         consistent.  */
    recheck:
      addr = dyn_list_addr;
      if (fetchw (as, a, &addr, &gen2, arg) < 0)
        return ret;
    }
  while (gen1 != gen2);

  if (ret < 0 && di)
    free (di);

  return ret;
}

HIDDEN void
unwi_dyn_remote_put_unwind_info (unw_addr_space_t as, unw_proc_info_t *pi,
                                 void *arg)
{
  if (!pi->unwind_info)
    return;

  free_dyn_info (pi->unwind_info);
  free (pi->unwind_info);
  pi->unwind_info = NULL;
}

/* Returns 1 if the cache is up-to-date or -1 if the cache contained
   stale data and had to be flushed.  */

HIDDEN int
unwi_dyn_validate_cache (unw_addr_space_t as, void *arg)
{
  unw_word_t addr, gen;
  unw_accessors_t *a;

  if (!as->dyn_info_list_addr)
    /* If we don't have the dyn_info_list_addr, we don't have anything
       in the cache.  */
    return 0;

  a = unw_get_accessors (as);
  addr = as->dyn_info_list_addr;

  if (fetchw (as, a, &addr, &gen, arg) < 0)
    return 1;

  if (gen == as->dyn_generation)
    return 1;

  unw_flush_cache (as, 0, 0);
  as->dyn_generation = gen;
  return -1;
}
