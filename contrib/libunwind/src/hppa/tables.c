#include "unwind_i.h"

static inline int
is_local_addr_space (unw_addr_space_t as)
{
  extern unw_addr_space_t _ULhppa_local_addr_space;

  return (as == _Uhppa_local_addr_space
#ifndef UNW_REMOTE_ONLY
          || as == _ULhppa_local_addr_space
#endif
          );
}

HIDDEN int
tdep_find_proc_info (unw_addr_space_t as, unw_word_t ip,
                     unw_proc_info_t *pi, int need_unwind_info, void *arg)
{
  printf ("%s: begging to get implemented...\n", __FUNCTION__);
  return 0;
}

HIDDEN int
tdep_search_unwind_table (unw_addr_space_t as, unw_word_t ip,
                          unw_dyn_info_t *di,
                          unw_proc_info_t *pi, int need_unwind_info, void *arg)
{
  printf ("%s: the biggest beggar of them all...\n", __FUNCTION__);
  return 0;
}

HIDDEN void
tdep_put_unwind_info (unw_addr_space_t as, unw_proc_info_t *pi, void *arg)
{
  if (!pi->unwind_info)
    return;

  if (!is_local_addr_space (as))
    {
      free (pi->unwind_info);
      pi->unwind_info = NULL;
    }
}
