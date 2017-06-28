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

#include "libunwind_i.h"
#include "remote.h"

static inline int
intern_string (unw_addr_space_t as, unw_accessors_t *a,
               unw_word_t addr, char *buf, size_t buf_len, void *arg)
{
  size_t i;
  int ret;

  for (i = 0; i < buf_len; ++i)
    {
      if ((ret = fetch8 (as, a, &addr, (int8_t *) buf + i, arg)) < 0)
        return ret;

      if (buf[i] == '\0')
        return 0;               /* copied full string; return success */
    }
  buf[buf_len - 1] = '\0';      /* ensure string is NUL terminated */
  return -UNW_ENOMEM;
}

static inline int
get_proc_name (unw_addr_space_t as, unw_word_t ip,
               char *buf, size_t buf_len, unw_word_t *offp, void *arg)
{
  unw_accessors_t *a = unw_get_accessors (as);
  unw_proc_info_t pi;
  int ret;

  buf[0] = '\0';        /* always return a valid string, even if it's empty */

  ret = unwi_find_dynamic_proc_info (as, ip, &pi, 1, arg);
  if (ret == 0)
    {
      unw_dyn_info_t *di = pi.unwind_info;

      if (offp)
        *offp = ip - pi.start_ip;

      switch (di->format)
        {
        case UNW_INFO_FORMAT_DYNAMIC:
          ret = intern_string (as, a, di->u.pi.name_ptr, buf, buf_len, arg);
          break;

        case UNW_INFO_FORMAT_TABLE:
        case UNW_INFO_FORMAT_REMOTE_TABLE:
          /* XXX should we create a fake name, e.g.: "tablenameN",
             where N is the index of the function in the table??? */
          ret = -UNW_ENOINFO;
          break;

        default:
          ret = -UNW_EINVAL;
          break;
        }
      unwi_put_dynamic_unwind_info (as, &pi, arg);
      return ret;
    }

  if (ret != -UNW_ENOINFO)
    return ret;

  /* not a dynamic procedure, try to lookup static procedure name: */

  if (a->get_proc_name)
    return (*a->get_proc_name) (as, ip, buf, buf_len, offp, arg);

  return -UNW_ENOINFO;
}

PROTECTED int
unw_get_proc_name (unw_cursor_t *cursor, char *buf, size_t buf_len,
                   unw_word_t *offp)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t ip;
  int error;

  ip = tdep_get_ip (c);
  if (c->dwarf.use_prev_instr)
    --ip;
  error = get_proc_name (tdep_get_as (c), ip, buf, buf_len, offp,
                         tdep_get_as_arg (c));
  if (c->dwarf.use_prev_instr && offp != NULL && error == 0)
    *offp += 1;
  return error;
}
