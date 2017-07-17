/* libunwind - a platform-independent unwind library
   Copyright 2011 Linaro Limited

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

/* This file contains functionality for parsing and interpreting the ARM
specific unwind information.  Documentation about the exception handling
ABI for the ARM architecture can be found at:
http://infocenter.arm.com/help/topic/com.arm.doc.ihi0038a/IHI0038A_ehabi.pdf
*/ 

#include "libunwind_i.h"

#define ARM_EXBUF_START(x)      (((x) >> 4) & 0x0f)
#define ARM_EXBUF_COUNT(x)      ((x) & 0x0f)
#define ARM_EXBUF_END(x)        (ARM_EXBUF_START(x) + ARM_EXBUF_COUNT(x))

#define ARM_EXIDX_CANT_UNWIND   0x00000001
#define ARM_EXIDX_COMPACT       0x80000000

#define ARM_EXTBL_OP_FINISH     0xb0

enum arm_exbuf_cmd_flags {
  ARM_EXIDX_VFP_SHIFT_16 = 1 << 16,
  ARM_EXIDX_VFP_DOUBLE = 1 << 17,
};

struct arm_cb_data
  {
    /* in: */
    unw_word_t ip;             /* instruction-pointer we're looking for */
    unw_proc_info_t *pi;       /* proc-info pointer */
    /* out: */
    unw_dyn_info_t di;         /* info about the ARM exidx segment */
  };

static inline uint32_t CONST_ATTR
prel31_read (uint32_t prel31)
{
  return ((int32_t)prel31 << 1) >> 1;
}

static inline int
prel31_to_addr (unw_addr_space_t as, void *arg, unw_word_t prel31,
                unw_word_t *val)
{
  unw_word_t offset;

  if ((*as->acc.access_mem)(as, prel31, &offset, 0, arg) < 0)
    return -UNW_EINVAL;

  offset = ((long)offset << 1) >> 1;
  *val = prel31 + offset;

  return 0;
}

/**
 * Applies the given command onto the new state to the given dwarf_cursor.
 */
HIDDEN int
arm_exidx_apply_cmd (struct arm_exbuf_data *edata, struct dwarf_cursor *c)
{
  int ret = 0;
  unsigned i;

  switch (edata->cmd)
    {
    case ARM_EXIDX_CMD_FINISH:
      /* Set LR to PC if not set already.  */
      if (DWARF_IS_NULL_LOC (c->loc[UNW_ARM_R15]))
        c->loc[UNW_ARM_R15] = c->loc[UNW_ARM_R14];
      /* Set IP.  */
      dwarf_get (c, c->loc[UNW_ARM_R15], &c->ip);
      break;
    case ARM_EXIDX_CMD_DATA_PUSH:
      Debug (2, "vsp = vsp - %d\n", edata->data);
      c->cfa -= edata->data;
      break;
    case ARM_EXIDX_CMD_DATA_POP:
      Debug (2, "vsp = vsp + %d\n", edata->data);
      c->cfa += edata->data;
      break;
    case ARM_EXIDX_CMD_REG_POP:
      for (i = 0; i < 16; i++)
        if (edata->data & (1 << i))
          {
            Debug (2, "pop {r%d}\n", i);
            c->loc[UNW_ARM_R0 + i] = DWARF_LOC (c->cfa, 0);
            c->cfa += 4;
          }
      /* Set cfa in case the SP got popped. */
      if (edata->data & (1 << 13))
        dwarf_get (c, c->loc[UNW_ARM_R13], &c->cfa);
      break;
    case ARM_EXIDX_CMD_REG_TO_SP:
      assert (edata->data < 16);
      Debug (2, "vsp = r%d\n", edata->data);
      c->loc[UNW_ARM_R13] = c->loc[UNW_ARM_R0 + edata->data];
      dwarf_get (c, c->loc[UNW_ARM_R13], &c->cfa);
      break;
    case ARM_EXIDX_CMD_VFP_POP:
      /* Skip VFP registers, but be sure to adjust stack */
      for (i = ARM_EXBUF_START (edata->data); i <= ARM_EXBUF_END (edata->data);
           i++)
        c->cfa += 8;
      if (!(edata->data & ARM_EXIDX_VFP_DOUBLE))
        c->cfa += 4;
      break;
    case ARM_EXIDX_CMD_WREG_POP:
      for (i = ARM_EXBUF_START (edata->data); i <= ARM_EXBUF_END (edata->data);
           i++)
        c->cfa += 8;
      break;
    case ARM_EXIDX_CMD_WCGR_POP:
      for (i = 0; i < 4; i++)
        if (edata->data & (1 << i))
          c->cfa += 4;
      break;
    case ARM_EXIDX_CMD_REFUSED:
    case ARM_EXIDX_CMD_RESERVED:
      ret = -1;
      break;
    }
  return ret;
}

/**
 * Decodes the given unwind instructions into arm_exbuf_data and calls
 * arm_exidx_apply_cmd that applies the command onto the dwarf_cursor.
 */
HIDDEN int
arm_exidx_decode (const uint8_t *buf, uint8_t len, struct dwarf_cursor *c)
{
#define READ_OP() *buf++
  const uint8_t *end = buf + len;
  int ret;
  struct arm_exbuf_data edata;

  assert(buf != NULL);
  assert(len > 0);

  while (buf < end)
    {
      uint8_t op = READ_OP ();
      if ((op & 0xc0) == 0x00)
        {
          edata.cmd = ARM_EXIDX_CMD_DATA_POP;
          edata.data = (((int)op & 0x3f) << 2) + 4;
        }
      else if ((op & 0xc0) == 0x40)
        {
          edata.cmd = ARM_EXIDX_CMD_DATA_PUSH;
          edata.data = (((int)op & 0x3f) << 2) + 4;
        }
      else if ((op & 0xf0) == 0x80)
        {
          uint8_t op2 = READ_OP ();
          if (op == 0x80 && op2 == 0x00)
            edata.cmd = ARM_EXIDX_CMD_REFUSED;
          else
            {
              edata.cmd = ARM_EXIDX_CMD_REG_POP;
              edata.data = ((op & 0xf) << 8) | op2;
              edata.data = edata.data << 4;
            }
        }
      else if ((op & 0xf0) == 0x90)
        {
          if (op == 0x9d || op == 0x9f)
            edata.cmd = ARM_EXIDX_CMD_RESERVED;
          else
            {
              edata.cmd = ARM_EXIDX_CMD_REG_TO_SP;
              edata.data = op & 0x0f;
            }
        }
      else if ((op & 0xf0) == 0xa0)
        {
          unsigned end = (op & 0x07);
          edata.data = (1 << (end + 1)) - 1;
          edata.data = edata.data << 4;
          if (op & 0x08)
            edata.data |= 1 << 14;
          edata.cmd = ARM_EXIDX_CMD_REG_POP;
        }
      else if (op == ARM_EXTBL_OP_FINISH)
        {
          edata.cmd = ARM_EXIDX_CMD_FINISH;
          buf = end;
        }
      else if (op == 0xb1)
        {
          uint8_t op2 = READ_OP ();
          if (op2 == 0 || (op2 & 0xf0))
            edata.cmd = ARM_EXIDX_CMD_RESERVED;
          else
            {
              edata.cmd = ARM_EXIDX_CMD_REG_POP;
              edata.data = op2 & 0x0f;
            }
        }
      else if (op == 0xb2)
        {
          uint32_t offset = 0;
          uint8_t byte, shift = 0;
          do
            {
              byte = READ_OP ();
              offset |= (byte & 0x7f) << shift;
              shift += 7;
            }
          while (byte & 0x80);
          edata.data = offset * 4 + 0x204;
          edata.cmd = ARM_EXIDX_CMD_DATA_POP;
        }
      else if (op == 0xb3 || op == 0xc8 || op == 0xc9)
        {
          edata.cmd = ARM_EXIDX_CMD_VFP_POP;
          edata.data = READ_OP ();
          if (op == 0xc8)
            edata.data |= ARM_EXIDX_VFP_SHIFT_16;
          if (op != 0xb3)
            edata.data |= ARM_EXIDX_VFP_DOUBLE;
        }
      else if ((op & 0xf8) == 0xb8 || (op & 0xf8) == 0xd0)
        {
          edata.cmd = ARM_EXIDX_CMD_VFP_POP;
          edata.data = 0x80 | (op & 0x07);
          if ((op & 0xf8) == 0xd0)
            edata.data |= ARM_EXIDX_VFP_DOUBLE;
        }
      else if (op >= 0xc0 && op <= 0xc5)
        {
          edata.cmd = ARM_EXIDX_CMD_WREG_POP;
          edata.data = 0xa0 | (op & 0x07);
        }
      else if (op == 0xc6)
        {
          edata.cmd = ARM_EXIDX_CMD_WREG_POP;
          edata.data = READ_OP ();
        }
      else if (op == 0xc7)
        {
          uint8_t op2 = READ_OP ();
          if (op2 == 0 || (op2 & 0xf0))
            edata.cmd = ARM_EXIDX_CMD_RESERVED;
          else
            {
              edata.cmd = ARM_EXIDX_CMD_WCGR_POP;
              edata.data = op2 & 0x0f;
            }
        }
      else
        edata.cmd = ARM_EXIDX_CMD_RESERVED;

      ret = arm_exidx_apply_cmd (&edata, c);
      if (ret < 0)
        return ret;
    }
  return 0;
}

/**
 * Reads the entry from the given cursor and extracts the unwind instructions
 * into buf.  Returns the number of the extracted unwind insns or 
 * -UNW_ESTOPUNWIND if the special bit pattern ARM_EXIDX_CANT_UNWIND (0x1) was
 * found.
 */
HIDDEN int
arm_exidx_extract (struct dwarf_cursor *c, uint8_t *buf)
{
  int nbuf = 0;
  unw_word_t entry = (unw_word_t) c->pi.unwind_info;
  unw_word_t addr;
  uint32_t data;

  /* An ARM unwind entry consists of a prel31 offset to the start of a
     function followed by 31bits of data: 
       * if set to 0x1: the function cannot be unwound (EXIDX_CANTUNWIND)
       * if bit 31 is one: this is a table entry itself (ARM_EXIDX_COMPACT)
       * if bit 31 is zero: this is a prel31 offset of the start of the
         table entry for this function  */
  if (prel31_to_addr(c->as, c->as_arg, entry, &addr) < 0)
    return -UNW_EINVAL;

  if ((*c->as->acc.access_mem)(c->as, entry + 4, &data, 0, c->as_arg) < 0)
    return -UNW_EINVAL;

  if (data == ARM_EXIDX_CANT_UNWIND)
    {
      Debug (2, "0x1 [can't unwind]\n");
      nbuf = -UNW_ESTOPUNWIND;
    }
  else if (data & ARM_EXIDX_COMPACT)
    {
      Debug (2, "%p compact model %d [%8.8x]\n", (void *)addr,
             (data >> 24) & 0x7f, data);
      buf[nbuf++] = data >> 16;
      buf[nbuf++] = data >> 8;
      buf[nbuf++] = data;
    }
  else
    {
      unw_word_t extbl_data;
      unsigned int n_table_words = 0;

      if (prel31_to_addr(c->as, c->as_arg, entry + 4, &extbl_data) < 0)
        return -UNW_EINVAL;

      if ((*c->as->acc.access_mem)(c->as, extbl_data, &data, 0, c->as_arg) < 0)
        return -UNW_EINVAL;

      if (data & ARM_EXIDX_COMPACT)
        {
          int pers = (data >> 24) & 0x0f;
          Debug (2, "%p compact model %d [%8.8x]\n", (void *)addr, pers, data);
          if (pers == 1 || pers == 2)
            {
              n_table_words = (data >> 16) & 0xff;
              extbl_data += 4;
            }
          else
            buf[nbuf++] = data >> 16;
          buf[nbuf++] = data >> 8;
          buf[nbuf++] = data;
        }
      else
        {
          unw_word_t pers;
          if (prel31_to_addr (c->as, c->as_arg, extbl_data, &pers) < 0)
            return -UNW_EINVAL;
          Debug (2, "%p Personality routine: %8p\n", (void *)addr,
                 (void *)pers);
          if ((*c->as->acc.access_mem)(c->as, extbl_data + 4, &data, 0,
                                       c->as_arg) < 0)
            return -UNW_EINVAL;
          n_table_words = data >> 24;
          buf[nbuf++] = data >> 16;
          buf[nbuf++] = data >> 8;
          buf[nbuf++] = data;
          extbl_data += 8;
        }
      assert (n_table_words <= 5);
      unsigned j;
      for (j = 0; j < n_table_words; j++)
        {
          if ((*c->as->acc.access_mem)(c->as, extbl_data, &data, 0,
                                       c->as_arg) < 0)
            return -UNW_EINVAL;
          extbl_data += 4;
          buf[nbuf++] = data >> 24;
          buf[nbuf++] = data >> 16;
          buf[nbuf++] = data >> 8;
          buf[nbuf++] = data >> 0;
        }
    }

  if (nbuf > 0 && buf[nbuf - 1] != ARM_EXTBL_OP_FINISH)
    buf[nbuf++] = ARM_EXTBL_OP_FINISH;

  return nbuf;
}

PROTECTED int
arm_search_unwind_table (unw_addr_space_t as, unw_word_t ip,
			 unw_dyn_info_t *di, unw_proc_info_t *pi,
			 int need_unwind_info, void *arg)
{
  /* The .ARM.exidx section contains a sorted list of key-value pairs -
     the unwind entries.  The 'key' is a prel31 offset to the start of a
     function.  We binary search this section in order to find the
     appropriate unwind entry.  */
  unw_word_t first = di->u.rti.table_data;
  unw_word_t last = di->u.rti.table_data + di->u.rti.table_len - 8;
  unw_word_t entry, val;

  if (prel31_to_addr (as, arg, first, &val) < 0 || ip < val)
    return -UNW_ENOINFO;

  if (prel31_to_addr (as, arg, last, &val) < 0)
    return -UNW_EINVAL;

  if (ip >= val)
    {
      entry = last;

      if (prel31_to_addr (as, arg, last, &pi->start_ip) < 0)
	return -UNW_EINVAL;

      pi->end_ip = di->end_ip -1;
    }
  else
    {
      while (first < last - 8)
	{
	  entry = first + (((last - first) / 8 + 1) >> 1) * 8;

	  if (prel31_to_addr (as, arg, entry, &val) < 0)
	    return -UNW_EINVAL;

	  if (ip < val)
	    last = entry;
	  else
	    first = entry;
	}

      entry = first;

      if (prel31_to_addr (as, arg, entry, &pi->start_ip) < 0)
	return -UNW_EINVAL;

      if (prel31_to_addr (as, arg, entry + 8, &pi->end_ip) < 0)
	return -UNW_EINVAL;

      pi->end_ip--;
    }

  if (need_unwind_info)
    {
      pi->unwind_info_size = 8;
      pi->unwind_info = (void *) entry;
      pi->format = UNW_INFO_FORMAT_ARM_EXIDX;
    }
  return 0;
}

PROTECTED int
tdep_search_unwind_table (unw_addr_space_t as, unw_word_t ip,
                             unw_dyn_info_t *di, unw_proc_info_t *pi,
                             int need_unwind_info, void *arg)
{
  if (UNW_TRY_METHOD (UNW_ARM_METHOD_EXIDX)
      && di->format == UNW_INFO_FORMAT_ARM_EXIDX)
    return arm_search_unwind_table (as, ip, di, pi, need_unwind_info, arg);
  else if (UNW_TRY_METHOD(UNW_ARM_METHOD_DWARF)
           && di->format != UNW_INFO_FORMAT_ARM_EXIDX)
    return dwarf_search_unwind_table (as, ip, di, pi, need_unwind_info, arg);

  return -UNW_ENOINFO; 
}

#ifndef UNW_REMOTE_ONLY
/**
 * Callback to dl_iterate_phdr to find infos about the ARM exidx segment.
 */
static int
arm_phdr_cb (struct dl_phdr_info *info, size_t size, void *data)
{
  struct arm_cb_data *cb_data = data;
  const Elf_W(Phdr) *p_text = NULL;
  const Elf_W(Phdr) *p_arm_exidx = NULL;
  const Elf_W(Phdr) *phdr = info->dlpi_phdr;
  long n;

  for (n = info->dlpi_phnum; --n >= 0; phdr++)
    {
      switch (phdr->p_type)
        {
        case PT_LOAD:
          if (cb_data->ip >= phdr->p_vaddr + info->dlpi_addr &&
              cb_data->ip < phdr->p_vaddr + info->dlpi_addr + phdr->p_memsz)
            p_text = phdr;
          break;

        case PT_ARM_EXIDX:
          p_arm_exidx = phdr;
          break;

        default:
          break;
        }
    }

  if (p_text && p_arm_exidx)
    {
      cb_data->di.format = UNW_INFO_FORMAT_ARM_EXIDX;
      cb_data->di.start_ip = p_text->p_vaddr + info->dlpi_addr;
      cb_data->di.end_ip = cb_data->di.start_ip + p_text->p_memsz;
      cb_data->di.u.rti.name_ptr = (unw_word_t) info->dlpi_name;
      cb_data->di.u.rti.table_data = p_arm_exidx->p_vaddr + info->dlpi_addr;
      cb_data->di.u.rti.table_len = p_arm_exidx->p_memsz;
      return 1;
    }

  return 0;
}

HIDDEN int
arm_find_proc_info (unw_addr_space_t as, unw_word_t ip,
                    unw_proc_info_t *pi, int need_unwind_info, void *arg)
{
  int ret = -1;
  intrmask_t saved_mask;

  Debug (14, "looking for IP=0x%lx\n", (long) ip);

  if (UNW_TRY_METHOD(UNW_ARM_METHOD_DWARF))
    ret = dwarf_find_proc_info (as, ip, pi, need_unwind_info, arg);

  if (ret < 0 && UNW_TRY_METHOD (UNW_ARM_METHOD_EXIDX))
    {
      struct arm_cb_data cb_data;

      memset (&cb_data, 0, sizeof (cb_data));
      cb_data.ip = ip;
      cb_data.pi = pi;
      cb_data.di.format = -1;

      SIGPROCMASK (SIG_SETMASK, &unwi_full_mask, &saved_mask);
      ret = dl_iterate_phdr (arm_phdr_cb, &cb_data);
      SIGPROCMASK (SIG_SETMASK, &saved_mask, NULL);

      if (cb_data.di.format != -1)
        ret = arm_search_unwind_table (as, ip, &cb_data.di, pi,
				       need_unwind_info, arg);
      else
        ret = -UNW_ENOINFO;
    }

  return ret;
}

HIDDEN void
arm_put_unwind_info (unw_addr_space_t as, unw_proc_info_t *proc_info, void *arg)
{
  /* it's a no-op */
}
#endif /* !UNW_REMOTE_ONLY */

