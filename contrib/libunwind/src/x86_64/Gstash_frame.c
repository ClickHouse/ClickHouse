/* libunwind - a platform-independent unwind library
   Copyright (C) 2010, 2011 by FERMI NATIONAL ACCELERATOR LABORATORY

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
#include "ucontext_i.h"

HIDDEN void
tdep_stash_frame (struct dwarf_cursor *d, struct dwarf_reg_state *rs)
{
  struct cursor *c = (struct cursor *) dwarf_to_cursor (d);
  unw_tdep_frame_t *f = &c->frame_info;

  Debug (4, "ip=0x%lx cfa=0x%lx type %d cfa [where=%d val=%ld] cfaoff=%ld"
         " ra=0x%lx rbp [where=%d val=%ld @0x%lx] rsp [where=%d val=%ld @0x%lx]\n",
         d->ip, d->cfa, f->frame_type,
         rs->reg.where[DWARF_CFA_REG_COLUMN],
         rs->reg.val[DWARF_CFA_REG_COLUMN],
         rs->reg.val[DWARF_CFA_OFF_COLUMN],
         DWARF_GET_LOC(d->loc[rs->ret_addr_column]),
         rs->reg.where[RBP], rs->reg.val[RBP], DWARF_GET_LOC(d->loc[RBP]),
         rs->reg.where[RSP], rs->reg.val[RSP], DWARF_GET_LOC(d->loc[RSP]));

  if (rs->reg.where[DWARF_CFA_REG_COLUMN] == DWARF_WHERE_EXPR &&
    rs->reg.where[RBP] == DWARF_WHERE_EXPR) {
    /* Check for GCC generated alignment frame for rsp.  A simple
     * def_cfa_expr that loads a constant offset from rbp, where the
     * addres of the rip was pushed on the stack */
    unw_word_t cfa_addr = rs->reg.val[DWARF_CFA_REG_COLUMN];
    unw_word_t rbp_addr = rs->reg.val[RBP];
    unw_word_t cfa_offset;

    int ret = dwarf_stack_aligned(d, cfa_addr, rbp_addr, &cfa_offset);
    if (ret) {
      f->frame_type = UNW_X86_64_FRAME_ALIGNED;
      f->cfa_reg_offset = cfa_offset;
      f->cfa_reg_rsp = 0;
    }
  }

  /* A standard frame is defined as:
      - CFA is register-relative offset off RBP or RSP;
      - Return address is saved at CFA-8;
      - RBP is unsaved or saved at CFA+offset, offset != -1;
      - RSP is unsaved or saved at CFA+offset, offset != -1.  */
  if (f->frame_type == UNW_X86_64_FRAME_OTHER
      && (rs->reg.where[DWARF_CFA_REG_COLUMN] == DWARF_WHERE_REG)
      && (rs->reg.val[DWARF_CFA_REG_COLUMN] == RBP
          || rs->reg.val[DWARF_CFA_REG_COLUMN] == RSP)
      && labs((long) rs->reg.val[DWARF_CFA_OFF_COLUMN]) < (1 << 28)
      && DWARF_GET_LOC(d->loc[rs->ret_addr_column]) == d->cfa-8
      && (rs->reg.where[RBP] == DWARF_WHERE_UNDEF
          || rs->reg.where[RBP] == DWARF_WHERE_SAME
          || (rs->reg.where[RBP] == DWARF_WHERE_CFAREL
              && labs((long) rs->reg.val[RBP]) < (1 << 14)
              && rs->reg.val[RBP]+1 != 0))
      && (rs->reg.where[RSP] == DWARF_WHERE_UNDEF
          || rs->reg.where[RSP] == DWARF_WHERE_SAME
          || (rs->reg.where[RSP] == DWARF_WHERE_CFAREL
              && labs((long) rs->reg.val[RSP]) < (1 << 14)
              && rs->reg.val[RSP]+1 != 0)))
  {
    /* Save information for a standard frame. */
    f->frame_type = UNW_X86_64_FRAME_STANDARD;
    f->cfa_reg_rsp = (rs->reg.val[DWARF_CFA_REG_COLUMN] == RSP);
    f->cfa_reg_offset = rs->reg.val[DWARF_CFA_OFF_COLUMN];
    if (rs->reg.where[RBP] == DWARF_WHERE_CFAREL)
      f->rbp_cfa_offset = rs->reg.val[RBP];
    if (rs->reg.where[RSP] == DWARF_WHERE_CFAREL)
      f->rsp_cfa_offset = rs->reg.val[RSP];
    Debug (4, " standard frame\n");
  }

  /* Signal frame was detected via augmentation in tdep_fetch_frame()  */
  else if (f->frame_type == UNW_X86_64_FRAME_SIGRETURN)
  {
    /* Later we are going to fish out {RBP,RSP,RIP} from sigcontext via
       their ucontext_t offsets.  Confirm DWARF info agrees with the
       offsets we expect.  */

#ifndef NDEBUG
    const unw_word_t uc = c->sigcontext_addr;

    assert (DWARF_GET_LOC(d->loc[RIP]) - uc == UC_MCONTEXT_GREGS_RIP);
    assert (DWARF_GET_LOC(d->loc[RBP]) - uc == UC_MCONTEXT_GREGS_RBP);
    assert (DWARF_GET_LOC(d->loc[RSP]) - uc == UC_MCONTEXT_GREGS_RSP);
#endif

    Debug (4, " sigreturn frame\n");
  }

  else if (f->frame_type == UNW_X86_64_FRAME_ALIGNED) {
    Debug (4, " aligned frame, offset %li\n", f->cfa_reg_offset);
  }

  /* PLT and guessed RBP-walked frames are handled in unw_step(). */
  else
    Debug (4, " unusual frame\n");
}
