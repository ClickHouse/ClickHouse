/* Target-dependent globals.
   Copyright (C) 2010-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef TARGET_GLOBALS_H
#define TARGET_GLOBALS_H 1

#if SWITCHABLE_TARGET
extern struct target_flag_state *this_target_flag_state;
extern struct target_regs *this_target_regs;
extern struct target_rtl *this_target_rtl;
extern struct target_recog *this_target_recog;
extern struct target_hard_regs *this_target_hard_regs;
extern struct target_reload *this_target_reload;
extern struct target_expmed *this_target_expmed;
extern struct target_optabs *this_target_optabs;
extern struct target_libfuncs *this_target_libfuncs;
extern struct target_cfgloop *this_target_cfgloop;
extern struct target_ira *this_target_ira;
extern struct target_ira_int *this_target_ira_int;
extern struct target_builtins *this_target_builtins;
extern struct target_gcse *this_target_gcse;
extern struct target_bb_reorder *this_target_bb_reorder;
extern struct target_lower_subreg *this_target_lower_subreg;
#endif

struct GTY(()) target_globals {
  ~target_globals ();

  struct target_flag_state *GTY((skip)) flag_state;
  struct target_regs *GTY((skip)) regs;
  struct target_rtl *rtl;
  struct target_recog *GTY((skip)) recog;
  struct target_hard_regs *GTY((skip)) hard_regs;
  struct target_reload *GTY((skip)) reload;
  struct target_expmed *GTY((skip)) expmed;
  struct target_optabs *GTY((skip)) optabs;
  struct target_libfuncs *libfuncs;
  struct target_cfgloop *GTY((skip)) cfgloop;
  struct target_ira *GTY((skip)) ira;
  struct target_ira_int *GTY((skip)) ira_int;
  struct target_builtins *GTY((skip)) builtins;
  struct target_gcse *GTY((skip)) gcse;
  struct target_bb_reorder *GTY((skip)) bb_reorder;
  struct target_lower_subreg *GTY((skip)) lower_subreg;
};

#if SWITCHABLE_TARGET
extern struct target_globals default_target_globals;

extern struct target_globals *save_target_globals (void);
extern struct target_globals *save_target_globals_default_opts (void);

static inline void
restore_target_globals (struct target_globals *g)
{
  this_target_flag_state = g->flag_state;
  this_target_regs = g->regs;
  this_target_rtl = g->rtl;
  this_target_recog = g->recog;
  this_target_hard_regs = g->hard_regs;
  this_target_reload = g->reload;
  this_target_expmed = g->expmed;
  this_target_optabs = g->optabs;
  this_target_libfuncs = g->libfuncs;
  this_target_cfgloop = g->cfgloop;
  this_target_ira = g->ira;
  this_target_ira_int = g->ira_int;
  this_target_builtins = g->builtins;
  this_target_gcse = g->gcse;
  this_target_bb_reorder = g->bb_reorder;
  this_target_lower_subreg = g->lower_subreg;
}
#endif

#endif
