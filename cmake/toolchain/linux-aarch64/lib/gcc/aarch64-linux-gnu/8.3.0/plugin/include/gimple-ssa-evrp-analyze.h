/* Support routines for Value Range Propagation (VRP).
   Copyright (C) 2016-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_GIMPLE_SSA_EVRP_ANALYZE_H
#define GCC_GIMPLE_SSA_EVRP_ANALYZE_H

class evrp_range_analyzer
{
 public:
  evrp_range_analyzer (void);
  ~evrp_range_analyzer (void)
  {
    delete vr_values;
    stack.release ();
  }

  void enter (basic_block);
  void push_marker (void);
  void pop_to_marker (void);
  void leave (basic_block);
  void record_ranges_from_stmt (gimple *, bool);

  /* Main interface to retrieve range information.  */
  value_range *get_value_range (const_tree op)
    { return vr_values->get_value_range (op); }

  /* Record a new unwindable range.  */
  void push_value_range (tree var, value_range *vr);

  /* Dump all the current value ranges.  This is primarily
     a debugging interface.  */
  void dump_all_value_ranges (FILE *fp)
    { vr_values->dump_all_value_ranges (fp); }

  /* A bit of a wart.  This should ideally go away.  */
  void vrp_visit_cond_stmt (gcond *cond, edge *e)
    { return vr_values->vrp_visit_cond_stmt (cond, e); }

  /* Get the underlying vr_values class instance.  If TRANSFER is
     true, then we are transferring ownership.  Else we keep ownership.

     This should be converted to a unique_ptr.  */
  class vr_values *get_vr_values (void) { return vr_values; }

 private:
  DISABLE_COPY_AND_ASSIGN (evrp_range_analyzer);
  class vr_values *vr_values;

  value_range *pop_value_range (tree var);
  value_range *try_find_new_range (tree, tree op, tree_code code, tree limit);
  void record_ranges_from_incoming_edge (basic_block);
  void record_ranges_from_phis (basic_block);
  void set_ssa_range_info (tree, value_range *);

  /* STACK holds the old VR.  */
  auto_vec<std::pair <tree, value_range*> > stack;
};

#endif /* GCC_GIMPLE_SSA_EVRP_ANALYZE_H */
