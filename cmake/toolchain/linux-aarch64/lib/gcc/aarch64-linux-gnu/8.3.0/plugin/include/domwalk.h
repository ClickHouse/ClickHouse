/* Generic dominator tree walker
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Diego Novillo <dnovillo@redhat.com>

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

#ifndef GCC_DOM_WALK_H
#define GCC_DOM_WALK_H

/**
 * This is the main class for the dominator walker. It is expected that
 * consumers will have a custom class inheriting from it, which will over ride
 * at least one of before_dom_children and after_dom_children to implement the
 * custom behavior.
 */
class dom_walker
{
public:
  static const edge STOP;

  /* An enum for determining whether the dom walk should be constrained to
     blocks reachable by executable edges.  */

  enum reachability
  {
    /* Walk all blocks within the CFG.  */
    ALL_BLOCKS,

    /* Use REACHABLE_BLOCKS when your subclass can discover that some edges
       are not executable.

       If a subclass can discover that a COND, SWITCH or GOTO has a static
       target in the before_dom_children callback, the taken edge should
       be returned.  The generic walker will clear EDGE_EXECUTABLE on all
       edges it can determine are not executable.

       With REACHABLE_BLOCKS, EDGE_EXECUTABLE will be set on every edge in
       the dom_walker ctor; the flag will then be cleared on edges that are
       determined to be not executable.  */
    REACHABLE_BLOCKS,

    /* Identical to REACHABLE_BLOCKS, but the initial state of EDGE_EXECUTABLE
       will instead be preserved in the ctor, allowing for information about
       non-executable edges to be merged in from an earlier analysis (and
       potentially for additional edges to be marked as non-executable).  */
    REACHABLE_BLOCKS_PRESERVING_FLAGS
  };

  dom_walker (cdi_direction direction, enum reachability = ALL_BLOCKS);

  /* You can provide a mapping of basic-block index to RPO if you
     have that readily available or you do multiple walks.  If you
     specify NULL as BB_INDEX_TO_RPO dominator children will not be
     walked in RPO order.  */
  dom_walker (cdi_direction direction, enum reachability, int *bb_index_to_rpo);

  ~dom_walker ();

  /* Walk the dominator tree.  */
  void walk (basic_block);

  /* Function to call before the recursive walk of the dominator children.

     Return value is the always taken edge if the block has multiple outgoing
     edges, NULL otherwise.  When skipping unreachable blocks, the walker
     uses the taken edge information to clear EDGE_EXECUTABLE on the other
     edges, exposing unreachable blocks.  A NULL return value means all
     outgoing edges should still be considered executable.  A return value
     of STOP means to stop the domwalk from processing dominated blocks from
     here.  This can be used to process a SEME region only (note domwalk
     will still do work linear in function size).  */
  virtual edge before_dom_children (basic_block) { return NULL; }

  /* Function to call after the recursive walk of the dominator children.  */
  virtual void after_dom_children (basic_block) {}

private:
  /* This is the direction of the dominator tree we want to walk.  i.e.,
     if it is set to CDI_DOMINATORS, then we walk the dominator tree,
     if it is set to CDI_POST_DOMINATORS, then we walk the post
     dominator tree.  */
  const ENUM_BITFIELD (cdi_direction) m_dom_direction : 2;
  bool m_skip_unreachable_blocks;
  bool m_user_bb_to_rpo;
  basic_block m_unreachable_dom;
  int *m_bb_to_rpo;

  /* Query whether or not the given block is reachable or not.  */
  bool bb_reachable (struct function *, basic_block);

  /* Given an unreachable block, propagate that property to outgoing
     and possibly incoming edges for the block.  Typically called after
     determining a block is unreachable in the before_dom_children
     callback.  */
  void propagate_unreachable_to_edges (basic_block, FILE *, dump_flags_t);

};

extern void set_all_edges_as_executable (function *fn);

#endif
