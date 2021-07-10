/* Header file for gimple iterators.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GIMPLE_ITERATOR_H
#define GCC_GIMPLE_ITERATOR_H

/* Iterator object for GIMPLE statement sequences.  */

struct gimple_stmt_iterator
{
  /* Sequence node holding the current statement.  */
  gimple_seq_node ptr;

  /* Sequence and basic block holding the statement.  These fields
     are necessary to handle edge cases such as when statement is
     added to an empty basic block or when the last statement of a
     block/sequence is removed.  */
  gimple_seq *seq;
  basic_block bb;
};

/* Iterator over GIMPLE_PHI statements.  */
struct gphi_iterator : public gimple_stmt_iterator
{
  gphi *phi () const
  {
    return as_a <gphi *> (ptr);
  }
};
 
enum gsi_iterator_update
{
  GSI_NEW_STMT,		/* Only valid when single statement is added, move
			   iterator to it.  */
  GSI_SAME_STMT,	/* Leave the iterator at the same statement.  */
  GSI_CONTINUE_LINKING	/* Move iterator to whatever position is suitable
			   for linking other statements in the same
			   direction.  */
};

extern void gsi_insert_seq_before_without_update (gimple_stmt_iterator *,
						  gimple_seq,
						  enum gsi_iterator_update);
extern void gsi_insert_seq_before (gimple_stmt_iterator *, gimple_seq,
				   enum gsi_iterator_update);
extern void gsi_insert_seq_after_without_update (gimple_stmt_iterator *,
						 gimple_seq,
						 enum gsi_iterator_update);
extern void gsi_insert_seq_after (gimple_stmt_iterator *, gimple_seq,
				  enum gsi_iterator_update);
extern gimple_seq gsi_split_seq_after (gimple_stmt_iterator);
extern void gsi_set_stmt (gimple_stmt_iterator *, gimple *);
extern void gsi_split_seq_before (gimple_stmt_iterator *, gimple_seq *);
extern bool gsi_replace (gimple_stmt_iterator *, gimple *, bool);
extern void gsi_replace_with_seq (gimple_stmt_iterator *, gimple_seq, bool);
extern void gsi_insert_before_without_update (gimple_stmt_iterator *, gimple *,
					      enum gsi_iterator_update);
extern void gsi_insert_before (gimple_stmt_iterator *, gimple *,
			       enum gsi_iterator_update);
extern void gsi_insert_after_without_update (gimple_stmt_iterator *, gimple *,
					     enum gsi_iterator_update);
extern void gsi_insert_after (gimple_stmt_iterator *, gimple *,
			      enum gsi_iterator_update);
extern bool gsi_remove (gimple_stmt_iterator *, bool);
extern gimple_stmt_iterator gsi_for_stmt (gimple *);
extern gphi_iterator gsi_for_phi (gphi *);
extern void gsi_move_after (gimple_stmt_iterator *, gimple_stmt_iterator *);
extern void gsi_move_before (gimple_stmt_iterator *, gimple_stmt_iterator *);
extern void gsi_move_to_bb_end (gimple_stmt_iterator *, basic_block);
extern void gsi_insert_on_edge (edge, gimple *);
extern void gsi_insert_seq_on_edge (edge, gimple_seq);
extern basic_block gsi_insert_on_edge_immediate (edge, gimple *);
extern basic_block gsi_insert_seq_on_edge_immediate (edge, gimple_seq);
extern void gsi_commit_edge_inserts (void);
extern void gsi_commit_one_edge_insert (edge, basic_block *);
extern gphi_iterator gsi_start_phis (basic_block);
extern void update_modified_stmts (gimple_seq);

/* Return a new iterator pointing to GIMPLE_SEQ's first statement.  */

static inline gimple_stmt_iterator
gsi_start_1 (gimple_seq *seq)
{
  gimple_stmt_iterator i;

  i.ptr = gimple_seq_first (*seq);
  i.seq = seq;
  i.bb = i.ptr ? gimple_bb (i.ptr) : NULL;

  return i;
}

#define gsi_start(x) gsi_start_1 (&(x))

static inline gimple_stmt_iterator
gsi_none (void)
{
  gimple_stmt_iterator i;
  i.ptr = NULL;
  i.seq = NULL;
  i.bb = NULL;
  return i;
}

/* Return a new iterator pointing to the first statement in basic block BB.  */

static inline gimple_stmt_iterator
gsi_start_bb (basic_block bb)
{
  gimple_stmt_iterator i;
  gimple_seq *seq;

  seq = bb_seq_addr (bb);
  i.ptr = gimple_seq_first (*seq);
  i.seq = seq;
  i.bb = bb;

  return i;
}

gimple_stmt_iterator gsi_start_edge (edge e);

/* Return a new iterator initially pointing to GIMPLE_SEQ's last statement.  */

static inline gimple_stmt_iterator
gsi_last_1 (gimple_seq *seq)
{
  gimple_stmt_iterator i;

  i.ptr = gimple_seq_last (*seq);
  i.seq = seq;
  i.bb = i.ptr ? gimple_bb (i.ptr) : NULL;

  return i;
}

#define gsi_last(x) gsi_last_1 (&(x))

/* Return a new iterator pointing to the last statement in basic block BB.  */

static inline gimple_stmt_iterator
gsi_last_bb (basic_block bb)
{
  gimple_stmt_iterator i;
  gimple_seq *seq;

  seq = bb_seq_addr (bb);
  i.ptr = gimple_seq_last (*seq);
  i.seq = seq;
  i.bb = bb;

  return i;
}

/* Return true if I is at the end of its sequence.  */

static inline bool
gsi_end_p (gimple_stmt_iterator i)
{
  return i.ptr == NULL;
}

/* Return true if I is one statement before the end of its sequence.  */

static inline bool
gsi_one_before_end_p (gimple_stmt_iterator i)
{
  return i.ptr != NULL && i.ptr->next == NULL;
}

/* Advance the iterator to the next gimple statement.  */

static inline void
gsi_next (gimple_stmt_iterator *i)
{
  i->ptr = i->ptr->next;
}

/* Advance the iterator to the previous gimple statement.  */

static inline void
gsi_prev (gimple_stmt_iterator *i)
{
  gimple *prev = i->ptr->prev;
  if (prev->next)
    i->ptr = prev;
  else
    i->ptr = NULL;
}

/* Return the current stmt.  */

static inline gimple *
gsi_stmt (gimple_stmt_iterator i)
{
  return i.ptr;
}

/* Return a block statement iterator that points to the first
   non-label statement in block BB.  */

static inline gimple_stmt_iterator
gsi_after_labels (basic_block bb)
{
  gimple_stmt_iterator gsi = gsi_start_bb (bb);

  for (; !gsi_end_p (gsi); )
    {
      if (gimple_code (gsi_stmt (gsi)) == GIMPLE_LABEL)
	gsi_next (&gsi);
      else
	break;
    }

  return gsi;
}

/* Advance the iterator to the next non-debug gimple statement.  */

static inline void
gsi_next_nondebug (gimple_stmt_iterator *i)
{
  do
    {
      gsi_next (i);
    }
  while (!gsi_end_p (*i) && is_gimple_debug (gsi_stmt (*i)));
}

/* Advance the iterator to the previous non-debug gimple statement.  */

static inline void
gsi_prev_nondebug (gimple_stmt_iterator *i)
{
  do
    {
      gsi_prev (i);
    }
  while (!gsi_end_p (*i) && is_gimple_debug (gsi_stmt (*i)));
}

/* Return a new iterator pointing to the first non-debug statement in
   SEQ.  */

static inline gimple_stmt_iterator
gsi_start_nondebug (gimple_seq seq)
{
  gimple_stmt_iterator gsi = gsi_start (seq);
  if (!gsi_end_p (gsi) && is_gimple_debug (gsi_stmt (gsi)))
    gsi_next_nondebug (&gsi);

  return gsi;
}

/* Return a new iterator pointing to the first non-debug statement in
   basic block BB.  */

static inline gimple_stmt_iterator
gsi_start_nondebug_bb (basic_block bb)
{
  gimple_stmt_iterator i = gsi_start_bb (bb);

  if (!gsi_end_p (i) && is_gimple_debug (gsi_stmt (i)))
    gsi_next_nondebug (&i);

  return i;
}

/* Return a new iterator pointing to the first non-debug non-label statement in
   basic block BB.  */

static inline gimple_stmt_iterator
gsi_start_nondebug_after_labels_bb (basic_block bb)
{
  gimple_stmt_iterator i = gsi_after_labels (bb);

  if (!gsi_end_p (i) && is_gimple_debug (gsi_stmt (i)))
    gsi_next_nondebug (&i);

  return i;
}

/* Return a new iterator pointing to the last non-debug statement in
   basic block BB.  */

static inline gimple_stmt_iterator
gsi_last_nondebug_bb (basic_block bb)
{
  gimple_stmt_iterator i = gsi_last_bb (bb);

  if (!gsi_end_p (i) && is_gimple_debug (gsi_stmt (i)))
    gsi_prev_nondebug (&i);

  return i;
}

/* Return true if I is followed only by debug statements in its
   sequence.  */

static inline bool
gsi_one_nondebug_before_end_p (gimple_stmt_iterator i)
{
  if (gsi_one_before_end_p (i))
    return true;
  if (gsi_end_p (i))
    return false;
  gsi_next_nondebug (&i);
  return gsi_end_p (i);
}

/* Iterates I statement iterator to the next non-virtual statement.  */

static inline void
gsi_next_nonvirtual_phi (gphi_iterator *i)
{
  gphi *phi;

  if (gsi_end_p (*i))
    return;

  phi = i->phi ();
  gcc_assert (phi != NULL);

  while (virtual_operand_p (gimple_phi_result (phi)))
    {
      gsi_next (i);

      if (gsi_end_p (*i))
	return;

      phi = i->phi ();
    }
}

/* Return the basic block associated with this iterator.  */

static inline basic_block
gsi_bb (gimple_stmt_iterator i)
{
  return i.bb;
}

/* Return the sequence associated with this iterator.  */

static inline gimple_seq
gsi_seq (gimple_stmt_iterator i)
{
  return *i.seq;
}

/* Determine whether SEQ is a nondebug singleton.  */

static inline bool
gimple_seq_nondebug_singleton_p (gimple_seq seq)
{
  gimple_stmt_iterator gsi;

  /* Find a nondebug gimple.  */
  gsi.ptr = gimple_seq_first (seq);
  gsi.seq = &seq;
  gsi.bb = NULL;
  while (!gsi_end_p (gsi)
	 && is_gimple_debug (gsi_stmt (gsi)))
    gsi_next (&gsi);

  /* No nondebug gimple found, not a singleton.  */
  if (gsi_end_p (gsi))
    return false;

  /* Find a next nondebug gimple.  */
  gsi_next (&gsi);
  while (!gsi_end_p (gsi)
	 && is_gimple_debug (gsi_stmt (gsi)))
    gsi_next (&gsi);

  /* Only a singleton if there's no next nondebug gimple.  */
  return gsi_end_p (gsi);
}

#endif /* GCC_GIMPLE_ITERATOR_H */
