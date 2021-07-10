/* Header file for SSA iterators.
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

#ifndef GCC_SSA_ITERATORS_H
#define GCC_SSA_ITERATORS_H

/* Immediate use lists are used to directly access all uses for an SSA
   name and get pointers to the statement for each use.

   The structure ssa_use_operand_t consists of PREV and NEXT pointers
   to maintain the list.  A USE pointer, which points to address where
   the use is located and a LOC pointer which can point to the
   statement where the use is located, or, in the case of the root
   node, it points to the SSA name itself.

   The list is anchored by an occurrence of ssa_operand_d *in* the
   ssa_name node itself (named 'imm_uses').  This node is uniquely
   identified by having a NULL USE pointer. and the LOC pointer
   pointing back to the ssa_name node itself.  This node forms the
   base for a circular list, and initially this is the only node in
   the list.

   Fast iteration allows each use to be examined, but does not allow
   any modifications to the uses or stmts.

   Normal iteration allows insertion, deletion, and modification. the
   iterator manages this by inserting a marker node into the list
   immediately before the node currently being examined in the list.
   this marker node is uniquely identified by having null stmt *and* a
   null use pointer.

   When iterating to the next use, the iteration routines check to see
   if the node after the marker has changed. if it has, then the node
   following the marker is now the next one to be visited. if not, the
   marker node is moved past that node in the list (visualize it as
   bumping the marker node through the list).  this continues until
   the marker node is moved to the original anchor position. the
   marker node is then removed from the list.

   If iteration is halted early, the marker node must be removed from
   the list before continuing.  */
struct imm_use_iterator
{
  /* This is the current use the iterator is processing.  */
  ssa_use_operand_t *imm_use;
  /* This marks the last use in the list (use node from SSA_NAME)  */
  ssa_use_operand_t *end_p;
  /* This node is inserted and used to mark the end of the uses for a stmt.  */
  ssa_use_operand_t iter_node;
  /* This is the next ssa_name to visit.  IMM_USE may get removed before
     the next one is traversed to, so it must be cached early.  */
  ssa_use_operand_t *next_imm_name;
};


/* Use this iterator when simply looking at stmts.  Adding, deleting or
   modifying stmts will cause this iterator to malfunction.  */

#define FOR_EACH_IMM_USE_FAST(DEST, ITER, SSAVAR)		\
  for ((DEST) = first_readonly_imm_use (&(ITER), (SSAVAR));	\
       !end_readonly_imm_use_p (&(ITER));			\
       (void) ((DEST) = next_readonly_imm_use (&(ITER))))

/* Use this iterator to visit each stmt which has a use of SSAVAR.  */

#define FOR_EACH_IMM_USE_STMT(STMT, ITER, SSAVAR)		\
  for ((STMT) = first_imm_use_stmt (&(ITER), (SSAVAR));		\
       !end_imm_use_stmt_p (&(ITER));				\
       (void) ((STMT) = next_imm_use_stmt (&(ITER))))

/* Use this to terminate the FOR_EACH_IMM_USE_STMT loop early.  Failure to
   do so will result in leaving a iterator marker node in the immediate
   use list, and nothing good will come from that.   */
#define BREAK_FROM_IMM_USE_STMT(ITER)				\
   {								\
     end_imm_use_stmt_traverse (&(ITER));			\
     break;							\
   }

/* Similarly for return.  */
#define RETURN_FROM_IMM_USE_STMT(ITER, VAL)			\
  {								\
    end_imm_use_stmt_traverse (&(ITER));			\
    return (VAL);						\
  }

/* Use this iterator in combination with FOR_EACH_IMM_USE_STMT to
   get access to each occurrence of ssavar on the stmt returned by
   that iterator..  for instance:

     FOR_EACH_IMM_USE_STMT (stmt, iter, ssavar)
       {
         FOR_EACH_IMM_USE_ON_STMT (use_p, iter)
	   {
	     SET_USE (use_p, blah);
	   }
	 update_stmt (stmt);
       }							 */

#define FOR_EACH_IMM_USE_ON_STMT(DEST, ITER)			\
  for ((DEST) = first_imm_use_on_stmt (&(ITER));		\
       !end_imm_use_on_stmt_p (&(ITER));			\
       (void) ((DEST) = next_imm_use_on_stmt (&(ITER))))



extern bool single_imm_use_1 (const ssa_use_operand_t *head,
			      use_operand_p *use_p, gimple **stmt);


enum ssa_op_iter_type {
  ssa_op_iter_none = 0,
  ssa_op_iter_tree,
  ssa_op_iter_use,
  ssa_op_iter_def
};

/* This structure is used in the operand iterator loops.  It contains the
   items required to determine which operand is retrieved next.  During
   optimization, this structure is scalarized, and any unused fields are
   optimized away, resulting in little overhead.  */

struct ssa_op_iter
{
  enum ssa_op_iter_type iter_type;
  bool done;
  int flags;
  unsigned i;
  unsigned numops;
  use_optype_p uses;
  gimple *stmt;
};

/* NOTE: Keep these in sync with doc/tree-ssa.texi.  */
/* These flags are used to determine which operands are returned during
   execution of the loop.  */
#define SSA_OP_USE		0x01	/* Real USE operands.  */
#define SSA_OP_DEF		0x02	/* Real DEF operands.  */
#define SSA_OP_VUSE		0x04	/* VUSE operands.  */
#define SSA_OP_VDEF		0x08	/* VDEF operands.  */
/* These are commonly grouped operand flags.  */
#define SSA_OP_VIRTUAL_USES	(SSA_OP_VUSE)
#define SSA_OP_VIRTUAL_DEFS	(SSA_OP_VDEF)
#define SSA_OP_ALL_VIRTUALS     (SSA_OP_VIRTUAL_USES | SSA_OP_VIRTUAL_DEFS)
#define SSA_OP_ALL_USES		(SSA_OP_VIRTUAL_USES | SSA_OP_USE)
#define SSA_OP_ALL_DEFS		(SSA_OP_VIRTUAL_DEFS | SSA_OP_DEF)
#define SSA_OP_ALL_OPERANDS	(SSA_OP_ALL_USES | SSA_OP_ALL_DEFS)

/* This macro executes a loop over the operands of STMT specified in FLAG,
   returning each operand as a 'tree' in the variable TREEVAR.  ITER is an
   ssa_op_iter structure used to control the loop.  */
#define FOR_EACH_SSA_TREE_OPERAND(TREEVAR, STMT, ITER, FLAGS)	\
  for (TREEVAR = op_iter_init_tree (&(ITER), STMT, FLAGS);	\
       !op_iter_done (&(ITER));					\
       (void) (TREEVAR = op_iter_next_tree (&(ITER))))

/* This macro executes a loop over the operands of STMT specified in FLAG,
   returning each operand as a 'use_operand_p' in the variable USEVAR.
   ITER is an ssa_op_iter structure used to control the loop.  */
#define FOR_EACH_SSA_USE_OPERAND(USEVAR, STMT, ITER, FLAGS)	\
  for (USEVAR = op_iter_init_use (&(ITER), STMT, FLAGS);	\
       !op_iter_done (&(ITER));					\
       USEVAR = op_iter_next_use (&(ITER)))

/* This macro executes a loop over the operands of STMT specified in FLAG,
   returning each operand as a 'def_operand_p' in the variable DEFVAR.
   ITER is an ssa_op_iter structure used to control the loop.  */
#define FOR_EACH_SSA_DEF_OPERAND(DEFVAR, STMT, ITER, FLAGS)	\
  for (DEFVAR = op_iter_init_def (&(ITER), STMT, FLAGS);	\
       !op_iter_done (&(ITER));					\
       DEFVAR = op_iter_next_def (&(ITER)))

/* This macro will execute a loop over all the arguments of a PHI which
   match FLAGS.   A use_operand_p is always returned via USEVAR.  FLAGS
   can be either SSA_OP_USE or SSA_OP_VIRTUAL_USES or SSA_OP_ALL_USES.  */
#define FOR_EACH_PHI_ARG(USEVAR, STMT, ITER, FLAGS)		\
  for ((USEVAR) = op_iter_init_phiuse (&(ITER), STMT, FLAGS);	\
       !op_iter_done (&(ITER));					\
       (USEVAR) = op_iter_next_use (&(ITER)))


/* This macro will execute a loop over a stmt, regardless of whether it is
   a real stmt or a PHI node, looking at the USE nodes matching FLAGS.  */
#define FOR_EACH_PHI_OR_STMT_USE(USEVAR, STMT, ITER, FLAGS)	\
  for ((USEVAR) = (gimple_code (STMT) == GIMPLE_PHI 		\
		   ? op_iter_init_phiuse (&(ITER),              \
					  as_a <gphi *> (STMT), \
					  FLAGS)		\
		   : op_iter_init_use (&(ITER), STMT, FLAGS));	\
       !op_iter_done (&(ITER));					\
       (USEVAR) = op_iter_next_use (&(ITER)))

/* This macro will execute a loop over a stmt, regardless of whether it is
   a real stmt or a PHI node, looking at the DEF nodes matching FLAGS.  */
#define FOR_EACH_PHI_OR_STMT_DEF(DEFVAR, STMT, ITER, FLAGS)	\
  for ((DEFVAR) = (gimple_code (STMT) == GIMPLE_PHI 		\
		   ? op_iter_init_phidef (&(ITER),		\
					  as_a <gphi *> (STMT), \
					  FLAGS)		\
		   : op_iter_init_def (&(ITER), STMT, FLAGS));	\
       !op_iter_done (&(ITER));					\
       (DEFVAR) = op_iter_next_def (&(ITER)))

/* This macro returns an operand in STMT as a tree if it is the ONLY
   operand matching FLAGS.  If there are 0 or more than 1 operand matching
   FLAGS, then NULL_TREE is returned.  */
#define SINGLE_SSA_TREE_OPERAND(STMT, FLAGS)			\
  single_ssa_tree_operand (STMT, FLAGS)

/* This macro returns an operand in STMT as a use_operand_p if it is the ONLY
   operand matching FLAGS.  If there are 0 or more than 1 operand matching
   FLAGS, then NULL_USE_OPERAND_P is returned.  */
#define SINGLE_SSA_USE_OPERAND(STMT, FLAGS)			\
  single_ssa_use_operand (STMT, FLAGS)

/* This macro returns an operand in STMT as a def_operand_p if it is the ONLY
   operand matching FLAGS.  If there are 0 or more than 1 operand matching
   FLAGS, then NULL_DEF_OPERAND_P is returned.  */
#define SINGLE_SSA_DEF_OPERAND(STMT, FLAGS)			\
  single_ssa_def_operand (STMT, FLAGS)

/* This macro returns TRUE if there are no operands matching FLAGS in STMT.  */
#define ZERO_SSA_OPERANDS(STMT, FLAGS) 	zero_ssa_operands (STMT, FLAGS)

/* This macro counts the number of operands in STMT matching FLAGS.  */
#define NUM_SSA_OPERANDS(STMT, FLAGS)	num_ssa_operands (STMT, FLAGS)


/* Delink an immediate_uses node from its chain.  */
static inline void
delink_imm_use (ssa_use_operand_t *linknode)
{
  /* Return if this node is not in a list.  */
  if (linknode->prev == NULL)
    return;

  linknode->prev->next = linknode->next;
  linknode->next->prev = linknode->prev;
  linknode->prev = NULL;
  linknode->next = NULL;
}

/* Link ssa_imm_use node LINKNODE into the chain for LIST.  */
static inline void
link_imm_use_to_list (ssa_use_operand_t *linknode, ssa_use_operand_t *list)
{
  /* Link the new node at the head of the list.  If we are in the process of
     traversing the list, we won't visit any new nodes added to it.  */
  linknode->prev = list;
  linknode->next = list->next;
  list->next->prev = linknode;
  list->next = linknode;
}

/* Link ssa_imm_use node LINKNODE into the chain for DEF.  */
static inline void
link_imm_use (ssa_use_operand_t *linknode, tree def)
{
  ssa_use_operand_t *root;

  if (!def || TREE_CODE (def) != SSA_NAME)
    linknode->prev = NULL;
  else
    {
      root = &(SSA_NAME_IMM_USE_NODE (def));
      if (linknode->use)
        gcc_checking_assert (*(linknode->use) == def);
      link_imm_use_to_list (linknode, root);
    }
}

/* Set the value of a use pointed to by USE to VAL.  */
static inline void
set_ssa_use_from_ptr (use_operand_p use, tree val)
{
  delink_imm_use (use);
  *(use->use) = val;
  link_imm_use (use, val);
}

/* Link ssa_imm_use node LINKNODE into the chain for DEF, with use occurring
   in STMT.  */
static inline void
link_imm_use_stmt (ssa_use_operand_t *linknode, tree def, gimple *stmt)
{
  if (stmt)
    link_imm_use (linknode, def);
  else
    link_imm_use (linknode, NULL);
  linknode->loc.stmt = stmt;
}

/* Relink a new node in place of an old node in the list.  */
static inline void
relink_imm_use (ssa_use_operand_t *node, ssa_use_operand_t *old)
{
  /* The node one had better be in the same list.  */
  gcc_checking_assert (*(old->use) == *(node->use));
  node->prev = old->prev;
  node->next = old->next;
  if (old->prev)
    {
      old->prev->next = node;
      old->next->prev = node;
      /* Remove the old node from the list.  */
      old->prev = NULL;
    }
}

/* Relink ssa_imm_use node LINKNODE into the chain for OLD, with use occurring
   in STMT.  */
static inline void
relink_imm_use_stmt (ssa_use_operand_t *linknode, ssa_use_operand_t *old,
		     gimple *stmt)
{
  if (stmt)
    relink_imm_use (linknode, old);
  else
    link_imm_use (linknode, NULL);
  linknode->loc.stmt = stmt;
}


/* Return true is IMM has reached the end of the immediate use list.  */
static inline bool
end_readonly_imm_use_p (const imm_use_iterator *imm)
{
  return (imm->imm_use == imm->end_p);
}

/* Initialize iterator IMM to process the list for VAR.  */
static inline use_operand_p
first_readonly_imm_use (imm_use_iterator *imm, tree var)
{
  imm->end_p = &(SSA_NAME_IMM_USE_NODE (var));
  imm->imm_use = imm->end_p->next;
  imm->iter_node.next = imm->imm_use->next;
  if (end_readonly_imm_use_p (imm))
    return NULL_USE_OPERAND_P;
  return imm->imm_use;
}

/* Bump IMM to the next use in the list.  */
static inline use_operand_p
next_readonly_imm_use (imm_use_iterator *imm)
{
  use_operand_p old = imm->imm_use;

  /* If this assertion fails, it indicates the 'next' pointer has changed
     since the last bump.  This indicates that the list is being modified
     via stmt changes, or SET_USE, or somesuch thing, and you need to be
     using the SAFE version of the iterator.  */
  if (flag_checking)
    {
      gcc_assert (imm->iter_node.next == old->next);
      imm->iter_node.next = old->next->next;
    }

  imm->imm_use = old->next;
  if (end_readonly_imm_use_p (imm))
    return NULL_USE_OPERAND_P;
  return imm->imm_use;
}


/* Return true if VAR has no nondebug uses.  */
static inline bool
has_zero_uses (const_tree var)
{
  const ssa_use_operand_t *const head = &(SSA_NAME_IMM_USE_NODE (var));
  const ssa_use_operand_t *ptr;

  for (ptr = head->next; ptr != head; ptr = ptr->next)
    if (USE_STMT (ptr) && !is_gimple_debug (USE_STMT (ptr)))
      return false;

  return true;
}

/* Return true if VAR has a single nondebug use.  */
static inline bool
has_single_use (const_tree var)
{
  const ssa_use_operand_t *const head = &(SSA_NAME_IMM_USE_NODE (var));
  const ssa_use_operand_t *ptr;
  bool single = false;
   
  for (ptr = head->next; ptr != head; ptr = ptr->next)
    if (USE_STMT(ptr) && !is_gimple_debug (USE_STMT (ptr)))
      {
	if (single)
	  return false;
	else 
	  single = true;
      }

  return single;
}
    
/* If VAR has only a single immediate nondebug use, return true, and
   set USE_P and STMT to the use pointer and stmt of occurrence.  */
static inline bool
single_imm_use (const_tree var, use_operand_p *use_p, gimple **stmt)
{
  const ssa_use_operand_t *const ptr = &(SSA_NAME_IMM_USE_NODE (var));

  /* If there aren't any uses whatsoever, we're done.  */
  if (ptr == ptr->next)
    {
    return_false:
      *use_p = NULL_USE_OPERAND_P;
      *stmt = NULL;
      return false;
    }

  /* If there's a single use, check that it's not a debug stmt.  */
  if (ptr == ptr->next->next)
    {
      if (USE_STMT (ptr->next) && !is_gimple_debug (USE_STMT (ptr->next)))
	{
	  *use_p = ptr->next;
	  *stmt = ptr->next->loc.stmt;
	  return true;
	}
      else
	goto return_false;
    }

  return single_imm_use_1 (ptr, use_p, stmt);
}

/* Return the number of nondebug immediate uses of VAR.  */
static inline unsigned int
num_imm_uses (const_tree var)
{
  const ssa_use_operand_t *const start = &(SSA_NAME_IMM_USE_NODE (var));
  const ssa_use_operand_t *ptr;
  unsigned int num = 0;

  if (!MAY_HAVE_DEBUG_BIND_STMTS)
    {
      for (ptr = start->next; ptr != start; ptr = ptr->next)
	if (USE_STMT (ptr))
	  num++;
    }
  else
    for (ptr = start->next; ptr != start; ptr = ptr->next)
      if (USE_STMT (ptr) && !is_gimple_debug (USE_STMT (ptr)))
	num++;

  return num;
}

/*  -----------------------------------------------------------------------  */

/* The following set of routines are used to iterator over various type of
   SSA operands.  */

/* Return true if PTR is finished iterating.  */
static inline bool
op_iter_done (const ssa_op_iter *ptr)
{
  return ptr->done;
}

/* Get the next iterator use value for PTR.  */
static inline use_operand_p
op_iter_next_use (ssa_op_iter *ptr)
{
  use_operand_p use_p;
  gcc_checking_assert (ptr->iter_type == ssa_op_iter_use);
  if (ptr->uses)
    {
      use_p = USE_OP_PTR (ptr->uses);
      ptr->uses = ptr->uses->next;
      return use_p;
    }
  if (ptr->i < ptr->numops)
    {
      return PHI_ARG_DEF_PTR (ptr->stmt, (ptr->i)++);
    }
  ptr->done = true;
  return NULL_USE_OPERAND_P;
}

/* Get the next iterator def value for PTR.  */
static inline def_operand_p
op_iter_next_def (ssa_op_iter *ptr)
{
  gcc_checking_assert (ptr->iter_type == ssa_op_iter_def);
  if (ptr->flags & SSA_OP_VDEF)
    {
      tree *p;
      ptr->flags &= ~SSA_OP_VDEF;
      p = gimple_vdef_ptr (ptr->stmt);
      if (p && *p)
	return p;
    }
  if (ptr->flags & SSA_OP_DEF)
    {
      while (ptr->i < ptr->numops)
	{
	  tree *val = gimple_op_ptr (ptr->stmt, ptr->i);
	  ptr->i++;
	  if (*val)
	    {
	      if (TREE_CODE (*val) == TREE_LIST)
		val = &TREE_VALUE (*val);
	      if (TREE_CODE (*val) == SSA_NAME
		  || is_gimple_reg (*val))
		return val;
	    }
	}
      ptr->flags &= ~SSA_OP_DEF;
    }

  ptr->done = true;
  return NULL_DEF_OPERAND_P;
}

/* Get the next iterator tree value for PTR.  */
static inline tree
op_iter_next_tree (ssa_op_iter *ptr)
{
  tree val;
  gcc_checking_assert (ptr->iter_type == ssa_op_iter_tree);
  if (ptr->uses)
    {
      val = USE_OP (ptr->uses);
      ptr->uses = ptr->uses->next;
      return val;
    }
  if (ptr->flags & SSA_OP_VDEF)
    {
      ptr->flags &= ~SSA_OP_VDEF;
      if ((val = gimple_vdef (ptr->stmt)))
	return val;
    }
  if (ptr->flags & SSA_OP_DEF)
    {
      while (ptr->i < ptr->numops)
	{
	  val = gimple_op (ptr->stmt, ptr->i);
	  ptr->i++;
	  if (val)
	    {
	      if (TREE_CODE (val) == TREE_LIST)
		val = TREE_VALUE (val);
	      if (TREE_CODE (val) == SSA_NAME
		  || is_gimple_reg (val))
		return val;
	    }
	}
      ptr->flags &= ~SSA_OP_DEF;
    }

  ptr->done = true;
  return NULL_TREE;
}


/* This functions clears the iterator PTR, and marks it done.  This is normally
   used to prevent warnings in the compile about might be uninitialized
   components.  */

static inline void
clear_and_done_ssa_iter (ssa_op_iter *ptr)
{
  ptr->i = 0;
  ptr->numops = 0;
  ptr->uses = NULL;
  ptr->iter_type = ssa_op_iter_none;
  ptr->stmt = NULL;
  ptr->done = true;
  ptr->flags = 0;
}

/* Initialize the iterator PTR to the virtual defs in STMT.  */
static inline void
op_iter_init (ssa_op_iter *ptr, gimple *stmt, int flags)
{
  /* PHI nodes require a different iterator initialization path.  We
     do not support iterating over virtual defs or uses without
     iterating over defs or uses at the same time.  */
  gcc_checking_assert (gimple_code (stmt) != GIMPLE_PHI
		       && (!(flags & SSA_OP_VDEF) || (flags & SSA_OP_DEF))
		       && (!(flags & SSA_OP_VUSE) || (flags & SSA_OP_USE)));
  ptr->numops = 0;
  if (flags & (SSA_OP_DEF | SSA_OP_VDEF))
    {
      switch (gimple_code (stmt))
	{
	  case GIMPLE_ASSIGN:
	  case GIMPLE_CALL:
	    ptr->numops = 1;
	    break;
	  case GIMPLE_ASM:
	    ptr->numops = gimple_asm_noutputs (as_a <gasm *> (stmt));
	    break;
	  case GIMPLE_TRANSACTION:
	    ptr->numops = 0;
	    flags &= ~SSA_OP_DEF;
	    break;
	  default:
	    ptr->numops = 0;
	    flags &= ~(SSA_OP_DEF | SSA_OP_VDEF);
	    break;
	}
    }
  ptr->uses = (flags & (SSA_OP_USE|SSA_OP_VUSE)) ? gimple_use_ops (stmt) : NULL;
  if (!(flags & SSA_OP_VUSE)
      && ptr->uses
      && gimple_vuse (stmt) != NULL_TREE)
    ptr->uses = ptr->uses->next;
  ptr->done = false;
  ptr->i = 0;

  ptr->stmt = stmt;
  ptr->flags = flags;
}

/* Initialize iterator PTR to the use operands in STMT based on FLAGS. Return
   the first use.  */
static inline use_operand_p
op_iter_init_use (ssa_op_iter *ptr, gimple *stmt, int flags)
{
  gcc_checking_assert ((flags & SSA_OP_ALL_DEFS) == 0
		       && (flags & SSA_OP_USE));
  op_iter_init (ptr, stmt, flags);
  ptr->iter_type = ssa_op_iter_use;
  return op_iter_next_use (ptr);
}

/* Initialize iterator PTR to the def operands in STMT based on FLAGS. Return
   the first def.  */
static inline def_operand_p
op_iter_init_def (ssa_op_iter *ptr, gimple *stmt, int flags)
{
  gcc_checking_assert ((flags & SSA_OP_ALL_USES) == 0
		       && (flags & SSA_OP_DEF));
  op_iter_init (ptr, stmt, flags);
  ptr->iter_type = ssa_op_iter_def;
  return op_iter_next_def (ptr);
}

/* Initialize iterator PTR to the operands in STMT based on FLAGS. Return
   the first operand as a tree.  */
static inline tree
op_iter_init_tree (ssa_op_iter *ptr, gimple *stmt, int flags)
{
  op_iter_init (ptr, stmt, flags);
  ptr->iter_type = ssa_op_iter_tree;
  return op_iter_next_tree (ptr);
}


/* If there is a single operand in STMT matching FLAGS, return it.  Otherwise
   return NULL.  */
static inline tree
single_ssa_tree_operand (gimple *stmt, int flags)
{
  tree var;
  ssa_op_iter iter;

  var = op_iter_init_tree (&iter, stmt, flags);
  if (op_iter_done (&iter))
    return NULL_TREE;
  op_iter_next_tree (&iter);
  if (op_iter_done (&iter))
    return var;
  return NULL_TREE;
}


/* If there is a single operand in STMT matching FLAGS, return it.  Otherwise
   return NULL.  */
static inline use_operand_p
single_ssa_use_operand (gimple *stmt, int flags)
{
  use_operand_p var;
  ssa_op_iter iter;

  var = op_iter_init_use (&iter, stmt, flags);
  if (op_iter_done (&iter))
    return NULL_USE_OPERAND_P;
  op_iter_next_use (&iter);
  if (op_iter_done (&iter))
    return var;
  return NULL_USE_OPERAND_P;
}

/* Return the single virtual use operand in STMT if present.  Otherwise
   return NULL.  */
static inline use_operand_p
ssa_vuse_operand (gimple *stmt)
{
  if (! gimple_vuse (stmt))
    return NULL_USE_OPERAND_P;
  return USE_OP_PTR (gimple_use_ops (stmt));
}


/* If there is a single operand in STMT matching FLAGS, return it.  Otherwise
   return NULL.  */
static inline def_operand_p
single_ssa_def_operand (gimple *stmt, int flags)
{
  def_operand_p var;
  ssa_op_iter iter;

  var = op_iter_init_def (&iter, stmt, flags);
  if (op_iter_done (&iter))
    return NULL_DEF_OPERAND_P;
  op_iter_next_def (&iter);
  if (op_iter_done (&iter))
    return var;
  return NULL_DEF_OPERAND_P;
}


/* Return true if there are zero operands in STMT matching the type
   given in FLAGS.  */
static inline bool
zero_ssa_operands (gimple *stmt, int flags)
{
  ssa_op_iter iter;

  op_iter_init_tree (&iter, stmt, flags);
  return op_iter_done (&iter);
}


/* Return the number of operands matching FLAGS in STMT.  */
static inline int
num_ssa_operands (gimple *stmt, int flags)
{
  ssa_op_iter iter;
  tree t;
  int num = 0;

  gcc_checking_assert (gimple_code (stmt) != GIMPLE_PHI);
  FOR_EACH_SSA_TREE_OPERAND (t, stmt, iter, flags)
    num++;
  return num;
}

/* If there is a single DEF in the PHI node which matches FLAG, return it.
   Otherwise return NULL_DEF_OPERAND_P.  */
static inline tree
single_phi_def (gphi *stmt, int flags)
{
  tree def = PHI_RESULT (stmt);
  if ((flags & SSA_OP_DEF) && is_gimple_reg (def))
    return def;
  if ((flags & SSA_OP_VIRTUAL_DEFS) && !is_gimple_reg (def))
    return def;
  return NULL_TREE;
}

/* Initialize the iterator PTR for uses matching FLAGS in PHI.  FLAGS should
   be either SSA_OP_USES or SSA_OP_VIRTUAL_USES.  */
static inline use_operand_p
op_iter_init_phiuse (ssa_op_iter *ptr, gphi *phi, int flags)
{
  tree phi_def = gimple_phi_result (phi);
  int comp;

  clear_and_done_ssa_iter (ptr);
  ptr->done = false;

  gcc_checking_assert ((flags & (SSA_OP_USE | SSA_OP_VIRTUAL_USES)) != 0);

  comp = (is_gimple_reg (phi_def) ? SSA_OP_USE : SSA_OP_VIRTUAL_USES);

  /* If the PHI node doesn't the operand type we care about, we're done.  */
  if ((flags & comp) == 0)
    {
      ptr->done = true;
      return NULL_USE_OPERAND_P;
    }

  ptr->stmt = phi;
  ptr->numops = gimple_phi_num_args (phi);
  ptr->iter_type = ssa_op_iter_use;
  ptr->flags = flags;
  return op_iter_next_use (ptr);
}


/* Start an iterator for a PHI definition.  */

static inline def_operand_p
op_iter_init_phidef (ssa_op_iter *ptr, gphi *phi, int flags)
{
  tree phi_def = PHI_RESULT (phi);
  int comp;

  clear_and_done_ssa_iter (ptr);
  ptr->done = false;

  gcc_checking_assert ((flags & (SSA_OP_DEF | SSA_OP_VIRTUAL_DEFS)) != 0);

  comp = (is_gimple_reg (phi_def) ? SSA_OP_DEF : SSA_OP_VIRTUAL_DEFS);

  /* If the PHI node doesn't have the operand type we care about,
     we're done.  */
  if ((flags & comp) == 0)
    {
      ptr->done = true;
      return NULL_DEF_OPERAND_P;
    }

  ptr->iter_type = ssa_op_iter_def;
  /* The first call to op_iter_next_def will terminate the iterator since
     all the fields are NULL.  Simply return the result here as the first and
     therefore only result.  */
  return PHI_RESULT_PTR (phi);
}

/* Return true is IMM has reached the end of the immediate use stmt list.  */

static inline bool
end_imm_use_stmt_p (const imm_use_iterator *imm)
{
  return (imm->imm_use == imm->end_p);
}

/* Finished the traverse of an immediate use stmt list IMM by removing the
   placeholder node from the list.  */

static inline void
end_imm_use_stmt_traverse (imm_use_iterator *imm)
{
  delink_imm_use (&(imm->iter_node));
}

/* Immediate use traversal of uses within a stmt require that all the
   uses on a stmt be sequentially listed.  This routine is used to build up
   this sequential list by adding USE_P to the end of the current list
   currently delimited by HEAD and LAST_P.  The new LAST_P value is
   returned.  */

static inline use_operand_p
move_use_after_head (use_operand_p use_p, use_operand_p head,
		      use_operand_p last_p)
{
  gcc_checking_assert (USE_FROM_PTR (use_p) == USE_FROM_PTR (head));
  /* Skip head when we find it.  */
  if (use_p != head)
    {
      /* If use_p is already linked in after last_p, continue.  */
      if (last_p->next == use_p)
	last_p = use_p;
      else
	{
	  /* Delink from current location, and link in at last_p.  */
	  delink_imm_use (use_p);
	  link_imm_use_to_list (use_p, last_p);
	  last_p = use_p;
	}
    }
  return last_p;
}


/* This routine will relink all uses with the same stmt as HEAD into the list
   immediately following HEAD for iterator IMM.  */

static inline void
link_use_stmts_after (use_operand_p head, imm_use_iterator *imm)
{
  use_operand_p use_p;
  use_operand_p last_p = head;
  gimple *head_stmt = USE_STMT (head);
  tree use = USE_FROM_PTR (head);
  ssa_op_iter op_iter;
  int flag;

  /* Only look at virtual or real uses, depending on the type of HEAD.  */
  flag = (is_gimple_reg (use) ? SSA_OP_USE : SSA_OP_VIRTUAL_USES);

  if (gphi *phi = dyn_cast <gphi *> (head_stmt))
    {
      FOR_EACH_PHI_ARG (use_p, phi, op_iter, flag)
	if (USE_FROM_PTR (use_p) == use)
	  last_p = move_use_after_head (use_p, head, last_p);
    }
  else
    {
      if (flag == SSA_OP_USE)
	{
	  FOR_EACH_SSA_USE_OPERAND (use_p, head_stmt, op_iter, flag)
	    if (USE_FROM_PTR (use_p) == use)
	      last_p = move_use_after_head (use_p, head, last_p);
	}
      else if ((use_p = gimple_vuse_op (head_stmt)) != NULL_USE_OPERAND_P)
	{
	  if (USE_FROM_PTR (use_p) == use)
	    last_p = move_use_after_head (use_p, head, last_p);
	}
    }
  /* Link iter node in after last_p.  */
  if (imm->iter_node.prev != NULL)
    delink_imm_use (&imm->iter_node);
  link_imm_use_to_list (&(imm->iter_node), last_p);
}

/* Initialize IMM to traverse over uses of VAR.  Return the first statement.  */
static inline gimple *
first_imm_use_stmt (imm_use_iterator *imm, tree var)
{
  imm->end_p = &(SSA_NAME_IMM_USE_NODE (var));
  imm->imm_use = imm->end_p->next;
  imm->next_imm_name = NULL_USE_OPERAND_P;

  /* iter_node is used as a marker within the immediate use list to indicate
     where the end of the current stmt's uses are.  Initialize it to NULL
     stmt and use, which indicates a marker node.  */
  imm->iter_node.prev = NULL_USE_OPERAND_P;
  imm->iter_node.next = NULL_USE_OPERAND_P;
  imm->iter_node.loc.stmt = NULL;
  imm->iter_node.use = NULL;

  if (end_imm_use_stmt_p (imm))
    return NULL;

  link_use_stmts_after (imm->imm_use, imm);

  return USE_STMT (imm->imm_use);
}

/* Bump IMM to the next stmt which has a use of var.  */

static inline gimple *
next_imm_use_stmt (imm_use_iterator *imm)
{
  imm->imm_use = imm->iter_node.next;
  if (end_imm_use_stmt_p (imm))
    {
      if (imm->iter_node.prev != NULL)
	delink_imm_use (&imm->iter_node);
      return NULL;
    }

  link_use_stmts_after (imm->imm_use, imm);
  return USE_STMT (imm->imm_use);
}

/* This routine will return the first use on the stmt IMM currently refers
   to.  */

static inline use_operand_p
first_imm_use_on_stmt (imm_use_iterator *imm)
{
  imm->next_imm_name = imm->imm_use->next;
  return imm->imm_use;
}

/*  Return TRUE if the last use on the stmt IMM refers to has been visited.  */

static inline bool
end_imm_use_on_stmt_p (const imm_use_iterator *imm)
{
  return (imm->imm_use == &(imm->iter_node));
}

/* Bump to the next use on the stmt IMM refers to, return NULL if done.  */

static inline use_operand_p
next_imm_use_on_stmt (imm_use_iterator *imm)
{
  imm->imm_use = imm->next_imm_name;
  if (end_imm_use_on_stmt_p (imm))
    return NULL_USE_OPERAND_P;
  else
    {
      imm->next_imm_name = imm->imm_use->next;
      return imm->imm_use;
    }
}

/* Delink all immediate_use information for STMT.  */
static inline void
delink_stmt_imm_use (gimple *stmt)
{
   ssa_op_iter iter;
   use_operand_p use_p;

   if (ssa_operands_active (cfun))
     FOR_EACH_PHI_OR_STMT_USE (use_p, stmt, iter, SSA_OP_ALL_USES)
       delink_imm_use (use_p);
}

#endif /* GCC_TREE_SSA_ITERATORS_H */
