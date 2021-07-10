/* Integrated Register Allocator (IRA) intercommunication header file.
   Copyright (C) 2006-2018 Free Software Foundation, Inc.
   Contributed by Vladimir Makarov <vmakarov@redhat.com>.

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

#ifndef GCC_IRA_INT_H
#define GCC_IRA_INT_H

#include "recog.h"

/* To provide consistency in naming, all IRA external variables,
   functions, common typedefs start with prefix ira_.  */

#if CHECKING_P
#define ENABLE_IRA_CHECKING
#endif

#ifdef ENABLE_IRA_CHECKING
#define ira_assert(c) gcc_assert (c)
#else
/* Always define and include C, so that warnings for empty body in an
  'if' statement and unused variable do not occur.  */
#define ira_assert(c) ((void)(0 && (c)))
#endif

/* Compute register frequency from edge frequency FREQ.  It is
   analogous to REG_FREQ_FROM_BB.  When optimizing for size, or
   profile driven feedback is available and the function is never
   executed, frequency is always equivalent.  Otherwise rescale the
   edge frequency.  */
#define REG_FREQ_FROM_EDGE_FREQ(freq)				   \
  (optimize_function_for_size_p (cfun)				   \
   ? REG_FREQ_MAX : (freq * REG_FREQ_MAX / BB_FREQ_MAX)		   \
   ? (freq * REG_FREQ_MAX / BB_FREQ_MAX) : 1)

/* A modified value of flag `-fira-verbose' used internally.  */
extern int internal_flag_ira_verbose;

/* Dump file of the allocator if it is not NULL.  */
extern FILE *ira_dump_file;

/* Typedefs for pointers to allocno live range, allocno, and copy of
   allocnos.  */
typedef struct live_range *live_range_t;
typedef struct ira_allocno *ira_allocno_t;
typedef struct ira_allocno_pref *ira_pref_t;
typedef struct ira_allocno_copy *ira_copy_t;
typedef struct ira_object *ira_object_t;

/* Definition of vector of allocnos and copies.  */

/* Typedef for pointer to the subsequent structure.  */
typedef struct ira_loop_tree_node *ira_loop_tree_node_t;

typedef unsigned short move_table[N_REG_CLASSES];

/* In general case, IRA is a regional allocator.  The regions are
   nested and form a tree.  Currently regions are natural loops.  The
   following structure describes loop tree node (representing basic
   block or loop).  We need such tree because the loop tree from
   cfgloop.h is not convenient for the optimization: basic blocks are
   not a part of the tree from cfgloop.h.  We also use the nodes for
   storing additional information about basic blocks/loops for the
   register allocation purposes.  */
struct ira_loop_tree_node
{
  /* The node represents basic block if children == NULL.  */
  basic_block bb;    /* NULL for loop.  */
  /* NULL for BB or for loop tree root if we did not build CFG loop tree.  */
  struct loop *loop;
  /* NEXT/SUBLOOP_NEXT is the next node/loop-node of the same parent.
     SUBLOOP_NEXT is always NULL for BBs.  */
  ira_loop_tree_node_t subloop_next, next;
  /* CHILDREN/SUBLOOPS is the first node/loop-node immediately inside
     the node.  They are NULL for BBs.  */
  ira_loop_tree_node_t subloops, children;
  /* The node immediately containing given node.  */
  ira_loop_tree_node_t parent;

  /* Loop level in range [0, ira_loop_tree_height).  */
  int level;

  /* All the following members are defined only for nodes representing
     loops.  */

  /* The loop number from CFG loop tree.  The root number is 0.  */
  int loop_num;

  /* True if the loop was marked for removal from the register
     allocation.  */
  bool to_remove_p;

  /* Allocnos in the loop corresponding to their regnos.  If it is
     NULL the loop does not form a separate register allocation region
     (e.g. because it has abnormal enter/exit edges and we can not put
     code for register shuffling on the edges if a different
     allocation is used for a pseudo-register on different sides of
     the edges).  Caps are not in the map (remember we can have more
     one cap with the same regno in a region).  */
  ira_allocno_t *regno_allocno_map;

  /* True if there is an entry to given loop not from its parent (or
     grandparent) basic block.  For example, it is possible for two
     adjacent loops inside another loop.  */
  bool entered_from_non_parent_p;

  /* Maximal register pressure inside loop for given register class
     (defined only for the pressure classes).  */
  int reg_pressure[N_REG_CLASSES];

  /* Numbers of allocnos referred or living in the loop node (except
     for its subloops).  */
  bitmap all_allocnos;

  /* Numbers of allocnos living at the loop borders.  */
  bitmap border_allocnos;

  /* Regnos of pseudos modified in the loop node (including its
     subloops).  */
  bitmap modified_regnos;

  /* Numbers of copies referred in the corresponding loop.  */
  bitmap local_copies;
};

/* The root of the loop tree corresponding to the all function.  */
extern ira_loop_tree_node_t ira_loop_tree_root;

/* Height of the loop tree.  */
extern int ira_loop_tree_height;

/* All nodes representing basic blocks are referred through the
   following array.  We can not use basic block member `aux' for this
   because it is used for insertion of insns on edges.  */
extern ira_loop_tree_node_t ira_bb_nodes;

/* Two access macros to the nodes representing basic blocks.  */
#if defined ENABLE_IRA_CHECKING && (GCC_VERSION >= 2007)
#define IRA_BB_NODE_BY_INDEX(index) __extension__			\
(({ ira_loop_tree_node_t _node = (&ira_bb_nodes[index]);		\
     if (_node->children != NULL || _node->loop != NULL || _node->bb == NULL)\
       {								\
         fprintf (stderr,						\
                  "\n%s: %d: error in %s: it is not a block node\n",	\
                  __FILE__, __LINE__, __FUNCTION__);			\
         gcc_unreachable ();						\
       }								\
     _node; }))
#else
#define IRA_BB_NODE_BY_INDEX(index) (&ira_bb_nodes[index])
#endif

#define IRA_BB_NODE(bb) IRA_BB_NODE_BY_INDEX ((bb)->index)

/* All nodes representing loops are referred through the following
   array.  */
extern ira_loop_tree_node_t ira_loop_nodes;

/* Two access macros to the nodes representing loops.  */
#if defined ENABLE_IRA_CHECKING && (GCC_VERSION >= 2007)
#define IRA_LOOP_NODE_BY_INDEX(index) __extension__			\
(({ ira_loop_tree_node_t const _node = (&ira_loop_nodes[index]);	\
     if (_node->children == NULL || _node->bb != NULL			\
         || (_node->loop == NULL && current_loops != NULL))		\
       {								\
         fprintf (stderr,						\
                  "\n%s: %d: error in %s: it is not a loop node\n",	\
                  __FILE__, __LINE__, __FUNCTION__);			\
         gcc_unreachable ();						\
       }								\
     _node; }))
#else
#define IRA_LOOP_NODE_BY_INDEX(index) (&ira_loop_nodes[index])
#endif

#define IRA_LOOP_NODE(loop) IRA_LOOP_NODE_BY_INDEX ((loop)->num)


/* The structure describes program points where a given allocno lives.
   If the live ranges of two allocnos are intersected, the allocnos
   are in conflict.  */
struct live_range
{
  /* Object whose live range is described by given structure.  */
  ira_object_t object;
  /* Program point range.  */
  int start, finish;
  /* Next structure describing program points where the allocno
     lives.  */
  live_range_t next;
  /* Pointer to structures with the same start/finish.  */
  live_range_t start_next, finish_next;
};

/* Program points are enumerated by numbers from range
   0..IRA_MAX_POINT-1.  There are approximately two times more program
   points than insns.  Program points are places in the program where
   liveness info can be changed.  In most general case (there are more
   complicated cases too) some program points correspond to places
   where input operand dies and other ones correspond to places where
   output operands are born.  */
extern int ira_max_point;

/* Arrays of size IRA_MAX_POINT mapping a program point to the allocno
   live ranges with given start/finish point.  */
extern live_range_t *ira_start_point_ranges, *ira_finish_point_ranges;

/* A structure representing conflict information for an allocno
   (or one of its subwords).  */
struct ira_object
{
  /* The allocno associated with this record.  */
  ira_allocno_t allocno;
  /* Vector of accumulated conflicting conflict_redords with NULL end
     marker (if OBJECT_CONFLICT_VEC_P is true) or conflict bit vector
     otherwise.  */
  void *conflicts_array;
  /* Pointer to structures describing at what program point the
     object lives.  We always maintain the list in such way that *the
     ranges in the list are not intersected and ordered by decreasing
     their program points*.  */
  live_range_t live_ranges;
  /* The subword within ALLOCNO which is represented by this object.
     Zero means the lowest-order subword (or the entire allocno in case
     it is not being tracked in subwords).  */
  int subword;
  /* Allocated size of the conflicts array.  */
  unsigned int conflicts_array_size;
  /* A unique number for every instance of this structure, which is used
     to represent it in conflict bit vectors.  */
  int id;
  /* Before building conflicts, MIN and MAX are initialized to
     correspondingly minimal and maximal points of the accumulated
     live ranges.  Afterwards, they hold the minimal and maximal ids
     of other ira_objects that this one can conflict with.  */
  int min, max;
  /* Initial and accumulated hard registers conflicting with this
     object and as a consequences can not be assigned to the allocno.
     All non-allocatable hard regs and hard regs of register classes
     different from given allocno one are included in the sets.  */
  HARD_REG_SET conflict_hard_regs, total_conflict_hard_regs;
  /* Number of accumulated conflicts in the vector of conflicting
     objects.  */
  int num_accumulated_conflicts;
  /* TRUE if conflicts are represented by a vector of pointers to
     ira_object structures.  Otherwise, we use a bit vector indexed
     by conflict ID numbers.  */
  unsigned int conflict_vec_p : 1;
};

/* A structure representing an allocno (allocation entity).  Allocno
   represents a pseudo-register in an allocation region.  If
   pseudo-register does not live in a region but it lives in the
   nested regions, it is represented in the region by special allocno
   called *cap*.  There may be more one cap representing the same
   pseudo-register in region.  It means that the corresponding
   pseudo-register lives in more one non-intersected subregion.  */
struct ira_allocno
{
  /* The allocno order number starting with 0.  Each allocno has an
     unique number and the number is never changed for the
     allocno.  */
  int num;
  /* Regno for allocno or cap.  */
  int regno;
  /* Mode of the allocno which is the mode of the corresponding
     pseudo-register.  */
  ENUM_BITFIELD (machine_mode) mode : 8;
  /* Widest mode of the allocno which in at least one case could be
     for paradoxical subregs where wmode > mode.  */
  ENUM_BITFIELD (machine_mode) wmode : 8;
  /* Register class which should be used for allocation for given
     allocno.  NO_REGS means that we should use memory.  */
  ENUM_BITFIELD (reg_class) aclass : 16;
  /* During the reload, value TRUE means that we should not reassign a
     hard register to the allocno got memory earlier.  It is set up
     when we removed memory-memory move insn before each iteration of
     the reload.  */
  unsigned int dont_reassign_p : 1;
#ifdef STACK_REGS
  /* Set to TRUE if allocno can't be assigned to the stack hard
     register correspondingly in this region and area including the
     region and all its subregions recursively.  */
  unsigned int no_stack_reg_p : 1, total_no_stack_reg_p : 1;
#endif
  /* TRUE value means that there is no sense to spill the allocno
     during coloring because the spill will result in additional
     reloads in reload pass.  */
  unsigned int bad_spill_p : 1;
  /* TRUE if a hard register or memory has been assigned to the
     allocno.  */
  unsigned int assigned_p : 1;
  /* TRUE if conflicts for given allocno are represented by vector of
     pointers to the conflicting allocnos.  Otherwise, we use a bit
     vector where a bit with given index represents allocno with the
     same number.  */
  unsigned int conflict_vec_p : 1;
  /* Hard register assigned to given allocno.  Negative value means
     that memory was allocated to the allocno.  During the reload,
     spilled allocno has value equal to the corresponding stack slot
     number (0, ...) - 2.  Value -1 is used for allocnos spilled by the
     reload (at this point pseudo-register has only one allocno) which
     did not get stack slot yet.  */
  signed int hard_regno : 16;
  /* Allocnos with the same regno are linked by the following member.
     Allocnos corresponding to inner loops are first in the list (it
     corresponds to depth-first traverse of the loops).  */
  ira_allocno_t next_regno_allocno;
  /* There may be different allocnos with the same regno in different
     regions.  Allocnos are bound to the corresponding loop tree node.
     Pseudo-register may have only one regular allocno with given loop
     tree node but more than one cap (see comments above).  */
  ira_loop_tree_node_t loop_tree_node;
  /* Accumulated usage references of the allocno.  Here and below,
     word 'accumulated' means info for given region and all nested
     subregions.  In this case, 'accumulated' means sum of references
     of the corresponding pseudo-register in this region and in all
     nested subregions recursively. */
  int nrefs;
  /* Accumulated frequency of usage of the allocno.  */
  int freq;
  /* Minimal accumulated and updated costs of usage register of the
     allocno class.  */
  int class_cost, updated_class_cost;
  /* Minimal accumulated, and updated costs of memory for the allocno.
     At the allocation start, the original and updated costs are
     equal.  The updated cost may be changed after finishing
     allocation in a region and starting allocation in a subregion.
     The change reflects the cost of spill/restore code on the
     subregion border if we assign memory to the pseudo in the
     subregion.  */
  int memory_cost, updated_memory_cost;
  /* Accumulated number of points where the allocno lives and there is
     excess pressure for its class.  Excess pressure for a register
     class at some point means that there are more allocnos of given
     register class living at the point than number of hard-registers
     of the class available for the allocation.  */
  int excess_pressure_points_num;
  /* Allocno hard reg preferences.  */
  ira_pref_t allocno_prefs;
  /* Copies to other non-conflicting allocnos.  The copies can
     represent move insn or potential move insn usually because of two
     operand insn constraints.  */
  ira_copy_t allocno_copies;
  /* It is a allocno (cap) representing given allocno on upper loop tree
     level.  */
  ira_allocno_t cap;
  /* It is a link to allocno (cap) on lower loop level represented by
     given cap.  Null if given allocno is not a cap.  */
  ira_allocno_t cap_member;
  /* The number of objects tracked in the following array.  */
  int num_objects;
  /* An array of structures describing conflict information and live
     ranges for each object associated with the allocno.  There may be
     more than one such object in cases where the allocno represents a
     multi-word register.  */
  ira_object_t objects[2];
  /* Accumulated frequency of calls which given allocno
     intersects.  */
  int call_freq;
  /* Accumulated number of the intersected calls.  */
  int calls_crossed_num;
  /* The number of calls across which it is live, but which should not
     affect register preferences.  */
  int cheap_calls_crossed_num;
  /* Registers clobbered by intersected calls.  */
   HARD_REG_SET crossed_calls_clobbered_regs;
  /* Array of usage costs (accumulated and the one updated during
     coloring) for each hard register of the allocno class.  The
     member value can be NULL if all costs are the same and equal to
     CLASS_COST.  For example, the costs of two different hard
     registers can be different if one hard register is callee-saved
     and another one is callee-used and the allocno lives through
     calls.  Another example can be case when for some insn the
     corresponding pseudo-register value should be put in specific
     register class (e.g. AREG for x86) which is a strict subset of
     the allocno class (GENERAL_REGS for x86).  We have updated costs
     to reflect the situation when the usage cost of a hard register
     is decreased because the allocno is connected to another allocno
     by a copy and the another allocno has been assigned to the hard
     register.  */
  int *hard_reg_costs, *updated_hard_reg_costs;
  /* Array of decreasing costs (accumulated and the one updated during
     coloring) for allocnos conflicting with given allocno for hard
     regno of the allocno class.  The member value can be NULL if all
     costs are the same.  These costs are used to reflect preferences
     of other allocnos not assigned yet during assigning to given
     allocno.  */
  int *conflict_hard_reg_costs, *updated_conflict_hard_reg_costs;
  /* Different additional data.  It is used to decrease size of
     allocno data footprint.  */
  void *add_data;
};


/* All members of the allocno structures should be accessed only
   through the following macros.  */
#define ALLOCNO_NUM(A) ((A)->num)
#define ALLOCNO_REGNO(A) ((A)->regno)
#define ALLOCNO_REG(A) ((A)->reg)
#define ALLOCNO_NEXT_REGNO_ALLOCNO(A) ((A)->next_regno_allocno)
#define ALLOCNO_LOOP_TREE_NODE(A) ((A)->loop_tree_node)
#define ALLOCNO_CAP(A) ((A)->cap)
#define ALLOCNO_CAP_MEMBER(A) ((A)->cap_member)
#define ALLOCNO_NREFS(A) ((A)->nrefs)
#define ALLOCNO_FREQ(A) ((A)->freq)
#define ALLOCNO_HARD_REGNO(A) ((A)->hard_regno)
#define ALLOCNO_CALL_FREQ(A) ((A)->call_freq)
#define ALLOCNO_CALLS_CROSSED_NUM(A) ((A)->calls_crossed_num)
#define ALLOCNO_CHEAP_CALLS_CROSSED_NUM(A) ((A)->cheap_calls_crossed_num)
#define ALLOCNO_CROSSED_CALLS_CLOBBERED_REGS(A) \
  ((A)->crossed_calls_clobbered_regs)
#define ALLOCNO_MEM_OPTIMIZED_DEST(A) ((A)->mem_optimized_dest)
#define ALLOCNO_MEM_OPTIMIZED_DEST_P(A) ((A)->mem_optimized_dest_p)
#define ALLOCNO_SOMEWHERE_RENAMED_P(A) ((A)->somewhere_renamed_p)
#define ALLOCNO_CHILD_RENAMED_P(A) ((A)->child_renamed_p)
#define ALLOCNO_DONT_REASSIGN_P(A) ((A)->dont_reassign_p)
#ifdef STACK_REGS
#define ALLOCNO_NO_STACK_REG_P(A) ((A)->no_stack_reg_p)
#define ALLOCNO_TOTAL_NO_STACK_REG_P(A) ((A)->total_no_stack_reg_p)
#endif
#define ALLOCNO_BAD_SPILL_P(A) ((A)->bad_spill_p)
#define ALLOCNO_ASSIGNED_P(A) ((A)->assigned_p)
#define ALLOCNO_MODE(A) ((A)->mode)
#define ALLOCNO_WMODE(A) ((A)->wmode)
#define ALLOCNO_PREFS(A) ((A)->allocno_prefs)
#define ALLOCNO_COPIES(A) ((A)->allocno_copies)
#define ALLOCNO_HARD_REG_COSTS(A) ((A)->hard_reg_costs)
#define ALLOCNO_UPDATED_HARD_REG_COSTS(A) ((A)->updated_hard_reg_costs)
#define ALLOCNO_CONFLICT_HARD_REG_COSTS(A) \
  ((A)->conflict_hard_reg_costs)
#define ALLOCNO_UPDATED_CONFLICT_HARD_REG_COSTS(A) \
  ((A)->updated_conflict_hard_reg_costs)
#define ALLOCNO_CLASS(A) ((A)->aclass)
#define ALLOCNO_CLASS_COST(A) ((A)->class_cost)
#define ALLOCNO_UPDATED_CLASS_COST(A) ((A)->updated_class_cost)
#define ALLOCNO_MEMORY_COST(A) ((A)->memory_cost)
#define ALLOCNO_UPDATED_MEMORY_COST(A) ((A)->updated_memory_cost)
#define ALLOCNO_EXCESS_PRESSURE_POINTS_NUM(A) \
  ((A)->excess_pressure_points_num)
#define ALLOCNO_OBJECT(A,N) ((A)->objects[N])
#define ALLOCNO_NUM_OBJECTS(A) ((A)->num_objects)
#define ALLOCNO_ADD_DATA(A) ((A)->add_data)

/* Typedef for pointer to the subsequent structure.  */
typedef struct ira_emit_data *ira_emit_data_t;

/* Allocno bound data used for emit pseudo live range split insns and
   to flattening IR.  */
struct ira_emit_data
{
  /* TRUE if the allocno assigned to memory was a destination of
     removed move (see ira-emit.c) at loop exit because the value of
     the corresponding pseudo-register is not changed inside the
     loop.  */
  unsigned int mem_optimized_dest_p : 1;
  /* TRUE if the corresponding pseudo-register has disjoint live
     ranges and the other allocnos of the pseudo-register except this
     one changed REG.  */
  unsigned int somewhere_renamed_p : 1;
  /* TRUE if allocno with the same REGNO in a subregion has been
     renamed, in other words, got a new pseudo-register.  */
  unsigned int child_renamed_p : 1;
  /* Final rtx representation of the allocno.  */
  rtx reg;
  /* Non NULL if we remove restoring value from given allocno to
     MEM_OPTIMIZED_DEST at loop exit (see ira-emit.c) because the
     allocno value is not changed inside the loop.  */
  ira_allocno_t mem_optimized_dest;
};

#define ALLOCNO_EMIT_DATA(a) ((ira_emit_data_t) ALLOCNO_ADD_DATA (a))

/* Data used to emit live range split insns and to flattening IR.  */
extern ira_emit_data_t ira_allocno_emit_data;

/* Abbreviation for frequent emit data access.  */
static inline rtx
allocno_emit_reg (ira_allocno_t a)
{
  return ALLOCNO_EMIT_DATA (a)->reg;
}

#define OBJECT_ALLOCNO(O) ((O)->allocno)
#define OBJECT_SUBWORD(O) ((O)->subword)
#define OBJECT_CONFLICT_ARRAY(O) ((O)->conflicts_array)
#define OBJECT_CONFLICT_VEC(O) ((ira_object_t *)(O)->conflicts_array)
#define OBJECT_CONFLICT_BITVEC(O) ((IRA_INT_TYPE *)(O)->conflicts_array)
#define OBJECT_CONFLICT_ARRAY_SIZE(O) ((O)->conflicts_array_size)
#define OBJECT_CONFLICT_VEC_P(O) ((O)->conflict_vec_p)
#define OBJECT_NUM_CONFLICTS(O) ((O)->num_accumulated_conflicts)
#define OBJECT_CONFLICT_HARD_REGS(O) ((O)->conflict_hard_regs)
#define OBJECT_TOTAL_CONFLICT_HARD_REGS(O) ((O)->total_conflict_hard_regs)
#define OBJECT_MIN(O) ((O)->min)
#define OBJECT_MAX(O) ((O)->max)
#define OBJECT_CONFLICT_ID(O) ((O)->id)
#define OBJECT_LIVE_RANGES(O) ((O)->live_ranges)

/* Map regno -> allocnos with given regno (see comments for
   allocno member `next_regno_allocno').  */
extern ira_allocno_t *ira_regno_allocno_map;

/* Array of references to all allocnos.  The order number of the
   allocno corresponds to the index in the array.  Removed allocnos
   have NULL element value.  */
extern ira_allocno_t *ira_allocnos;

/* The size of the previous array.  */
extern int ira_allocnos_num;

/* Map a conflict id to its corresponding ira_object structure.  */
extern ira_object_t *ira_object_id_map;

/* The size of the previous array.  */
extern int ira_objects_num;

/* The following structure represents a hard register preference of
   allocno.  The preference represent move insns or potential move
   insns usually because of two operand insn constraints.  One move
   operand is a hard register.  */
struct ira_allocno_pref
{
  /* The unique order number of the preference node starting with 0.  */
  int num;
  /* Preferred hard register.  */
  int hard_regno;
  /* Accumulated execution frequency of insns from which the
     preference created.  */
  int freq;
  /* Given allocno.  */
  ira_allocno_t allocno;
  /* All preferences with the same allocno are linked by the following
     member.  */
  ira_pref_t next_pref;
};

/* Array of references to all allocno preferences.  The order number
   of the preference corresponds to the index in the array.  */
extern ira_pref_t *ira_prefs;

/* Size of the previous array.  */
extern int ira_prefs_num;

/* The following structure represents a copy of two allocnos.  The
   copies represent move insns or potential move insns usually because
   of two operand insn constraints.  To remove register shuffle, we
   also create copies between allocno which is output of an insn and
   allocno becoming dead in the insn.  */
struct ira_allocno_copy
{
  /* The unique order number of the copy node starting with 0.  */
  int num;
  /* Allocnos connected by the copy.  The first allocno should have
     smaller order number than the second one.  */
  ira_allocno_t first, second;
  /* Execution frequency of the copy.  */
  int freq;
  bool constraint_p;
  /* It is a move insn which is an origin of the copy.  The member
     value for the copy representing two operand insn constraints or
     for the copy created to remove register shuffle is NULL.  In last
     case the copy frequency is smaller than the corresponding insn
     execution frequency.  */
  rtx_insn *insn;
  /* All copies with the same allocno as FIRST are linked by the two
     following members.  */
  ira_copy_t prev_first_allocno_copy, next_first_allocno_copy;
  /* All copies with the same allocno as SECOND are linked by the two
     following members.  */
  ira_copy_t prev_second_allocno_copy, next_second_allocno_copy;
  /* Region from which given copy is originated.  */
  ira_loop_tree_node_t loop_tree_node;
};

/* Array of references to all copies.  The order number of the copy
   corresponds to the index in the array.  Removed copies have NULL
   element value.  */
extern ira_copy_t *ira_copies;

/* Size of the previous array.  */
extern int ira_copies_num;

/* The following structure describes a stack slot used for spilled
   pseudo-registers.  */
struct ira_spilled_reg_stack_slot
{
  /* pseudo-registers assigned to the stack slot.  */
  bitmap_head spilled_regs;
  /* RTL representation of the stack slot.  */
  rtx mem;
  /* Size of the stack slot.  */
  poly_uint64_pod width;
};

/* The number of elements in the following array.  */
extern int ira_spilled_reg_stack_slots_num;

/* The following array contains info about spilled pseudo-registers
   stack slots used in current function so far.  */
extern struct ira_spilled_reg_stack_slot *ira_spilled_reg_stack_slots;

/* Correspondingly overall cost of the allocation, cost of the
   allocnos assigned to hard-registers, cost of the allocnos assigned
   to memory, cost of loads, stores and register move insns generated
   for pseudo-register live range splitting (see ira-emit.c).  */
extern int64_t ira_overall_cost;
extern int64_t ira_reg_cost, ira_mem_cost;
extern int64_t ira_load_cost, ira_store_cost, ira_shuffle_cost;
extern int ira_move_loops_num, ira_additional_jumps_num;


/* This page contains a bitset implementation called 'min/max sets' used to
   record conflicts in IRA.
   They are named min/maxs set since we keep track of a minimum and a maximum
   bit number for each set representing the bounds of valid elements.  Otherwise,
   the implementation resembles sbitmaps in that we store an array of integers
   whose bits directly represent the members of the set.  */

/* The type used as elements in the array, and the number of bits in
   this type.  */

#define IRA_INT_BITS HOST_BITS_PER_WIDE_INT
#define IRA_INT_TYPE HOST_WIDE_INT

/* Set, clear or test bit number I in R, a bit vector of elements with
   minimal index and maximal index equal correspondingly to MIN and
   MAX.  */
#if defined ENABLE_IRA_CHECKING && (GCC_VERSION >= 2007)

#define SET_MINMAX_SET_BIT(R, I, MIN, MAX) __extension__	        \
  (({ int _min = (MIN), _max = (MAX), _i = (I);				\
     if (_i < _min || _i > _max)					\
       {								\
         fprintf (stderr,						\
                  "\n%s: %d: error in %s: %d not in range [%d,%d]\n",   \
                  __FILE__, __LINE__, __FUNCTION__, _i, _min, _max);	\
         gcc_unreachable ();						\
       }								\
     ((R)[(unsigned) (_i - _min) / IRA_INT_BITS]			\
      |= ((IRA_INT_TYPE) 1 << ((unsigned) (_i - _min) % IRA_INT_BITS))); }))


#define CLEAR_MINMAX_SET_BIT(R, I, MIN, MAX) __extension__	        \
  (({ int _min = (MIN), _max = (MAX), _i = (I);				\
     if (_i < _min || _i > _max)					\
       {								\
         fprintf (stderr,						\
                  "\n%s: %d: error in %s: %d not in range [%d,%d]\n",   \
                  __FILE__, __LINE__, __FUNCTION__, _i, _min, _max);	\
         gcc_unreachable ();						\
       }								\
     ((R)[(unsigned) (_i - _min) / IRA_INT_BITS]			\
      &= ~((IRA_INT_TYPE) 1 << ((unsigned) (_i - _min) % IRA_INT_BITS))); }))

#define TEST_MINMAX_SET_BIT(R, I, MIN, MAX) __extension__	        \
  (({ int _min = (MIN), _max = (MAX), _i = (I);				\
     if (_i < _min || _i > _max)					\
       {								\
         fprintf (stderr,						\
                  "\n%s: %d: error in %s: %d not in range [%d,%d]\n",   \
                  __FILE__, __LINE__, __FUNCTION__, _i, _min, _max);	\
         gcc_unreachable ();						\
       }								\
     ((R)[(unsigned) (_i - _min) / IRA_INT_BITS]			\
      & ((IRA_INT_TYPE) 1 << ((unsigned) (_i - _min) % IRA_INT_BITS))); }))

#else

#define SET_MINMAX_SET_BIT(R, I, MIN, MAX)			\
  ((R)[(unsigned) ((I) - (MIN)) / IRA_INT_BITS]			\
   |= ((IRA_INT_TYPE) 1 << ((unsigned) ((I) - (MIN)) % IRA_INT_BITS)))

#define CLEAR_MINMAX_SET_BIT(R, I, MIN, MAX)			\
  ((R)[(unsigned) ((I) - (MIN)) / IRA_INT_BITS]			\
   &= ~((IRA_INT_TYPE) 1 << ((unsigned) ((I) - (MIN)) % IRA_INT_BITS)))

#define TEST_MINMAX_SET_BIT(R, I, MIN, MAX)			\
  ((R)[(unsigned) ((I) - (MIN)) / IRA_INT_BITS]			\
   & ((IRA_INT_TYPE) 1 << ((unsigned) ((I) - (MIN)) % IRA_INT_BITS)))

#endif

/* The iterator for min/max sets.  */
struct minmax_set_iterator {

  /* Array containing the bit vector.  */
  IRA_INT_TYPE *vec;

  /* The number of the current element in the vector.  */
  unsigned int word_num;

  /* The number of bits in the bit vector.  */
  unsigned int nel;

  /* The current bit index of the bit vector.  */
  unsigned int bit_num;

  /* Index corresponding to the 1st bit of the bit vector.   */
  int start_val;

  /* The word of the bit vector currently visited.  */
  unsigned IRA_INT_TYPE word;
};

/* Initialize the iterator I for bit vector VEC containing minimal and
   maximal values MIN and MAX.  */
static inline void
minmax_set_iter_init (minmax_set_iterator *i, IRA_INT_TYPE *vec, int min,
		      int max)
{
  i->vec = vec;
  i->word_num = 0;
  i->nel = max < min ? 0 : max - min + 1;
  i->start_val = min;
  i->bit_num = 0;
  i->word = i->nel == 0 ? 0 : vec[0];
}

/* Return TRUE if we have more allocnos to visit, in which case *N is
   set to the number of the element to be visited.  Otherwise, return
   FALSE.  */
static inline bool
minmax_set_iter_cond (minmax_set_iterator *i, int *n)
{
  /* Skip words that are zeros.  */
  for (; i->word == 0; i->word = i->vec[i->word_num])
    {
      i->word_num++;
      i->bit_num = i->word_num * IRA_INT_BITS;

      /* If we have reached the end, break.  */
      if (i->bit_num >= i->nel)
	return false;
    }

  /* Skip bits that are zero.  */
  for (; (i->word & 1) == 0; i->word >>= 1)
    i->bit_num++;

  *n = (int) i->bit_num + i->start_val;

  return true;
}

/* Advance to the next element in the set.  */
static inline void
minmax_set_iter_next (minmax_set_iterator *i)
{
  i->word >>= 1;
  i->bit_num++;
}

/* Loop over all elements of a min/max set given by bit vector VEC and
   their minimal and maximal values MIN and MAX.  In each iteration, N
   is set to the number of next allocno.  ITER is an instance of
   minmax_set_iterator used to iterate over the set.  */
#define FOR_EACH_BIT_IN_MINMAX_SET(VEC, MIN, MAX, N, ITER)	\
  for (minmax_set_iter_init (&(ITER), (VEC), (MIN), (MAX));	\
       minmax_set_iter_cond (&(ITER), &(N));			\
       minmax_set_iter_next (&(ITER)))

struct target_ira_int {
  ~target_ira_int ();

  void free_ira_costs ();
  void free_register_move_costs ();

  /* Initialized once.  It is a maximal possible size of the allocated
     struct costs.  */
  size_t x_max_struct_costs_size;

  /* Allocated and initialized once, and used to initialize cost values
     for each insn.  */
  struct costs *x_init_cost;

  /* Allocated once, and used for temporary purposes.  */
  struct costs *x_temp_costs;

  /* Allocated once, and used for the cost calculation.  */
  struct costs *x_op_costs[MAX_RECOG_OPERANDS];
  struct costs *x_this_op_costs[MAX_RECOG_OPERANDS];

  /* Hard registers that can not be used for the register allocator for
     all functions of the current compilation unit.  */
  HARD_REG_SET x_no_unit_alloc_regs;

  /* Map: hard regs X modes -> set of hard registers for storing value
     of given mode starting with given hard register.  */
  HARD_REG_SET (x_ira_reg_mode_hard_regset
		[FIRST_PSEUDO_REGISTER][NUM_MACHINE_MODES]);

  /* Maximum cost of moving from a register in one class to a register
     in another class.  Based on TARGET_REGISTER_MOVE_COST.  */
  move_table *x_ira_register_move_cost[MAX_MACHINE_MODE];

  /* Similar, but here we don't have to move if the first index is a
     subset of the second so in that case the cost is zero.  */
  move_table *x_ira_may_move_in_cost[MAX_MACHINE_MODE];

  /* Similar, but here we don't have to move if the first index is a
     superset of the second so in that case the cost is zero.  */
  move_table *x_ira_may_move_out_cost[MAX_MACHINE_MODE];

  /* Keep track of the last mode we initialized move costs for.  */
  int x_last_mode_for_init_move_cost;

  /* Array analog of the macro MEMORY_MOVE_COST but they contain maximal
     cost not minimal.  */
  short int x_ira_max_memory_move_cost[MAX_MACHINE_MODE][N_REG_CLASSES][2];

  /* Map class->true if class is a possible allocno class, false
     otherwise. */
  bool x_ira_reg_allocno_class_p[N_REG_CLASSES];

  /* Map class->true if class is a pressure class, false otherwise. */
  bool x_ira_reg_pressure_class_p[N_REG_CLASSES];

  /* Array of the number of hard registers of given class which are
     available for allocation.  The order is defined by the hard
     register numbers.  */
  short x_ira_non_ordered_class_hard_regs[N_REG_CLASSES][FIRST_PSEUDO_REGISTER];

  /* Index (in ira_class_hard_regs; for given register class and hard
     register (in general case a hard register can belong to several
     register classes;.  The index is negative for hard registers
     unavailable for the allocation.  */
  short x_ira_class_hard_reg_index[N_REG_CLASSES][FIRST_PSEUDO_REGISTER];

  /* Index [CL][M] contains R if R appears somewhere in a register of the form:

         (reg:M R'), R' not in x_ira_prohibited_class_mode_regs[CL][M]

     For example, if:

     - (reg:M 2) is valid and occupies two registers;
     - register 2 belongs to CL; and
     - register 3 belongs to the same pressure class as CL

     then (reg:M 2) contributes to [CL][M] and registers 2 and 3 will be
     in the set.  */
  HARD_REG_SET x_ira_useful_class_mode_regs[N_REG_CLASSES][NUM_MACHINE_MODES];

  /* The value is number of elements in the subsequent array.  */
  int x_ira_important_classes_num;

  /* The array containing all non-empty classes.  Such classes is
     important for calculation of the hard register usage costs.  */
  enum reg_class x_ira_important_classes[N_REG_CLASSES];

  /* The array containing indexes of important classes in the previous
     array.  The array elements are defined only for important
     classes.  */
  int x_ira_important_class_nums[N_REG_CLASSES];

  /* Map class->true if class is an uniform class, false otherwise.  */
  bool x_ira_uniform_class_p[N_REG_CLASSES];

  /* The biggest important class inside of intersection of the two
     classes (that is calculated taking only hard registers available
     for allocation into account;.  If the both classes contain no hard
     registers available for allocation, the value is calculated with
     taking all hard-registers including fixed ones into account.  */
  enum reg_class x_ira_reg_class_intersect[N_REG_CLASSES][N_REG_CLASSES];

  /* Classes with end marker LIM_REG_CLASSES which are intersected with
     given class (the first index).  That includes given class itself.
     This is calculated taking only hard registers available for
     allocation into account.  */
  enum reg_class x_ira_reg_class_super_classes[N_REG_CLASSES][N_REG_CLASSES];

  /* The biggest (smallest) important class inside of (covering) union
     of the two classes (that is calculated taking only hard registers
     available for allocation into account).  If the both classes
     contain no hard registers available for allocation, the value is
     calculated with taking all hard-registers including fixed ones
     into account.  In other words, the value is the corresponding
     reg_class_subunion (reg_class_superunion) value.  */
  enum reg_class x_ira_reg_class_subunion[N_REG_CLASSES][N_REG_CLASSES];
  enum reg_class x_ira_reg_class_superunion[N_REG_CLASSES][N_REG_CLASSES];

  /* For each reg class, table listing all the classes contained in it
     (excluding the class itself.  Non-allocatable registers are
     excluded from the consideration).  */
  enum reg_class x_alloc_reg_class_subclasses[N_REG_CLASSES][N_REG_CLASSES];

  /* Array whose values are hard regset of hard registers for which
     move of the hard register in given mode into itself is
     prohibited.  */
  HARD_REG_SET x_ira_prohibited_mode_move_regs[NUM_MACHINE_MODES];

  /* Flag of that the above array has been initialized.  */
  bool x_ira_prohibited_mode_move_regs_initialized_p;
};

extern struct target_ira_int default_target_ira_int;
#if SWITCHABLE_TARGET
extern struct target_ira_int *this_target_ira_int;
#else
#define this_target_ira_int (&default_target_ira_int)
#endif

#define ira_reg_mode_hard_regset \
  (this_target_ira_int->x_ira_reg_mode_hard_regset)
#define ira_register_move_cost \
  (this_target_ira_int->x_ira_register_move_cost)
#define ira_max_memory_move_cost \
  (this_target_ira_int->x_ira_max_memory_move_cost)
#define ira_may_move_in_cost \
  (this_target_ira_int->x_ira_may_move_in_cost)
#define ira_may_move_out_cost \
  (this_target_ira_int->x_ira_may_move_out_cost)
#define ira_reg_allocno_class_p \
  (this_target_ira_int->x_ira_reg_allocno_class_p)
#define ira_reg_pressure_class_p \
  (this_target_ira_int->x_ira_reg_pressure_class_p)
#define ira_non_ordered_class_hard_regs \
  (this_target_ira_int->x_ira_non_ordered_class_hard_regs)
#define ira_class_hard_reg_index \
  (this_target_ira_int->x_ira_class_hard_reg_index)
#define ira_useful_class_mode_regs \
  (this_target_ira_int->x_ira_useful_class_mode_regs)
#define ira_important_classes_num \
  (this_target_ira_int->x_ira_important_classes_num)
#define ira_important_classes \
  (this_target_ira_int->x_ira_important_classes)
#define ira_important_class_nums \
  (this_target_ira_int->x_ira_important_class_nums)
#define ira_uniform_class_p \
  (this_target_ira_int->x_ira_uniform_class_p)
#define ira_reg_class_intersect \
  (this_target_ira_int->x_ira_reg_class_intersect)
#define ira_reg_class_super_classes \
  (this_target_ira_int->x_ira_reg_class_super_classes)
#define ira_reg_class_subunion \
  (this_target_ira_int->x_ira_reg_class_subunion)
#define ira_reg_class_superunion \
  (this_target_ira_int->x_ira_reg_class_superunion)
#define ira_prohibited_mode_move_regs \
  (this_target_ira_int->x_ira_prohibited_mode_move_regs)

/* ira.c: */

extern void *ira_allocate (size_t);
extern void ira_free (void *addr);
extern bitmap ira_allocate_bitmap (void);
extern void ira_free_bitmap (bitmap);
extern void ira_print_disposition (FILE *);
extern void ira_debug_disposition (void);
extern void ira_debug_allocno_classes (void);
extern void ira_init_register_move_cost (machine_mode);
extern void ira_setup_alts (rtx_insn *insn, HARD_REG_SET &alts);
extern int ira_get_dup_out_num (int op_num, HARD_REG_SET &alts);

/* ira-build.c */

/* The current loop tree node and its regno allocno map.  */
extern ira_loop_tree_node_t ira_curr_loop_tree_node;
extern ira_allocno_t *ira_curr_regno_allocno_map;

extern void ira_debug_pref (ira_pref_t);
extern void ira_debug_prefs (void);
extern void ira_debug_allocno_prefs (ira_allocno_t);

extern void ira_debug_copy (ira_copy_t);
extern void debug (ira_allocno_copy &ref);
extern void debug (ira_allocno_copy *ptr);

extern void ira_debug_copies (void);
extern void ira_debug_allocno_copies (ira_allocno_t);
extern void debug (ira_allocno &ref);
extern void debug (ira_allocno *ptr);

extern void ira_traverse_loop_tree (bool, ira_loop_tree_node_t,
				    void (*) (ira_loop_tree_node_t),
				    void (*) (ira_loop_tree_node_t));
extern ira_allocno_t ira_parent_allocno (ira_allocno_t);
extern ira_allocno_t ira_parent_or_cap_allocno (ira_allocno_t);
extern ira_allocno_t ira_create_allocno (int, bool, ira_loop_tree_node_t);
extern void ira_create_allocno_objects (ira_allocno_t);
extern void ira_set_allocno_class (ira_allocno_t, enum reg_class);
extern bool ira_conflict_vector_profitable_p (ira_object_t, int);
extern void ira_allocate_conflict_vec (ira_object_t, int);
extern void ira_allocate_object_conflicts (ira_object_t, int);
extern void ior_hard_reg_conflicts (ira_allocno_t, HARD_REG_SET *);
extern void ira_print_expanded_allocno (ira_allocno_t);
extern void ira_add_live_range_to_object (ira_object_t, int, int);
extern live_range_t ira_create_live_range (ira_object_t, int, int,
					   live_range_t);
extern live_range_t ira_copy_live_range_list (live_range_t);
extern live_range_t ira_merge_live_ranges (live_range_t, live_range_t);
extern bool ira_live_ranges_intersect_p (live_range_t, live_range_t);
extern void ira_finish_live_range (live_range_t);
extern void ira_finish_live_range_list (live_range_t);
extern void ira_free_allocno_updated_costs (ira_allocno_t);
extern ira_pref_t ira_create_pref (ira_allocno_t, int, int);
extern void ira_add_allocno_pref (ira_allocno_t, int, int);
extern void ira_remove_pref (ira_pref_t);
extern void ira_remove_allocno_prefs (ira_allocno_t);
extern ira_copy_t ira_create_copy (ira_allocno_t, ira_allocno_t,
				   int, bool, rtx_insn *,
				   ira_loop_tree_node_t);
extern ira_copy_t ira_add_allocno_copy (ira_allocno_t, ira_allocno_t, int,
					bool, rtx_insn *,
					ira_loop_tree_node_t);

extern int *ira_allocate_cost_vector (reg_class_t);
extern void ira_free_cost_vector (int *, reg_class_t);

extern void ira_flattening (int, int);
extern bool ira_build (void);
extern void ira_destroy (void);

/* ira-costs.c */
extern void ira_init_costs_once (void);
extern void ira_init_costs (void);
extern void ira_costs (void);
extern void ira_tune_allocno_costs (void);

/* ira-lives.c */

extern void ira_rebuild_start_finish_chains (void);
extern void ira_print_live_range_list (FILE *, live_range_t);
extern void debug (live_range &ref);
extern void debug (live_range *ptr);
extern void ira_debug_live_range_list (live_range_t);
extern void ira_debug_allocno_live_ranges (ira_allocno_t);
extern void ira_debug_live_ranges (void);
extern void ira_create_allocno_live_ranges (void);
extern void ira_compress_allocno_live_ranges (void);
extern void ira_finish_allocno_live_ranges (void);
extern void ira_implicitly_set_insn_hard_regs (HARD_REG_SET *,
					       alternative_mask);

/* ira-conflicts.c */
extern void ira_debug_conflicts (bool);
extern void ira_build_conflicts (void);

/* ira-color.c */
extern void ira_debug_hard_regs_forest (void);
extern int ira_loop_edge_freq (ira_loop_tree_node_t, int, bool);
extern void ira_reassign_conflict_allocnos (int);
extern void ira_initiate_assign (void);
extern void ira_finish_assign (void);
extern void ira_color (void);

/* ira-emit.c */
extern void ira_initiate_emit_data (void);
extern void ira_finish_emit_data (void);
extern void ira_emit (bool);



/* Return true if equivalence of pseudo REGNO is not a lvalue.  */
static inline bool
ira_equiv_no_lvalue_p (int regno)
{
  if (regno >= ira_reg_equiv_len)
    return false;
  return (ira_reg_equiv[regno].constant != NULL_RTX
	  || ira_reg_equiv[regno].invariant != NULL_RTX
	  || (ira_reg_equiv[regno].memory != NULL_RTX
	      && MEM_READONLY_P (ira_reg_equiv[regno].memory)));
}



/* Initialize register costs for MODE if necessary.  */
static inline void
ira_init_register_move_cost_if_necessary (machine_mode mode)
{
  if (ira_register_move_cost[mode] == NULL)
    ira_init_register_move_cost (mode);
}



/* The iterator for all allocnos.  */
struct ira_allocno_iterator {
  /* The number of the current element in IRA_ALLOCNOS.  */
  int n;
};

/* Initialize the iterator I.  */
static inline void
ira_allocno_iter_init (ira_allocno_iterator *i)
{
  i->n = 0;
}

/* Return TRUE if we have more allocnos to visit, in which case *A is
   set to the allocno to be visited.  Otherwise, return FALSE.  */
static inline bool
ira_allocno_iter_cond (ira_allocno_iterator *i, ira_allocno_t *a)
{
  int n;

  for (n = i->n; n < ira_allocnos_num; n++)
    if (ira_allocnos[n] != NULL)
      {
	*a = ira_allocnos[n];
	i->n = n + 1;
	return true;
      }
  return false;
}

/* Loop over all allocnos.  In each iteration, A is set to the next
   allocno.  ITER is an instance of ira_allocno_iterator used to iterate
   the allocnos.  */
#define FOR_EACH_ALLOCNO(A, ITER)			\
  for (ira_allocno_iter_init (&(ITER));			\
       ira_allocno_iter_cond (&(ITER), &(A));)

/* The iterator for all objects.  */
struct ira_object_iterator {
  /* The number of the current element in ira_object_id_map.  */
  int n;
};

/* Initialize the iterator I.  */
static inline void
ira_object_iter_init (ira_object_iterator *i)
{
  i->n = 0;
}

/* Return TRUE if we have more objects to visit, in which case *OBJ is
   set to the object to be visited.  Otherwise, return FALSE.  */
static inline bool
ira_object_iter_cond (ira_object_iterator *i, ira_object_t *obj)
{
  int n;

  for (n = i->n; n < ira_objects_num; n++)
    if (ira_object_id_map[n] != NULL)
      {
	*obj = ira_object_id_map[n];
	i->n = n + 1;
	return true;
      }
  return false;
}

/* Loop over all objects.  In each iteration, OBJ is set to the next
   object.  ITER is an instance of ira_object_iterator used to iterate
   the objects.  */
#define FOR_EACH_OBJECT(OBJ, ITER)			\
  for (ira_object_iter_init (&(ITER));			\
       ira_object_iter_cond (&(ITER), &(OBJ));)

/* The iterator for objects associated with an allocno.  */
struct ira_allocno_object_iterator {
  /* The number of the element the allocno's object array.  */
  int n;
};

/* Initialize the iterator I.  */
static inline void
ira_allocno_object_iter_init (ira_allocno_object_iterator *i)
{
  i->n = 0;
}

/* Return TRUE if we have more objects to visit in allocno A, in which
   case *O is set to the object to be visited.  Otherwise, return
   FALSE.  */
static inline bool
ira_allocno_object_iter_cond (ira_allocno_object_iterator *i, ira_allocno_t a,
			      ira_object_t *o)
{
  int n = i->n++;
  if (n < ALLOCNO_NUM_OBJECTS (a))
    {
      *o = ALLOCNO_OBJECT (a, n);
      return true;
    }
  return false;
}

/* Loop over all objects associated with allocno A.  In each
   iteration, O is set to the next object.  ITER is an instance of
   ira_allocno_object_iterator used to iterate the conflicts.  */
#define FOR_EACH_ALLOCNO_OBJECT(A, O, ITER)			\
  for (ira_allocno_object_iter_init (&(ITER));			\
       ira_allocno_object_iter_cond (&(ITER), (A), &(O));)


/* The iterator for prefs.  */
struct ira_pref_iterator {
  /* The number of the current element in IRA_PREFS.  */
  int n;
};

/* Initialize the iterator I.  */
static inline void
ira_pref_iter_init (ira_pref_iterator *i)
{
  i->n = 0;
}

/* Return TRUE if we have more prefs to visit, in which case *PREF is
   set to the pref to be visited.  Otherwise, return FALSE.  */
static inline bool
ira_pref_iter_cond (ira_pref_iterator *i, ira_pref_t *pref)
{
  int n;

  for (n = i->n; n < ira_prefs_num; n++)
    if (ira_prefs[n] != NULL)
      {
	*pref = ira_prefs[n];
	i->n = n + 1;
	return true;
      }
  return false;
}

/* Loop over all prefs.  In each iteration, P is set to the next
   pref.  ITER is an instance of ira_pref_iterator used to iterate
   the prefs.  */
#define FOR_EACH_PREF(P, ITER)				\
  for (ira_pref_iter_init (&(ITER));			\
       ira_pref_iter_cond (&(ITER), &(P));)


/* The iterator for copies.  */
struct ira_copy_iterator {
  /* The number of the current element in IRA_COPIES.  */
  int n;
};

/* Initialize the iterator I.  */
static inline void
ira_copy_iter_init (ira_copy_iterator *i)
{
  i->n = 0;
}

/* Return TRUE if we have more copies to visit, in which case *CP is
   set to the copy to be visited.  Otherwise, return FALSE.  */
static inline bool
ira_copy_iter_cond (ira_copy_iterator *i, ira_copy_t *cp)
{
  int n;

  for (n = i->n; n < ira_copies_num; n++)
    if (ira_copies[n] != NULL)
      {
	*cp = ira_copies[n];
	i->n = n + 1;
	return true;
      }
  return false;
}

/* Loop over all copies.  In each iteration, C is set to the next
   copy.  ITER is an instance of ira_copy_iterator used to iterate
   the copies.  */
#define FOR_EACH_COPY(C, ITER)				\
  for (ira_copy_iter_init (&(ITER));			\
       ira_copy_iter_cond (&(ITER), &(C));)

/* The iterator for object conflicts.  */
struct ira_object_conflict_iterator {

  /* TRUE if the conflicts are represented by vector of allocnos.  */
  bool conflict_vec_p;

  /* The conflict vector or conflict bit vector.  */
  void *vec;

  /* The number of the current element in the vector (of type
     ira_object_t or IRA_INT_TYPE).  */
  unsigned int word_num;

  /* The bit vector size.  It is defined only if
     OBJECT_CONFLICT_VEC_P is FALSE.  */
  unsigned int size;

  /* The current bit index of bit vector.  It is defined only if
     OBJECT_CONFLICT_VEC_P is FALSE.  */
  unsigned int bit_num;

  /* The object id corresponding to the 1st bit of the bit vector.  It
     is defined only if OBJECT_CONFLICT_VEC_P is FALSE.  */
  int base_conflict_id;

  /* The word of bit vector currently visited.  It is defined only if
     OBJECT_CONFLICT_VEC_P is FALSE.  */
  unsigned IRA_INT_TYPE word;
};

/* Initialize the iterator I with ALLOCNO conflicts.  */
static inline void
ira_object_conflict_iter_init (ira_object_conflict_iterator *i,
			       ira_object_t obj)
{
  i->conflict_vec_p = OBJECT_CONFLICT_VEC_P (obj);
  i->vec = OBJECT_CONFLICT_ARRAY (obj);
  i->word_num = 0;
  if (i->conflict_vec_p)
    i->size = i->bit_num = i->base_conflict_id = i->word = 0;
  else
    {
      if (OBJECT_MIN (obj) > OBJECT_MAX (obj))
	i->size = 0;
      else
	i->size = ((OBJECT_MAX (obj) - OBJECT_MIN (obj)
		    + IRA_INT_BITS)
		   / IRA_INT_BITS) * sizeof (IRA_INT_TYPE);
      i->bit_num = 0;
      i->base_conflict_id = OBJECT_MIN (obj);
      i->word = (i->size == 0 ? 0 : ((IRA_INT_TYPE *) i->vec)[0]);
    }
}

/* Return TRUE if we have more conflicting allocnos to visit, in which
   case *A is set to the allocno to be visited.  Otherwise, return
   FALSE.  */
static inline bool
ira_object_conflict_iter_cond (ira_object_conflict_iterator *i,
			       ira_object_t *pobj)
{
  ira_object_t obj;

  if (i->conflict_vec_p)
    {
      obj = ((ira_object_t *) i->vec)[i->word_num++];
      if (obj == NULL)
	return false;
    }
  else
    {
      unsigned IRA_INT_TYPE word = i->word;
      unsigned int bit_num = i->bit_num;

      /* Skip words that are zeros.  */
      for (; word == 0; word = ((IRA_INT_TYPE *) i->vec)[i->word_num])
	{
	  i->word_num++;

	  /* If we have reached the end, break.  */
	  if (i->word_num * sizeof (IRA_INT_TYPE) >= i->size)
	    return false;

	  bit_num = i->word_num * IRA_INT_BITS;
	}

      /* Skip bits that are zero.  */
      for (; (word & 1) == 0; word >>= 1)
	bit_num++;

      obj = ira_object_id_map[bit_num + i->base_conflict_id];
      i->bit_num = bit_num + 1;
      i->word = word >> 1;
    }

  *pobj = obj;
  return true;
}

/* Loop over all objects conflicting with OBJ.  In each iteration,
   CONF is set to the next conflicting object.  ITER is an instance
   of ira_object_conflict_iterator used to iterate the conflicts.  */
#define FOR_EACH_OBJECT_CONFLICT(OBJ, CONF, ITER)			\
  for (ira_object_conflict_iter_init (&(ITER), (OBJ));			\
       ira_object_conflict_iter_cond (&(ITER), &(CONF));)



/* The function returns TRUE if at least one hard register from ones
   starting with HARD_REGNO and containing value of MODE are in set
   HARD_REGSET.  */
static inline bool
ira_hard_reg_set_intersection_p (int hard_regno, machine_mode mode,
				 HARD_REG_SET hard_regset)
{
  int i;

  gcc_assert (hard_regno >= 0);
  for (i = hard_regno_nregs (hard_regno, mode) - 1; i >= 0; i--)
    if (TEST_HARD_REG_BIT (hard_regset, hard_regno + i))
      return true;
  return false;
}

/* Return number of hard registers in hard register SET.  */
static inline int
hard_reg_set_size (HARD_REG_SET set)
{
  int i, size;

  for (size = i = 0; i < FIRST_PSEUDO_REGISTER; i++)
    if (TEST_HARD_REG_BIT (set, i))
      size++;
  return size;
}

/* The function returns TRUE if hard registers starting with
   HARD_REGNO and containing value of MODE are fully in set
   HARD_REGSET.  */
static inline bool
ira_hard_reg_in_set_p (int hard_regno, machine_mode mode,
		       HARD_REG_SET hard_regset)
{
  int i;

  ira_assert (hard_regno >= 0);
  for (i = hard_regno_nregs (hard_regno, mode) - 1; i >= 0; i--)
    if (!TEST_HARD_REG_BIT (hard_regset, hard_regno + i))
      return false;
  return true;
}



/* To save memory we use a lazy approach for allocation and
   initialization of the cost vectors.  We do this only when it is
   really necessary.  */

/* Allocate cost vector *VEC for hard registers of ACLASS and
   initialize the elements by VAL if it is necessary */
static inline void
ira_allocate_and_set_costs (int **vec, reg_class_t aclass, int val)
{
  int i, *reg_costs;
  int len;

  if (*vec != NULL)
    return;
  *vec = reg_costs = ira_allocate_cost_vector (aclass);
  len = ira_class_hard_regs_num[(int) aclass];
  for (i = 0; i < len; i++)
    reg_costs[i] = val;
}

/* Allocate cost vector *VEC for hard registers of ACLASS and copy
   values of vector SRC into the vector if it is necessary */
static inline void
ira_allocate_and_copy_costs (int **vec, enum reg_class aclass, int *src)
{
  int len;

  if (*vec != NULL || src == NULL)
    return;
  *vec = ira_allocate_cost_vector (aclass);
  len = ira_class_hard_regs_num[aclass];
  memcpy (*vec, src, sizeof (int) * len);
}

/* Allocate cost vector *VEC for hard registers of ACLASS and add
   values of vector SRC into the vector if it is necessary */
static inline void
ira_allocate_and_accumulate_costs (int **vec, enum reg_class aclass, int *src)
{
  int i, len;

  if (src == NULL)
    return;
  len = ira_class_hard_regs_num[aclass];
  if (*vec == NULL)
    {
      *vec = ira_allocate_cost_vector (aclass);
      memset (*vec, 0, sizeof (int) * len);
    }
  for (i = 0; i < len; i++)
    (*vec)[i] += src[i];
}

/* Allocate cost vector *VEC for hard registers of ACLASS and copy
   values of vector SRC into the vector or initialize it by VAL (if
   SRC is null).  */
static inline void
ira_allocate_and_set_or_copy_costs (int **vec, enum reg_class aclass,
				    int val, int *src)
{
  int i, *reg_costs;
  int len;

  if (*vec != NULL)
    return;
  *vec = reg_costs = ira_allocate_cost_vector (aclass);
  len = ira_class_hard_regs_num[aclass];
  if (src != NULL)
    memcpy (reg_costs, src, sizeof (int) * len);
  else
    {
      for (i = 0; i < len; i++)
	reg_costs[i] = val;
    }
}

extern rtx ira_create_new_reg (rtx);
extern int first_moveable_pseudo, last_moveable_pseudo;

#endif /* GCC_IRA_INT_H */
