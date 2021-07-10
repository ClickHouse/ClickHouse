/* Instruction scheduling pass.  This file contains definitions used
   internally in the scheduler.
   Copyright (C) 1992-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SCHED_INT_H
#define GCC_SCHED_INT_H

#ifdef INSN_SCHEDULING

/* Identificator of a scheduler pass.  */
enum sched_pass_id_t { SCHED_PASS_UNKNOWN, SCHED_RGN_PASS, SCHED_EBB_PASS,
		       SCHED_SMS_PASS, SCHED_SEL_PASS };

/* The algorithm used to implement -fsched-pressure.  */
enum sched_pressure_algorithm
{
  SCHED_PRESSURE_NONE,
  SCHED_PRESSURE_WEIGHTED,
  SCHED_PRESSURE_MODEL
};

typedef vec<basic_block> bb_vec_t;
typedef vec<rtx_insn *> insn_vec_t;
typedef vec<rtx_insn *> rtx_vec_t;

extern void sched_init_bbs (void);

extern void sched_extend_luids (void);
extern void sched_init_insn_luid (rtx_insn *);
extern void sched_init_luids (bb_vec_t);
extern void sched_finish_luids (void);

extern void sched_extend_target (void);

extern void haifa_init_h_i_d (bb_vec_t);
extern void haifa_finish_h_i_d (void);

/* Hooks that are common to all the schedulers.  */
struct common_sched_info_def
{
  /* Called after blocks were rearranged due to movement of jump instruction.
     The first parameter - index of basic block, in which jump currently is.
     The second parameter - index of basic block, in which jump used
     to be.
     The third parameter - index of basic block, that follows the second
     parameter.  */
  void (*fix_recovery_cfg) (int, int, int);

  /* Called to notify frontend, that new basic block is being added.
     The first parameter - new basic block.
     The second parameter - block, after which new basic block is being added,
     or the exit block, if recovery block is being added,
     or NULL, if standalone block is being added.  */
  void (*add_block) (basic_block, basic_block);

  /* Estimate number of insns in the basic block.  */
  int (*estimate_number_of_insns) (basic_block);

  /* Given a non-insn (!INSN_P (x)) return
     -1 - if this rtx don't need a luid.
     0 - if it should have the same luid as the previous insn.
     1 - if it needs a separate luid.  */
  int (*luid_for_non_insn) (rtx);

  /* Scheduler pass identifier.  It is preferably used in assertions.  */
  enum sched_pass_id_t sched_pass_id;
};

extern struct common_sched_info_def *common_sched_info;

extern const struct common_sched_info_def haifa_common_sched_info;

/* Return true if selective scheduling pass is working.  */
static inline bool
sel_sched_p (void)
{
  return common_sched_info->sched_pass_id == SCHED_SEL_PASS;
}

/* Returns maximum priority that an insn was assigned to.  */
extern int get_rgn_sched_max_insns_priority (void);

/* Increases effective priority for INSN by AMOUNT.  */
extern void sel_add_to_insn_priority (rtx, int);

/* True if during selective scheduling we need to emulate some of haifa
   scheduler behavior.  */
extern int sched_emulate_haifa_p;

/* Mapping from INSN_UID to INSN_LUID.  In the end all other per insn data
   structures should be indexed by luid.  */
extern vec<int> sched_luids;
#define INSN_LUID(INSN) (sched_luids[INSN_UID (INSN)])
#define LUID_BY_UID(UID) (sched_luids[UID])

#define SET_INSN_LUID(INSN, LUID) \
(sched_luids[INSN_UID (INSN)] = (LUID))

/* The highest INSN_LUID.  */
extern int sched_max_luid;

extern int insn_luid (rtx);

/* This list holds ripped off notes from the current block.  These notes will
   be attached to the beginning of the block when its scheduling is
   finished.  */
extern rtx_insn *note_list;

extern void remove_notes (rtx_insn *, rtx_insn *);
extern rtx_insn *restore_other_notes (rtx_insn *, basic_block);
extern void sched_insns_init (rtx);
extern void sched_insns_finish (void);

extern void *xrecalloc (void *, size_t, size_t, size_t);

extern void reemit_notes (rtx_insn *);

/* Functions in haifa-sched.c.  */
extern int haifa_classify_insn (const_rtx);

/* Functions in sel-sched-ir.c.  */
extern void sel_find_rgns (void);
extern void sel_mark_hard_insn (rtx);

extern size_t dfa_state_size;

extern void advance_state (state_t);

extern void setup_sched_dump (void);
extern void sched_init (void);
extern void sched_finish (void);

extern bool sel_insn_is_speculation_check (rtx);

/* Describe the ready list of the scheduler.
   VEC holds space enough for all insns in the current region.  VECLEN
   says how many exactly.
   FIRST is the index of the element with the highest priority; i.e. the
   last one in the ready list, since elements are ordered by ascending
   priority.
   N_READY determines how many insns are on the ready list.
   N_DEBUG determines how many debug insns are on the ready list.  */
struct ready_list
{
  rtx_insn **vec;
  int veclen;
  int first;
  int n_ready;
  int n_debug;
};

extern signed char *ready_try;
extern struct ready_list ready;

extern int max_issue (struct ready_list *, int, state_t, bool, int *);

extern void ebb_compute_jump_reg_dependencies (rtx, regset);

extern edge find_fallthru_edge_from (basic_block);

extern void (* sched_init_only_bb) (basic_block, basic_block);
extern basic_block (* sched_split_block) (basic_block, rtx);
extern basic_block sched_split_block_1 (basic_block, rtx);
extern basic_block (* sched_create_empty_bb) (basic_block);
extern basic_block sched_create_empty_bb_1 (basic_block);

extern basic_block sched_create_recovery_block (basic_block *);
extern void sched_create_recovery_edges (basic_block, basic_block,
					 basic_block);

/* Pointer to data describing the current DFA state.  */
extern state_t curr_state;

/* Type to represent status of a dependence.  */
typedef unsigned int ds_t;
#define BITS_PER_DEP_STATUS HOST_BITS_PER_INT

/* Type to represent weakness of speculative dependence.  */
typedef unsigned int dw_t;

extern enum reg_note ds_to_dk (ds_t);
extern ds_t dk_to_ds (enum reg_note);

/* Describe a dependency that can be broken by making a replacement
   in one of the patterns.  LOC is the location, ORIG and NEWVAL the
   two alternative contents, and INSN the instruction that must be
   changed.  */
struct dep_replacement
{
  rtx *loc;
  rtx orig;
  rtx newval;
  rtx_insn *insn;
};

/* Information about the dependency.  */
struct _dep
{
  /* Producer.  */
  rtx_insn *pro;

  /* Consumer.  */
  rtx_insn *con;

  /* If nonnull, holds a pointer to information about how to break the
     dependency by making a replacement in one of the insns.  There is
     only one such dependency for each insn that must be modified in
     order to break such a dependency.  */
  struct dep_replacement *replace;

  /* Dependency status.  This field holds all dependency types and additional
     information for speculative dependencies.  */
  ds_t status;

  /* Dependency major type.  This field is superseded by STATUS above.
     Though, it is still in place because some targets use it.  */
  ENUM_BITFIELD(reg_note) type:6;

  unsigned nonreg:1;
  unsigned multiple:1;

  /* Cached cost of the dependency.  Make sure to update UNKNOWN_DEP_COST
     when changing the size of this field.  */
  int cost:20;
};

#define UNKNOWN_DEP_COST ((int) ((unsigned int) -1 << 19))

typedef struct _dep dep_def;
typedef dep_def *dep_t;

#define DEP_PRO(D) ((D)->pro)
#define DEP_CON(D) ((D)->con)
#define DEP_TYPE(D) ((D)->type)
#define DEP_STATUS(D) ((D)->status)
#define DEP_COST(D) ((D)->cost)
#define DEP_NONREG(D) ((D)->nonreg)
#define DEP_MULTIPLE(D) ((D)->multiple)
#define DEP_REPLACE(D) ((D)->replace)

/* Functions to work with dep.  */

extern void init_dep_1 (dep_t, rtx_insn *, rtx_insn *, enum reg_note, ds_t);
extern void init_dep (dep_t, rtx_insn *, rtx_insn *, enum reg_note);

extern void sd_debug_dep (dep_t);

/* Definition of this struct resides below.  */
struct _dep_node;
typedef struct _dep_node *dep_node_t;

/* A link in the dependency list.  This is essentially an equivalent of a
   single {INSN, DEPS}_LIST rtx.  */
struct _dep_link
{
  /* Dep node with all the data.  */
  dep_node_t node;

  /* Next link in the list. For the last one it is NULL.  */
  struct _dep_link *next;

  /* Pointer to the next field of the previous link in the list.
     For the first link this points to the deps_list->first.

     With help of this field it is easy to remove and insert links to the
     list.  */
  struct _dep_link **prev_nextp;
};
typedef struct _dep_link *dep_link_t;

#define DEP_LINK_NODE(N) ((N)->node)
#define DEP_LINK_NEXT(N) ((N)->next)
#define DEP_LINK_PREV_NEXTP(N) ((N)->prev_nextp)

/* Macros to work dep_link.  For most usecases only part of the dependency
   information is need.  These macros conveniently provide that piece of
   information.  */

#define DEP_LINK_DEP(N) (DEP_NODE_DEP (DEP_LINK_NODE (N)))
#define DEP_LINK_PRO(N) (DEP_PRO (DEP_LINK_DEP (N)))
#define DEP_LINK_CON(N) (DEP_CON (DEP_LINK_DEP (N)))
#define DEP_LINK_TYPE(N) (DEP_TYPE (DEP_LINK_DEP (N)))
#define DEP_LINK_STATUS(N) (DEP_STATUS (DEP_LINK_DEP (N)))

/* A list of dep_links.  */
struct _deps_list
{
  /* First element.  */
  dep_link_t first;

  /* Total number of elements in the list.  */
  int n_links;
};
typedef struct _deps_list *deps_list_t;

#define DEPS_LIST_FIRST(L) ((L)->first)
#define DEPS_LIST_N_LINKS(L) ((L)->n_links)

/* Suppose we have a dependence Y between insn pro1 and con1, where pro1 has
   additional dependents con0 and con2, and con1 is dependent on additional
   insns pro0 and pro1:

   .con0      pro0
   . ^         |
   . |         |
   . |         |
   . X         A
   . |         |
   . |         |
   . |         V
   .pro1--Y-->con1
   . |         ^
   . |         |
   . |         |
   . Z         B
   . |         |
   . |         |
   . V         |
   .con2      pro2

   This is represented using a "dep_node" for each dependence arc, which are
   connected as follows (diagram is centered around Y which is fully shown;
   other dep_nodes shown partially):

   .          +------------+    +--------------+    +------------+
   .          : dep_node X :    |  dep_node Y  |    : dep_node Z :
   .          :            :    |              |    :            :
   .          :            :    |              |    :            :
   .          : forw       :    |  forw        |    : forw       :
   .          : +--------+ :    |  +--------+  |    : +--------+ :
   forw_deps  : |dep_link| :    |  |dep_link|  |    : |dep_link| :
   +-----+    : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   |first|----->| |next|-+------+->| |next|-+--+----->| |next|-+--->NULL
   +-----+    : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   . ^  ^     : |     ^  | :    |  |     ^  |  |    : |        | :
   . |  |     : |     |  | :    |  |     |  |  |    : |        | :
   . |  +--<----+--+  +--+---<--+--+--+  +--+--+--<---+--+     | :
   . |        : |  |     | :    |  |  |     |  |    : |  |     | :
   . |        : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   . |        : | |prev| | :    |  | |prev| |  |    : | |prev| | :
   . |        : | |next| | :    |  | |next| |  |    : | |next| | :
   . |        : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   . |        : |        | :<-+ |  |        |  |<-+ : |        | :<-+
   . |        : | +----+ | :  | |  | +----+ |  |  | : | +----+ | :  |
   . |        : | |node|-+----+ |  | |node|-+--+--+ : | |node|-+----+
   . |        : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   . |        : |        | :    |  |        |  |    : |        | :
   . |        : +--------+ :    |  +--------+  |    : +--------+ :
   . |        :            :    |              |    :            :
   . |        :  SAME pro1 :    |  +--------+  |    :  SAME pro1 :
   . |        :  DIFF con0 :    |  |dep     |  |    :  DIFF con2 :
   . |        :            :    |  |        |  |    :            :
   . |                          |  | +----+ |  |
   .RTX<------------------------+--+-|pro1| |  |
   .pro1                        |  | +----+ |  |
   .                            |  |        |  |
   .                            |  | +----+ |  |
   .RTX<------------------------+--+-|con1| |  |
   .con1                        |  | +----+ |  |
   . |                          |  |        |  |
   . |                          |  | +----+ |  |
   . |                          |  | |kind| |  |
   . |                          |  | +----+ |  |
   . |        :            :    |  | |stat| |  |    :            :
   . |        :  DIFF pro0 :    |  | +----+ |  |    :  DIFF pro2 :
   . |        :  SAME con1 :    |  |        |  |    :  SAME con1 :
   . |        :            :    |  +--------+  |    :            :
   . |        :            :    |              |    :            :
   . |        : back       :    |  back        |    : back       :
   . v        : +--------+ :    |  +--------+  |    : +--------+ :
   back_deps  : |dep_link| :    |  |dep_link|  |    : |dep_link| :
   +-----+    : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   |first|----->| |next|-+------+->| |next|-+--+----->| |next|-+--->NULL
   +-----+    : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   .    ^     : |     ^  | :    |  |     ^  |  |    : |        | :
   .    |     : |     |  | :    |  |     |  |  |    : |        | :
   .    +--<----+--+  +--+---<--+--+--+  +--+--+--<---+--+     | :
   .          : |  |     | :    |  |  |     |  |    : |  |     | :
   .          : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   .          : | |prev| | :    |  | |prev| |  |    : | |prev| | :
   .          : | |next| | :    |  | |next| |  |    : | |next| | :
   .          : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   .          : |        | :<-+ |  |        |  |<-+ : |        | :<-+
   .          : | +----+ | :  | |  | +----+ |  |  | : | +----+ | :  |
   .          : | |node|-+----+ |  | |node|-+--+--+ : | |node|-+----+
   .          : | +----+ | :    |  | +----+ |  |    : | +----+ | :
   .          : |        | :    |  |        |  |    : |        | :
   .          : +--------+ :    |  +--------+  |    : +--------+ :
   .          :            :    |              |    :            :
   .          : dep_node A :    |  dep_node Y  |    : dep_node B :
   .          +------------+    +--------------+    +------------+
*/

struct _dep_node
{
  /* Backward link.  */
  struct _dep_link back;

  /* The dep.  */
  struct _dep dep;

  /* Forward link.  */
  struct _dep_link forw;
};

#define DEP_NODE_BACK(N) (&(N)->back)
#define DEP_NODE_DEP(N) (&(N)->dep)
#define DEP_NODE_FORW(N) (&(N)->forw)

/* The following enumeration values tell us what dependencies we
   should use to implement the barrier.  We use true-dependencies for
   TRUE_BARRIER and anti-dependencies for MOVE_BARRIER.  */
enum reg_pending_barrier_mode
{
  NOT_A_BARRIER = 0,
  MOVE_BARRIER,
  TRUE_BARRIER
};

/* Whether a register movement is associated with a call.  */
enum post_call_group
{
  not_post_call,
  post_call,
  post_call_initial
};

/* Insns which affect pseudo-registers.  */
struct deps_reg
{
  rtx_insn_list *uses;
  rtx_insn_list *sets;
  rtx_insn_list *implicit_sets;
  rtx_insn_list *control_uses;
  rtx_insn_list *clobbers;
  int uses_length;
  int clobbers_length;
};

/* Describe state of dependencies used during sched_analyze phase.  */
struct deps_desc
{
  /* The *_insns and *_mems are paired lists.  Each pending memory operation
     will have a pointer to the MEM rtx on one list and a pointer to the
     containing insn on the other list in the same place in the list.  */

  /* We can't use add_dependence like the old code did, because a single insn
     may have multiple memory accesses, and hence needs to be on the list
     once for each memory access.  Add_dependence won't let you add an insn
     to a list more than once.  */

  /* An INSN_LIST containing all insns with pending read operations.  */
  rtx_insn_list *pending_read_insns;

  /* An EXPR_LIST containing all MEM rtx's which are pending reads.  */
  rtx_expr_list *pending_read_mems;

  /* An INSN_LIST containing all insns with pending write operations.  */
  rtx_insn_list *pending_write_insns;

  /* An EXPR_LIST containing all MEM rtx's which are pending writes.  */
  rtx_expr_list *pending_write_mems;

  /* An INSN_LIST containing all jump insns.  */
  rtx_insn_list *pending_jump_insns;

  /* We must prevent the above lists from ever growing too large since
     the number of dependencies produced is at least O(N*N),
     and execution time is at least O(4*N*N), as a function of the
     length of these pending lists.  */

  /* Indicates the length of the pending_read list.  */
  int pending_read_list_length;

  /* Indicates the length of the pending_write list.  */
  int pending_write_list_length;

  /* Length of the pending memory flush list plus the length of the pending
     jump insn list.  Large functions with no calls may build up extremely
     large lists.  */
  int pending_flush_length;

  /* The last insn upon which all memory references must depend.
     This is an insn which flushed the pending lists, creating a dependency
     between it and all previously pending memory references.  This creates
     a barrier (or a checkpoint) which no memory reference is allowed to cross.

     This includes all non constant CALL_INSNs.  When we do interprocedural
     alias analysis, this restriction can be relaxed.
     This may also be an INSN that writes memory if the pending lists grow
     too large.  */
  rtx_insn_list *last_pending_memory_flush;

  /* A list of the last function calls we have seen.  We use a list to
     represent last function calls from multiple predecessor blocks.
     Used to prevent register lifetimes from expanding unnecessarily.  */
  rtx_insn_list *last_function_call;

  /* A list of the last function calls that may not return normally
     we have seen.  We use a list to represent last function calls from
     multiple predecessor blocks.  Used to prevent moving trapping insns
     across such calls.  */
  rtx_insn_list *last_function_call_may_noreturn;

  /* A list of insns which use a pseudo register that does not already
     cross a call.  We create dependencies between each of those insn
     and the next call insn, to ensure that they won't cross a call after
     scheduling is done.  */
  rtx_insn_list *sched_before_next_call;

  /* Similarly, a list of insns which should not cross a branch.  */
  rtx_insn_list *sched_before_next_jump;

  /* Used to keep post-call pseudo/hard reg movements together with
     the call.  */
  enum post_call_group in_post_call_group_p;

  /* The last debug insn we've seen.  */
  rtx_insn *last_debug_insn;

  /* The last insn bearing REG_ARGS_SIZE that we've seen.  */
  rtx_insn *last_args_size;

  /* A list of all prologue insns we have seen without intervening epilogue
     insns, and one of all epilogue insns we have seen without intervening
     prologue insns.  This is used to prevent mixing prologue and epilogue
     insns.  See PR78029.  */
  rtx_insn_list *last_prologue;
  rtx_insn_list *last_epilogue;

  /* Whether the last *logue insn was an epilogue insn or a prologue insn
     instead.  */
  bool last_logue_was_epilogue;

  /* The maximum register number for the following arrays.  Before reload
     this is max_reg_num; after reload it is FIRST_PSEUDO_REGISTER.  */
  int max_reg;

  /* Element N is the next insn that sets (hard or pseudo) register
     N within the current basic block; or zero, if there is no
     such insn.  Needed for new registers which may be introduced
     by splitting insns.  */
  struct deps_reg *reg_last;

  /* Element N is set for each register that has any nonzero element
     in reg_last[N].{uses,sets,clobbers}.  */
  regset_head reg_last_in_use;

  /* Shows the last value of reg_pending_barrier associated with the insn.  */
  enum reg_pending_barrier_mode last_reg_pending_barrier;

  /* True when this context should be treated as a readonly by
     the analysis.  */
  BOOL_BITFIELD readonly : 1;
};

typedef struct deps_desc *deps_t;

/* This structure holds some state of the current scheduling pass, and
   contains some function pointers that abstract out some of the non-generic
   functionality from functions such as schedule_block or schedule_insn.
   There is one global variable, current_sched_info, which points to the
   sched_info structure currently in use.  */
struct haifa_sched_info
{
  /* Add all insns that are initially ready to the ready list.  Called once
     before scheduling a set of insns.  */
  void (*init_ready_list) (void);
  /* Called after taking an insn from the ready list.  Returns nonzero if
     this insn can be scheduled, nonzero if we should silently discard it.  */
  int (*can_schedule_ready_p) (rtx_insn *);
  /* Return nonzero if there are more insns that should be scheduled.  */
  int (*schedule_more_p) (void);
  /* Called after an insn has all its hard dependencies resolved.
     Adjusts status of instruction (which is passed through second parameter)
     to indicate if instruction should be moved to the ready list or the
     queue, or if it should silently discard it (until next resolved
     dependence).  */
  ds_t (*new_ready) (rtx_insn *, ds_t);
  /* Compare priority of two insns.  Return a positive number if the second
     insn is to be preferred for scheduling, and a negative one if the first
     is to be preferred.  Zero if they are equally good.  */
  int (*rank) (rtx_insn *, rtx_insn *);
  /* Return a string that contains the insn uid and optionally anything else
     necessary to identify this insn in an output.  It's valid to use a
     static buffer for this.  The ALIGNED parameter should cause the string
     to be formatted so that multiple output lines will line up nicely.  */
  const char *(*print_insn) (const rtx_insn *, int);
  /* Return nonzero if an insn should be included in priority
     calculations.  */
  int (*contributes_to_priority) (rtx_insn *, rtx_insn *);

  /* Return true if scheduling insn (passed as the parameter) will trigger
     finish of scheduling current block.  */
  bool (*insn_finishes_block_p) (rtx_insn *);

  /* The boundaries of the set of insns to be scheduled.  */
  rtx_insn *prev_head, *next_tail;

  /* Filled in after the schedule is finished; the first and last scheduled
     insns.  */
  rtx_insn *head, *tail;

  /* If nonzero, enables an additional sanity check in schedule_block.  */
  unsigned int queue_must_finish_empty:1;

  /* Maximum priority that has been assigned to an insn.  */
  int sched_max_insns_priority;

  /* Hooks to support speculative scheduling.  */

  /* Called to notify frontend that instruction is being added (second
     parameter == 0) or removed (second parameter == 1).  */
  void (*add_remove_insn) (rtx_insn *, int);

  /* Called to notify the frontend that instruction INSN is being
     scheduled.  */
  void (*begin_schedule_ready) (rtx_insn *insn);

  /* Called to notify the frontend that an instruction INSN is about to be
     moved to its correct place in the final schedule.  This is done for all
     insns in order of the schedule.  LAST indicates the last scheduled
     instruction.  */
  void (*begin_move_insn) (rtx_insn *insn, rtx_insn *last);

  /* If the second parameter is not NULL, return nonnull value, if the
     basic block should be advanced.
     If the second parameter is NULL, return the next basic block in EBB.
     The first parameter is the current basic block in EBB.  */
  basic_block (*advance_target_bb) (basic_block, rtx_insn *);

  /* Allocate memory, store the frontend scheduler state in it, and
     return it.  */
  void *(*save_state) (void);
  /* Restore frontend scheduler state from the argument, and free the
     memory.  */
  void (*restore_state) (void *);

  /* ??? FIXME: should use straight bitfields inside sched_info instead of
     this flag field.  */
  unsigned int flags;
};

/* This structure holds description of the properties for speculative
   scheduling.  */
struct spec_info_def
{
  /* Holds types of allowed speculations: BEGIN_{DATA|CONTROL},
     BE_IN_{DATA_CONTROL}.  */
  int mask;

  /* A dump file for additional information on speculative scheduling.  */
  FILE *dump;

  /* Minimal cumulative weakness of speculative instruction's
     dependencies, so that insn will be scheduled.  */
  dw_t data_weakness_cutoff;

  /* Minimal usefulness of speculative instruction to be considered for
     scheduling.  */
  int control_weakness_cutoff;

  /* Flags from the enum SPEC_SCHED_FLAGS.  */
  int flags;
};
typedef struct spec_info_def *spec_info_t;

extern spec_info_t spec_info;

extern struct haifa_sched_info *current_sched_info;

/* Do register pressure sensitive insn scheduling if the flag is set
   up.  */
extern enum sched_pressure_algorithm sched_pressure;

/* Map regno -> its pressure class.  The map defined only when
   SCHED_PRESSURE_P is true.  */
extern enum reg_class *sched_regno_pressure_class;

/* Indexed by INSN_UID, the collection of all data associated with
   a single instruction.  */

struct _haifa_deps_insn_data
{
  /* The number of incoming edges in the forward dependency graph.
     As scheduling proceeds, counts are decreased.  An insn moves to
     the ready queue when its counter reaches zero.  */
  int dep_count;

  /* Nonzero if instruction has internal dependence
     (e.g. add_dependence was invoked with (insn == elem)).  */
  unsigned int has_internal_dep;

  /* NB: We can't place 'struct _deps_list' here instead of deps_list_t into
     h_i_d because when h_i_d extends, addresses of the deps_list->first
     change without updating deps_list->first->next->prev_nextp.  Thus
     BACK_DEPS and RESOLVED_BACK_DEPS are allocated on the heap and FORW_DEPS
     list is allocated on the obstack.  */

  /* A list of hard backward dependencies.  The insn is a consumer of all the
     deps mentioned here.  */
  deps_list_t hard_back_deps;

  /* A list of speculative (weak) dependencies.  The insn is a consumer of all
     the deps mentioned here.  */
  deps_list_t spec_back_deps;

  /* A list of insns which depend on the instruction.  Unlike 'back_deps',
     it represents forward dependencies.  */
  deps_list_t forw_deps;

  /* A list of scheduled producers of the instruction.  Links are being moved
     from 'back_deps' to 'resolved_back_deps' while scheduling.  */
  deps_list_t resolved_back_deps;

  /* A list of scheduled consumers of the instruction.  Links are being moved
     from 'forw_deps' to 'resolved_forw_deps' while scheduling to fasten the
     search in 'forw_deps'.  */
  deps_list_t resolved_forw_deps;

  /* If the insn is conditional (either through COND_EXEC, or because
     it is a conditional branch), this records the condition.  NULL
     for insns that haven't been seen yet or don't have a condition;
     const_true_rtx to mark an insn without a condition, or with a
     condition that has been clobbered by a subsequent insn.  */
  rtx cond;

  /* For a conditional insn, a list of insns that could set the condition
     register.  Used when generating control dependencies.  */
  rtx_insn_list *cond_deps;

  /* True if the condition in 'cond' should be reversed to get the actual
     condition.  */
  unsigned int reverse_cond : 1;

  /* Some insns (e.g. call) are not allowed to move across blocks.  */
  unsigned int cant_move : 1;
};


/* Bits used for storing values of the fields in the following
   structure.  */
#define INCREASE_BITS 8

/* The structure describes how the corresponding insn increases the
   register pressure for each pressure class.  */
struct reg_pressure_data
{
  /* Pressure increase for given class because of clobber.  */
  unsigned int clobber_increase : INCREASE_BITS;
  /* Increase in register pressure for given class because of register
     sets. */
  unsigned int set_increase : INCREASE_BITS;
  /* Pressure increase for given class because of unused register
     set.  */
  unsigned int unused_set_increase : INCREASE_BITS;
  /* Pressure change: #sets - #deaths.  */
  int change : INCREASE_BITS;
};

/* The following structure describes usage of registers by insns.  */
struct reg_use_data
{
  /* Regno used in the insn.  */
  int regno;
  /* Insn using the regno.  */
  rtx_insn *insn;
  /* Cyclic list of elements with the same regno.  */
  struct reg_use_data *next_regno_use;
  /* List of elements with the same insn.  */
  struct reg_use_data *next_insn_use;
};

/* The following structure describes used sets of registers by insns.
   Registers are pseudos whose pressure class is not NO_REGS or hard
   registers available for allocations.  */
struct reg_set_data
{
  /* Regno used in the insn.  */
  int regno;
  /* Insn setting the regno.  */
  rtx insn;
  /* List of elements with the same insn.  */
  struct reg_set_data *next_insn_set;
};

enum autopref_multipass_data_status {
  /* Entry is irrelevant for auto-prefetcher.  */
  AUTOPREF_MULTIPASS_DATA_IRRELEVANT = -2,
  /* Entry is uninitialized.  */
  AUTOPREF_MULTIPASS_DATA_UNINITIALIZED = -1,
  /* Entry is relevant for auto-prefetcher and insn can be delayed
     to allow another insn through.  */
  AUTOPREF_MULTIPASS_DATA_NORMAL = 0,
  /* Entry is relevant for auto-prefetcher, but insn should not be
     delayed as that will break scheduling.  */
  AUTOPREF_MULTIPASS_DATA_DONT_DELAY = 1
};

/* Data for modeling cache auto-prefetcher.  */
struct autopref_multipass_data_
{
  /* Base part of memory address.  */
  rtx base;

  /* Memory offsets from the base.  */
  int offset;

  /* Entry status.  */
  enum autopref_multipass_data_status status;
};
typedef struct autopref_multipass_data_ autopref_multipass_data_def;
typedef autopref_multipass_data_def *autopref_multipass_data_t;

struct _haifa_insn_data
{
  /* We can't place 'struct _deps_list' into h_i_d instead of deps_list_t
     because when h_i_d extends, addresses of the deps_list->first
     change without updating deps_list->first->next->prev_nextp.  */

  /* Logical uid gives the original ordering of the insns.  */
  int luid;

  /* A priority for each insn.  */
  int priority;

  /* The fusion priority for each insn.  */
  int fusion_priority;

  /* The minimum clock tick at which the insn becomes ready.  This is
     used to note timing constraints for the insns in the pending list.  */
  int tick;

  /* For insns that are scheduled at a fixed difference from another,
     this records the tick in which they must be ready.  */
  int exact_tick;

  /* INTER_TICK is used to adjust INSN_TICKs of instructions from the
     subsequent blocks in a region.  */
  int inter_tick;

  /* Used temporarily to estimate an INSN_TICK value for an insn given
     current knowledge.  */
  int tick_estimate;

  /* See comment on QUEUE_INDEX macro in haifa-sched.c.  */
  int queue_index;

  short cost;

  /* '> 0' if priority is valid,
     '== 0' if priority was not yet computed,
     '< 0' if priority in invalid and should be recomputed.  */
  signed char priority_status;

  /* Set if there's DEF-USE dependence between some speculatively
     moved load insn and this one.  */
  unsigned int fed_by_spec_load : 1;
  unsigned int is_load_insn : 1;
  /* Nonzero if this insn has negative-cost forward dependencies against
     an already scheduled insn.  */
  unsigned int feeds_backtrack_insn : 1;

  /* Nonzero if this insn is a shadow of another, scheduled after a fixed
     delay.  We only emit shadows at the end of a cycle, with no other
     real insns following them.  */
  unsigned int shadow_p : 1;

  /* Used internally in unschedule_insns_until to mark insns that must have
     their TODO_SPEC recomputed.  */
  unsigned int must_recompute_spec : 1;

  /* What speculations are necessary to apply to schedule the instruction.  */
  ds_t todo_spec;

  /* What speculations were already applied.  */
  ds_t done_spec;

  /* What speculations are checked by this instruction.  */
  ds_t check_spec;

  /* Recovery block for speculation checks.  */
  basic_block recovery_block;

  /* Original pattern of the instruction.  */
  rtx orig_pat;

  /* For insns with DEP_CONTROL dependencies, the predicated pattern if it
     was ever successfully constructed.  */
  rtx predicated_pat;

  /* The following array contains info how the insn increases register
     pressure.  There is an element for each cover class of pseudos
     referenced in insns.  */
  struct reg_pressure_data *reg_pressure;
  /* The following array contains maximal reg pressure between last
     scheduled insn and given insn.  There is an element for each
     pressure class of pseudos referenced in insns.  This info updated
     after scheduling each insn for each insn between the two
     mentioned insns.  */
  int *max_reg_pressure;
  /* The following list contains info about used pseudos and hard
     registers available for allocation.  */
  struct reg_use_data *reg_use_list;
  /* The following list contains info about set pseudos and hard
     registers available for allocation.  */
  struct reg_set_data *reg_set_list;
  /* Info about how scheduling the insn changes cost of register
     pressure excess (between source and target).  */
  int reg_pressure_excess_cost_change;
  int model_index;

  /* Original order of insns in the ready list.  */
  int rfs_debug_orig_order;

  /* The deciding reason for INSN's place in the ready list.  */
  int last_rfs_win;

  /* Two entries for cache auto-prefetcher model: one for mem reads,
     and one for mem writes.  */
  autopref_multipass_data_def autopref_multipass_data[2];
};

typedef struct _haifa_insn_data haifa_insn_data_def;
typedef haifa_insn_data_def *haifa_insn_data_t;


extern vec<haifa_insn_data_def> h_i_d;

#define HID(INSN) (&h_i_d[INSN_UID (INSN)])

/* Accessor macros for h_i_d.  There are more in haifa-sched.c and
   sched-rgn.c.  */
#define INSN_PRIORITY(INSN) (HID (INSN)->priority)
#define INSN_FUSION_PRIORITY(INSN) (HID (INSN)->fusion_priority)
#define INSN_REG_PRESSURE(INSN) (HID (INSN)->reg_pressure)
#define INSN_MAX_REG_PRESSURE(INSN) (HID (INSN)->max_reg_pressure)
#define INSN_REG_USE_LIST(INSN) (HID (INSN)->reg_use_list)
#define INSN_REG_SET_LIST(INSN) (HID (INSN)->reg_set_list)
#define INSN_REG_PRESSURE_EXCESS_COST_CHANGE(INSN) \
  (HID (INSN)->reg_pressure_excess_cost_change)
#define INSN_PRIORITY_STATUS(INSN) (HID (INSN)->priority_status)
#define INSN_MODEL_INDEX(INSN) (HID (INSN)->model_index)
#define INSN_AUTOPREF_MULTIPASS_DATA(INSN) \
  (HID (INSN)->autopref_multipass_data)

typedef struct _haifa_deps_insn_data haifa_deps_insn_data_def;
typedef haifa_deps_insn_data_def *haifa_deps_insn_data_t;


extern vec<haifa_deps_insn_data_def> h_d_i_d;

#define HDID(INSN) (&h_d_i_d[INSN_LUID (INSN)])
#define INSN_DEP_COUNT(INSN)	(HDID (INSN)->dep_count)
#define HAS_INTERNAL_DEP(INSN)  (HDID (INSN)->has_internal_dep)
#define INSN_FORW_DEPS(INSN) (HDID (INSN)->forw_deps)
#define INSN_RESOLVED_BACK_DEPS(INSN) (HDID (INSN)->resolved_back_deps)
#define INSN_RESOLVED_FORW_DEPS(INSN) (HDID (INSN)->resolved_forw_deps)
#define INSN_HARD_BACK_DEPS(INSN) (HDID (INSN)->hard_back_deps)
#define INSN_SPEC_BACK_DEPS(INSN) (HDID (INSN)->spec_back_deps)
#define INSN_CACHED_COND(INSN)	(HDID (INSN)->cond)
#define INSN_REVERSE_COND(INSN) (HDID (INSN)->reverse_cond)
#define INSN_COND_DEPS(INSN)	(HDID (INSN)->cond_deps)
#define CANT_MOVE(INSN)	(HDID (INSN)->cant_move)
#define CANT_MOVE_BY_LUID(LUID)	(h_d_i_d[LUID].cant_move)


#define INSN_PRIORITY(INSN)	(HID (INSN)->priority)
#define INSN_PRIORITY_STATUS(INSN) (HID (INSN)->priority_status)
#define INSN_PRIORITY_KNOWN(INSN) (INSN_PRIORITY_STATUS (INSN) > 0)
#define TODO_SPEC(INSN) (HID (INSN)->todo_spec)
#define DONE_SPEC(INSN) (HID (INSN)->done_spec)
#define CHECK_SPEC(INSN) (HID (INSN)->check_spec)
#define RECOVERY_BLOCK(INSN) (HID (INSN)->recovery_block)
#define ORIG_PAT(INSN) (HID (INSN)->orig_pat)
#define PREDICATED_PAT(INSN) (HID (INSN)->predicated_pat)

/* INSN is either a simple or a branchy speculation check.  */
#define IS_SPECULATION_CHECK_P(INSN) \
  (sel_sched_p () ? sel_insn_is_speculation_check (INSN) : RECOVERY_BLOCK (INSN) != NULL)

/* INSN is a speculation check that will simply reexecute the speculatively
   scheduled instruction if the speculation fails.  */
#define IS_SPECULATION_SIMPLE_CHECK_P(INSN) \
  (RECOVERY_BLOCK (INSN) == EXIT_BLOCK_PTR_FOR_FN (cfun))

/* INSN is a speculation check that will branch to RECOVERY_BLOCK if the
   speculation fails.  Insns in that block will reexecute the speculatively
   scheduled code and then will return immediately after INSN thus preserving
   semantics of the program.  */
#define IS_SPECULATION_BRANCHY_CHECK_P(INSN) \
  (RECOVERY_BLOCK (INSN) != NULL             \
   && RECOVERY_BLOCK (INSN) != EXIT_BLOCK_PTR_FOR_FN (cfun))


/* Dep status (aka ds_t) of the link encapsulates all information for a given
   dependency, including everything that is needed for speculative scheduling.

   The lay-out of a ds_t is as follows:

   1. Integers corresponding to the probability of the dependence to *not*
      exist.  This is the probability that overcoming this dependence will
      not be followed by execution of the recovery code.  Note that however
      high this probability is, the recovery code should still always be
      generated to preserve semantics of the program.

      The probability values can be set or retrieved using the functions
      the set_dep_weak() and get_dep_weak() in sched-deps.c.  The values
      are always in the range [0, MAX_DEP_WEAK].

	BEGIN_DATA	: BITS_PER_DEP_WEAK
	BE_IN_DATA	: BITS_PER_DEP_WEAK
	BEGIN_CONTROL	: BITS_PER_DEP_WEAK
	BE_IN_CONTROL	: BITS_PER_DEP_WEAK

      The basic type of DS_T is a host int.  For a 32-bits int, the values
      will each take 6 bits.

   2. The type of dependence.  This supercedes the old-style REG_NOTE_KIND
      values.  TODO: Use this field instead of DEP_TYPE, or make DEP_TYPE
      extract the dependence type from here.

	dep_type	:  4 => DEP_{TRUE|OUTPUT|ANTI|CONTROL}

   3. Various flags:

	HARD_DEP	:  1 =>	Set if an instruction has a non-speculative
				dependence.  This is an instruction property
				so this bit can only appear in the TODO_SPEC
				field of an instruction.
	DEP_POSTPONED	:  1 =>	Like HARD_DEP, but the hard dependence may
				still be broken by adjusting the instruction.
	DEP_CANCELLED	:  1 =>	Set if a dependency has been broken using
				some form of speculation.
	RESERVED	:  1 => Reserved for use in the delay slot scheduler.

   See also: check_dep_status () in sched-deps.c .  */

/* The number of bits per weakness probability.  There are 4 weakness types
   and we need 8 bits for other data in a DS_T.  */
#define BITS_PER_DEP_WEAK ((BITS_PER_DEP_STATUS - 8) / 4)

/* Mask of speculative weakness in dep_status.  */
#define DEP_WEAK_MASK ((1 << BITS_PER_DEP_WEAK) - 1)

/* This constant means that dependence is fake with 99.999...% probability.
   This is the maximum value, that can appear in dep_status.
   Note, that we don't want MAX_DEP_WEAK to be the same as DEP_WEAK_MASK for
   debugging reasons.  Though, it can be set to DEP_WEAK_MASK, and, when
   done so, we'll get fast (mul for)/(div by) NO_DEP_WEAK.  */
#define MAX_DEP_WEAK (DEP_WEAK_MASK - 1)

/* This constant means that dependence is 99.999...% real and it is a really
   bad idea to overcome it (though this can be done, preserving program
   semantics).  */
#define MIN_DEP_WEAK 1

/* This constant represents 100% probability.
   E.g. it is used to represent weakness of dependence, that doesn't exist.
   This value never appears in a ds_t, it is only used for computing the
   weakness of a dependence.  */
#define NO_DEP_WEAK (MAX_DEP_WEAK + MIN_DEP_WEAK)

/* Default weakness of speculative dependence.  Used when we can't say
   neither bad nor good about the dependence.  */
#define UNCERTAIN_DEP_WEAK (MAX_DEP_WEAK - MAX_DEP_WEAK / 4)

/* Offset for speculative weaknesses in dep_status.  */
enum SPEC_TYPES_OFFSETS {
  BEGIN_DATA_BITS_OFFSET = 0,
  BE_IN_DATA_BITS_OFFSET = BEGIN_DATA_BITS_OFFSET + BITS_PER_DEP_WEAK,
  BEGIN_CONTROL_BITS_OFFSET = BE_IN_DATA_BITS_OFFSET + BITS_PER_DEP_WEAK,
  BE_IN_CONTROL_BITS_OFFSET = BEGIN_CONTROL_BITS_OFFSET + BITS_PER_DEP_WEAK
};

/* The following defines provide numerous constants used to distinguish
   between different types of speculative dependencies.  They are also
   used as masks to clear/preserve the bits corresponding to the type
   of dependency weakness.  */

/* Dependence can be overcome with generation of new data speculative
   instruction.  */
#define BEGIN_DATA (((ds_t) DEP_WEAK_MASK) << BEGIN_DATA_BITS_OFFSET)

/* This dependence is to the instruction in the recovery block, that was
   formed to recover after data-speculation failure.
   Thus, this dependence can overcome with generating of the copy of
   this instruction in the recovery block.  */
#define BE_IN_DATA (((ds_t) DEP_WEAK_MASK) << BE_IN_DATA_BITS_OFFSET)

/* Dependence can be overcome with generation of new control speculative
   instruction.  */
#define BEGIN_CONTROL (((ds_t) DEP_WEAK_MASK) << BEGIN_CONTROL_BITS_OFFSET)

/* This dependence is to the instruction in the recovery block, that was
   formed to recover after control-speculation failure.
   Thus, this dependence can be overcome with generating of the copy of
   this instruction in the recovery block.  */
#define BE_IN_CONTROL (((ds_t) DEP_WEAK_MASK) << BE_IN_CONTROL_BITS_OFFSET)

/* A few convenient combinations.  */
#define BEGIN_SPEC (BEGIN_DATA | BEGIN_CONTROL)
#define DATA_SPEC (BEGIN_DATA | BE_IN_DATA)
#define CONTROL_SPEC (BEGIN_CONTROL | BE_IN_CONTROL)
#define SPECULATIVE (DATA_SPEC | CONTROL_SPEC)
#define BE_IN_SPEC (BE_IN_DATA | BE_IN_CONTROL)

/* Constants, that are helpful in iterating through dep_status.  */
#define FIRST_SPEC_TYPE BEGIN_DATA
#define LAST_SPEC_TYPE BE_IN_CONTROL
#define SPEC_TYPE_SHIFT BITS_PER_DEP_WEAK

/* Dependence on instruction can be of multiple types
   (e.g. true and output). This fields enhance REG_NOTE_KIND information
   of the dependence.  */
#define DEP_TRUE (((ds_t) 1) << (BE_IN_CONTROL_BITS_OFFSET + BITS_PER_DEP_WEAK))
#define DEP_OUTPUT (DEP_TRUE << 1)
#define DEP_ANTI (DEP_OUTPUT << 1)
#define DEP_CONTROL (DEP_ANTI << 1)

#define DEP_TYPES (DEP_TRUE | DEP_OUTPUT | DEP_ANTI | DEP_CONTROL)

/* Instruction has non-speculative dependence.  This bit represents the
   property of an instruction - not the one of a dependence.
   Therefore, it can appear only in the TODO_SPEC field of an instruction.  */
#define HARD_DEP (DEP_CONTROL << 1)

/* Like HARD_DEP, but dependencies can perhaps be broken by modifying
   the instructions.  This is used for example to change:

   rn++		=>	rm=[rn + 4]
   rm=[rn]		rn++

   For instructions that have this bit set, one of the dependencies of
   the instructions will have a non-NULL REPLACE field in its DEP_T.
   Just like HARD_DEP, this bit is only ever set in TODO_SPEC.  */
#define DEP_POSTPONED (HARD_DEP << 1)

/* Set if a dependency is cancelled via speculation.  */
#define DEP_CANCELLED (DEP_POSTPONED << 1)


/* This represents the results of calling sched-deps.c functions,
   which modify dependencies.  */
enum DEPS_ADJUST_RESULT {
  /* No dependence needed (e.g. producer == consumer).  */
  DEP_NODEP,
  /* Dependence is already present and wasn't modified.  */
  DEP_PRESENT,
  /* Existing dependence was modified to include additional information.  */
  DEP_CHANGED,
  /* New dependence has been created.  */
  DEP_CREATED
};

/* Represents the bits that can be set in the flags field of the
   sched_info structure.  */
enum SCHED_FLAGS {
  /* If set, generate links between instruction as DEPS_LIST.
     Otherwise, generate usual INSN_LIST links.  */
  USE_DEPS_LIST = 1,
  /* Perform data or control (or both) speculation.
     Results in generation of data and control speculative dependencies.
     Requires USE_DEPS_LIST set.  */
  DO_SPECULATION = USE_DEPS_LIST << 1,
  DO_BACKTRACKING = DO_SPECULATION << 1,
  DO_PREDICATION = DO_BACKTRACKING << 1,
  DONT_BREAK_DEPENDENCIES = DO_PREDICATION << 1,
  SCHED_RGN = DONT_BREAK_DEPENDENCIES << 1,
  SCHED_EBB = SCHED_RGN << 1,
  /* Scheduler can possibly create new basic blocks.  Used for assertions.  */
  NEW_BBS = SCHED_EBB << 1,
  SEL_SCHED = NEW_BBS << 1
};

enum SPEC_SCHED_FLAGS {
  COUNT_SPEC_IN_CRITICAL_PATH = 1,
  SEL_SCHED_SPEC_DONT_CHECK_CONTROL = COUNT_SPEC_IN_CRITICAL_PATH << 1
};

#define NOTE_NOT_BB_P(NOTE) (NOTE_P (NOTE) && (NOTE_KIND (NOTE)	\
					       != NOTE_INSN_BASIC_BLOCK))

extern FILE *sched_dump;
extern int sched_verbose;

extern spec_info_t spec_info;
extern bool haifa_recovery_bb_ever_added_p;

/* Exception Free Loads:

   We define five classes of speculative loads: IFREE, IRISKY,
   PFREE, PRISKY, and MFREE.

   IFREE loads are loads that are proved to be exception-free, just
   by examining the load insn.  Examples for such loads are loads
   from TOC and loads of global data.

   IRISKY loads are loads that are proved to be exception-risky,
   just by examining the load insn.  Examples for such loads are
   volatile loads and loads from shared memory.

   PFREE loads are loads for which we can prove, by examining other
   insns, that they are exception-free.  Currently, this class consists
   of loads for which we are able to find a "similar load", either in
   the target block, or, if only one split-block exists, in that split
   block.  Load2 is similar to load1 if both have same single base
   register.  We identify only part of the similar loads, by finding
   an insn upon which both load1 and load2 have a DEF-USE dependence.

   PRISKY loads are loads for which we can prove, by examining other
   insns, that they are exception-risky.  Currently we have two proofs for
   such loads.  The first proof detects loads that are probably guarded by a
   test on the memory address.  This proof is based on the
   backward and forward data dependence information for the region.
   Let load-insn be the examined load.
   Load-insn is PRISKY iff ALL the following hold:

   - insn1 is not in the same block as load-insn
   - there is a DEF-USE dependence chain (insn1, ..., load-insn)
   - test-insn is either a compare or a branch, not in the same block
     as load-insn
   - load-insn is reachable from test-insn
   - there is a DEF-USE dependence chain (insn1, ..., test-insn)

   This proof might fail when the compare and the load are fed
   by an insn not in the region.  To solve this, we will add to this
   group all loads that have no input DEF-USE dependence.

   The second proof detects loads that are directly or indirectly
   fed by a speculative load.  This proof is affected by the
   scheduling process.  We will use the flag  fed_by_spec_load.
   Initially, all insns have this flag reset.  After a speculative
   motion of an insn, if insn is either a load, or marked as
   fed_by_spec_load, we will also mark as fed_by_spec_load every
   insn1 for which a DEF-USE dependence (insn, insn1) exists.  A
   load which is fed_by_spec_load is also PRISKY.

   MFREE (maybe-free) loads are all the remaining loads. They may be
   exception-free, but we cannot prove it.

   Now, all loads in IFREE and PFREE classes are considered
   exception-free, while all loads in IRISKY and PRISKY classes are
   considered exception-risky.  As for loads in the MFREE class,
   these are considered either exception-free or exception-risky,
   depending on whether we are pessimistic or optimistic.  We have
   to take the pessimistic approach to assure the safety of
   speculative scheduling, but we can take the optimistic approach
   by invoking the -fsched_spec_load_dangerous option.  */

enum INSN_TRAP_CLASS
{
  TRAP_FREE = 0, IFREE = 1, PFREE_CANDIDATE = 2,
  PRISKY_CANDIDATE = 3, IRISKY = 4, TRAP_RISKY = 5
};

#define WORST_CLASS(class1, class2) \
((class1 > class2) ? class1 : class2)

#ifndef __GNUC__
#define __inline
#endif

#ifndef HAIFA_INLINE
#define HAIFA_INLINE __inline
#endif

struct sched_deps_info_def
{
  /* Called when computing dependencies for a JUMP_INSN.  This function
     should store the set of registers that must be considered as set by
     the jump in the regset.  */
  void (*compute_jump_reg_dependencies) (rtx, regset);

  /* Start analyzing insn.  */
  void (*start_insn) (rtx_insn *);

  /* Finish analyzing insn.  */
  void (*finish_insn) (void);

  /* Start analyzing insn LHS (Left Hand Side).  */
  void (*start_lhs) (rtx);

  /* Finish analyzing insn LHS.  */
  void (*finish_lhs) (void);

  /* Start analyzing insn RHS (Right Hand Side).  */
  void (*start_rhs) (rtx);

  /* Finish analyzing insn RHS.  */
  void (*finish_rhs) (void);

  /* Note set of the register.  */
  void (*note_reg_set) (int);

  /* Note clobber of the register.  */
  void (*note_reg_clobber) (int);

  /* Note use of the register.  */
  void (*note_reg_use) (int);

  /* Note memory dependence of type DS between MEM1 and MEM2 (which is
     in the INSN2).  */
  void (*note_mem_dep) (rtx mem1, rtx mem2, rtx_insn *insn2, ds_t ds);

  /* Note a dependence of type DS from the INSN.  */
  void (*note_dep) (rtx_insn *, ds_t ds);

  /* Nonzero if we should use cselib for better alias analysis.  This
     must be 0 if the dependency information is used after sched_analyze
     has completed, e.g. if we're using it to initialize state for successor
     blocks in region scheduling.  */
  unsigned int use_cselib : 1;

  /* If set, generate links between instruction as DEPS_LIST.
     Otherwise, generate usual INSN_LIST links.  */
  unsigned int use_deps_list : 1;

  /* Generate data and control speculative dependencies.
     Requires USE_DEPS_LIST set.  */
  unsigned int generate_spec_deps : 1;
};

extern struct sched_deps_info_def *sched_deps_info;


/* Functions in sched-deps.c.  */
extern rtx sched_get_reverse_condition_uncached (const rtx_insn *);
extern bool sched_insns_conditions_mutex_p (const rtx_insn *,
					    const rtx_insn *);
extern bool sched_insn_is_legitimate_for_speculation_p (const rtx_insn *, ds_t);
extern void add_dependence (rtx_insn *, rtx_insn *, enum reg_note);
extern void sched_analyze (struct deps_desc *, rtx_insn *, rtx_insn *);
extern void init_deps (struct deps_desc *, bool);
extern void init_deps_reg_last (struct deps_desc *);
extern void free_deps (struct deps_desc *);
extern void init_deps_global (void);
extern void finish_deps_global (void);
extern void deps_analyze_insn (struct deps_desc *, rtx_insn *);
extern void remove_from_deps (struct deps_desc *, rtx_insn *);
extern void init_insn_reg_pressure_info (rtx_insn *);
extern void get_implicit_reg_pending_clobbers (HARD_REG_SET *, rtx_insn *);

extern dw_t get_dep_weak (ds_t, ds_t);
extern ds_t set_dep_weak (ds_t, ds_t, dw_t);
extern dw_t estimate_dep_weak (rtx, rtx);
extern ds_t ds_merge (ds_t, ds_t);
extern ds_t ds_full_merge (ds_t, ds_t, rtx, rtx);
extern ds_t ds_max_merge (ds_t, ds_t);
extern dw_t ds_weak (ds_t);
extern ds_t ds_get_speculation_types (ds_t);
extern ds_t ds_get_max_dep_weak (ds_t);

extern void sched_deps_init (bool);
extern void sched_deps_finish (void);

extern void haifa_note_reg_set (int);
extern void haifa_note_reg_clobber (int);
extern void haifa_note_reg_use (int);

extern void maybe_extend_reg_info_p (void);

extern void deps_start_bb (struct deps_desc *, rtx_insn *);
extern enum reg_note ds_to_dt (ds_t);

extern bool deps_pools_are_empty_p (void);
extern void sched_free_deps (rtx_insn *, rtx_insn *, bool);
extern void extend_dependency_caches (int, bool);

extern void debug_ds (ds_t);


/* Functions in haifa-sched.c.  */
extern void initialize_live_range_shrinkage (void);
extern void finish_live_range_shrinkage (void);
extern void sched_init_region_reg_pressure_info (void);
extern void free_global_sched_pressure_data (void);
extern int haifa_classify_insn (const_rtx);
extern void get_ebb_head_tail (basic_block, basic_block,
			       rtx_insn **, rtx_insn **);
extern int no_real_insns_p (const rtx_insn *, const rtx_insn *);

extern int insn_sched_cost (rtx_insn *);
extern int dep_cost_1 (dep_t, dw_t);
extern int dep_cost (dep_t);
extern int set_priorities (rtx_insn *, rtx_insn *);

extern void sched_setup_bb_reg_pressure_info (basic_block, rtx_insn *);
extern bool schedule_block (basic_block *, state_t);

extern int cycle_issued_insns;
extern int issue_rate;
extern int dfa_lookahead;

extern int autopref_multipass_dfa_lookahead_guard (rtx_insn *, int);

extern rtx_insn *ready_element (struct ready_list *, int);
extern rtx_insn **ready_lastpos (struct ready_list *);

extern int try_ready (rtx_insn *);
extern void sched_extend_ready_list (int);
extern void sched_finish_ready_list (void);
extern void sched_change_pattern (rtx, rtx);
extern int sched_speculate_insn (rtx_insn *, ds_t, rtx *);
extern void unlink_bb_notes (basic_block, basic_block);
extern void add_block (basic_block, basic_block);
extern rtx_note *bb_note (basic_block);
extern void concat_note_lists (rtx_insn *, rtx_insn **);
extern rtx_insn *sched_emit_insn (rtx);
extern rtx_insn *get_ready_element (int);
extern int number_in_ready (void);

/* Types and functions in sched-ebb.c.  */

extern basic_block schedule_ebb (rtx_insn *, rtx_insn *, bool);
extern void schedule_ebbs_init (void);
extern void schedule_ebbs_finish (void);

/* Types and functions in sched-rgn.c.  */

/* A region is the main entity for interblock scheduling: insns
   are allowed to move between blocks in the same region, along
   control flow graph edges, in the 'up' direction.  */
struct region
{
  /* Number of extended basic blocks in region.  */
  int rgn_nr_blocks;
  /* cblocks in the region (actually index in rgn_bb_table).  */
  int rgn_blocks;
  /* Dependencies for this region are already computed.  Basically, indicates,
     that this is a recovery block.  */
  unsigned int dont_calc_deps : 1;
  /* This region has at least one non-trivial ebb.  */
  unsigned int has_real_ebb : 1;
};

extern int nr_regions;
extern region *rgn_table;
extern int *rgn_bb_table;
extern int *block_to_bb;
extern int *containing_rgn;

/* Often used short-hand in the scheduler.  The rest of the compiler uses
   BLOCK_FOR_INSN(INSN) and an indirect reference to get the basic block
   number ("index").  For historical reasons, the scheduler does not.  */
#define BLOCK_NUM(INSN)	      (BLOCK_FOR_INSN (INSN)->index + 0)

#define RGN_NR_BLOCKS(rgn) (rgn_table[rgn].rgn_nr_blocks)
#define RGN_BLOCKS(rgn) (rgn_table[rgn].rgn_blocks)
#define RGN_DONT_CALC_DEPS(rgn) (rgn_table[rgn].dont_calc_deps)
#define RGN_HAS_REAL_EBB(rgn) (rgn_table[rgn].has_real_ebb)
#define BLOCK_TO_BB(block) (block_to_bb[block])
#define CONTAINING_RGN(block) (containing_rgn[block])

/* The mapping from ebb to block.  */
extern int *ebb_head;
#define BB_TO_BLOCK(ebb) (rgn_bb_table[ebb_head[ebb]])
#define EBB_FIRST_BB(ebb) BASIC_BLOCK_FOR_FN (cfun, BB_TO_BLOCK (ebb))
#define EBB_LAST_BB(ebb) \
  BASIC_BLOCK_FOR_FN (cfun, rgn_bb_table[ebb_head[ebb + 1] - 1])
#define INSN_BB(INSN) (BLOCK_TO_BB (BLOCK_NUM (INSN)))

extern int current_nr_blocks;
extern int current_blocks;
extern int target_bb;
extern bool sched_no_dce;

extern void set_modulo_params (int, int, int, int);
extern void record_delay_slot_pair (rtx_insn *, rtx_insn *, int, int);
extern rtx_insn *real_insn_for_shadow (rtx_insn *);
extern void discard_delay_pairs_above (int);
extern void free_delay_pairs (void);
extern void add_delay_dependencies (rtx_insn *);
extern bool sched_is_disabled_for_current_region_p (void);
extern void sched_rgn_init (bool);
extern void sched_rgn_finish (void);
extern void rgn_setup_region (int);
extern void sched_rgn_compute_dependencies (int);
extern void sched_rgn_local_init (int);
extern void sched_rgn_local_finish (void);
extern void sched_rgn_local_free (void);
extern void extend_regions (void);
extern void rgn_make_new_region_out_of_new_block (basic_block);

extern void compute_priorities (void);
extern void increase_insn_priority (rtx_insn *, int);
extern void debug_rgn_dependencies (int);
extern void debug_dependencies (rtx_insn *, rtx_insn *);
extern void dump_rgn_dependencies_dot (FILE *);
extern void dump_rgn_dependencies_dot (const char *);

extern void free_rgn_deps (void);
extern int contributes_to_priority (rtx_insn *, rtx_insn *);
extern void extend_rgns (int *, int *, sbitmap, int *);
extern void deps_join (struct deps_desc *, struct deps_desc *);

extern void rgn_setup_common_sched_info (void);
extern void rgn_setup_sched_infos (void);

extern void debug_regions (void);
extern void debug_region (int);
extern void dump_region_dot (FILE *, int);
extern void dump_region_dot_file (const char *, int);

extern void haifa_sched_init (void);
extern void haifa_sched_finish (void);

extern void find_modifiable_mems (rtx_insn *, rtx_insn *);

/* sched-deps.c interface to walk, add, search, update, resolve, delete
   and debug instruction dependencies.  */

/* Constants defining dependences lists.  */

/* No list.  */
#define SD_LIST_NONE (0)

/* hard_back_deps.  */
#define SD_LIST_HARD_BACK (1)

/* spec_back_deps.  */
#define SD_LIST_SPEC_BACK (2)

/* forw_deps.  */
#define SD_LIST_FORW (4)

/* resolved_back_deps.  */
#define SD_LIST_RES_BACK (8)

/* resolved_forw_deps.  */
#define SD_LIST_RES_FORW (16)

#define SD_LIST_BACK (SD_LIST_HARD_BACK | SD_LIST_SPEC_BACK)

/* A type to hold above flags.  */
typedef int sd_list_types_def;

extern void sd_next_list (const_rtx, sd_list_types_def *, deps_list_t *, bool *);

/* Iterator to walk through, resolve and delete dependencies.  */
struct _sd_iterator
{
  /* What lists to walk.  Can be any combination of SD_LIST_* flags.  */
  sd_list_types_def types;

  /* Instruction dependencies lists of which will be walked.  */
  rtx insn;

  /* Pointer to the next field of the previous element.  This is not
     simply a pointer to the next element to allow easy deletion from the
     list.  When a dep is being removed from the list the iterator
     will automatically advance because the value in *linkp will start
     referring to the next element.  */
  dep_link_t *linkp;

  /* True if the current list is a resolved one.  */
  bool resolved_p;
};

typedef struct _sd_iterator sd_iterator_def;

/* ??? We can move some definitions that are used in below inline functions
   out of sched-int.h to sched-deps.c provided that the below functions will
   become global externals.
   These definitions include:
   * struct _deps_list: opaque pointer is needed at global scope.
   * struct _dep_link: opaque pointer is needed at scope of sd_iterator_def.
   * struct _dep_node: opaque pointer is needed at scope of
   struct _deps_link.  */

/* Return initialized iterator.  */
static inline sd_iterator_def
sd_iterator_start (rtx insn, sd_list_types_def types)
{
  /* Some dep_link a pointer to which will return NULL.  */
  static dep_link_t null_link = NULL;

  sd_iterator_def i;

  i.types = types;
  i.insn = insn;
  i.linkp = &null_link;

  /* Avoid 'uninitialized warning'.  */
  i.resolved_p = false;

  return i;
}

/* Return the current element.  */
static inline bool
sd_iterator_cond (sd_iterator_def *it_ptr, dep_t *dep_ptr)
{
  while (true)
    {
      dep_link_t link = *it_ptr->linkp;

      if (link != NULL)
	{
	  *dep_ptr = DEP_LINK_DEP (link);
	  return true;
	}
      else
	{
	  sd_list_types_def types = it_ptr->types;

	  if (types != SD_LIST_NONE)
	    /* Switch to next list.  */
	    {
	      deps_list_t list;

	      sd_next_list (it_ptr->insn,
			    &it_ptr->types, &list, &it_ptr->resolved_p);

	      if (list)
		{
		  it_ptr->linkp = &DEPS_LIST_FIRST (list);
		  continue;
		}
	    }

	  *dep_ptr = NULL;
	  return false;
	}
   }
}

/* Advance iterator.  */
static inline void
sd_iterator_next (sd_iterator_def *it_ptr)
{
  it_ptr->linkp = &DEP_LINK_NEXT (*it_ptr->linkp);
}

/* A cycle wrapper.  */
#define FOR_EACH_DEP(INSN, LIST_TYPES, ITER, DEP)		\
  for ((ITER) = sd_iterator_start ((INSN), (LIST_TYPES));	\
       sd_iterator_cond (&(ITER), &(DEP));			\
       sd_iterator_next (&(ITER)))

#define IS_DISPATCH_ON 1
#define IS_CMP 2
#define DISPATCH_VIOLATION 3
#define FITS_DISPATCH_WINDOW 4
#define DISPATCH_INIT 5
#define ADD_TO_DISPATCH_WINDOW 6

extern int sd_lists_size (const_rtx, sd_list_types_def);
extern bool sd_lists_empty_p (const_rtx, sd_list_types_def);
extern void sd_init_insn (rtx_insn *);
extern void sd_finish_insn (rtx_insn *);
extern dep_t sd_find_dep_between (rtx, rtx, bool);
extern void sd_add_dep (dep_t, bool);
extern enum DEPS_ADJUST_RESULT sd_add_or_update_dep (dep_t, bool);
extern void sd_resolve_dep (sd_iterator_def);
extern void sd_unresolve_dep (sd_iterator_def);
extern void sd_copy_back_deps (rtx_insn *, rtx_insn *, bool);
extern void sd_delete_dep (sd_iterator_def);
extern void sd_debug_lists (rtx, sd_list_types_def);

/* Macros and declarations for scheduling fusion.  */
#define FUSION_MAX_PRIORITY (INT_MAX)
extern bool sched_fusion;

#endif /* INSN_SCHEDULING */

#endif /* GCC_SCHED_INT_H */

