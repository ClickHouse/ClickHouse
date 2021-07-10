/* Interprocedural analyses.
   Copyright (C) 2005-2018 Free Software Foundation, Inc.

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

#ifndef IPA_PROP_H
#define IPA_PROP_H

/* The following definitions and interfaces are used by
   interprocedural analyses or parameters.  */

#define IPA_UNDESCRIBED_USE -1

/* ipa-prop.c stuff (ipa-cp, indirect inlining):  */

/* A jump function for a callsite represents the values passed as actual
   arguments of the callsite.  They were originally proposed in a paper called
   "Interprocedural Constant Propagation", by David Callahan, Keith D Cooper,
   Ken Kennedy, Linda Torczon in Comp86, pg 152-161.  There are three main
   types of values :

   Pass-through - the caller's formal parameter is passed as an actual
                  argument, possibly one simple operation performed on it.
   Constant     - a constant (is_gimple_ip_invariant)is passed as an actual
                  argument.
   Unknown      - neither of the above.

   IPA_JF_ANCESTOR is a special pass-through jump function, which means that
   the result is an address of a part of the object pointed to by the formal
   parameter to which the function refers.  It is mainly intended to represent
   getting addresses of ancestor fields in C++
   (e.g. &this_1(D)->D.1766.D.1756).  Note that if the original pointer is
   NULL, ancestor jump function must behave like a simple pass-through.

   Other pass-through functions can either simply pass on an unchanged formal
   parameter or can apply one simple binary operation to it (such jump
   functions are called polynomial).

   Jump functions are computed in ipa-prop.c by function
   update_call_notes_after_inlining.  Some information can be lost and jump
   functions degraded accordingly when inlining, see
   update_call_notes_after_inlining in the same file.  */

enum jump_func_type
{
  IPA_JF_UNKNOWN = 0,  /* newly allocated and zeroed jump functions default */
  IPA_JF_CONST,             /* represented by field costant */
  IPA_JF_PASS_THROUGH,	    /* represented by field pass_through */
  IPA_JF_ANCESTOR	    /* represented by field ancestor */
};

struct ipa_cst_ref_desc;

/* Structure holding data required to describe a constant jump function.  */
struct GTY(()) ipa_constant_data
{
  /* THe value of the constant.  */
  tree value;
  /* Pointer to the structure that describes the reference.  */
  struct ipa_cst_ref_desc GTY((skip)) *rdesc;
};

/* Structure holding data required to describe a pass-through jump function.  */

struct GTY(()) ipa_pass_through_data
{
  /* If an operation is to be performed on the original parameter, this is the
     second (constant) operand.  */
  tree operand;
  /* Number of the caller's formal parameter being passed.  */
  int formal_id;
  /* Operation that is performed on the argument before it is passed on.
     NOP_EXPR means no operation.  Otherwise oper must be a simple binary
     arithmetic operation where the caller's parameter is the first operand and
     operand field from this structure is the second one.  */
  enum tree_code operation;
  /* When the passed value is a pointer, it is set to true only when we are
     certain that no write to the object it points to has occurred since the
     caller functions started execution, except for changes noted in the
     aggregate part of the jump function (see description of
     ipa_agg_jump_function).  The flag is used only when the operation is
     NOP_EXPR.  */
  unsigned agg_preserved : 1;
};

/* Structure holding data required to describe an ancestor pass-through
   jump function.  */

struct GTY(()) ipa_ancestor_jf_data
{
  /* Offset of the field representing the ancestor.  */
  HOST_WIDE_INT offset;
  /* Number of the caller's formal parameter being passed.  */
  int formal_id;
  /* Flag with the same meaning like agg_preserve in ipa_pass_through_data.  */
  unsigned agg_preserved : 1;
};

/* An element in an aggegate part of a jump function describing a known value
   at a given offset.  When it is part of a pass-through jump function with
   agg_preserved set or an ancestor jump function with agg_preserved set, all
   unlisted positions are assumed to be preserved but the value can be a type
   node, which means that the particular piece (starting at offset and having
   the size of the type) is clobbered with an unknown value.  When
   agg_preserved is false or the type of the containing jump function is
   different, all unlisted parts are assumed to be unknown and all values must
   fulfill is_gimple_ip_invariant.  */

struct GTY(()) ipa_agg_jf_item
{
  /* The offset at which the known value is located within the aggregate.  */
  HOST_WIDE_INT offset;

  /* The known constant or type if this is a clobber.  */
  tree value;
};


/* Aggregate jump function - i.e. description of contents of aggregates passed
   either by reference or value.  */

struct GTY(()) ipa_agg_jump_function
{
  /* Description of the individual items.  */
  vec<ipa_agg_jf_item, va_gc> *items;
  /* True if the data was passed by reference (as opposed to by value). */
  bool by_ref;
};

typedef struct ipa_agg_jump_function *ipa_agg_jump_function_p;

/* Information about zero/non-zero bits.  */
struct GTY(()) ipa_bits
{
  /* The propagated value.  */
  widest_int value;
  /* Mask corresponding to the value.
     Similar to ccp_lattice_t, if xth bit of mask is 0,
     implies xth bit of value is constant.  */
  widest_int mask;
};

/* Info about value ranges.  */

struct GTY(()) ipa_vr
{
  /* The data fields below are valid only if known is true.  */
  bool known;
  enum value_range_type type;
  wide_int min;
  wide_int max;
};

/* A jump function for a callsite represents the values passed as actual
   arguments of the callsite. See enum jump_func_type for the various
   types of jump functions supported.  */
struct GTY (()) ipa_jump_func
{
  /* Aggregate contants description.  See struct ipa_agg_jump_function and its
     description.  */
  struct ipa_agg_jump_function agg;

  /* Information about zero/non-zero bits.  The pointed to structure is shared
     betweed different jump functions.  Use ipa_set_jfunc_bits to set this
     field.  */
  struct ipa_bits *bits;

  /* Information about value range, containing valid data only when vr_known is
     true.  The pointed to structure is shared betweed different jump
     functions.  Use ipa_set_jfunc_vr to set this field.  */
  struct value_range *m_vr;

  enum jump_func_type type;
  /* Represents a value of a jump function.  pass_through is used only in jump
     function context.  constant represents the actual constant in constant jump
     functions and member_cst holds constant c++ member functions.  */
  union jump_func_value
  {
    struct ipa_constant_data GTY ((tag ("IPA_JF_CONST"))) constant;
    struct ipa_pass_through_data GTY ((tag ("IPA_JF_PASS_THROUGH"))) pass_through;
    struct ipa_ancestor_jf_data GTY ((tag ("IPA_JF_ANCESTOR"))) ancestor;
  } GTY ((desc ("%1.type"))) value;
};


/* Return the constant stored in a constant jump functin JFUNC.  */

static inline tree
ipa_get_jf_constant (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_CONST);
  return jfunc->value.constant.value;
}

static inline struct ipa_cst_ref_desc *
ipa_get_jf_constant_rdesc (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_CONST);
  return jfunc->value.constant.rdesc;
}

/* Return the operand of a pass through jmp function JFUNC.  */

static inline tree
ipa_get_jf_pass_through_operand (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_PASS_THROUGH);
  return jfunc->value.pass_through.operand;
}

/* Return the number of the caller's formal parameter that a pass through jump
   function JFUNC refers to.  */

static inline int
ipa_get_jf_pass_through_formal_id (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_PASS_THROUGH);
  return jfunc->value.pass_through.formal_id;
}

/* Return operation of a pass through jump function JFUNC.  */

static inline enum tree_code
ipa_get_jf_pass_through_operation (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_PASS_THROUGH);
  return jfunc->value.pass_through.operation;
}

/* Return the agg_preserved flag of a pass through jump function JFUNC.  */

static inline bool
ipa_get_jf_pass_through_agg_preserved (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_PASS_THROUGH);
  return jfunc->value.pass_through.agg_preserved;
}

/* Return true if pass through jump function JFUNC preserves type
   information.  */

static inline bool
ipa_get_jf_pass_through_type_preserved (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_PASS_THROUGH);
  return jfunc->value.pass_through.agg_preserved;
}

/* Return the offset of an ancestor jump function JFUNC.  */

static inline HOST_WIDE_INT
ipa_get_jf_ancestor_offset (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_ANCESTOR);
  return jfunc->value.ancestor.offset;
}

/* Return the number of the caller's formal parameter that an ancestor jump
   function JFUNC refers to.  */

static inline int
ipa_get_jf_ancestor_formal_id (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_ANCESTOR);
  return jfunc->value.ancestor.formal_id;
}

/* Return the agg_preserved flag of an ancestor jump function JFUNC.  */

static inline bool
ipa_get_jf_ancestor_agg_preserved (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_ANCESTOR);
  return jfunc->value.ancestor.agg_preserved;
}

/* Return true if ancestor jump function JFUNC presrves type information.  */

static inline bool
ipa_get_jf_ancestor_type_preserved (struct ipa_jump_func *jfunc)
{
  gcc_checking_assert (jfunc->type == IPA_JF_ANCESTOR);
  return jfunc->value.ancestor.agg_preserved;
}

/* Summary describing a single formal parameter.  */

struct GTY(()) ipa_param_descriptor
{
  /* In analysis and modification phase, this is the PARAM_DECL of this
     parameter, in IPA LTO phase, this is the type of the the described
     parameter or NULL if not known.  Do not read this field directly but
     through ipa_get_param and ipa_get_type as appropriate.  */
  tree decl_or_type;
  /* If all uses of the parameter are described by ipa-prop structures, this
     says how many there are.  If any use could not be described by means of
     ipa-prop structures, this is IPA_UNDESCRIBED_USE.  */
  int controlled_uses;
  unsigned int move_cost : 31;
  /* The parameter is used.  */
  unsigned used : 1;
};

/* ipa_node_params stores information related to formal parameters of functions
   and some other information for interprocedural passes that operate on
   parameters (such as ipa-cp).  */

struct GTY((for_user)) ipa_node_params
{
  /* Default constructor.  */
  ipa_node_params ();

  /* Default destructor.  */
  ~ipa_node_params ();

  /* Information about individual formal parameters that are gathered when
     summaries are generated. */
  vec<ipa_param_descriptor, va_gc> *descriptors;
  /* Pointer to an array of structures describing individual formal
     parameters.  */
  struct ipcp_param_lattices * GTY((skip)) lattices;
  /* Only for versioned nodes this field would not be NULL,
     it points to the node that IPA cp cloned from.  */
  struct cgraph_node * GTY((skip)) ipcp_orig_node;
  /* If this node is an ipa-cp clone, these are the known constants that
     describe what it has been specialized for.  */
  vec<tree> GTY((skip)) known_csts;
  /* If this node is an ipa-cp clone, these are the known polymorphic contexts
     that describe what it has been specialized for.  */
  vec<ipa_polymorphic_call_context> GTY((skip)) known_contexts;
  /* Whether the param uses analysis and jump function computation has already
     been performed.  */
  unsigned analysis_done : 1;
  /* Whether the function is enqueued in ipa-cp propagation stack.  */
  unsigned node_enqueued : 1;
  /* Whether we should create a specialized version based on values that are
     known to be constant in all contexts.  */
  unsigned do_clone_for_all_contexts : 1;
  /* Set if this is an IPA-CP clone for all contexts.  */
  unsigned is_all_contexts_clone : 1;
  /* Node has been completely replaced by clones and will be removed after
     ipa-cp is finished.  */
  unsigned node_dead : 1;
  /* Node is involved in a recursion, potentionally indirect.  */
  unsigned node_within_scc : 1;
  /* Node is calling a private function called only once.  */
  unsigned node_calling_single_call : 1;
  /* False when there is something makes versioning impossible.  */
  unsigned versionable : 1;
};

inline
ipa_node_params::ipa_node_params ()
: descriptors (NULL), lattices (NULL), ipcp_orig_node (NULL),
  known_csts (vNULL), known_contexts (vNULL), analysis_done (0),
  node_enqueued (0), do_clone_for_all_contexts (0), is_all_contexts_clone (0),
  node_dead (0), node_within_scc (0), node_calling_single_call (0),
  versionable (0)
{
}

inline
ipa_node_params::~ipa_node_params ()
{
  free (lattices);
  known_csts.release ();
  known_contexts.release ();
}

/* Intermediate information that we get from alias analysis about a particular
   parameter in a particular basic_block.  When a parameter or the memory it
   references is marked modified, we use that information in all dominated
   blocks without consulting alias analysis oracle.  */

struct ipa_param_aa_status
{
  /* Set when this structure contains meaningful information.  If not, the
     structure describing a dominating BB should be used instead.  */
  bool valid;

  /* Whether we have seen something which might have modified the data in
     question.  PARM is for the parameter itself, REF is for data it points to
     but using the alias type of individual accesses and PT is the same thing
     but for computing aggregate pass-through functions using a very inclusive
     ao_ref.  */
  bool parm_modified, ref_modified, pt_modified;
};

/* Information related to a given BB that used only when looking at function
   body.  */

struct ipa_bb_info
{
  /* Call graph edges going out of this BB.  */
  vec<cgraph_edge *> cg_edges;
  /* Alias analysis statuses of each formal parameter at this bb.  */
  vec<ipa_param_aa_status> param_aa_statuses;
};

/* Structure with global information that is only used when looking at function
   body. */

struct ipa_func_body_info
{
  /* The node that is being analyzed.  */
  cgraph_node *node;

  /* Its info.  */
  struct ipa_node_params *info;

  /* Information about individual BBs. */
  vec<ipa_bb_info> bb_infos;

  /* Number of parameters.  */
  int param_count;

  /* Number of statements already walked by when analyzing this function.  */
  unsigned int aa_walked;
};

/* ipa_node_params access functions.  Please use these to access fields that
   are or will be shared among various passes.  */

/* Return the number of formal parameters. */

static inline int
ipa_get_param_count (struct ipa_node_params *info)
{
  return vec_safe_length (info->descriptors);
}

/* Return the declaration of Ith formal parameter of the function corresponding
   to INFO.  Note there is no setter function as this array is built just once
   using ipa_initialize_node_params.  This function should not be called in
   WPA.  */

static inline tree
ipa_get_param (struct ipa_node_params *info, int i)
{
  gcc_checking_assert (info->descriptors);
  gcc_checking_assert (!flag_wpa);
  tree t = (*info->descriptors)[i].decl_or_type;
  gcc_checking_assert (TREE_CODE (t) == PARM_DECL);
  return t;
}

/* Return the type of Ith formal parameter of the function corresponding
   to INFO if it is known or NULL if not.  */

static inline tree
ipa_get_type (struct ipa_node_params *info, int i)
{
  if (vec_safe_length (info->descriptors) <= (unsigned) i)
    return NULL;
  tree t = (*info->descriptors)[i].decl_or_type;
  if (!t)
    return NULL;
  if (TYPE_P (t))
    return t;
  gcc_checking_assert (TREE_CODE (t) == PARM_DECL);
  return TREE_TYPE (t);
}

/* Return the move cost of Ith formal parameter of the function corresponding
   to INFO.  */

static inline int
ipa_get_param_move_cost (struct ipa_node_params *info, int i)
{
  gcc_checking_assert (info->descriptors);
  return (*info->descriptors)[i].move_cost;
}

/* Set the used flag corresponding to the Ith formal parameter of the function
   associated with INFO to VAL.  */

static inline void
ipa_set_param_used (struct ipa_node_params *info, int i, bool val)
{
  gcc_checking_assert (info->descriptors);
  (*info->descriptors)[i].used = val;
}

/* Return how many uses described by ipa-prop a parameter has or
   IPA_UNDESCRIBED_USE if there is a use that is not described by these
   structures.  */
static inline int
ipa_get_controlled_uses (struct ipa_node_params *info, int i)
{
  /* FIXME: introducing speculation causes out of bounds access here.  */
  if (vec_safe_length (info->descriptors) > (unsigned)i)
    return (*info->descriptors)[i].controlled_uses;
  return IPA_UNDESCRIBED_USE;
}

/* Set the controlled counter of a given parameter.  */

static inline void
ipa_set_controlled_uses (struct ipa_node_params *info, int i, int val)
{
  gcc_checking_assert (info->descriptors);
  (*info->descriptors)[i].controlled_uses = val;
}

/* Return the used flag corresponding to the Ith formal parameter of the
   function associated with INFO.  */

static inline bool
ipa_is_param_used (struct ipa_node_params *info, int i)
{
  gcc_checking_assert (info->descriptors);
  return (*info->descriptors)[i].used;
}

/* Information about replacements done in aggregates for a given node (each
   node has its linked list).  */
struct GTY(()) ipa_agg_replacement_value
{
  /* Next item in the linked list.  */
  struct ipa_agg_replacement_value *next;
  /* Offset within the aggregate.  */
  HOST_WIDE_INT offset;
  /* The constant value.  */
  tree value;
  /* The paramter index.  */
  int index;
  /* Whether the value was passed by reference.  */
  bool by_ref;
};

/* Structure holding information for the transformation phase of IPA-CP.  */

struct GTY(()) ipcp_transformation_summary
{
  /* Linked list of known aggregate values.  */
  ipa_agg_replacement_value *agg_values;
  /* Known bits information.  */
  vec<ipa_bits *, va_gc> *bits;
  /* Value range information.  */
  vec<ipa_vr, va_gc> *m_vr;
};

void ipa_set_node_agg_value_chain (struct cgraph_node *node,
				   struct ipa_agg_replacement_value *aggvals);
void ipcp_grow_transformations_if_necessary (void);

/* ipa_edge_args stores information related to a callsite and particularly its
   arguments.  It can be accessed by the IPA_EDGE_REF macro.  */

class GTY((for_user)) ipa_edge_args
{
 public:

  /* Default constructor.  */
  ipa_edge_args () : jump_functions (NULL), polymorphic_call_contexts (NULL)
    {}

  /* Destructor.  */
  ~ipa_edge_args ()
    {
      vec_free (jump_functions);
      vec_free (polymorphic_call_contexts);
    }

  /* Vectors of the callsite's jump function and polymorphic context
     information of each parameter.  */
  vec<ipa_jump_func, va_gc> *jump_functions;
  vec<ipa_polymorphic_call_context, va_gc> *polymorphic_call_contexts;
};

/* ipa_edge_args access functions.  Please use these to access fields that
   are or will be shared among various passes.  */

/* Return the number of actual arguments. */

static inline int
ipa_get_cs_argument_count (struct ipa_edge_args *args)
{
  return vec_safe_length (args->jump_functions);
}

/* Returns a pointer to the jump function for the ith argument.  Please note
   there is no setter function as jump functions are all set up in
   ipa_compute_jump_functions. */

static inline struct ipa_jump_func *
ipa_get_ith_jump_func (struct ipa_edge_args *args, int i)
{
  return &(*args->jump_functions)[i];
}

/* Returns a pointer to the polymorphic call context for the ith argument.
   NULL if contexts are not computed.  */
static inline struct ipa_polymorphic_call_context *
ipa_get_ith_polymorhic_call_context (struct ipa_edge_args *args, int i)
{
  if (!args->polymorphic_call_contexts)
    return NULL;
  return &(*args->polymorphic_call_contexts)[i];
}

/* Function summary for ipa_node_params.  */
class GTY((user)) ipa_node_params_t: public function_summary <ipa_node_params *>
{
public:
  ipa_node_params_t (symbol_table *table, bool ggc):
    function_summary<ipa_node_params *> (table, ggc) { }

  /* Hook that is called by summary when a node is duplicated.  */
  virtual void duplicate (cgraph_node *node,
			  cgraph_node *node2,
			  ipa_node_params *data,
			  ipa_node_params *data2);
};

/* Summary to manange ipa_edge_args structures.  */

class GTY((user)) ipa_edge_args_sum_t : public call_summary <ipa_edge_args *>
{
 public:
  ipa_edge_args_sum_t (symbol_table *table, bool ggc)
    : call_summary<ipa_edge_args *> (table, ggc) { }

  /* Hook that is called by summary when an edge is duplicated.  */
  virtual void remove (cgraph_edge *cs, ipa_edge_args *args);
  /* Hook that is called by summary when an edge is duplicated.  */
  virtual void duplicate (cgraph_edge *src,
			  cgraph_edge *dst,
			  ipa_edge_args *old_args,
			  ipa_edge_args *new_args);
};

/* Function summary where the parameter infos are actually stored. */
extern GTY(()) ipa_node_params_t * ipa_node_params_sum;
/* Call summary to store information about edges such as jump functions.  */
extern GTY(()) ipa_edge_args_sum_t *ipa_edge_args_sum;

/* Vector of IPA-CP transformation data for each clone.  */
extern GTY(()) vec<ipcp_transformation_summary, va_gc> *ipcp_transformations;

/* Return the associated parameter/argument info corresponding to the given
   node/edge.  */
#define IPA_NODE_REF(NODE) (ipa_node_params_sum->get (NODE))
#define IPA_EDGE_REF(EDGE) (ipa_edge_args_sum->get (EDGE))
/* This macro checks validity of index returned by
   ipa_get_param_decl_index function.  */
#define IS_VALID_JUMP_FUNC_INDEX(I) ((I) != -1)

/* Creating and freeing ipa_node_params and ipa_edge_args.  */
void ipa_create_all_node_params (void);
void ipa_create_all_edge_args (void);
void ipa_check_create_edge_args (void);
void ipa_free_edge_args_substructures (struct ipa_edge_args *);
void ipa_free_all_node_params (void);
void ipa_free_all_edge_args (void);
void ipa_free_all_structures_after_ipa_cp (void);
void ipa_free_all_structures_after_iinln (void);

void ipa_register_cgraph_hooks (void);
int count_formal_params (tree fndecl);

/* This function ensures the array of node param infos is big enough to
   accommodate a structure for all nodes and reallocates it if not.  */

static inline void
ipa_check_create_node_params (void)
{
  if (!ipa_node_params_sum)
    ipa_node_params_sum
      = (new (ggc_cleared_alloc <ipa_node_params_t> ())
	 ipa_node_params_t (symtab, true));
}

/* Returns true if edge summary contains a record for EDGE.  The main purpose
   of this function is that debug dumping function can check info availability
   without causing allocations.  */

static inline bool
ipa_edge_args_info_available_for_edge_p (struct cgraph_edge *edge)
{
  return ipa_edge_args_sum->exists (edge);
}

static inline ipcp_transformation_summary *
ipcp_get_transformation_summary (cgraph_node *node)
{
  if ((unsigned) node->uid >= vec_safe_length (ipcp_transformations))
    return NULL;
  return &(*ipcp_transformations)[node->uid];
}

/* Return the aggregate replacements for NODE, if there are any.  */

static inline struct ipa_agg_replacement_value *
ipa_get_agg_replacements_for_node (cgraph_node *node)
{
  ipcp_transformation_summary *ts = ipcp_get_transformation_summary (node);
  return ts ? ts->agg_values : NULL;
}

/* Function formal parameters related computations.  */
void ipa_initialize_node_params (struct cgraph_node *node);
bool ipa_propagate_indirect_call_infos (struct cgraph_edge *cs,
					vec<cgraph_edge *> *new_edges);

/* Indirect edge and binfo processing.  */
tree ipa_get_indirect_edge_target (struct cgraph_edge *ie,
				   vec<tree> ,
				   vec<ipa_polymorphic_call_context>,
				   vec<ipa_agg_jump_function_p>,
				   bool *);
struct cgraph_edge *ipa_make_edge_direct_to_target (struct cgraph_edge *, tree,
						    bool speculative = false);
tree ipa_impossible_devirt_target (struct cgraph_edge *, tree);
ipa_bits *ipa_get_ipa_bits_for_value (const widest_int &value,
				      const widest_int &mask);


/* Functions related to both.  */
void ipa_analyze_node (struct cgraph_node *);

/* Aggregate jump function related functions.  */
tree ipa_find_agg_cst_for_param (struct ipa_agg_jump_function *agg, tree scalar,
				 HOST_WIDE_INT offset, bool by_ref,
				 bool *from_global_constant = NULL);
bool ipa_load_from_parm_agg (struct ipa_func_body_info *fbi,
			     vec<ipa_param_descriptor, va_gc> *descriptors,
			     gimple *stmt, tree op, int *index_p,
			     HOST_WIDE_INT *offset_p, HOST_WIDE_INT *size_p,
			     bool *by_ref, bool *guaranteed_unmodified = NULL);

/* Debugging interface.  */
void ipa_print_node_params (FILE *, struct cgraph_node *node);
void ipa_print_all_params (FILE *);
void ipa_print_node_jump_functions (FILE *f, struct cgraph_node *node);
void ipa_print_all_jump_functions (FILE * f);
void ipcp_verify_propagated_values (void);

template <typename value>
class ipcp_value;

extern object_allocator<ipcp_value<tree> > ipcp_cst_values_pool;
extern object_allocator<ipcp_value<ipa_polymorphic_call_context> >
  ipcp_poly_ctx_values_pool;

template <typename valtype>
class ipcp_value_source;

extern object_allocator<ipcp_value_source<tree> > ipcp_sources_pool;

class ipcp_agg_lattice;

extern object_allocator<ipcp_agg_lattice> ipcp_agg_lattice_pool;

void ipa_dump_agg_replacement_values (FILE *f,
				      struct ipa_agg_replacement_value *av);
void ipa_prop_write_jump_functions (void);
void ipa_prop_read_jump_functions (void);
void ipcp_write_transformation_summaries (void);
void ipcp_read_transformation_summaries (void);
int ipa_get_param_decl_index (struct ipa_node_params *, tree);
tree ipa_value_from_jfunc (struct ipa_node_params *info,
			   struct ipa_jump_func *jfunc, tree type);
unsigned int ipcp_transform_function (struct cgraph_node *node);
ipa_polymorphic_call_context ipa_context_from_jfunc (ipa_node_params *,
						     cgraph_edge *,
						     int,
						     ipa_jump_func *);
void ipa_dump_param (FILE *, struct ipa_node_params *info, int i);
void ipa_release_body_info (struct ipa_func_body_info *);
tree ipa_get_callee_param_type (struct cgraph_edge *e, int i);

/* From tree-sra.c:  */
tree build_ref_for_offset (location_t, tree, poly_int64, bool, tree,
			   gimple_stmt_iterator *, bool);

/* In ipa-cp.c  */
void ipa_cp_c_finalize (void);

#endif /* IPA_PROP_H */
