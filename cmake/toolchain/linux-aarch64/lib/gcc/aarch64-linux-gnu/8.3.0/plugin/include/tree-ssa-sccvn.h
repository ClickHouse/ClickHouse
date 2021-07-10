/* Tree SCC value numbering
   Copyright (C) 2007-2018 Free Software Foundation, Inc.
   Contributed by Daniel Berlin <dberlin@dberlin.org>

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   GCC is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef TREE_SSA_SCCVN_H
#define TREE_SSA_SCCVN_H

/* In tree-ssa-sccvn.c  */
bool expressions_equal_p (tree, tree);


/* TOP of the VN lattice.  */
extern tree VN_TOP;

/* N-ary operations in the hashtable consist of length operands, an
   opcode, and a type.  Result is the value number of the operation,
   and hashcode is stored to avoid having to calculate it
   repeatedly.  */

typedef struct vn_nary_op_s
{
  /* Unique identify that all expressions with the same value have. */
  unsigned int value_id;
  ENUM_BITFIELD(tree_code) opcode : 16;
  unsigned length : 16;
  hashval_t hashcode;
  tree result;
  tree type;
  tree op[1];
} *vn_nary_op_t;
typedef const struct vn_nary_op_s *const_vn_nary_op_t;

/* Return the size of a vn_nary_op_t with LENGTH operands.  */

static inline size_t
sizeof_vn_nary_op (unsigned int length)
{
  return sizeof (struct vn_nary_op_s) + sizeof (tree) * length - sizeof (tree);
}

/* Phi nodes in the hashtable consist of their non-VN_TOP phi
   arguments, and the basic block the phi is in. Result is the value
   number of the operation, and hashcode is stored to avoid having to
   calculate it repeatedly.  Phi nodes not in the same block are never
   considered equivalent.  */

typedef struct vn_phi_s
{
  /* Unique identifier that all expressions with the same value have. */
  unsigned int value_id;
  hashval_t hashcode;
  vec<tree> phiargs;
  basic_block block;
  /* Controlling condition lhs/rhs.  */
  tree cclhs;
  tree ccrhs;
  tree type;
  tree result;
} *vn_phi_t;
typedef const struct vn_phi_s *const_vn_phi_t;

/* Reference operands only exist in reference operations structures.
   They consist of an opcode, type, and some number of operands.  For
   a given opcode, some, all, or none of the operands may be used.
   The operands are there to store the information that makes up the
   portion of the addressing calculation that opcode performs.  */

typedef struct vn_reference_op_struct
{
  ENUM_BITFIELD(tree_code) opcode : 16;
  /* Dependence info, used for [TARGET_]MEM_REF only.  */
  unsigned short clique;
  unsigned short base;
  /* 1 for instrumented calls.  */
  unsigned with_bounds : 1;
  unsigned reverse : 1;
  /* For storing TYPE_ALIGN for array ref element size computation.  */
  unsigned align : 6;
  /* Constant offset this op adds or -1 if it is variable.  */
  poly_int64_pod off;
  tree type;
  tree op0;
  tree op1;
  tree op2;
} vn_reference_op_s;
typedef vn_reference_op_s *vn_reference_op_t;
typedef const vn_reference_op_s *const_vn_reference_op_t;

inline unsigned
vn_ref_op_align_unit (vn_reference_op_t op)
{
  return op->align ? ((unsigned)1 << (op->align - 1)) / BITS_PER_UNIT : 0;
}

/* A reference operation in the hashtable is representation as
   the vuse, representing the memory state at the time of
   the operation, and a collection of operands that make up the
   addressing calculation.  If two vn_reference_t's have the same set
   of operands, they access the same memory location. We also store
   the resulting value number, and the hashcode.  */

typedef struct vn_reference_s
{
  /* Unique identifier that all expressions with the same value have. */
  unsigned int value_id;
  hashval_t hashcode;
  tree vuse;
  alias_set_type set;
  tree type;
  vec<vn_reference_op_s> operands;
  tree result;
  tree result_vdef;
} *vn_reference_t;
typedef const struct vn_reference_s *const_vn_reference_t;

typedef struct vn_constant_s
{
  unsigned int value_id;
  hashval_t hashcode;
  tree constant;
} *vn_constant_t;

enum vn_kind { VN_NONE, VN_CONSTANT, VN_NARY, VN_REFERENCE, VN_PHI };
enum vn_kind vn_get_stmt_kind (gimple *);

/* Hash the type TYPE using bits that distinguishes it in the
   types_compatible_p sense.  */

static inline hashval_t
vn_hash_type (tree type)
{
  return (INTEGRAL_TYPE_P (type)
	  + (INTEGRAL_TYPE_P (type)
	     ? TYPE_PRECISION (type) + TYPE_UNSIGNED (type) : 0));
}

/* Hash the constant CONSTANT with distinguishing type incompatible
   constants in the types_compatible_p sense.  */

static inline hashval_t
vn_hash_constant_with_type (tree constant)
{
  inchash::hash hstate;
  inchash::add_expr (constant, hstate);
  hstate.merge_hash (vn_hash_type (TREE_TYPE (constant)));
  return hstate.end ();
}

/* Compare the constants C1 and C2 with distinguishing type incompatible
   constants in the types_compatible_p sense.  */

static inline bool
vn_constant_eq_with_type (tree c1, tree c2)
{
  return (expressions_equal_p (c1, c2)
	  && types_compatible_p (TREE_TYPE (c1), TREE_TYPE (c2)));
}

typedef struct vn_ssa_aux
{
  /* Value number. This may be an SSA name or a constant.  */
  tree valnum;
  /* Statements to insert if needs_insertion is true.  */
  gimple_seq expr;

  /* Saved SSA name info.  */
  tree_ssa_name::ssa_name_info_type info;

  /* Unique identifier that all expressions with the same value have. */
  unsigned int value_id;

  /* SCC information.  */
  unsigned int dfsnum;
  unsigned int low;
  unsigned visited : 1;
  unsigned on_sccstack : 1;

  /* Whether the SSA_NAME has been value numbered already.  This is
     only saying whether visit_use has been called on it at least
     once.  It cannot be used to avoid visitation for SSA_NAME's
     involved in non-singleton SCC's.  */
  unsigned use_processed : 1;

  /* Whether the SSA_NAME has no defining statement and thus an
     insertion of such with EXPR as definition is required before
     a use can be created of it.  */
  unsigned needs_insertion : 1;

  /* Whether range-info is anti-range.  */
  unsigned range_info_anti_range_p : 1;
} *vn_ssa_aux_t;

enum vn_lookup_kind { VN_NOWALK, VN_WALK, VN_WALKREWRITE };

/* Return the value numbering info for an SSA_NAME.  */
bool has_VN_INFO (tree);
extern vn_ssa_aux_t VN_INFO (tree);
extern vn_ssa_aux_t VN_INFO_GET (tree);
tree vn_get_expr_for (tree);
void run_scc_vn (vn_lookup_kind);
unsigned int vn_eliminate (bitmap);
void free_scc_vn (void);
void scc_vn_restore_ssa_info (void);
tree vn_nary_op_lookup (tree, vn_nary_op_t *);
tree vn_nary_op_lookup_stmt (gimple *, vn_nary_op_t *);
tree vn_nary_op_lookup_pieces (unsigned int, enum tree_code,
			       tree, tree *, vn_nary_op_t *);
vn_nary_op_t vn_nary_op_insert (tree, tree);
vn_nary_op_t vn_nary_op_insert_pieces (unsigned int, enum tree_code,
				       tree, tree *, tree, unsigned int);
bool ao_ref_init_from_vn_reference (ao_ref *, alias_set_type, tree,
				    vec<vn_reference_op_s> );
vec<vn_reference_op_s> vn_reference_operands_for_lookup (tree);
tree vn_reference_lookup_pieces (tree, alias_set_type, tree,
				 vec<vn_reference_op_s> ,
				 vn_reference_t *, vn_lookup_kind);
tree vn_reference_lookup (tree, tree, vn_lookup_kind, vn_reference_t *, bool);
void vn_reference_lookup_call (gcall *, vn_reference_t *, vn_reference_t);
vn_reference_t vn_reference_insert_pieces (tree, alias_set_type, tree,
					   vec<vn_reference_op_s> ,
					   tree, unsigned int);

bool vn_nary_op_eq (const_vn_nary_op_t const vno1,
		    const_vn_nary_op_t const vno2);
bool vn_nary_may_trap (vn_nary_op_t);
bool vn_reference_eq (const_vn_reference_t const, const_vn_reference_t const);
unsigned int get_max_value_id (void);
unsigned int get_next_value_id (void);
unsigned int get_constant_value_id (tree);
unsigned int get_or_alloc_constant_value_id (tree);
bool value_id_constant_p (unsigned int);
tree fully_constant_vn_reference_p (vn_reference_t);
tree vn_nary_simplify (vn_nary_op_t);

/* Valueize NAME if it is an SSA name, otherwise just return it.  */

static inline tree
vn_valueize (tree name)
{
  if (TREE_CODE (name) == SSA_NAME)
    {
      tree tem = VN_INFO (name)->valnum;
      return tem == VN_TOP ? name : tem;
    }
  return name;
}

/* Get at the original range info for NAME.  */

inline range_info_def *
VN_INFO_RANGE_INFO (tree name)
{
  return (VN_INFO (name)->info.range_info
	  ? VN_INFO (name)->info.range_info
	  : SSA_NAME_RANGE_INFO (name));
}

/* Whether the original range info of NAME is an anti-range.  */

inline bool
VN_INFO_ANTI_RANGE_P (tree name)
{
  return (VN_INFO (name)->info.range_info
	  ? VN_INFO (name)->range_info_anti_range_p
	  : SSA_NAME_ANTI_RANGE_P (name));
}

/* Get at the original range info kind for NAME.  */

inline value_range_type
VN_INFO_RANGE_TYPE (tree name)
{
  return VN_INFO_ANTI_RANGE_P (name) ? VR_ANTI_RANGE : VR_RANGE;
}

/* Get at the original pointer info for NAME.  */

inline ptr_info_def *
VN_INFO_PTR_INFO (tree name)
{
  return (VN_INFO (name)->info.ptr_info
	  ? VN_INFO (name)->info.ptr_info
	  : SSA_NAME_PTR_INFO (name));
}

#endif /* TREE_SSA_SCCVN_H  */
