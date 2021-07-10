/* Header file for SSA dominator optimizations.
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

#ifndef GCC_TREE_SSA_SCOPED_TABLES_H
#define GCC_TREE_SSA_SCOPED_TABLES_H

/* Representation of a "naked" right-hand-side expression, to be used
   in recording available expressions in the expression hash table.  */

enum expr_kind
{
  EXPR_SINGLE,
  EXPR_UNARY,
  EXPR_BINARY,
  EXPR_TERNARY,
  EXPR_CALL,
  EXPR_PHI
};

struct hashable_expr
{
  tree type;
  enum expr_kind kind;
  union {
    struct { tree rhs; } single;
    struct { enum tree_code op;  tree opnd; } unary;
    struct { enum tree_code op;  tree opnd0, opnd1; } binary;
    struct { enum tree_code op;  tree opnd0, opnd1, opnd2; } ternary;
    struct { gcall *fn_from; bool pure; size_t nargs; tree *args; } call;
    struct { size_t nargs; tree *args; } phi;
  } ops;
};

/* Structure for recording known value of a conditional expression.

   Clients build vectors of these objects to record known values
   that occur on edges.  */

struct cond_equivalence
{
  /* The condition, in a HASHABLE_EXPR form.  */
  struct hashable_expr cond;

  /* The result of the condition (true or false.  */
  tree value;
};

/* Structure for entries in the expression hash table.  */

typedef class expr_hash_elt * expr_hash_elt_t;

class expr_hash_elt
{
 public:
  expr_hash_elt (gimple *, tree);
  expr_hash_elt (tree);
  expr_hash_elt (struct hashable_expr *, tree);
  expr_hash_elt (class expr_hash_elt &);
  ~expr_hash_elt ();
  void print (FILE *);
  tree vop (void) { return m_vop; }
  tree lhs (void) { return m_lhs; }
  struct hashable_expr *expr (void) { return &m_expr; }
  expr_hash_elt *stamp (void) { return m_stamp; }
  hashval_t hash (void) { return m_hash; }

 private:
  /* The expression (rhs) we want to record.  */
  struct hashable_expr m_expr;

  /* The value (lhs) of this expression.  */
  tree m_lhs;

  /* The virtual operand associated with the nearest dominating stmt
     loading from or storing to expr.  */
  tree m_vop;

  /* The hash value for RHS.  */
  hashval_t m_hash;

  /* A unique stamp, typically the address of the hash
     element itself, used in removing entries from the table.  */
  struct expr_hash_elt *m_stamp;

  /* We should never be making assignments between objects in this class.
     Though it might allow us to exploit C++11 move semantics if we
     defined the move constructor and move assignment operator.  */
  expr_hash_elt& operator= (const expr_hash_elt&);
};

/* Hashtable helpers.  */

struct expr_elt_hasher : pointer_hash <expr_hash_elt>
{
  static inline hashval_t hash (const value_type &p)
    { return p->hash (); }
  static bool equal (const value_type &, const compare_type &);
  static inline void remove (value_type &element)
    { delete element; }
};


/* This class defines a unwindable expression equivalence table
   layered on top of the expression hash table.

   Essentially it's just a stack of available expression value pairs with
   a special marker (NULL, NULL) to indicate unwind points.   */

class avail_exprs_stack
{
 public:
  /* We need access to the AVAIL_EXPR hash table so that we can
     remove entries from the hash table when unwinding the stack.  */
  avail_exprs_stack (hash_table<expr_elt_hasher> *table)
    { m_stack.create (20); m_avail_exprs = table; }
  ~avail_exprs_stack (void) { m_stack.release (); }

  /* Push the unwinding marker onto the stack.  */
  void push_marker (void) { record_expr (NULL, NULL, 'M'); }

  /* Restore the AVAIL_EXPRs table to its state when the last marker
     was pushed.  */
  void pop_to_marker (void);

  /* Record a single available expression that can be unwound.  */
  void record_expr (expr_hash_elt_t, expr_hash_elt_t, char);

  /* Get the underlying hash table.  Would this be better as
     class inheritance?  */
  hash_table<expr_elt_hasher> *avail_exprs (void)
    { return m_avail_exprs; }

  /* Lookup and conditionally insert an expression into the table,
     recording enough information to unwind as needed.  */
  tree lookup_avail_expr (gimple *, bool, bool);

  void record_cond (cond_equivalence *);

 private:
  vec<std::pair<expr_hash_elt_t, expr_hash_elt_t> > m_stack;
  hash_table<expr_elt_hasher> *m_avail_exprs;

  /* For some assignments where the RHS is a binary operator, if we know
     a equality relationship between the operands, we may be able to compute
     a result, even if we don't know the exact value of the operands.  */
  tree simplify_binary_operation (gimple *, class expr_hash_elt);

  /* We do not allow copying this object or initializing one
     from another.  */
  avail_exprs_stack& operator= (const avail_exprs_stack&);
  avail_exprs_stack (class avail_exprs_stack &);
};

/* This class defines an unwindable const/copy equivalence table
   layered on top of SSA_NAME_VALUE/set_ssa_name_value.

   Essentially it's just a stack of name,prev value pairs with a
   special marker (NULL) to indicate unwind points.  */

class const_and_copies
{
 public:
  const_and_copies (void) { m_stack.create (20); };
  ~const_and_copies (void) { m_stack.release (); }

  /* Push the unwinding marker onto the stack.  */
  void push_marker (void) { m_stack.safe_push (NULL_TREE); }

  /* Restore the const/copies table to its state when the last marker
     was pushed.  */
  void pop_to_marker (void);

  /* Record a single const/copy pair that can be unwound.  This version
     may follow the value chain for the RHS.  */
  void record_const_or_copy (tree, tree);

  /* Special entry point when we want to provide an explicit previous
     value for the first argument.  Try to get rid of this in the future. 

     This version may also follow the value chain for the RHS.  */
  void record_const_or_copy (tree, tree, tree);

 private:
  /* Record a single const/copy pair that can be unwound.  This version
     does not follow the value chain for the RHS.  */
  void record_const_or_copy_raw (tree, tree, tree);

  vec<tree> m_stack;
  const_and_copies& operator= (const const_and_copies&);
  const_and_copies (class const_and_copies &);
};

void initialize_expr_from_cond (tree cond, struct hashable_expr *expr);
void record_conditions (vec<cond_equivalence> *p, tree, tree);

#endif /* GCC_TREE_SSA_SCOPED_TABLES_H */
