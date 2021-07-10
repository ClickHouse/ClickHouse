/* Gimple simplify definitions.

   Copyright (C) 2011-2018 Free Software Foundation, Inc.
   Contributed by Richard Guenther <rguenther@suse.de>

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

#ifndef GCC_GIMPLE_MATCH_H
#define GCC_GIMPLE_MATCH_H


/* Helper to transparently allow tree codes and builtin function codes
   exist in one storage entity.  */
class code_helper
{
public:
  code_helper () {}
  code_helper (tree_code code) : rep ((int) code) {}
  code_helper (combined_fn fn) : rep (-(int) fn) {}
  operator tree_code () const { return (tree_code) rep; }
  operator combined_fn () const { return (combined_fn) -rep; }
  bool is_tree_code () const { return rep > 0; }
  bool is_fn_code () const { return rep < 0; }
  int get_rep () const { return rep; }
private:
  int rep;
};

/* Return whether OPS[0] with CODE is a non-expression result and
   a gimple value.  */

inline bool
gimple_simplified_result_is_gimple_val (code_helper code, tree *ops)
{
  return (code.is_tree_code ()
	  && (TREE_CODE_LENGTH ((tree_code) code) == 0
	      || ((tree_code) code) == ADDR_EXPR)
	  && is_gimple_val (ops[0]));
}

extern tree (*mprts_hook) (code_helper, tree, tree *);

bool gimple_simplify (gimple *, code_helper *, tree *, gimple_seq *,
		      tree (*)(tree), tree (*)(tree));
bool gimple_resimplify1 (gimple_seq *, code_helper *, tree, tree *,
			 tree (*)(tree));
bool gimple_resimplify2 (gimple_seq *, code_helper *, tree, tree *,
			 tree (*)(tree));
bool gimple_resimplify3 (gimple_seq *, code_helper *, tree, tree *,
			 tree (*)(tree));
tree maybe_push_res_to_seq (code_helper, tree, tree *,
			    gimple_seq *, tree res = NULL_TREE);
void maybe_build_generic_op (enum tree_code, tree, tree *);


#endif  /* GCC_GIMPLE_MATCH_H */
