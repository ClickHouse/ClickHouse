/* Interprocedural semantic function equality pass
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

   Contributed by Jan Hubicka <hubicka@ucw.cz> and Martin Liska <mliska@suse.cz>

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

namespace ipa_icf {
class sem_item;

/* Congruence class encompasses a collection of either functions or
   read-only variables. These items are considered to be equivalent
   if not proved the oposite.  */
class congruence_class
{
public:
  /* Congruence class constructor for a new class with _ID.  */
  congruence_class (unsigned int _id): in_worklist (false), id(_id)
  {
  }

  /* Destructor.  */
  ~congruence_class ()
  {
  }

  /* Dump function prints all class members to a FILE with an INDENT.  */
  void dump (FILE *file, unsigned int indent = 0) const;

  /* Returns true if there's a member that is used from another group.  */
  bool is_class_used (void);

  /* Flag is used in case we want to remove a class from worklist and
     delete operation is quite expensive for
     the data structure (linked list).  */
  bool in_worklist;

  /* Vector of all group members.  */
  auto_vec <sem_item *> members;

  /* Global unique class identifier.  */
  unsigned int id;
};

/* Semantic item type enum.  */
enum sem_item_type
{
  FUNC,
  VAR
};

/* Class is container for address references for a symtab_node.  */

class symbol_compare_collection
{
public:
  /* Constructor.  */
  symbol_compare_collection (symtab_node *node);

  /* Destructor.  */
  ~symbol_compare_collection ()
  {
    m_references.release ();
    m_interposables.release ();
  }

  /* Vector of address references.  */
  vec<symtab_node *> m_references;

  /* Vector of interposable references.  */
  vec<symtab_node *> m_interposables;
};

/* Hash traits for symbol_compare_collection map.  */

struct symbol_compare_hash : nofree_ptr_hash <symbol_compare_collection>
{
  static hashval_t
  hash (value_type v)
  {
    inchash::hash hstate;
    hstate.add_int (v->m_references.length ());

    for (unsigned i = 0; i < v->m_references.length (); i++)
      hstate.add_int (v->m_references[i]->ultimate_alias_target ()->order);

    hstate.add_int (v->m_interposables.length ());

    for (unsigned i = 0; i < v->m_interposables.length (); i++)
      hstate.add_int (v->m_interposables[i]->ultimate_alias_target ()->order);

    return hstate.end ();
  }

  static bool
  equal (value_type a, value_type b)
  {
    if (a->m_references.length () != b->m_references.length ()
	|| a->m_interposables.length () != b->m_interposables.length ())
      return false;

    for (unsigned i = 0; i < a->m_references.length (); i++)
      if (a->m_references[i]->equal_address_to (b->m_references[i]) != 1)
	return false;

    for (unsigned i = 0; i < a->m_interposables.length (); i++)
      if (!a->m_interposables[i]->semantically_equivalent_p
	(b->m_interposables[i]))
	return false;

    return true;
  }
};


/* Semantic item usage pair.  */
class sem_usage_pair
{
public:
  /* Constructor for key value pair, where _ITEM is key and _INDEX is a target.  */
  sem_usage_pair (sem_item *_item, unsigned int _index);

  /* Target semantic item where an item is used.  */
  sem_item *item;

  /* Index of usage of such an item.  */
  unsigned int index;
};

typedef std::pair<symtab_node *, symtab_node *> symtab_pair;

/* Semantic item is a base class that encapsulates all shared functionality
   for both semantic function and variable items.  */
class sem_item
{
public:
  /* Semantic item constructor for a node of _TYPE, where STACK is used
     for bitmap memory allocation.  */
  sem_item (sem_item_type _type, bitmap_obstack *stack);

  /* Semantic item constructor for a node of _TYPE, where STACK is used
     for bitmap memory allocation.  The item is based on symtab node _NODE.  */
  sem_item (sem_item_type _type, symtab_node *_node, bitmap_obstack *stack);

  virtual ~sem_item ();

  /* Dump function for debugging purpose.  */
  DEBUG_FUNCTION void dump (void);

  /* Initialize semantic item by info reachable during LTO WPA phase.  */
  virtual void init_wpa (void) = 0;

  /* Semantic item initialization function.  */
  virtual void init (void) = 0;

  /* Add reference to a semantic TARGET.  */
  void add_reference (sem_item *target);

  /* Fast equality function based on knowledge known in WPA.  */
  virtual bool equals_wpa (sem_item *item,
			   hash_map <symtab_node *, sem_item *> &ignored_nodes) = 0;

  /* Returns true if the item equals to ITEM given as arguemnt.  */
  virtual bool equals (sem_item *item,
		       hash_map <symtab_node *, sem_item *> &ignored_nodes) = 0;

  /* References independent hash function.  */
  virtual hashval_t get_hash (void) = 0;

  /* Set new hash value of the item.  */
  void set_hash (hashval_t hash);

  /* Merges instance with an ALIAS_ITEM, where alias, thunk or redirection can
     be applied.  */
  virtual bool merge (sem_item *alias_item) = 0;

  /* Dump symbol to FILE.  */
  virtual void dump_to_file (FILE *file) = 0;

  /* Update hash by address sensitive references.  */
  void update_hash_by_addr_refs (hash_map <symtab_node *,
				 sem_item *> &m_symtab_node_map);

  /* Update hash by computed local hash values taken from different
     semantic items.  */
  void update_hash_by_local_refs (hash_map <symtab_node *,
				  sem_item *> &m_symtab_node_map);

  /* Return base tree that can be used for compatible_types_p and
     contains_polymorphic_type_p comparison.  */
  static bool get_base_types (tree *t1, tree *t2);

  /* Return true if target supports alias symbols.  */
  bool target_supports_symbol_aliases_p (void);

  /* Item type.  */
  sem_item_type type;

  /* Symtab node.  */
  symtab_node *node;

  /* Declaration tree node.  */
  tree decl;

  /* Semantic references used that generate congruence groups.  */
  vec <sem_item *> refs;

  /* Pointer to a congruence class the item belongs to.  */
  congruence_class *cls;

  /* Index of the item in a class belonging to.  */
  unsigned int index_in_class;

  /* List of semantic items where the instance is used.  */
  vec <sem_usage_pair *> usages;

  /* A bitmap with indices of all classes referencing this item.  */
  bitmap usage_index_bitmap;

  /* List of tree references (either FUNC_DECL or VAR_DECL).  */
  vec <tree> tree_refs;

  /* A set with symbol table references.  */
  hash_set <symtab_node *> refs_set;

  /* Temporary hash used where hash values of references are added.  */
  hashval_t global_hash;
protected:
  /* Cached, once calculated hash for the item.  */

  /* Accumulate to HSTATE a hash of expression EXP.  */
  static void add_expr (const_tree exp, inchash::hash &hstate);
  /* Accumulate to HSTATE a hash of type T.  */
  static void add_type (const_tree t, inchash::hash &hstate);

  /* Compare properties of symbol that does not affect semantics of symbol
     itself but affects semantics of its references.
     If ADDRESS is true, do extra checking needed for IPA_REF_ADDR.  */
  static bool compare_referenced_symbol_properties (symtab_node *used_by,
						    symtab_node *n1,
					            symtab_node *n2,
					            bool address);

  /* Compare two attribute lists.  */
  static bool compare_attributes (const_tree list1, const_tree list2);

  /* Hash properties compared by compare_referenced_symbol_properties.  */
  void hash_referenced_symbol_properties (symtab_node *ref,
					  inchash::hash &hstate,
					  bool address);

  /* For a given symbol table nodes N1 and N2, we check that FUNCTION_DECLs
     point to a same function. Comparison can be skipped if IGNORED_NODES
     contains these nodes.  ADDRESS indicate if address is taken.  */
  bool compare_symbol_references (hash_map <symtab_node *, sem_item *>
				  &ignored_nodes,
				  symtab_node *n1, symtab_node *n2,
				  bool address);
protected:
  /* Hash of item.  */
  hashval_t m_hash;

  /* Indicated whether a hash value has been set or not.  */
  bool m_hash_set;

private:
  /* Initialize internal data structures. Bitmap STACK is used for
     bitmap memory allocation process.  */
  void setup (bitmap_obstack *stack);
}; // class sem_item

class sem_function: public sem_item
{
public:
  /* Semantic function constructor that uses STACK as bitmap memory stack.  */
  sem_function (bitmap_obstack *stack);

  /*  Constructor based on callgraph node _NODE.
      Bitmap STACK is used for memory allocation.  */
  sem_function (cgraph_node *_node, bitmap_obstack *stack);

  ~sem_function ();

  inline virtual void init_wpa (void)
  {
  }

  virtual void init (void);
  virtual bool equals_wpa (sem_item *item,
			   hash_map <symtab_node *, sem_item *> &ignored_nodes);
  virtual hashval_t get_hash (void);
  virtual bool equals (sem_item *item,
		       hash_map <symtab_node *, sem_item *> &ignored_nodes);
  virtual bool merge (sem_item *alias_item);

  /* Dump symbol to FILE.  */
  virtual void dump_to_file (FILE *file)
  {
    gcc_assert (file);
    dump_function_to_file (decl, file, TDF_DETAILS);
  }

  /* Returns cgraph_node.  */
  inline cgraph_node *get_node (void)
  {
    return dyn_cast <cgraph_node *> (node);
  }

  /* Improve accumulated hash for HSTATE based on a gimple statement STMT.  */
  void hash_stmt (gimple *stmt, inchash::hash &inchash);

  /* Return true if polymorphic comparison must be processed.  */
  bool compare_polymorphic_p (void);

  /* For a given call graph NODE, the function constructs new
     semantic function item.  */
  static sem_function *parse (cgraph_node *node, bitmap_obstack *stack);

  /* Perform additional checks needed to match types of used function
     paramters.  */
  bool compatible_parm_types_p (tree, tree);

  /* Exception handling region tree.  */
  eh_region region_tree;

  /* Number of function arguments.  */
  unsigned int arg_count;

  /* Total amount of edges in the function.  */
  unsigned int edge_count;

  /* Vector of sizes of all basic blocks.  */
  vec <unsigned int> bb_sizes;

  /* Control flow graph checksum.  */
  hashval_t cfg_checksum;

  /* GIMPLE codes hash value.  */
  hashval_t gcode_hash;

  /* Total number of SSA names used in the function.  */
  unsigned ssa_names_size;

  /* Array of structures for all basic blocks.  */
  vec <ipa_icf_gimple::sem_bb *> bb_sorted;

  /* Return true if parameter I may be used.  */
  bool param_used_p (unsigned int i);

private:
  /* Calculates hash value based on a BASIC_BLOCK.  */
  hashval_t get_bb_hash (const ipa_icf_gimple::sem_bb *basic_block);

  /* For given basic blocks BB1 and BB2 (from functions FUNC1 and FUNC),
     true value is returned if phi nodes are semantically
     equivalent in these blocks .  */
  bool compare_phi_node (basic_block bb1, basic_block bb2);

  /* Basic blocks dictionary BB_DICT returns true if SOURCE index BB
     corresponds to TARGET.  */
  bool bb_dict_test (vec<int> *bb_dict, int source, int target);

  /* If cgraph edges E1 and E2 are indirect calls, verify that
     ICF flags are the same.  */
  bool compare_edge_flags (cgraph_edge *e1, cgraph_edge *e2);

  /* Processes function equality comparison.  */
  bool equals_private (sem_item *item);

  /* Returns true if tree T can be compared as a handled component.  */
  static bool icf_handled_component_p (tree t);

  /* Function checker stores binding between functions.   */
  ipa_icf_gimple::func_checker *m_checker;

  /* COMPARED_FUNC is a function that we compare to.  */
  sem_function *m_compared_func;
}; // class sem_function

class sem_variable: public sem_item
{
public:
  /* Semantic variable constructor that uses STACK as bitmap memory stack.  */
  sem_variable (bitmap_obstack *stack);

  /*  Constructor based on callgraph node _NODE.
      Bitmap STACK is used for memory allocation.  */

  sem_variable (varpool_node *_node, bitmap_obstack *stack);

  inline virtual void init_wpa (void) {}

  /* Semantic variable initialization function.  */
  inline virtual void init (void)
  {
    decl = get_node ()->decl;
  }

  virtual hashval_t get_hash (void);
  virtual bool merge (sem_item *alias_item);
  virtual void dump_to_file (FILE *file);
  virtual bool equals (sem_item *item,
		       hash_map <symtab_node *, sem_item *> &ignored_nodes);

  /* Fast equality variable based on knowledge known in WPA.  */
  virtual bool equals_wpa (sem_item *item,
			   hash_map <symtab_node *, sem_item *> &ignored_nodes);

  /* Returns varpool_node.  */
  inline varpool_node *get_node (void)
  {
    return dyn_cast <varpool_node *> (node);
  }

  /* Parser function that visits a varpool NODE.  */
  static sem_variable *parse (varpool_node *node, bitmap_obstack *stack);

private:
  /* Compares trees T1 and T2 for semantic equality.  */
  static bool equals (tree t1, tree t2);
}; // class sem_variable

class sem_item_optimizer;

struct congruence_class_group
{
  hashval_t hash;
  sem_item_type type;
  vec <congruence_class *> classes;
};

/* Congruence class set structure.  */
struct congruence_class_hash : nofree_ptr_hash <congruence_class_group>
{
  static inline hashval_t hash (const congruence_class_group *item)
  {
    return item->hash;
  }

  static inline int equal (const congruence_class_group *item1,
			   const congruence_class_group *item2)
  {
    return item1->hash == item2->hash && item1->type == item2->type;
  }
};

struct traverse_split_pair
{
  sem_item_optimizer *optimizer;
  class congruence_class *cls;
};

/* Semantic item optimizer includes all top-level logic
   related to semantic equality comparison.  */
class sem_item_optimizer
{
public:
  sem_item_optimizer ();
  ~sem_item_optimizer ();

  /* Function responsible for visiting all potential functions and
     read-only variables that can be merged.  */
  void parse_funcs_and_vars (void);

  /* Optimizer entry point which returns true in case it processes
     a merge operation. True is returned if there's a merge operation
     processed.  */
  bool execute (void);

  /* Dump function. */
  void dump (void);

  /* Verify congruence classes if checking is enabled.  */
  void checking_verify_classes (void);

  /* Verify congruence classes.  */
  void verify_classes (void);

  /* Write IPA ICF summary for symbols.  */
  void write_summary (void);

  /* Read IPA ICF summary for symbols.  */
  void read_summary (void);

  /* Callgraph removal hook called for a NODE with a custom DATA.  */
  static void cgraph_removal_hook (cgraph_node *node, void *data);

  /* Varpool removal hook called for a NODE with a custom DATA.  */
  static void varpool_removal_hook (varpool_node *node, void *data);

  /* Worklist of congruence classes that can potentially
     refine classes of congruence.  */
  std::list<congruence_class *> worklist;

  /* Remove semantic ITEM and release memory.  */
  void remove_item (sem_item *item);

  /* Remove symtab NODE triggered by symtab removal hooks.  */
  void remove_symtab_node (symtab_node *node);

  /* Register callgraph and varpool hooks.  */
  void register_hooks (void);

  /* Unregister callgraph and varpool hooks.  */
  void unregister_hooks (void);

  /* Adds a CLS to hashtable associated by hash value.  */
  void add_class (congruence_class *cls);

  /* Gets a congruence class group based on given HASH value and TYPE.  */
  congruence_class_group *get_group_by_hash (hashval_t hash,
      sem_item_type type);

  /* Because types can be arbitrarily large, avoid quadratic bottleneck.  */
  hash_map<const_tree, hashval_t> m_type_hash_cache;
private:

  /* For each semantic item, append hash values of references.  */
  void update_hash_by_addr_refs ();

  /* Congruence classes are built by hash value.  */
  void build_hash_based_classes (void);

  /* Semantic items in classes having more than one element and initialized.
     In case of WPA, we load function body.  */
  void parse_nonsingleton_classes (void);

  /* Equality function for semantic items is used to subdivide existing
     classes. If IN_WPA, fast equality function is invoked.  */
  void subdivide_classes_by_equality (bool in_wpa = false);

  /* Subdivide classes by address and interposable references
     that members of the class reference.
     Example can be a pair of functions that have an address
     taken from a function. If these addresses are different the class
     is split.  */
  unsigned subdivide_classes_by_sensitive_refs();

  /* Debug function prints all informations about congruence classes.  */
  void dump_cong_classes (void);

  /* Build references according to call graph.  */
  void build_graph (void);

  /* Iterative congruence reduction function.  */
  void process_cong_reduction (void);

  /* After reduction is done, we can declare all items in a group
     to be equal. PREV_CLASS_COUNT is start number of classes
     before reduction. True is returned if there's a merge operation
     processed.  */
  bool merge_classes (unsigned int prev_class_count);

  /* Fixup points to analysis info.  */
  void fixup_points_to_sets (void);

  /* Fixup points to set PT.  */
  void fixup_pt_set (struct pt_solution *pt);

  /* Adds a newly created congruence class CLS to worklist.  */
  void worklist_push (congruence_class *cls);

  /* Pops a class from worklist. */
  congruence_class *worklist_pop ();

  /* Every usage of a congruence class CLS is a candidate that can split the
     collection of classes. Bitmap stack BMSTACK is used for bitmap
     allocation.  */
  void do_congruence_step (congruence_class *cls);

  /* Tests if a class CLS used as INDEXth splits any congruence classes.
     Bitmap stack BMSTACK is used for bitmap allocation.  */
  void do_congruence_step_for_index (congruence_class *cls, unsigned int index);

  /* Makes pairing between a congruence class CLS and semantic ITEM.  */
  static void add_item_to_class (congruence_class *cls, sem_item *item);

  /* Disposes split map traverse function. CLS is congruence
     class, BSLOT is bitmap slot we want to release. DATA is mandatory,
     but unused argument.  */
  static bool release_split_map (congruence_class * const &cls, bitmap const &b,
				 traverse_split_pair *pair);

  /* Process split operation for a cognruence class CLS,
     where bitmap B splits congruence class members. DATA is used
     as argument of split pair.  */
  static bool traverse_congruence_split (congruence_class * const &cls,
					 bitmap const &b,
					 traverse_split_pair *pair);

  /* Compare function for sorting pairs in do_congruence_step_f.  */
  static int sort_congruence_split (const void *, const void *);

  /* Reads a section from LTO stream file FILE_DATA. Input block for DATA
     contains LEN bytes.  */
  void read_section (lto_file_decl_data *file_data, const char *data,
		     size_t len);

  /* Removes all callgraph and varpool nodes that are marked by symtab
     as deleted.  */
  void filter_removed_items (void);

  /* Vector of semantic items.  */
  vec <sem_item *> m_items;

  /* A set containing all items removed by hooks.  */
  hash_set <symtab_node *> m_removed_items_set;

  /* Hashtable of congruence classes.  */
  hash_table <congruence_class_hash> m_classes;

  /* Count of congruence classes.  */
  unsigned int m_classes_count;

  /* Map data structure maps symtab nodes to semantic items.  */
  hash_map <symtab_node *, sem_item *> m_symtab_node_map;

  /* Set to true if a splitter class is removed.  */
  bool splitter_class_removed;

  /* Global unique class id counter.  */
  static unsigned int class_id;

  /* Callgraph node removal hook holder.  */
  cgraph_node_hook_list *m_cgraph_node_hooks;

  /* Varpool node removal hook holder.  */
  varpool_node_hook_list *m_varpool_node_hooks;

  /* Bitmap stack.  */
  bitmap_obstack m_bmstack;

  /* Vector of merged variables.  Needed for fixup of points-to-analysis
     info.  */
  vec <symtab_pair> m_merged_variables;
}; // class sem_item_optimizer

} // ipa_icf namespace
