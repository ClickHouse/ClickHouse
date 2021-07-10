/* Control flow graph analysis header file.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

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


#ifndef GCC_CFGANAL_H
#define GCC_CFGANAL_H

/* This structure maintains an edge list vector.  */
/* FIXME: Make this a vec<edge>.  */
struct edge_list
{
  int num_edges;
  edge *index_to_edge;
};


/* Class to compute and manage control dependences on an edge-list.  */
class control_dependences
{
public:
  control_dependences ();
  ~control_dependences ();
  bitmap get_edges_dependent_on (int);
  basic_block get_edge_src (int);
  basic_block get_edge_dest (int);

private:
  void set_control_dependence_map_bit (basic_block, int);
  void clear_control_dependence_bitmap (basic_block);
  void find_control_dependence (int);
  vec<bitmap> control_dependence_map;
  vec<std::pair<int, int> > m_el;
};

extern bool mark_dfs_back_edges (void);
extern void find_unreachable_blocks (void);
extern void verify_no_unreachable_blocks (void);
struct edge_list * create_edge_list (void);
void free_edge_list (struct edge_list *);
void print_edge_list (FILE *, struct edge_list *);
void verify_edge_list (FILE *, struct edge_list *);
edge find_edge (basic_block, basic_block);
int find_edge_index (struct edge_list *, basic_block, basic_block);
extern void remove_fake_edges (void);
extern void remove_fake_exit_edges (void);
extern void add_noreturn_fake_exit_edges (void);
extern void connect_infinite_loops_to_exit (void);
extern int post_order_compute (int *, bool, bool);
extern basic_block dfs_find_deadend (basic_block);
extern void inverted_post_order_compute (vec<int> *postorder, sbitmap *start_points = 0);
extern int pre_and_rev_post_order_compute_fn (struct function *,
					      int *, int *, bool);
extern int pre_and_rev_post_order_compute (int *, int *, bool);
extern int dfs_enumerate_from (basic_block, int,
			       bool (*)(const_basic_block, const void *),
			       basic_block *, int, const void *);
extern void compute_dominance_frontiers (struct bitmap_head *);
extern bitmap compute_idf (bitmap, struct bitmap_head *);
extern void bitmap_intersection_of_succs (sbitmap, sbitmap *, basic_block);
extern void bitmap_intersection_of_preds (sbitmap, sbitmap *, basic_block);
extern void bitmap_union_of_succs (sbitmap, sbitmap *, basic_block);
extern void bitmap_union_of_preds (sbitmap, sbitmap *, basic_block);
extern basic_block * single_pred_before_succ_order (void);
extern edge single_incoming_edge_ignoring_loop_edges (basic_block, bool);
extern edge single_pred_edge_ignoring_loop_edges (basic_block, bool);


#endif /* GCC_CFGANAL_H */
