/* Definitions for transformations based on profile information for values.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.

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

#ifndef GCC_VALUE_PROF_H
#define GCC_VALUE_PROF_H

/* Supported histogram types.  */
enum hist_type
{
  HIST_TYPE_INTERVAL,	/* Measures histogram of values inside a specified
			   interval.  */
  HIST_TYPE_POW2,	/* Histogram of power of 2 values.  */
  HIST_TYPE_SINGLE_VALUE, /* Tries to identify the value that is (almost)
			   always constant.  */
  HIST_TYPE_INDIR_CALL,   /* Tries to identify the function that is (almost)
			    called in indirect call */
  HIST_TYPE_AVERAGE,	/* Compute average value (sum of all values).  */
  HIST_TYPE_IOR,	/* Used to compute expected alignment.  */
  HIST_TYPE_TIME_PROFILE, /* Used for time profile */
  HIST_TYPE_INDIR_CALL_TOPN, /* Tries to identify the top N most frequently
                                called functions in indirect call.  */
  HIST_TYPE_MAX
};

#define COUNTER_FOR_HIST_TYPE(TYPE) ((int) (TYPE) + GCOV_FIRST_VALUE_COUNTER)
#define HIST_TYPE_FOR_COUNTER(COUNTER) \
  ((enum hist_type) ((COUNTER) - GCOV_FIRST_VALUE_COUNTER))


/* The value to measure.  */
struct histogram_value_t
{
  struct
    {
      tree value;		/* The value to profile.  */
      gimple *stmt;		/* Insn containing the value.  */
      gcov_type *counters;		        /* Pointer to first counter.  */
      struct histogram_value_t *next;		/* Linked list pointer.  */
    } hvalue;
  enum hist_type type;			/* Type of information to measure.  */
  unsigned n_counters;			/* Number of required counters.  */
  struct function *fun;
  union
    {
      struct
	{
	  int int_start;	/* First value in interval.  */
	  unsigned int steps;	/* Number of values in it.  */
	} intvl;	/* Interval histogram data.  */
    } hdata;		/* Profiled information specific data.  */
};

typedef struct histogram_value_t *histogram_value;
typedef const struct histogram_value_t *const_histogram_value;


typedef vec<histogram_value> histogram_values;

extern void gimple_find_values_to_profile (histogram_values *);
extern bool gimple_value_profile_transformations (void);

histogram_value gimple_alloc_histogram_value (struct function *, enum hist_type,
					      gimple *stmt, tree);
histogram_value gimple_histogram_value (struct function *, gimple *);
histogram_value gimple_histogram_value_of_type (struct function *, gimple *,
						enum hist_type);
void gimple_add_histogram_value (struct function *, gimple *, histogram_value);
void dump_histograms_for_stmt (struct function *, FILE *, gimple *);
void gimple_remove_histogram_value (struct function *, gimple *, histogram_value);
void gimple_remove_stmt_histograms (struct function *, gimple *);
void gimple_duplicate_stmt_histograms (struct function *, gimple *,
				       struct function *, gimple *);
void gimple_move_stmt_histograms (struct function *, gimple *, gimple *);
void verify_histograms (void);
void free_histograms (function *);
void stringop_block_profile (gimple *, unsigned int *, HOST_WIDE_INT *);
gcall *gimple_ic (gcall *, struct cgraph_node *, profile_probability);
bool check_ic_target (gcall *, struct cgraph_node *);


/* In tree-profile.c.  */
extern void gimple_init_gcov_profiler (void);
extern void gimple_gen_edge_profiler (int, edge);
extern void gimple_gen_interval_profiler (histogram_value, unsigned, unsigned);
extern void gimple_gen_pow2_profiler (histogram_value, unsigned, unsigned);
extern void gimple_gen_one_value_profiler (histogram_value, unsigned, unsigned);
extern void gimple_gen_ic_profiler (histogram_value, unsigned, unsigned);
extern void gimple_gen_ic_func_profiler (void);
extern void gimple_gen_time_profiler (unsigned, unsigned);
extern void gimple_gen_average_profiler (histogram_value, unsigned, unsigned);
extern void gimple_gen_ior_profiler (histogram_value, unsigned, unsigned);
extern void stream_out_histogram_value (struct output_block *, histogram_value);
extern void stream_in_histogram_value (struct lto_input_block *, gimple *);
extern struct cgraph_node* find_func_by_profile_id (int func_id);


/* In profile.c.  */
extern void init_branch_prob (void);
extern void branch_prob (bool);
extern void read_thunk_profile (struct cgraph_node *);
extern void end_branch_prob (void);

#endif	/* GCC_VALUE_PROF_H */

