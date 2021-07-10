/* A self-testing framework, for use by -fself-test.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SELFTEST_H
#define GCC_SELFTEST_H

/* The selftest code should entirely disappear in a production
   configuration, hence we guard all of it with #if CHECKING_P.  */

#if CHECKING_P

namespace selftest {

/* A struct describing the source-location of a selftest, to make it
   easier to track down failing tests.  */

struct location
{
  location (const char *file, int line, const char *function)
    : m_file (file), m_line (line), m_function (function) {}

  const char *m_file;
  int m_line;
  const char *m_function;
};

/* A macro for use in selftests and by the ASSERT_ macros below,
   constructing a selftest::location for the current source location.  */

#define SELFTEST_LOCATION \
  (::selftest::location (__FILE__, __LINE__, __FUNCTION__))

/* The entrypoint for running all tests.  */

extern void run_tests ();

/* Record the successful outcome of some aspect of the test.  */

extern void pass (const location &loc, const char *msg);

/* Report the failed outcome of some aspect of the test and abort.  */

extern void fail (const location &loc, const char *msg)
  ATTRIBUTE_NORETURN;

/* As "fail", but using printf-style formatted output.  */

extern void fail_formatted (const location &loc, const char *fmt, ...)
  ATTRIBUTE_PRINTF_2 ATTRIBUTE_NORETURN;

/* Implementation detail of ASSERT_STREQ.  */

extern void assert_streq (const location &loc,
			  const char *desc_expected, const char *desc_actual,
			  const char *val_expected, const char *val_actual);

/* Implementation detail of ASSERT_STR_CONTAINS.  */

extern void assert_str_contains (const location &loc,
				 const char *desc_haystack,
				 const char *desc_needle,
				 const char *val_haystack,
				 const char *val_needle);

/* A named temporary file for use in selftests.
   Usable for writing out files, and as the base class for
   temp_source_file.
   The file is unlinked in the destructor.  */

class named_temp_file
{
 public:
  named_temp_file (const char *suffix);
  ~named_temp_file ();
  const char *get_filename () const { return m_filename; }

 private:
  char *m_filename;
};

/* A class for writing out a temporary sourcefile for use in selftests
   of input handling.  */

class temp_source_file : public named_temp_file
{
 public:
  temp_source_file (const location &loc, const char *suffix,
		    const char *content);
};

/* Various selftests involving location-handling require constructing a
   line table and one or more line maps within it.

   For maximum test coverage we want to run these tests with a variety
   of situations:
   - line_table->default_range_bits: some frontends use a non-zero value
   and others use zero
   - the fallback modes within line-map.c: there are various threshold
   values for source_location/location_t beyond line-map.c changes
   behavior (disabling of the range-packing optimization, disabling
   of column-tracking).  We can exercise these by starting the line_table
   at interesting values at or near these thresholds.

   The following struct describes a particular case within our test
   matrix.  */

struct line_table_case;

/* A class for overriding the global "line_table" within a selftest,
   restoring its value afterwards.  At most one instance of this
   class can exist at once, due to the need to keep the old value
   of line_table as a GC root.  */

class line_table_test
{
 public:
  /* Default constructor.  Override "line_table", using sane defaults
     for the temporary line_table.  */
  line_table_test ();

  /* Constructor.  Override "line_table", using the case described by C.  */
  line_table_test (const line_table_case &c);

  /* Destructor.  Restore the saved line_table.  */
  ~line_table_test ();
};

/* Run TESTCASE multiple times, once for each case in our test matrix.  */

extern void
for_each_line_table_case (void (*testcase) (const line_table_case &));

/* Read the contents of PATH into memory, returning a 0-terminated buffer
   that must be freed by the caller.
   Fail (and abort) if there are any problems, with LOC as the reported
   location of the failure.  */

extern char *read_file (const location &loc, const char *path);

/* A helper function for writing tests that interact with the
   garbage collector.  */

extern void forcibly_ggc_collect ();

/* Convert a path relative to SRCDIR/gcc/testsuite/selftests
   to a real path (either absolute, or relative to pwd).
   The result should be freed by the caller.  */

extern char *locate_file (const char *path);

/* The path of SRCDIR/testsuite/selftests.  */

extern const char *path_to_selftest_files;

/* selftest::test_runner is an implementation detail of selftest::run_tests,
   exposed here to allow plugins to run their own suites of tests.  */

class test_runner
{
 public:
  test_runner (const char *name);
  ~test_runner ();

 private:
  const char *m_name;
  long m_start_time;
};

/* Declarations for specific families of tests (by source file), in
   alphabetical order.  */
extern void attribute_c_tests ();
extern void bitmap_c_tests ();
extern void sbitmap_c_tests ();
extern void diagnostic_c_tests ();
extern void diagnostic_show_locus_c_tests ();
extern void edit_context_c_tests ();
extern void et_forest_c_tests ();
extern void fold_const_c_tests ();
extern void fibonacci_heap_c_tests ();
extern void function_tests_c_tests ();
extern void gimple_c_tests ();
extern void ggc_tests_c_tests ();
extern void hash_map_tests_c_tests ();
extern void hash_set_tests_c_tests ();
extern void input_c_tests ();
extern void pretty_print_c_tests ();
extern void read_rtl_function_c_tests ();
extern void rtl_tests_c_tests ();
extern void selftest_c_tests ();
extern void spellcheck_c_tests ();
extern void spellcheck_tree_c_tests ();
extern void sreal_c_tests ();
extern void store_merging_c_tests ();
extern void typed_splay_tree_c_tests ();
extern void tree_c_tests ();
extern void tree_cfg_c_tests ();
extern void unique_ptr_tests_cc_tests ();
extern void vec_c_tests ();
extern void wide_int_cc_tests ();
extern void predict_c_tests ();
extern void simplify_rtx_c_tests ();
extern void vec_perm_indices_c_tests ();

extern int num_passes;

} /* end of namespace selftest.  */

/* Macros for writing tests.  */

/* Evaluate EXPR and coerce to bool, calling
   ::selftest::pass if it is true,
   ::selftest::fail if it false.  */

#define ASSERT_TRUE(EXPR)				\
  ASSERT_TRUE_AT (SELFTEST_LOCATION, (EXPR))

/* Like ASSERT_TRUE, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_TRUE_AT(LOC, EXPR)			\
  SELFTEST_BEGIN_STMT					\
  const char *desc_ = "ASSERT_TRUE (" #EXPR ")";	\
  bool actual_ = (EXPR);				\
  if (actual_)						\
    ::selftest::pass ((LOC), desc_);			\
  else							\
    ::selftest::fail ((LOC), desc_);			\
  SELFTEST_END_STMT

/* Evaluate EXPR and coerce to bool, calling
   ::selftest::pass if it is false,
   ::selftest::fail if it true.  */

#define ASSERT_FALSE(EXPR)					\
  ASSERT_FALSE_AT (SELFTEST_LOCATION, (EXPR))

/* Like ASSERT_FALSE, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_FALSE_AT(LOC, EXPR)				\
  SELFTEST_BEGIN_STMT						\
  const char *desc_ = "ASSERT_FALSE (" #EXPR ")";		\
  bool actual_ = (EXPR);					\
  if (actual_)							\
    ::selftest::fail ((LOC), desc_);				\
  else								\
    ::selftest::pass ((LOC), desc_);				\
  SELFTEST_END_STMT

/* Evaluate EXPECTED and ACTUAL and compare them with ==, calling
   ::selftest::pass if they are equal,
   ::selftest::fail if they are non-equal.  */

#define ASSERT_EQ(EXPECTED, ACTUAL) \
  ASSERT_EQ_AT ((SELFTEST_LOCATION), (EXPECTED), (ACTUAL))

/* Like ASSERT_EQ, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_EQ_AT(LOC, EXPECTED, ACTUAL)		       \
  SELFTEST_BEGIN_STMT					       \
  const char *desc_ = "ASSERT_EQ (" #EXPECTED ", " #ACTUAL ")"; \
  if ((EXPECTED) == (ACTUAL))				       \
    ::selftest::pass ((LOC), desc_);			       \
  else							       \
    ::selftest::fail ((LOC), desc_);			       \
  SELFTEST_END_STMT

/* Evaluate EXPECTED and ACTUAL and compare them with known_eq, calling
   ::selftest::pass if they are always equal,
   ::selftest::fail if they might be non-equal.  */

#define ASSERT_KNOWN_EQ(EXPECTED, ACTUAL) \
  ASSERT_KNOWN_EQ_AT ((SELFTEST_LOCATION), (EXPECTED), (ACTUAL))

/* Like ASSERT_KNOWN_EQ, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_KNOWN_EQ_AT(LOC, EXPECTED, ACTUAL)			\
  SELFTEST_BEGIN_STMT							\
  const char *desc = "ASSERT_KNOWN_EQ (" #EXPECTED ", " #ACTUAL ")";	\
  if (known_eq (EXPECTED, ACTUAL))					\
    ::selftest::pass ((LOC), desc);					\
  else									\
    ::selftest::fail ((LOC), desc);					\
  SELFTEST_END_STMT

/* Evaluate EXPECTED and ACTUAL and compare them with !=, calling
   ::selftest::pass if they are non-equal,
   ::selftest::fail if they are equal.  */

#define ASSERT_NE(EXPECTED, ACTUAL)			       \
  SELFTEST_BEGIN_STMT					       \
  const char *desc_ = "ASSERT_NE (" #EXPECTED ", " #ACTUAL ")"; \
  if ((EXPECTED) != (ACTUAL))				       \
    ::selftest::pass (SELFTEST_LOCATION, desc_);	       \
  else							       \
    ::selftest::fail (SELFTEST_LOCATION, desc_);	       \
  SELFTEST_END_STMT

/* Evaluate EXPECTED and ACTUAL and compare them with maybe_ne, calling
   ::selftest::pass if they might be non-equal,
   ::selftest::fail if they are known to be equal.  */

#define ASSERT_MAYBE_NE(EXPECTED, ACTUAL) \
  ASSERT_MAYBE_NE_AT ((SELFTEST_LOCATION), (EXPECTED), (ACTUAL))

/* Like ASSERT_MAYBE_NE, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_MAYBE_NE_AT(LOC, EXPECTED, ACTUAL)			\
  SELFTEST_BEGIN_STMT							\
  const char *desc = "ASSERT_MAYBE_NE (" #EXPECTED ", " #ACTUAL ")";	\
  if (maybe_ne (EXPECTED, ACTUAL))					\
    ::selftest::pass ((LOC), desc);					\
  else									\
    ::selftest::fail ((LOC), desc);					\
  SELFTEST_END_STMT

/* Evaluate LHS and RHS and compare them with >, calling
   ::selftest::pass if LHS > RHS,
   ::selftest::fail otherwise.  */

#define ASSERT_GT(LHS, RHS)				\
  ASSERT_GT_AT ((SELFTEST_LOCATION), (LHS), (RHS))

/* Like ASSERT_GT, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_GT_AT(LOC, LHS, RHS)		       \
  SELFTEST_BEGIN_STMT					       \
  const char *desc_ = "ASSERT_GT (" #LHS ", " #RHS ")";	       \
  if ((LHS) > (RHS))					       \
    ::selftest::pass ((LOC), desc_);			       \
  else							       \
    ::selftest::fail ((LOC), desc_);			       \
  SELFTEST_END_STMT

/* Evaluate LHS and RHS and compare them with <, calling
   ::selftest::pass if LHS < RHS,
   ::selftest::fail otherwise.  */

#define ASSERT_LT(LHS, RHS)				\
  ASSERT_LT_AT ((SELFTEST_LOCATION), (LHS), (RHS))

/* Like ASSERT_LT, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_LT_AT(LOC, LHS, RHS)		       \
  SELFTEST_BEGIN_STMT					       \
  const char *desc_ = "ASSERT_LT (" #LHS ", " #RHS ")";	       \
  if ((LHS) < (RHS))					       \
    ::selftest::pass ((LOC), desc_);			       \
  else							       \
    ::selftest::fail ((LOC), desc_);			       \
  SELFTEST_END_STMT

/* Evaluate EXPECTED and ACTUAL and compare them with strcmp, calling
   ::selftest::pass if they are equal,
   ::selftest::fail if they are non-equal.  */

#define ASSERT_STREQ(EXPECTED, ACTUAL)				    \
  SELFTEST_BEGIN_STMT						    \
  ::selftest::assert_streq (SELFTEST_LOCATION, #EXPECTED, #ACTUAL, \
			    (EXPECTED), (ACTUAL));		    \
  SELFTEST_END_STMT

/* Like ASSERT_STREQ, but treat LOC as the effective location of the
   selftest.  */

#define ASSERT_STREQ_AT(LOC, EXPECTED, ACTUAL)			    \
  SELFTEST_BEGIN_STMT						    \
  ::selftest::assert_streq ((LOC), #EXPECTED, #ACTUAL,		    \
			    (EXPECTED), (ACTUAL));		    \
  SELFTEST_END_STMT

/* Evaluate HAYSTACK and NEEDLE and use strstr to determine if NEEDLE
   is within HAYSTACK.
   ::selftest::pass if NEEDLE is found.
   ::selftest::fail if it is not found.  */

#define ASSERT_STR_CONTAINS(HAYSTACK, NEEDLE)				\
  SELFTEST_BEGIN_STMT							\
  ::selftest::assert_str_contains (SELFTEST_LOCATION, #HAYSTACK, #NEEDLE, \
				   (HAYSTACK), (NEEDLE));		\
  SELFTEST_END_STMT

/* Evaluate PRED1 (VAL1), calling ::selftest::pass if it is true,
   ::selftest::fail if it is false.  */

#define ASSERT_PRED1(PRED1, VAL1)				\
  SELFTEST_BEGIN_STMT						\
  const char *desc_ = "ASSERT_PRED1 (" #PRED1 ", " #VAL1 ")";	\
  bool actual_ = (PRED1) (VAL1);				\
  if (actual_)							\
    ::selftest::pass (SELFTEST_LOCATION, desc_);		\
  else								\
    ::selftest::fail (SELFTEST_LOCATION, desc_);		\
  SELFTEST_END_STMT

#define SELFTEST_BEGIN_STMT do {
#define SELFTEST_END_STMT   } while (0)

#endif /* #if CHECKING_P */

#endif /* GCC_SELFTEST_H */
