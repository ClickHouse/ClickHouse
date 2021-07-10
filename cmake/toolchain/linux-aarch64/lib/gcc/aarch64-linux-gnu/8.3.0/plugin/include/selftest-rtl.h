/* A self-testing framework, for use by -fself-test.
   Copyright (C) 2016-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SELFTEST_RTL_H
#define GCC_SELFTEST_RTL_H

/* The selftest code should entirely disappear in a production
   configuration, hence we guard all of it with #if CHECKING_P.  */

#if CHECKING_P

class rtx_reuse_manager;

namespace selftest {

/* Verify that X is dumped as EXPECTED_DUMP, using compact mode.
   Use LOC as the effective location when reporting errors.  */

extern void
assert_rtl_dump_eq (const location &loc, const char *expected_dump, rtx x,
		    rtx_reuse_manager *reuse_manager);

/* Verify that RTX is dumped as EXPECTED_DUMP, using compact mode.  */

#define ASSERT_RTL_DUMP_EQ(EXPECTED_DUMP, RTX) \
  assert_rtl_dump_eq (SELFTEST_LOCATION, (EXPECTED_DUMP), (RTX), NULL)

/* As above, but using REUSE_MANAGER when dumping.  */

#define ASSERT_RTL_DUMP_EQ_WITH_REUSE(EXPECTED_DUMP, RTX, REUSE_MANAGER) \
  assert_rtl_dump_eq (SELFTEST_LOCATION, (EXPECTED_DUMP), (RTX), \
		      (REUSE_MANAGER))

#define ASSERT_RTX_EQ(EXPECTED, ACTUAL) 				\
  SELFTEST_BEGIN_STMT							\
  const char *desc_ = "ASSERT_RTX_EQ (" #EXPECTED ", " #ACTUAL ")";	\
  ::selftest::assert_rtx_eq_at (SELFTEST_LOCATION, desc_, (EXPECTED),	\
				(ACTUAL));				\
  SELFTEST_END_STMT

extern void assert_rtx_eq_at (const location &, const char *, rtx, rtx);

/* Evaluate rtx EXPECTED and ACTUAL and compare them with ==
   (i.e. pointer equality), calling ::selftest::pass if they are
   equal, aborting if they are non-equal.  */

#define ASSERT_RTX_PTR_EQ(EXPECTED, ACTUAL) \
  SELFTEST_BEGIN_STMT							\
  const char *desc_ = "ASSERT_RTX_PTR_EQ (" #EXPECTED ", " #ACTUAL ")";  \
  ::selftest::assert_rtx_ptr_eq_at (SELFTEST_LOCATION, desc_, (EXPECTED), \
				    (ACTUAL));				\
  SELFTEST_END_STMT

/* Compare rtx EXPECTED and ACTUAL by pointer equality, calling
   ::selftest::pass if they are equal, aborting if they are non-equal.
   LOC is the effective location of the assertion, MSG describes it.  */

extern void assert_rtx_ptr_eq_at (const location &loc, const char *msg,
				  rtx expected, rtx actual);

/* A class for testing RTL function dumps.  */

class rtl_dump_test
{
 public:
  /* Takes ownership of PATH.  */
  rtl_dump_test (const location &loc, char *path);
  ~rtl_dump_test ();

 private:
  char *m_path;
};

/* Get the insn with the given uid, or NULL if not found.  */

extern rtx_insn *get_insn_by_uid (int uid);

extern void verify_three_block_rtl_cfg (function *fun);

} /* end of namespace selftest.  */

#endif /* #if CHECKING_P */

#endif /* GCC_SELFTEST_RTL_H */
