/* Selftest support for diagnostics.
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

#ifndef GCC_SELFTEST_DIAGNOSTIC_H
#define GCC_SELFTEST_DIAGNOSTIC_H

/* The selftest code should entirely disappear in a production
   configuration, hence we guard all of it with #if CHECKING_P.  */

#if CHECKING_P

namespace selftest {

/* Convenience subclass of diagnostic_context for testing
   the diagnostic subsystem.  */

class test_diagnostic_context : public diagnostic_context
{
 public:
  test_diagnostic_context ();
  ~test_diagnostic_context ();

  /* Implementation of diagnostic_start_span_fn, hiding the
     real filename (to avoid printing the names of tempfiles).  */
  static void
  start_span_cb (diagnostic_context *context, expanded_location exploc);
};

} // namespace selftest

#endif /* #if CHECKING_P */

#endif /* GCC_SELFTEST_DIAGNOSTIC_H */
