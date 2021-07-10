/* context.h - Holder for global state
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

#ifndef GCC_CONTEXT_H
#define GCC_CONTEXT_H

namespace gcc {

class pass_manager;
class dump_manager;

/* GCC's internal state can be divided into zero or more
   "parallel universe" of state; an instance of this class is one such
   context of state.  */
class context
{
public:
  context ();
  ~context ();

  /* The flag shows if there are symbols to be streamed for offloading.  */
  bool have_offload;

  /* Pass-management.  */

  void set_passes (pass_manager *m)
  {
    gcc_assert (!m_passes);
    m_passes = m;
  }

  pass_manager *get_passes () { gcc_assert (m_passes); return m_passes; }

  /* Handling dump files.  */

  dump_manager *get_dumps () {gcc_assert (m_dumps); return m_dumps; }

private:
  /* Pass-management.  */
  pass_manager *m_passes;

  /* Dump files.  */
  dump_manager *m_dumps;

}; // class context

} // namespace gcc

/* The global singleton context aka "g".
   (the name is chosen to be easy to type in a debugger).  */
extern gcc::context *g;

#endif /* ! GCC_CONTEXT_H */
