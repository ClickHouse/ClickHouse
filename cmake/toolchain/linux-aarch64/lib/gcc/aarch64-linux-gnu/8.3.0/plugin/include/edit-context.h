/* Determining the results of applying fix-it hints.
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

#ifndef GCC_EDIT_CONTEXT_H
#define GCC_EDIT_CONTEXT_H

#include "typed-splay-tree.h"

class edit_context;
class edited_file;

/* A set of changes to the source code.

   The changes are "atomic" - if any changes can't be applied,
   none of them can be (tracked by the m_valid flag).
   Similarly, attempts to add the changes from a rich_location flagged
   as containing invalid changes mean that the whole of the edit_context
   is flagged as invalid.

   A complication here is that fix-its are expressed relative to coordinates
   in the files when they were parsed, before any changes have been made, and
   so if there's more that one fix-it to be applied, we have to adjust
   later fix-its to allow for the changes made by earlier ones.  This
   is done by the various "get_effective_column" methods.  */

class edit_context
{
 public:
  edit_context ();

  bool valid_p () const { return m_valid; }

  void add_fixits (rich_location *richloc);

  char *get_content (const char *filename);

  int get_effective_column (const char *filename, int line, int column);

  char *generate_diff (bool show_filenames);
  void print_diff (pretty_printer *pp, bool show_filenames);

 private:
  bool apply_fixit (const fixit_hint *hint);
  edited_file *get_file (const char *filename);
  edited_file &get_or_insert_file (const char *filename);

  bool m_valid;
  typed_splay_tree<const char *, edited_file *> m_files;
};

#endif /* GCC_EDIT_CONTEXT_H.  */
