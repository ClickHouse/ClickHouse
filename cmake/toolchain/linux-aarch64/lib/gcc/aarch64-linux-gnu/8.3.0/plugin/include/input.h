/* Declarations for variables relating to reading the source file.
   Used by parsers, lexical analyzers, and error message routines.
   Copyright (C) 1993-2018 Free Software Foundation, Inc.

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

#ifndef GCC_INPUT_H
#define GCC_INPUT_H

#include "line-map.h"

extern GTY(()) struct line_maps *line_table;
extern GTY(()) struct line_maps *saved_line_table;

/* A value which will never be used to represent a real location.  */
#define UNKNOWN_LOCATION ((source_location) 0)

/* The location for declarations in "<built-in>" */
#define BUILTINS_LOCATION ((source_location) 1)

/* line-map.c reserves RESERVED_LOCATION_COUNT to the user.  Ensure
   both UNKNOWN_LOCATION and BUILTINS_LOCATION fit into that.  */
extern char builtins_location_check[(BUILTINS_LOCATION
				     < RESERVED_LOCATION_COUNT) ? 1 : -1];

extern bool is_location_from_builtin_token (source_location);
extern expanded_location expand_location (source_location);
extern const char *location_get_source_line (const char *file_path, int line,
					     int *line_size);
extern bool location_missing_trailing_newline (const char *file_path);
extern expanded_location expand_location_to_spelling_point (source_location);
extern source_location expansion_point_location_if_in_system_header (source_location);
extern source_location expansion_point_location (source_location);

/* Historically GCC used location_t, while cpp used source_location.
   This could be removed but it hardly seems worth the effort.  */
typedef source_location location_t;

extern location_t input_location;

#define LOCATION_FILE(LOC) ((expand_location (LOC)).file)
#define LOCATION_LINE(LOC) ((expand_location (LOC)).line)
#define LOCATION_COLUMN(LOC)((expand_location (LOC)).column)
#define LOCATION_LOCUS(LOC) \
  ((IS_ADHOC_LOC (LOC)) ? get_location_from_adhoc_loc (line_table, LOC) \
   : (LOC))
#define LOCATION_BLOCK(LOC) \
  ((tree) ((IS_ADHOC_LOC (LOC)) ? get_data_from_adhoc_loc (line_table, (LOC)) \
   : NULL))
#define RESERVED_LOCATION_P(LOC) \
  (LOCATION_LOCUS (LOC) < RESERVED_LOCATION_COUNT)

/* Return a positive value if LOCATION is the locus of a token that is
   located in a system header, O otherwise. It returns 1 if LOCATION
   is the locus of a token that is located in a system header, and 2
   if LOCATION is the locus of a token located in a C system header
   that therefore needs to be extern "C" protected in C++.

   Note that this function returns 1 if LOCATION belongs to a token
   that is part of a macro replacement-list defined in a system
   header, but expanded in a non-system file.  */
#define in_system_header_at(LOC) \
  (linemap_location_in_system_header_p (line_table, LOC))
/* Return a positive value if LOCATION is the locus of a token that
   comes from a macro expansion, O otherwise.  */
#define from_macro_expansion_at(LOC) \
  ((linemap_location_from_macro_expansion_p (line_table, LOC)))
/* Return a positive value if LOCATION is the locus of a token that comes from
   a macro definition, O otherwise.  This differs from from_macro_expansion_at
   in its treatment of macro arguments, for which this returns false.  */
#define from_macro_definition_at(LOC) \
  ((linemap_location_from_macro_definition_p (line_table, LOC)))

static inline location_t
get_pure_location (location_t loc)
{
  return get_pure_location (line_table, loc);
}

/* Get the start of any range encoded within location LOC.  */

static inline location_t
get_start (location_t loc)
{
  return get_range_from_loc (line_table, loc).m_start;
}

/* Get the endpoint of any range encoded within location LOC.  */

static inline location_t
get_finish (location_t loc)
{
  return get_range_from_loc (line_table, loc).m_finish;
}

extern location_t make_location (location_t caret,
				 location_t start, location_t finish);
extern location_t make_location (location_t caret, source_range src_range);

void dump_line_table_statistics (void);

void dump_location_info (FILE *stream);

void diagnostics_file_cache_fini (void);

void diagnostics_file_cache_forcibly_evict_file (const char *file_path);

struct GTY(()) string_concat
{
  string_concat (int num, location_t *locs);

  int m_num;
  location_t * GTY ((atomic)) m_locs;
};

struct location_hash : int_hash <location_t, UNKNOWN_LOCATION> { };

class GTY(()) string_concat_db
{
 public:
  string_concat_db ();
  void record_string_concatenation (int num, location_t *locs);

  bool get_string_concatenation (location_t loc,
				 int *out_num,
				 location_t **out_locs);

 private:
  static location_t get_key_loc (location_t loc);

  /* For the fields to be private, we must grant access to the
     generated code in gtype-desc.c.  */

  friend void ::gt_ggc_mx_string_concat_db (void *x_p);
  friend void ::gt_pch_nx_string_concat_db (void *x_p);
  friend void ::gt_pch_p_16string_concat_db (void *this_obj, void *x_p,
					     gt_pointer_operator op,
					     void *cookie);

  hash_map <location_hash, string_concat *> *m_table;
};

#endif
