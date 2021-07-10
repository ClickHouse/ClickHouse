/* Source locations within string literals.
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

#ifndef GCC_SUBSTRING_LOCATIONS_H
#define GCC_SUBSTRING_LOCATIONS_H

/* The substring_loc class encapsulates information on the source location
   of a range of characters within a STRING_CST.

   If needed by a diagnostic, the actual location_t of the substring_loc
   can be calculated by calling its get_location method.  This calls a
   langhook, since this is inherently frontend-specific.  For the C family
   of frontends, it calls back into libcpp to reparse the strings.  This
   gets the location information "on demand", rather than storing the
   location information in the initial lex for every string.  Thus the
   substring_loc can also be thought of as a deferred call into libcpp,
   to allow the non-trivial work of reparsing the string to be delayed
   until we actually need it (to emit a diagnostic for a particular range
   of characters).

   substring_loc::get_location returns NULL if it succeeds, or an
   error message if it fails.  Error messages are intended for GCC
   developers (to help debugging) rather than for end-users.

   The easiest way to use a substring_loc is via the format_warning_* APIs,
   which gracefully handle failure of substring_loc::get_location by using
   the location of the string as a whole if substring-information is
   unavailable.  */

class substring_loc
{
 public:
  /* Constructor.  FMT_STRING_LOC is the location of the string as
     a whole.  STRING_TYPE is the type of the string.  It should be an
     ARRAY_TYPE of INTEGER_TYPE, or a POINTER_TYPE to such an ARRAY_TYPE.
     CARET_IDX, START_IDX, and END_IDX are offsets from the start
     of the string data.  */
  substring_loc (location_t fmt_string_loc, tree string_type,
		 int caret_idx, int start_idx, int end_idx)
  : m_fmt_string_loc (fmt_string_loc), m_string_type (string_type),
    m_caret_idx (caret_idx), m_start_idx (start_idx), m_end_idx (end_idx) {}

  void set_caret_index (int caret_idx) { m_caret_idx = caret_idx; }

  const char *get_location (location_t *out_loc) const;

  location_t get_fmt_string_loc () const { return m_fmt_string_loc; }
  tree get_string_type () const { return m_string_type; }
  int get_caret_idx () const { return m_caret_idx; }
  int get_start_idx () const { return m_start_idx; }
  int get_end_idx () const { return m_end_idx; }

 private:
  location_t m_fmt_string_loc;
  tree m_string_type;
  int m_caret_idx;
  int m_start_idx;
  int m_end_idx;
};

/* Functions for emitting a warning about a format string.  */

extern bool format_warning_va (const substring_loc &fmt_loc,
			       location_t param_loc,
			       const char *corrected_substring,
			       int opt, const char *gmsgid, va_list *ap)
  ATTRIBUTE_GCC_DIAG (5, 0);

extern bool format_warning_n_va (const substring_loc &fmt_loc,
				 location_t param_loc,
				 const char *corrected_substring,
				 int opt, unsigned HOST_WIDE_INT n,
				 const char *singular_gmsgid,
				 const char *plural_gmsgid, va_list *ap)
  ATTRIBUTE_GCC_DIAG (6, 0) ATTRIBUTE_GCC_DIAG (7, 0);

extern bool format_warning_at_substring (const substring_loc &fmt_loc,
					 location_t param_loc,
					 const char *corrected_substring,
					 int opt, const char *gmsgid, ...)
  ATTRIBUTE_GCC_DIAG (5, 6);

extern bool format_warning_at_substring_n (const substring_loc &fmt_loc,
					   location_t param_loc,
					   const char *corrected_substring,
					   int opt, unsigned HOST_WIDE_INT n,
					   const char *singular_gmsgid,
					   const char *plural_gmsgid, ...)
  ATTRIBUTE_GCC_DIAG (6, 8) ATTRIBUTE_GCC_DIAG (7, 8);

/* Implementation detail, for use when implementing
   LANG_HOOKS_GET_SUBSTRING_LOCATION.  */

extern const char *get_source_location_for_substring (cpp_reader *pfile,
						      string_concat_db *concats,
						      location_t strloc,
						      enum cpp_ttype type,
						      int caret_idx,
						      int start_idx, int end_idx,
						      location_t *out_loc);

#endif /* ! GCC_SUBSTRING_LOCATIONS_H */
