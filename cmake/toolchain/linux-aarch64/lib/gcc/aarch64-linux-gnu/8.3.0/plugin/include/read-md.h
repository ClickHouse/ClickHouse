/* MD reader definitions.
   Copyright (C) 1987-2018 Free Software Foundation, Inc.

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

#ifndef GCC_READ_MD_H
#define GCC_READ_MD_H

#include "obstack.h"

/* Records a position in the file.  */
struct file_location {
  file_location () {}
  file_location (const char *, int, int);

  const char *filename;
  int lineno;
  int colno;
};

inline file_location::file_location (const char *filename_in, int lineno_in, int colno_in)
: filename (filename_in), lineno (lineno_in), colno (colno_in) {}

/* Holds one symbol or number in the .md file.  */
struct md_name {
  /* The name as it appeared in the .md file.  Names are syntactically
     limited to the length of this buffer.  */
  char buffer[256];

  /* The name that should actually be used by the generator programs.
     This is an expansion of NAME, after things like constant substitution.  */
  char *string;
};

/* This structure represents a constant defined by define_constant,
   define_enum, or such-like.  */
struct md_constant {
  /* The name of the constant.  */
  char *name;

  /* The string to which the constants expands.  */
  char *value;

  /* If the constant is associated with a enumeration, this field
     points to that enumeration, otherwise it is null.  */
  struct enum_type *parent_enum;
};

/* This structure represents one value in an enum_type.  */
struct enum_value {
  /* The next value in the enum, or null if this is the last.  */
  struct enum_value *next;

  /* The name of the value as it appears in the .md file.  */
  char *name;

  /* The definition of the related C value.  */
  struct md_constant *def;
};

/* This structure represents an enum defined by define_enum or the like.  */
struct enum_type {
  /* The C name of the enumeration.  */
  char *name;

  /* True if this is an md-style enum (DEFINE_ENUM) rather than
     a C-style enum (DEFINE_C_ENUM).  */
  bool md_p;

  /* The values of the enumeration.  There is always at least one.  */
  struct enum_value *values;

  /* A pointer to the null terminator in VALUES.  */
  struct enum_value **tail_ptr;

  /* The number of enumeration values.  */
  unsigned int num_values;
};

/* A class for reading .md files and RTL dump files.

   Implemented in read-md.c.

   This class has responsibility for reading chars from input files, and
   for certain common top-level directives including the "include"
   directive.

   It does not handle parsing the hierarchically-nested expressions of
   rtl.def; for that see the rtx_reader subclass below (implemented in
   read-rtl.c).  */

class md_reader
{
 public:
  md_reader (bool compact);
  virtual ~md_reader ();

  bool read_md_files (int, const char **, bool (*) (const char *));
  bool read_file (const char *filename);
  bool read_file_fragment (const char *filename,
			   int first_line,
			   int last_line);

  /* A hook that handles a single .md-file directive, up to but not
     including the closing ')'.  It takes two arguments: the file position
     at which the directive started, and the name of the directive.  The next
     unread character is the optional space after the directive name.  */
  virtual void handle_unknown_directive (file_location, const char *) = 0;

  file_location get_current_location () const;

  bool is_compact () const { return m_compact; }

  /* Defined in read-md.c.  */
  int read_char (void);
  void unread_char (int ch);
  file_location read_name (struct md_name *name);
  file_location read_name_or_nil (struct md_name *);
  void read_escape ();
  char *read_quoted_string ();
  char *read_braced_string ();
  char *read_string (int star_if_braced);
  void read_skip_construct (int depth, file_location loc);
  void require_char (char expected);
  void require_char_ws (char expected);
  void require_word_ws (const char *expected);
  int peek_char (void);

  void set_md_ptr_loc (const void *ptr, const char *filename, int lineno);
  const struct ptr_loc *get_md_ptr_loc (const void *ptr);
  void copy_md_ptr_loc (const void *new_ptr, const void *old_ptr);
  void fprint_md_ptr_loc (FILE *outf, const void *ptr);
  void print_md_ptr_loc (const void *ptr);

  struct enum_type *lookup_enum_type (const char *name);
  void traverse_enum_types (htab_trav callback, void *info);

  void handle_constants ();
  void traverse_md_constants (htab_trav callback, void *info);
  void handle_enum (file_location loc, bool md_p);

  const char *join_c_conditions (const char *cond1, const char *cond2);
  void fprint_c_condition (FILE *outf, const char *cond);
  void print_c_condition (const char *cond);

  /* Defined in read-rtl.c.  */
  const char *apply_iterator_to_string (const char *string);
  rtx copy_rtx_for_iterators (rtx original);
  void read_conditions ();
  void record_potential_iterator_use (struct iterator_group *group,
				      rtx x, unsigned int index,
				      const char *name);
  struct mapping *read_mapping (struct iterator_group *group, htab_t table);

  const char *get_top_level_filename () const { return m_toplevel_fname; }
  const char *get_filename () const { return m_read_md_filename; }
  int get_lineno () const { return m_read_md_lineno; }
  int get_colno () const { return m_read_md_colno; }

  struct obstack *get_string_obstack () { return &m_string_obstack; }
  htab_t get_md_constants () { return m_md_constants; }

 private:
  /* A singly-linked list of filenames.  */
  struct file_name_list {
    struct file_name_list *next;
    const char *fname;
  };

 private:
  void handle_file ();
  void handle_toplevel_file ();
  void handle_include (file_location loc);
  void add_include_path (const char *arg);

  bool read_name_1 (struct md_name *name, file_location *out_loc);

 private:
  /* Are we reading a compact dump?  */
  bool m_compact;

  /* The name of the toplevel file that indirectly included
     m_read_md_file.  */
  const char *m_toplevel_fname;

  /* The directory part of m_toplevel_fname
     NULL if m_toplevel_fname is a bare filename.  */
  char *m_base_dir;

  /* The file we are reading.  */
  FILE *m_read_md_file;

  /* The filename of m_read_md_file.  */
  const char *m_read_md_filename;

  /* The current line number in m_read_md_file.  */
  int m_read_md_lineno;

  /* The current column number in m_read_md_file.  */
  int m_read_md_colno;

  /* The column number before the last newline, so that
     we can handle unread_char ('\n') at least once whilst
     retaining column information.  */
  int m_last_line_colno;

  /* The first directory to search.  */
  file_name_list *m_first_dir_md_include;

  /* A pointer to the null terminator of the md include chain.  */
  file_name_list **m_last_dir_md_include_ptr;

  /* Obstack used for allocating MD strings.  */
  struct obstack m_string_obstack;

  /* A table of ptr_locs, hashed on the PTR field.  */
  htab_t m_ptr_locs;

  /* An obstack for the above.  Plain xmalloc is a bit heavyweight for a
     small structure like ptr_loc.  */
  struct obstack m_ptr_loc_obstack;

  /* A hash table of triples (A, B, C), where each of A, B and C is a condition
     and A is equivalent to "B && C".  This is used to keep track of the source
     of conditions that are made up of separate MD strings (such as the split
     condition of a define_insn_and_split).  */
  htab_t m_joined_conditions;

  /* An obstack for allocating joined_conditions entries.  */
  struct obstack m_joined_conditions_obstack;

  /* A table of md_constant structures, hashed by name.  Null if no
     constant expansion should occur.  */
  htab_t m_md_constants;

  /* A table of enum_type structures, hashed by name.  */
  htab_t m_enum_types;

  /* If non-zero, filter the input to just this subset of lines.  */
  int m_first_line;
  int m_last_line;
};

/* Global singleton; constrast with rtx_reader_ptr below.  */
extern md_reader *md_reader_ptr;

/* An md_reader subclass which skips unknown directives, for
   the gen* tools that purely use read-md.o.  */

class noop_reader : public md_reader
{
 public:
  noop_reader () : md_reader (false) {}

  /* A dummy implementation which skips unknown directives.  */
  void handle_unknown_directive (file_location, const char *);
};

/* An md_reader subclass that actually handles full hierarchical
   rtx expressions.

   Implemented in read-rtl.c.  */

class rtx_reader : public md_reader
{
 public:
  rtx_reader (bool compact);
  ~rtx_reader ();

  bool read_rtx (const char *rtx_name, vec<rtx> *rtxen);
  rtx read_rtx_code (const char *code_name);
  virtual rtx read_rtx_operand (rtx return_rtx, int idx);
  rtx read_nested_rtx ();
  rtx read_rtx_variadic (rtx form);
  char *read_until (const char *terminator_chars, bool consume_terminator);

  virtual void handle_any_trailing_information (rtx) {}
  virtual rtx postprocess (rtx x) { return x; }

  /* Hook to allow function_reader subclass to put STRINGBUF into gc-managed
     memory, rather than within an obstack.
     This base class implementation is a no-op.  */
  virtual const char *finalize_string (char *stringbuf) { return stringbuf; }

 protected:
  /* Analogous to rtx_writer's m_in_call_function_usage.  */
  bool m_in_call_function_usage;

  /* Support for "reuse_rtx" directives.  */
  auto_vec<rtx> m_reuse_rtx_by_id;
};

/* Global singleton; constrast with md_reader_ptr above.  */
extern rtx_reader *rtx_reader_ptr;

extern void (*include_callback) (const char *);

/* Read the next character from the MD file.  */

static inline int
read_char (void)
{
  return md_reader_ptr->read_char ();
}

/* Put back CH, which was the last character read from the MD file.  */

static inline void
unread_char (int ch)
{
  md_reader_ptr->unread_char (ch);
}

extern hashval_t leading_string_hash (const void *);
extern int leading_string_eq_p (const void *, const void *);
extern const char *join_c_conditions (const char *, const char *);
extern void message_at (file_location, const char *, ...) ATTRIBUTE_PRINTF_2;
extern void error_at (file_location, const char *, ...) ATTRIBUTE_PRINTF_2;
extern void fatal_at (file_location, const char *, ...) ATTRIBUTE_PRINTF_2;
extern void fatal_with_file_and_line (const char *, ...)
  ATTRIBUTE_PRINTF_1 ATTRIBUTE_NORETURN;
extern void fatal_expected_char (int, int) ATTRIBUTE_NORETURN;
extern int read_skip_spaces (void);
extern int n_comma_elts (const char *);
extern const char *scan_comma_elt (const char **);
extern void upcase_string (char *);
extern void traverse_enum_types (htab_trav, void *);
extern struct enum_type *lookup_enum_type (const char *);

#endif /* GCC_READ_MD_H */
