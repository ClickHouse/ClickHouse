/* Process source files and output type information.
   Copyright (C) 2002-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GENGTYPE_H
#define GCC_GENGTYPE_H

#define obstack_chunk_alloc    xmalloc
#define obstack_chunk_free     free
#define OBSTACK_CHUNK_SIZE     0

/* Sets of accepted source languages like C, C++, Ada... are
   represented by a bitmap.  */
typedef unsigned lang_bitmap;

/* Variable length structure representing an input file.  A hash table
   ensure uniqueness for a given input file name.  The only function
   allocating input_file-s is input_file_by_name.  */
struct input_file_st 
{
  struct outf* inpoutf;  /* Cached corresponding output file, computed
                            in get_output_file_with_visibility.  */
  lang_bitmap inpbitmap; /* The set of languages using this file.  */
  bool inpisplugin;      /* Flag set for plugin input files.  */
  char inpname[1];       /* A variable-length array, ended by a null
                            char.  */
};
typedef struct input_file_st input_file;

/* A file position, mostly for error messages.
   The FILE element may be compared using pointer equality.  */
struct fileloc
{
  const input_file *file;
  int line;
};


/* Table of all input files and its size.  */
extern const input_file** gt_files;
extern size_t num_gt_files;

/* A number of places use the name of this "gengtype.c" file for a
   location for things that we can't rely on the source to define.  We
   also need to refer to the "system.h" file specifically.  These two
   pointers are initialized early in main.  */
extern input_file* this_file;
extern input_file* system_h_file;

/* Retrieve or create the input_file for a given name, which is a file
   path.  This is the only function allocating input_file-s and it is
   hash-consing them.  */
input_file* input_file_by_name (const char* name);

/* For F an input_file, return the relative path to F from $(srcdir)
   if the latter is a prefix in F, NULL otherwise.  */
const char *get_file_srcdir_relative_path (const input_file *inpf);

/* Get the name of an input file.  */
static inline const char*
get_input_file_name (const input_file *inpf)
{
  if (inpf)
      return inpf->inpname;
  return NULL;
}

/* Return a bitmap which has bit `1 << BASE_FILE_<lang>' set iff
   INPUT_FILE is used by <lang>.

   This function should be written to assume that a file _is_ used
   if the situation is unclear.  If it wrongly assumes a file _is_ used,
   a linker error will result.  If it wrongly assumes a file _is not_ used,
   some GC roots may be missed, which is a much harder-to-debug problem.
  */

static inline lang_bitmap
get_lang_bitmap (const input_file* inpf)
{
  if (inpf == NULL)
    return 0;
  return inpf->inpbitmap;
}

/* Set the bitmap returned by get_lang_bitmap.  The only legitimate
   callers of this function are read_input_list & read_state_*.  */
static inline void
set_lang_bitmap (input_file* inpf, lang_bitmap n)
{
  gcc_assert (inpf);
  inpf->inpbitmap = n;
}

/* Vector of per-language directories.  */
extern const char **lang_dir_names;
extern size_t num_lang_dirs;

/* Data types handed around within, but opaque to, the lexer and parser.  */
typedef struct pair *pair_p;
typedef struct type *type_p;
typedef const struct type *const_type_p;
typedef struct options *options_p;

/* Variables used to communicate between the lexer and the parser.  */
extern int lexer_toplevel_done;
extern struct fileloc lexer_line;

/* Various things, organized as linked lists, needed both in
   gengtype.c & in gengtype-state.c files.  */
extern pair_p typedefs;
extern type_p structures;
extern pair_p variables;

/* An enum for distinguishing GGC vs PCH.  */

enum write_types_kinds
{
  WTK_GGC,
  WTK_PCH,

  NUM_WTK
};

/* Discrimating kind of types we can understand.  */

enum typekind {
  TYPE_NONE=0,          /* Never used, so zeroed memory is invalid.  */
  TYPE_UNDEFINED,	/* We have not yet seen a definition for this type.
			   If a type is still undefined when generating code,
			   an error will be generated.  */
  TYPE_SCALAR,          /* Scalar types like char.  */
  TYPE_STRING,          /* The string type.  */
  TYPE_STRUCT,          /* Type for GTY-ed structs.  */
  TYPE_UNION,           /* Type for GTY-ed discriminated unions.  */
  TYPE_POINTER,         /* Pointer type to GTY-ed type.  */
  TYPE_ARRAY,           /* Array of GTY-ed types.  */
  TYPE_LANG_STRUCT,     /* GCC front-end language specific structs.
                           Various languages may have homonymous but
                           different structs.  */
  TYPE_USER_STRUCT	/* User defined type.  Walkers and markers for
			   this type are assumed to be provided by the
			   user.  */
};

/* Discriminating kind for options.  */
enum option_kind {
  OPTION_NONE=0,        /* Never used, so zeroed memory is invalid.  */
  OPTION_STRING,        /* A string-valued option.  Most options are
                           strings.  */
  OPTION_TYPE,          /* A type-valued option.  */
  OPTION_NESTED         /* Option data for 'nested_ptr'.  */
};


/* A way to pass data through to the output end.  */
struct options {
  struct options *next;         /* next option of the same pair.  */
  const char *name;             /* GTY option name.  */
  enum option_kind kind;        /* discriminating option kind.  */
  union {
    const char* string;                    /* When OPTION_STRING.  */
    type_p type;                           /* When OPTION_TYPE.  */
    struct nested_ptr_data* nested;        /* when OPTION_NESTED.  */
  } info;
};


/* Option data for the 'nested_ptr' option.  */
struct nested_ptr_data {
  type_p type;
  const char *convert_to;
  const char *convert_from;
};

/* Some functions to create various options structures with name NAME
   and info INFO.  NEXT is the next option in the chain.  */

/* Create a string option.  */
options_p create_string_option (options_p next, const char* name,
                                const char* info);

/* Create a type option.  */
options_p create_type_option (options_p next, const char* name,
                              type_p info);

/* Create a nested option.  */
options_p create_nested_option (options_p next, const char* name,
				struct nested_ptr_data* info);

/* Create a nested pointer option.  */
options_p create_nested_ptr_option (options_p, type_p t,
			 	     const char *from, const char *to);

/* A name and a type.  */
struct pair {
  pair_p next;                  /* The next pair in the linked list.  */
  const char *name;             /* The defined name.  */
  type_p type;                  /* Its GTY-ed type.  */
  struct fileloc line;          /* The file location.  */
  options_p opt;                /* GTY options, as a linked list.  */
};

/* Usage information for GTY-ed types.  Gengtype has to care only of
   used GTY-ed types.  Types are initially unused, and their usage is
   computed by set_gc_used_type and set_gc_used functions.  */

enum gc_used_enum {

  /* We need that zeroed types are initially unused.  */
  GC_UNUSED=0,

  /* The GTY-ed type is used, e.g by a GTY-ed variable or a field
     inside a GTY-ed used type.  */
  GC_USED,

  /* For GTY-ed structures whose definitions we haven't seen so far
     when we encounter a pointer to it that is annotated with
     ``maybe_undef''.  If after reading in everything we don't have
     source file information for it, we assume that it never has been
     defined.  */
  GC_MAYBE_POINTED_TO,

  /* For known GTY-ed structures which are pointed to by GTY-ed
     variables or fields.  */
  GC_POINTED_TO
};

/* Our type structure describes all types handled by gengtype.  */
struct type {
  /* Discriminating kind, cannot be TYPE_NONE.  */
  enum typekind kind;

  /* For top-level structs or unions, the 'next' field links the
     global list 'structures'; for lang_structs, their homonymous structs are
     linked using this 'next' field.  The homonymous list starts at the
     s.lang_struct field of the lang_struct.  See the new_structure function
     for details.  This is tricky!  */
  type_p next;

  /* State number used when writing & reading the persistent state.  A
     type with a positive number has already been written.  For ease
     of debugging, newly allocated types have a unique negative
     number.  */
  int state_number;

  /* Each GTY-ed type which is pointed to by some GTY-ed type knows
     the GTY pointer type pointing to it.  See create_pointer
     function.  */
  type_p pointer_to;

  /* Type usage information, computed by set_gc_used_type and
     set_gc_used functions.  */
  enum gc_used_enum gc_used;

  /* The following union is discriminated by the 'kind' field above.  */
  union {
    /* TYPE__NONE is impossible.  */

    /* when TYPE_POINTER:  */
    type_p p;

    /* when TYPE_STRUCT or TYPE_UNION or TYPE_LANG_STRUCT, we have an
       aggregate type containing fields: */
    struct {
      const char *tag;          /* the aggregate tag, if any.  */
      struct fileloc line;      /* the source location.  */
      pair_p fields;            /* the linked list of fields.  */
      options_p opt;            /* the GTY options if any.  */
      lang_bitmap bitmap;       /* the set of front-end languages
                                   using that GTY-ed aggregate.  */
      /* For TYPE_LANG_STRUCT, the lang_struct field gives the first
         element of a linked list of homonymous struct or union types.
         Within this list, each homonymous type has as its lang_struct
         field the original TYPE_LANG_STRUCT type.  This is a dirty
         trick, see the new_structure function for details.  */
      type_p lang_struct;

      type_p base_class; /* the parent class, if any.  */

      /* The following two fields are not serialized in state files, and
	 are instead reconstructed on load.  */

      /* The head of a singly-linked list of immediate descendents in
	 the inheritance hierarchy.  */
      type_p first_subclass;
      /* The next in that list.  */
      type_p next_sibling_class;

      /* Have we already written ggc/pch user func for ptr to this?
	 (in write_user_func_for_structure_ptr).  */
      bool wrote_user_func_for_ptr[NUM_WTK];
    } s;

    /* when TYPE_SCALAR: */
    bool scalar_is_char;

    /* when TYPE_ARRAY: */
    struct {
      type_p p;                 /* The array component type.  */
      const char *len;          /* The string if any giving its length.  */
    } a;

  } u;
};

/* The one and only TYPE_STRING.  */
extern struct type string_type;

/* The two and only TYPE_SCALARs.  Their u.scalar_is_char flags are
   set early in main.  */
extern struct type scalar_nonchar;
extern struct type scalar_char;

/* Test if a type is a union, either a plain one or a language
   specific one.  */
#define UNION_P(x)					\
    ((x)->kind == TYPE_UNION				\
     || ((x)->kind == TYPE_LANG_STRUCT			\
         && (x)->u.s.lang_struct->kind == TYPE_UNION))

/* Test if a type is a union or a structure, perhaps a language
   specific one.  */
static inline bool
union_or_struct_p (enum typekind kind)
{
  return (kind == TYPE_UNION
	  || kind == TYPE_STRUCT
          || kind == TYPE_LANG_STRUCT
	  || kind == TYPE_USER_STRUCT);
}

static inline bool
union_or_struct_p (const_type_p x)
{
  return union_or_struct_p (x->kind);
}

/* Give the file location of a type, if any. */
static inline struct fileloc* 
type_fileloc (type_p t)
{
  if (!t) 
    return NULL;
  if (union_or_struct_p (t))
    return &t->u.s.line;
  return NULL;
}

/* Structure representing an output file.  */
struct outf
{
  struct outf *next;
  const char *name;
  size_t buflength;
  size_t bufused;
  char *buf;
};
typedef struct outf *outf_p;

/* The list of output files.  */
extern outf_p output_files;

/* The output header file that is included into pretty much every
   source file.  */
extern outf_p header_file;

/* Print, like fprintf, to O.  No-op if O is NULL.  */
void
oprintf (outf_p o, const char *S, ...)
  ATTRIBUTE_PRINTF_2;

/* An output file, suitable for definitions, that can see declarations
   made in INPF and is linked into every language that uses INPF.  May
   return NULL in plugin mode.  The INPF argument is almost const, but
   since the result is cached in its inpoutf field it cannot be
   declared const.  */
outf_p get_output_file_with_visibility (input_file* inpf);

/* The name of an output file, suitable for definitions, that can see
   declarations made in INPF and is linked into every language that
   uses INPF.  May return NULL.  */
const char *get_output_file_name (input_file *inpf);


/* Source directory.  */
extern const char *srcdir;	/* (-S) program argument. */

/* Length of srcdir name.  */
extern size_t srcdir_len;

/* Variable used for reading and writing the state.  */
extern const char *read_state_filename; /* (-r) program argument. */
extern const char *write_state_filename; /* (-w) program argument. */

/* Functions reading and writing the entire gengtype state, called from
   main, and implemented in file gengtype-state.c.  */
void read_state (const char* path);
/* Write the state, and update the state_number field in types.  */
void write_state (const char* path);


/* Print an error message.  */
extern void error_at_line
(const struct fileloc *pos, const char *msg, ...) ATTRIBUTE_PRINTF_2;

/* Constructor routines for types.  */
extern void do_typedef (const char *s, type_p t, struct fileloc *pos);
extern void do_scalar_typedef (const char *s, struct fileloc *pos);
extern type_p resolve_typedef (const char *s, struct fileloc *pos);
extern void add_subclass (type_p base, type_p subclass);
extern type_p new_structure (const char *name, enum typekind kind,
			     struct fileloc *pos, pair_p fields,
			     options_p o, type_p base);
type_p create_user_defined_type (const char *, struct fileloc *);
extern type_p find_structure (const char *s, enum typekind kind);
extern type_p create_scalar_type (const char *name);
extern type_p create_pointer (type_p t);
extern type_p create_array (type_p t, const char *len);
extern pair_p create_field_at (pair_p next, type_p type,
			       const char *name, options_p opt,
			       struct fileloc *pos);
extern pair_p nreverse_pairs (pair_p list);
extern type_p adjust_field_type (type_p, options_p);
extern void note_variable (const char *s, type_p t, options_p o,
			   struct fileloc *pos);

/* Lexer and parser routines.  */
extern int yylex (const char **yylval);
extern void yybegin (const char *fname);
extern void yyend (void);
extern void parse_file (const char *name);
extern bool hit_error;

/* Token codes.  */
enum gty_token
{
  EOF_TOKEN = 0,

  /* Per standard convention, codes in the range (0, UCHAR_MAX]
     represent single characters with those character codes.  */
  CHAR_TOKEN_OFFSET = UCHAR_MAX + 1,
  GTY_TOKEN = CHAR_TOKEN_OFFSET,
  TYPEDEF,
  EXTERN,
  STATIC,
  UNION,
  STRUCT,
  ENUM,
  ELLIPSIS,
  PTR_ALIAS,
  NESTED_PTR,
  USER_GTY,
  NUM,
  SCALAR,
  ID,
  STRING,
  CHAR,
  ARRAY,
  IGNORABLE_CXX_KEYWORD,

  /* print_token assumes that any token >= FIRST_TOKEN_WITH_VALUE may have
     a meaningful value to be printed.  */
  FIRST_TOKEN_WITH_VALUE = USER_GTY
};


/* Level for verbose messages, e.g. output file generation...  */
extern int verbosity_level;	/* (-v) program argument.  */

/* For debugging purposes we provide two flags.  */

/* Dump everything to understand gengtype's state. Might be useful to
   gengtype users.  */
extern int do_dump;		/* (-d) program argument. */

/* Trace the execution by many DBGPRINTF (with the position inside
   gengtype source code).  Only useful to debug gengtype itself.  */
extern int do_debug;		/* (-D) program argument. */

#define DBGPRINTF(Fmt,...) do {if (do_debug)				\
      fprintf (stderr, "%s:%d: " Fmt "\n",				\
	       lbasename (__FILE__),__LINE__, ##__VA_ARGS__);} while (0)
void dbgprint_count_type_at (const char *, int, const char *, type_p);
#define DBGPRINT_COUNT_TYPE(Msg,Ty) do {if (do_debug)			\
      dbgprint_count_type_at (__FILE__, __LINE__, Msg, Ty);}while (0)

#define FOR_ALL_INHERITED_FIELDS(TYPE, FIELD_VAR) \
  for (type_p sub = (TYPE); sub; sub = sub->u.s.base_class) \
    for (FIELD_VAR = sub->u.s.fields; FIELD_VAR; FIELD_VAR = FIELD_VAR->next)

extern bool
opts_have (options_p opts, const char *str);


#endif
