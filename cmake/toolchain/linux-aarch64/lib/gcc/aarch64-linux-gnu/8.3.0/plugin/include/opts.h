/* Command line option handling.
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

#ifndef GCC_OPTS_H
#define GCC_OPTS_H

#include "obstack.h"

/* Specifies how a switch's VAR_VALUE relates to its FLAG_VAR.  */
enum cl_var_type {
  /* The switch is enabled when FLAG_VAR is nonzero.  */
  CLVC_BOOLEAN,

  /* The switch is enabled when FLAG_VAR == VAR_VALUE.  */
  CLVC_EQUAL,

  /* The switch is enabled when VAR_VALUE is not set in FLAG_VAR.  */
  CLVC_BIT_CLEAR,

  /* The switch is enabled when VAR_VALUE is set in FLAG_VAR.  */
  CLVC_BIT_SET,

  /* The switch takes a string argument and FLAG_VAR points to that
     argument.  */
  CLVC_STRING,

  /* The switch takes an enumerated argument (VAR_ENUM says what
     enumeration) and FLAG_VAR points to that argument.  */
  CLVC_ENUM,

  /* The switch should be stored in the VEC pointed to by FLAG_VAR for
     later processing.  */
  CLVC_DEFER
};

struct cl_option
{
  /* Text of the option, including initial '-'.  */
  const char *opt_text;
  /* Help text for --help, or NULL.  */
  const char *help;
  /* Error message for missing argument, or NULL.  */
  const char *missing_argument_error;
  /* Warning to give when this option is used, or NULL.  */
  const char *warn_message;
  /* Argument of alias target when positive option given, or NULL.  */
  const char *alias_arg;
  /* Argument of alias target when negative option given, or NULL.  */
  const char *neg_alias_arg;
  /* Alias target, or N_OPTS if not an alias.  */
  unsigned short alias_target;
  /* Previous option that is an initial substring of this one, or
     N_OPTS if none.  */
  unsigned short back_chain;
  /* Option length, not including initial '-'.  */
  unsigned char opt_len;
  /* Next option in a sequence marked with Negative, or -1 if none.  */
  int neg_index;
  /* CL_* flags for this option.  */
  unsigned int flags;
  /* Disabled in this configuration.  */
  BOOL_BITFIELD cl_disabled : 1;
  /* Options marked with CL_SEPARATE take a number of separate
     arguments (1 to 4) that is one more than the number in this
     bit-field.  */
  unsigned int cl_separate_nargs : 2;
  /* Option is an alias when used with separate argument.  */
  BOOL_BITFIELD cl_separate_alias : 1;
  /* Alias to negative form of option.  */
  BOOL_BITFIELD cl_negative_alias : 1;
  /* Option takes no argument in the driver.  */
  BOOL_BITFIELD cl_no_driver_arg : 1;
  /* Reject this option in the driver.  */
  BOOL_BITFIELD cl_reject_driver : 1;
  /* Reject no- form.  */
  BOOL_BITFIELD cl_reject_negative : 1;
  /* Missing argument OK (joined).  */
  BOOL_BITFIELD cl_missing_ok : 1;
  /* Argument is an integer >=0.  */
  BOOL_BITFIELD cl_uinteger : 1;
  /* Argument is a HOST_WIDE_INT.  */
  BOOL_BITFIELD cl_host_wide_int : 1;
  /* Argument should be converted to lowercase.  */
  BOOL_BITFIELD cl_tolower : 1;
  /* Report argument with -fverbose-asm  */
  BOOL_BITFIELD cl_report : 1;
  /* Offset of field for this option in struct gcc_options, or
     (unsigned short) -1 if none.  */
  unsigned short flag_var_offset;
  /* Index in cl_enums of enum used for this option's arguments, for
     CLVC_ENUM options.  */
  unsigned short var_enum;
  /* How this option's value is determined and sets a field.  */
  enum cl_var_type var_type;
  /* Value or bit-mask with which to set a field.  */
  HOST_WIDE_INT var_value;
  /* Range info minimum, or -1.  */
  int range_min;
  /* Range info maximum, or -1.  */
  int range_max;
};

/* Records that the state of an option consists of SIZE bytes starting
   at DATA.  DATA might point to CH in some cases.  */
struct cl_option_state {
  const void *data;
  size_t size;
  char ch;
};

extern const struct cl_option cl_options[];
extern const unsigned int cl_options_count;
extern const char *const lang_names[];
extern const unsigned int cl_lang_count;

#define CL_PARAMS               (1U << 16) /* Fake entry.  Used to display --param info with --help.  */
#define CL_WARNING		(1U << 17) /* Enables an (optional) warning message.  */
#define CL_OPTIMIZATION		(1U << 18) /* Enables an (optional) optimization.  */
#define CL_DRIVER		(1U << 19) /* Driver option.  */
#define CL_TARGET		(1U << 20) /* Target-specific option.  */
#define CL_COMMON		(1U << 21) /* Language-independent.  */

#define CL_MIN_OPTION_CLASS	CL_PARAMS
#define CL_MAX_OPTION_CLASS	CL_COMMON

/* From here on the bits describe attributes of the options.
   Before this point the bits have described the class of the option.
   This distinction is important because --help will not list options
   which only have these higher bits set.  */

#define CL_JOINED		(1U << 22) /* If takes joined argument.  */
#define CL_SEPARATE		(1U << 23) /* If takes a separate argument.  */
#define CL_UNDOCUMENTED		(1U << 24) /* Do not output with --help.  */
#define CL_NO_DWARF_RECORD	(1U << 25) /* Do not add to producer string.  */
#define CL_PCH_IGNORE		(1U << 26) /* Do compare state for pch.  */

/* Flags for an enumerated option argument.  */
#define CL_ENUM_CANONICAL	(1 << 0) /* Canonical for this value.  */
#define CL_ENUM_DRIVER_ONLY	(1 << 1) /* Only accepted in the driver.  */

/* Structure describing an enumerated option argument.  */

struct cl_enum_arg
{
  /* The argument text, or NULL at the end of the array.  */
  const char *arg;

  /* The corresponding integer value.  */
  int value;

  /* Flags associated with this argument.  */
  unsigned int flags;
};

/* Structure describing an enumerated set of option arguments.  */

struct cl_enum
{
  /* Help text, or NULL if the values should not be listed in --help
     output.  */
  const char *help;

  /* Error message for unknown arguments, or NULL to use a generic
     error.  */
  const char *unknown_error;

  /* Array of possible values.  */
  const struct cl_enum_arg *values;

  /* The size of the type used to store a value.  */
  size_t var_size;

  /* Function to set a variable of this type.  */
  void (*set) (void *var, int value);

  /* Function to get the value of a variable of this type.  */
  int (*get) (const void *var);
};

extern const struct cl_enum cl_enums[];
extern const unsigned int cl_enums_count;

/* Possible ways in which a command-line option may be erroneous.
   These do not include not being known at all; an option index of
   OPT_SPECIAL_unknown is used for that.  */

#define CL_ERR_DISABLED		(1 << 0) /* Disabled in this configuration.  */
#define CL_ERR_MISSING_ARG	(1 << 1) /* Argument required but missing.  */
#define CL_ERR_WRONG_LANG	(1 << 2) /* Option for wrong language.  */
#define CL_ERR_UINT_ARG		(1 << 3) /* Bad unsigned integer argument.  */
#define CL_ERR_INT_RANGE_ARG	(1 << 4) /* Bad unsigned integer argument.  */
#define CL_ERR_ENUM_ARG		(1 << 5) /* Bad enumerated argument.  */
#define CL_ERR_NEGATIVE		(1 << 6) /* Negative form of option
					    not permitted (together
					    with OPT_SPECIAL_unknown).  */

/* Structure describing the result of decoding an option.  */

struct cl_decoded_option
{
  /* The index of this option, or an OPT_SPECIAL_* value for
     non-options and unknown options.  */
  size_t opt_index;

  /* Any warning to give for use of this option, or NULL if none.  */
  const char *warn_message;

  /* The string argument, or NULL if none.  For OPT_SPECIAL_* cases,
     the option or non-option command-line argument.  */
  const char *arg;

  /* The original text of option plus arguments, with separate argv
     elements concatenated into one string with spaces separating
     them.  This is for such uses as diagnostics and
     -frecord-gcc-switches.  */
  const char *orig_option_with_args_text;

  /* The canonical form of the option and its argument, for when it is
     necessary to reconstruct argv elements (in particular, for
     processing specs and passing options to subprocesses from the
     driver).  */
  const char *canonical_option[4];

  /* The number of elements in the canonical form of the option and
     arguments; always at least 1.  */
  size_t canonical_option_num_elements;

  /* For a boolean option, 1 for the true case and 0 for the "no-"
     case.  For an unsigned integer option, the value of the
     argument.  1 in all other cases.  */
  int value;

  /* Any flags describing errors detected in this option.  */
  int errors;
};

/* Structure describing an option deferred for handling after the main
   option handlers.  */

struct cl_deferred_option
{
  /* Elements from struct cl_decoded_option used for deferred
     options.  */
  size_t opt_index;
  const char *arg;
  int value;
};

/* Structure describing a single option-handling callback.  */

struct cl_option_handler_func
{
  /* The function called to handle the option.  */
  bool (*handler) (struct gcc_options *opts,
		   struct gcc_options *opts_set,
		   const struct cl_decoded_option *decoded,
		   unsigned int lang_mask, int kind, location_t loc,
		   const struct cl_option_handlers *handlers,
		   diagnostic_context *dc,
		   void (*target_option_override_hook) (void));

  /* The mask that must have some bit in common with the flags for the
     option for this particular handler to be used.  */
  unsigned int mask;
};

/* Structure describing the callbacks used in handling options.  */

struct cl_option_handlers
{
  /* Callback for an unknown option to determine whether to give an
     error for it, and possibly store information to diagnose the
     option at a later point.  Return true if an error should be
     given, false otherwise.  */
  bool (*unknown_option_callback) (const struct cl_decoded_option *decoded);

  /* Callback to handle, and possibly diagnose, an option for another
     language.  */
  void (*wrong_lang_callback) (const struct cl_decoded_option *decoded,
			       unsigned int lang_mask);

  /* Target option override hook.  */
  void (*target_option_override_hook) (void);

  /* The number of individual handlers.  */
  size_t num_handlers;

  /* The handlers themselves.  */
  struct cl_option_handler_func handlers[3];
};

/* Hold command-line options associated with stack limitation.  */
extern const char *opt_fstack_limit_symbol_arg;
extern int opt_fstack_limit_register_no;

/* Input file names.  */

extern const char **in_fnames;

/* The count of input filenames.  */

extern unsigned num_in_fnames;

extern char *opts_concat (const char *first, ...);

/* Obstack for option strings.  */

extern struct obstack opts_obstack;

size_t find_opt (const char *input, unsigned int lang_mask);
extern int integral_argument (const char *arg);
extern bool enum_value_to_arg (const struct cl_enum_arg *enum_args,
			       const char **argp, int value,
			       unsigned int lang_mask);
extern void decode_cmdline_options_to_array (unsigned int argc,
					     const char **argv, 
					     unsigned int lang_mask,
					     struct cl_decoded_option **decoded_options,
					     unsigned int *decoded_options_count);
extern void init_options_once (void);
extern void init_options_struct (struct gcc_options *opts,
				 struct gcc_options *opts_set);
extern void init_opts_obstack (void);
extern void finalize_options_struct (struct gcc_options *opts);
extern void decode_cmdline_options_to_array_default_mask (unsigned int argc,
							  const char **argv, 
							  struct cl_decoded_option **decoded_options,
							  unsigned int *decoded_options_count);
extern void set_default_handlers (struct cl_option_handlers *handlers,
				  void (*target_option_override_hook) (void));
extern void decode_options (struct gcc_options *opts,
			    struct gcc_options *opts_set,
			    struct cl_decoded_option *decoded_options,
			    unsigned int decoded_options_count,
			    location_t loc,
			    diagnostic_context *dc,
			    void (*target_option_override_hook) (void));
extern int option_enabled (int opt_idx, void *opts);
extern bool get_option_state (struct gcc_options *, int,
			      struct cl_option_state *);
extern void set_option (struct gcc_options *opts,
			struct gcc_options *opts_set,
			int opt_index, int value, const char *arg, int kind,
			location_t loc, diagnostic_context *dc);
extern void *option_flag_var (int opt_index, struct gcc_options *opts);
bool handle_generated_option (struct gcc_options *opts,
			      struct gcc_options *opts_set,
			      size_t opt_index, const char *arg, int value,
			      unsigned int lang_mask, int kind, location_t loc,
			      const struct cl_option_handlers *handlers,
			      bool generated_p, diagnostic_context *dc);
void generate_option (size_t opt_index, const char *arg, int value,
		      unsigned int lang_mask,
		      struct cl_decoded_option *decoded);
void generate_option_input_file (const char *file,
				 struct cl_decoded_option *decoded);
extern void read_cmdline_option (struct gcc_options *opts,
				 struct gcc_options *opts_set,
				 struct cl_decoded_option *decoded,
				 location_t loc,
				 unsigned int lang_mask,
				 const struct cl_option_handlers *handlers,
				 diagnostic_context *dc);
extern void control_warning_option (unsigned int opt_index, int kind,
				    const char *arg, bool imply, location_t loc,
				    unsigned int lang_mask,
				    const struct cl_option_handlers *handlers,
				    struct gcc_options *opts,
				    struct gcc_options *opts_set,
				    diagnostic_context *dc);
extern char *write_langs (unsigned int mask);
extern void print_ignored_options (void);
extern void handle_common_deferred_options (void);
unsigned int parse_sanitizer_options (const char *, location_t, int,
				      unsigned int, int, bool);

unsigned int parse_no_sanitize_attribute (char *value);
extern bool common_handle_option (struct gcc_options *opts,
				  struct gcc_options *opts_set,
				  const struct cl_decoded_option *decoded,
				  unsigned int lang_mask, int kind,
				  location_t loc,
				  const struct cl_option_handlers *handlers,
				  diagnostic_context *dc,
				  void (*target_option_override_hook) (void));
extern bool target_handle_option (struct gcc_options *opts,
				  struct gcc_options *opts_set,
				  const struct cl_decoded_option *decoded,
				  unsigned int lang_mask, int kind,
				  location_t loc,
				  const struct cl_option_handlers *handlers,
				  diagnostic_context *dc,
				  void (*target_option_override_hook) (void));
extern void finish_options (struct gcc_options *opts,
			    struct gcc_options *opts_set,
			    location_t loc);
extern void default_options_optimization (struct gcc_options *opts,
					  struct gcc_options *opts_set,
					  struct cl_decoded_option *decoded_options,
					  unsigned int decoded_options_count,
					  location_t loc,
					  unsigned int lang_mask,
					  const struct cl_option_handlers *handlers,
					  diagnostic_context *dc);
extern void set_struct_debug_option (struct gcc_options *opts,
				     location_t loc,
				     const char *value);
extern bool opt_enum_arg_to_value (size_t opt_index, const char *arg,
				   int *value, unsigned int lang_mask);

extern const struct sanitizer_opts_s
{
  const char *const name;
  unsigned int flag;
  size_t len;
  bool can_recover;
} sanitizer_opts[];

extern void add_misspelling_candidates (auto_vec<char *> *candidates,
					const struct cl_option *option,
					const char *base_option);
extern const char *candidates_list_and_hint (const char *arg, char *&str,
					     const auto_vec <const char *> &
					     candidates);

#endif
