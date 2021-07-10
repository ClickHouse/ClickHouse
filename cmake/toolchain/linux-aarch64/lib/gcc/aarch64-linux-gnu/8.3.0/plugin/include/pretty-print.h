/* Various declarations for language-independent pretty-print subroutines.
   Copyright (C) 2002-2018 Free Software Foundation, Inc.
   Contributed by Gabriel Dos Reis <gdr@integrable-solutions.net>

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

#ifndef GCC_PRETTY_PRINT_H
#define GCC_PRETTY_PRINT_H

#include "obstack.h"

/* Maximum number of format string arguments.  */
#define PP_NL_ARGMAX   30

/* The type of a text to be formatted according a format specification
   along with a list of things.  */
struct text_info
{
  const char *format_spec;
  va_list *args_ptr;
  int err_no;  /* for %m */
  void **x_data;
  rich_location *m_richloc;

  void set_location (unsigned int idx, location_t loc, bool caret_p);
  location_t get_location (unsigned int index_of_location) const;
};

/* How often diagnostics are prefixed by their locations:
   o DIAGNOSTICS_SHOW_PREFIX_NEVER: never - not yet supported;
   o DIAGNOSTICS_SHOW_PREFIX_ONCE: emit only once;
   o DIAGNOSTICS_SHOW_PREFIX_EVERY_LINE: emit each time a physical
   line is started.  */
enum diagnostic_prefixing_rule_t
{
  DIAGNOSTICS_SHOW_PREFIX_ONCE       = 0x0,
  DIAGNOSTICS_SHOW_PREFIX_NEVER      = 0x1,
  DIAGNOSTICS_SHOW_PREFIX_EVERY_LINE = 0x2
};

/* The chunk_info data structure forms a stack of the results from the
   first phase of formatting (pp_format) which have not yet been
   output (pp_output_formatted_text).  A stack is necessary because
   the diagnostic starter may decide to generate its own output by way
   of the formatter.  */
struct chunk_info
{
  /* Pointer to previous chunk on the stack.  */
  struct chunk_info *prev;

  /* Array of chunks to output.  Each chunk is a NUL-terminated string.
     In the first phase of formatting, even-numbered chunks are
     to be output verbatim, odd-numbered chunks are format specifiers.
     The second phase replaces all odd-numbered chunks with formatted
     text, and the third phase simply emits all the chunks in sequence
     with appropriate line-wrapping.  */
  const char *args[PP_NL_ARGMAX * 2];
};

/* The output buffer datatype.  This is best seen as an abstract datatype
   whose fields should not be accessed directly by clients.  */
struct output_buffer
{
  output_buffer ();
  ~output_buffer ();

  /* Obstack where the text is built up.  */
  struct obstack formatted_obstack;

  /* Obstack containing a chunked representation of the format
     specification plus arguments.  */
  struct obstack chunk_obstack;

  /* Currently active obstack: one of the above two.  This is used so
     that the text formatters don't need to know which phase we're in.  */
  struct obstack *obstack;

  /* Stack of chunk arrays.  These come from the chunk_obstack.  */
  struct chunk_info *cur_chunk_array;

  /* Where to output formatted text.  */
  FILE *stream;

  /* The amount of characters output so far.  */
  int line_length;

  /* This must be large enough to hold any printed integer or
     floating-point value.  */
  char digit_buffer[128];

  /* Nonzero means that text should be flushed when
     appropriate. Otherwise, text is buffered until either
     pp_really_flush or pp_clear_output_area are called.  */
  bool flush_p;
};

/* Finishes constructing a NULL-terminated character string representing
   the buffered text.  */
static inline const char *
output_buffer_formatted_text (output_buffer *buff)
{
  obstack_1grow (buff->obstack, '\0');
  return (const char *) obstack_base (buff->obstack);
}

/* Append to the output buffer a string specified by its
   STARTing character and LENGTH.  */
static inline void
output_buffer_append_r (output_buffer *buff, const char *start, int length)
{
  gcc_checking_assert (start);
  obstack_grow (buff->obstack, start, length);
  for (int i = 0; i < length; i++)
    if (start[i] == '\n')
      buff->line_length = 0;
    else
      buff->line_length++;
}

/*  Return a pointer to the last character emitted in the
    output_buffer.  A NULL pointer means no character available.  */
static inline const char *
output_buffer_last_position_in_text (const output_buffer *buff)
{
  const char *p = NULL;
  struct obstack *text = buff->obstack;

  if (obstack_base (text) != obstack_next_free (text))
    p = ((const char *) obstack_next_free (text)) - 1;
  return p;
}


/* The type of pretty-printer flags passed to clients.  */
typedef unsigned int pp_flags;

enum pp_padding
{
  pp_none, pp_before, pp_after
};

/* Structure for switching in and out of verbatim mode in a convenient
   manner.  */
struct pp_wrapping_mode_t
{
  /* Current prefixing rule.  */
  diagnostic_prefixing_rule_t rule;

  /* The ideal upper bound of number of characters per line, as suggested
     by front-end.  */
  int line_cutoff;
};

/* Maximum characters per line in automatic line wrapping mode.
   Zero means don't wrap lines.  */
#define pp_line_cutoff(PP)  (PP)->wrapping.line_cutoff

/* Prefixing rule used in formatting a diagnostic message.  */
#define pp_prefixing_rule(PP)  (PP)->wrapping.rule

/* Get or set the wrapping mode as a single entity.  */
#define pp_wrapping_mode(PP) (PP)->wrapping

/* The type of a hook that formats client-specific data onto a pretty_printer.
   A client-supplied formatter returns true if everything goes well,
   otherwise it returns false.  */
typedef bool (*printer_fn) (pretty_printer *, text_info *, const char *,
			    int, bool, bool, bool, bool *, const char **);

/* Client supplied function used to decode formats.  */
#define pp_format_decoder(PP) (PP)->format_decoder

/* Base class for an optional client-supplied object for doing additional
   processing between stages 2 and 3 of formatted printing.  */
class format_postprocessor
{
 public:
  virtual ~format_postprocessor () {}
  virtual void handle (pretty_printer *) = 0;
};

/* TRUE if a newline character needs to be added before further
   formatting.  */
#define pp_needs_newline(PP)  (PP)->need_newline

/* True if PRETTY-PRINTER is in line-wrapping mode.  */
#define pp_is_wrapping_line(PP) (pp_line_cutoff (PP) > 0)

/* The amount of whitespace to be emitted when starting a new line.  */
#define pp_indentation(PP) (PP)->indent_skip

/* True if identifiers are translated to the locale character set on
   output.  */
#define pp_translate_identifiers(PP) (PP)->translate_identifiers

/* True if colors should be shown.  */
#define pp_show_color(PP) (PP)->show_color

/* The data structure that contains the bare minimum required to do
   proper pretty-printing.  Clients may derived from this structure
   and add additional fields they need.  */
struct pretty_printer
{
  /* Default construct a pretty printer with specified
     maximum line length cut off limit.  */
  explicit pretty_printer (int = 0);

  virtual ~pretty_printer ();

  /* Where we print external representation of ENTITY.  */
  output_buffer *buffer;

  /* The prefix for each new line.  If non-NULL, this is "owned" by the
     pretty_printer, and will eventually be free-ed.  */
  char *prefix;

  /* Where to put whitespace around the entity being formatted.  */
  pp_padding padding;

  /* The real upper bound of number of characters per line, taking into
     account the case of a very very looong prefix.  */
  int maximum_length;

  /* Indentation count.  */
  int indent_skip;

  /* Current wrapping mode.  */
  pp_wrapping_mode_t wrapping;

  /* If non-NULL, this function formats a TEXT into the BUFFER.  When called,
     TEXT->format_spec points to a format code.  FORMAT_DECODER should call
     pp_string (and related functions) to add data to the BUFFER.
     FORMAT_DECODER can read arguments from *TEXT->args_pts using VA_ARG.
     If the BUFFER needs additional characters from the format string, it
     should advance the TEXT->format_spec as it goes.  When FORMAT_DECODER
     returns, TEXT->format_spec should point to the last character processed.
     The QUOTE and BUFFER_PTR are passed in, to allow for deferring-handling
     of format codes (e.g. %H and %I in the C++ frontend).  */
  printer_fn format_decoder;

  /* If non-NULL, this is called by pp_format once after all format codes
     have been processed, to allow for client-specific postprocessing.
     This is used by the C++ frontend for handling the %H and %I
     format codes (which interract with each other).  */
  format_postprocessor *m_format_postprocessor;

  /* Nonzero if current PREFIX was emitted at least once.  */
  bool emitted_prefix;

  /* Nonzero means one should emit a newline before outputting anything.  */
  bool need_newline;

  /* Nonzero means identifiers are translated to the locale character
     set on output.  */
  bool translate_identifiers;

  /* Nonzero means that text should be colorized.  */
  bool show_color;
};

static inline const char *
pp_get_prefix (const pretty_printer *pp) { return pp->prefix; }

#define pp_space(PP)            pp_character (PP, ' ')
#define pp_left_paren(PP)       pp_character (PP, '(')
#define pp_right_paren(PP)      pp_character (PP, ')')
#define pp_left_bracket(PP)     pp_character (PP, '[')
#define pp_right_bracket(PP)    pp_character (PP, ']')
#define pp_left_brace(PP)       pp_character (PP, '{')
#define pp_right_brace(PP)      pp_character (PP, '}')
#define pp_semicolon(PP)        pp_character (PP, ';')
#define pp_comma(PP)            pp_character (PP, ',')
#define pp_dot(PP)              pp_character (PP, '.')
#define pp_colon(PP)            pp_character (PP, ':')
#define pp_colon_colon(PP)      pp_string (PP, "::")
#define pp_arrow(PP)            pp_string (PP, "->")
#define pp_equal(PP)            pp_character (PP, '=')
#define pp_question(PP)         pp_character (PP, '?')
#define pp_bar(PP)              pp_character (PP, '|')
#define pp_bar_bar(PP)          pp_string (PP, "||")
#define pp_carret(PP)           pp_character (PP, '^')
#define pp_ampersand(PP)        pp_character (PP, '&')
#define pp_ampersand_ampersand(PP) pp_string (PP, "&&")
#define pp_less(PP)             pp_character (PP, '<')
#define pp_less_equal(PP)       pp_string (PP, "<=")
#define pp_greater(PP)          pp_character (PP, '>')
#define pp_greater_equal(PP)    pp_string (PP, ">=")
#define pp_plus(PP)             pp_character (PP, '+')
#define pp_minus(PP)            pp_character (PP, '-')
#define pp_star(PP)             pp_character (PP, '*')
#define pp_slash(PP)            pp_character (PP, '/')
#define pp_modulo(PP)           pp_character (PP, '%')
#define pp_exclamation(PP)      pp_character (PP, '!')
#define pp_complement(PP)       pp_character (PP, '~')
#define pp_quote(PP)            pp_character (PP, '\'')
#define pp_backquote(PP)        pp_character (PP, '`')
#define pp_doublequote(PP)      pp_character (PP, '"')
#define pp_underscore(PP)       pp_character (PP, '_')
#define pp_maybe_newline_and_indent(PP, N) \
  if (pp_needs_newline (PP)) pp_newline_and_indent (PP, N)
#define pp_scalar(PP, FORMAT, SCALAR)	                      \
  do					        	      \
    {			         			      \
      sprintf (pp_buffer (PP)->digit_buffer, FORMAT, SCALAR); \
      pp_string (PP, pp_buffer (PP)->digit_buffer);           \
    }						              \
  while (0)
#define pp_decimal_int(PP, I)  pp_scalar (PP, "%d", I)
#define pp_unsigned_wide_integer(PP, I) \
   pp_scalar (PP, HOST_WIDE_INT_PRINT_UNSIGNED, (unsigned HOST_WIDE_INT) I)
#define pp_wide_int(PP, W, SGN)					\
  do								\
    {								\
      print_dec (W, pp_buffer (PP)->digit_buffer, SGN);		\
      pp_string (PP, pp_buffer (PP)->digit_buffer);		\
    }								\
  while (0)
#define pp_pointer(PP, P)      pp_scalar (PP, "%p", P)

#define pp_identifier(PP, ID)  pp_string (PP, (pp_translate_identifiers (PP) \
					  ? identifier_to_locale (ID)	\
					  : (ID)))


#define pp_buffer(PP) (PP)->buffer

extern void pp_set_line_maximum_length (pretty_printer *, int);
extern void pp_set_prefix (pretty_printer *, char *);
extern char *pp_take_prefix (pretty_printer *);
extern void pp_destroy_prefix (pretty_printer *);
extern int pp_remaining_character_count_for_line (pretty_printer *);
extern void pp_clear_output_area (pretty_printer *);
extern const char *pp_formatted_text (pretty_printer *);
extern const char *pp_last_position_in_text (const pretty_printer *);
extern void pp_emit_prefix (pretty_printer *);
extern void pp_append_text (pretty_printer *, const char *, const char *);
extern void pp_newline_and_flush (pretty_printer *);
extern void pp_newline_and_indent (pretty_printer *, int);
extern void pp_separate_with (pretty_printer *, char);

/* If we haven't already defined a front-end-specific diagnostics
   style, use the generic one.  */
#ifdef GCC_DIAG_STYLE
#define GCC_PPDIAG_STYLE GCC_DIAG_STYLE
#else
#define GCC_PPDIAG_STYLE __gcc_diag__
#endif

/* This header may be included before diagnostics-core.h, hence the duplicate
   definitions to allow for GCC-specific formats.  */
#if GCC_VERSION >= 3005
#define ATTRIBUTE_GCC_PPDIAG(m, n) __attribute__ ((__format__ (GCC_PPDIAG_STYLE, m ,n))) ATTRIBUTE_NONNULL(m)
#else
#define ATTRIBUTE_GCC_PPDIAG(m, n) ATTRIBUTE_NONNULL(m)
#endif
extern void pp_printf (pretty_printer *, const char *, ...)
     ATTRIBUTE_GCC_PPDIAG(2,3);

extern void pp_verbatim (pretty_printer *, const char *, ...)
     ATTRIBUTE_GCC_PPDIAG(2,3);
extern void pp_flush (pretty_printer *);
extern void pp_really_flush (pretty_printer *);
extern void pp_format (pretty_printer *, text_info *);
extern void pp_output_formatted_text (pretty_printer *);
extern void pp_format_verbatim (pretty_printer *, text_info *);

extern void pp_indent (pretty_printer *);
extern void pp_newline (pretty_printer *);
extern void pp_character (pretty_printer *, int);
extern void pp_string (pretty_printer *, const char *);
extern void pp_write_text_to_stream (pretty_printer *);
extern void pp_write_text_as_dot_label_to_stream (pretty_printer *, bool);
extern void pp_maybe_space (pretty_printer *);

extern void pp_begin_quote (pretty_printer *, bool);
extern void pp_end_quote (pretty_printer *, bool);

/* Switch into verbatim mode and return the old mode.  */
static inline pp_wrapping_mode_t
pp_set_verbatim_wrapping_ (pretty_printer *pp)
{
  pp_wrapping_mode_t oldmode = pp_wrapping_mode (pp);
  pp_line_cutoff (pp) = 0;
  pp_prefixing_rule (pp) = DIAGNOSTICS_SHOW_PREFIX_NEVER;
  return oldmode;
}
#define pp_set_verbatim_wrapping(PP) pp_set_verbatim_wrapping_ (PP)

extern const char *identifier_to_locale (const char *);
extern void *(*identifier_to_locale_alloc) (size_t);
extern void (*identifier_to_locale_free) (void *);

/* Print I to PP in decimal.  */

inline void
pp_wide_integer (pretty_printer *pp, HOST_WIDE_INT i)
{
  pp_scalar (pp, HOST_WIDE_INT_PRINT_DEC, i);
}

template<unsigned int N, typename T>
void pp_wide_integer (pretty_printer *pp, const poly_int_pod<N, T> &);

#endif /* GCC_PRETTY_PRINT_H */
