/* elfos.h  --  operating system specific defines to be used when
   targeting GCC for some generic ELF system
   Copyright (C) 1991-2018 Free Software Foundation, Inc.
   Based on svr4.h contributed by Ron Guilmette (rfg@netcom.com).

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

Under Section 7 of GPL version 3, you are granted additional
permissions described in the GCC Runtime Library Exception, version
3.1, as published by the Free Software Foundation.

You should have received a copy of the GNU General Public License and
a copy of the GCC Runtime Library Exception along with this program;
see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
<http://www.gnu.org/licenses/>.  */

#define TARGET_OBJFMT_CPP_BUILTINS()		\
  do						\
    {						\
	builtin_define ("__ELF__");		\
    }						\
  while (0)

/* Define a symbol indicating that we are using elfos.h.
   Some CPU specific configuration files use this.  */
#define USING_ELFOS_H

/* The prefix to add to user-visible assembler symbols.

   For ELF systems the convention is *not* to prepend a leading
   underscore onto user-level symbol names.  */

#undef  USER_LABEL_PREFIX
#define USER_LABEL_PREFIX ""

/* The biggest alignment supported by ELF in bits. 32-bit ELF 
   supports section alignment up to (0x80000000 * 8), while 
   64-bit ELF supports (0x8000000000000000 * 8). If this macro 
   is not defined, the default is the largest alignment supported 
   by 32-bit ELF and representable on a 32-bit host. Use this
   macro to limit the alignment which can be specified using
   the `__attribute__ ((aligned (N)))' construct.  */
#ifndef MAX_OFILE_ALIGNMENT
#define MAX_OFILE_ALIGNMENT (((unsigned int) 1 << 28) * 8)
#endif

/* Use periods rather than dollar signs in special g++ assembler names.  */

#define NO_DOLLAR_IN_LABEL

/* Writing `int' for a bit-field forces int alignment for the structure.  */

#ifndef PCC_BITFIELD_TYPE_MATTERS
#define PCC_BITFIELD_TYPE_MATTERS 1
#endif

/* All ELF targets can support DWARF-2.  */

#define DWARF2_DEBUGGING_INFO 1

/* The GNU tools operate better with dwarf2, and it is required by some
   psABI's.  Since we don't have any native tools to be compatible with,
   default to dwarf2.  */

#ifndef PREFERRED_DEBUGGING_TYPE
#define PREFERRED_DEBUGGING_TYPE DWARF2_DEBUG
#endif

/* All SVR4 targets use the ELF object file format.  */
#define OBJECT_FORMAT_ELF


/* Output #ident as a .ident.  */

#undef TARGET_ASM_OUTPUT_IDENT
#define TARGET_ASM_OUTPUT_IDENT default_asm_output_ident_directive

#undef  SET_ASM_OP
#define SET_ASM_OP	"\t.set\t"

/* Most svr4 assemblers want a .file directive at the beginning of
   their input file.  */
#define TARGET_ASM_FILE_START_FILE_DIRECTIVE true

/* This is how to allocate empty space in some section.  The .zero
   pseudo-op is used for this on most svr4 assemblers.  */

#define SKIP_ASM_OP	"\t.zero\t"

#undef  ASM_OUTPUT_SKIP
#define ASM_OUTPUT_SKIP(FILE, SIZE) \
   fprintf ((FILE), "%s" HOST_WIDE_INT_PRINT_UNSIGNED "\n",\
	    SKIP_ASM_OP, (SIZE))

/* This is how to store into the string LABEL
   the symbol_ref name of an internal numbered label where
   PREFIX is the class of label and NUM is the number within the class.
   This is suitable for output with `assemble_name'.

   For most svr4 systems, the convention is that any symbol which begins
   with a period is not put into the linker symbol table by the assembler.  */

#undef  ASM_GENERATE_INTERNAL_LABEL
#define ASM_GENERATE_INTERNAL_LABEL(LABEL, PREFIX, NUM)		\
  do								\
    {								\
      char *__p;						\
      (LABEL)[0] = '*';						\
      (LABEL)[1] = '.';						\
      __p = stpcpy (&(LABEL)[2], PREFIX);			\
      sprint_ul (__p, (unsigned long) (NUM));			\
    }								\
  while (0)

/* Output the label which precedes a jumptable.  Note that for all svr4
   systems where we actually generate jumptables (which is to say every
   svr4 target except i386, where we use casesi instead) we put the jump-
   tables into the .rodata section and since other stuff could have been
   put into the .rodata section prior to any given jumptable, we have to
   make sure that the location counter for the .rodata section gets pro-
   perly re-aligned prior to the actual beginning of the jump table.  */

#undef ALIGN_ASM_OP
#define ALIGN_ASM_OP "\t.align\t"

#ifndef ASM_OUTPUT_BEFORE_CASE_LABEL
#define ASM_OUTPUT_BEFORE_CASE_LABEL(FILE, PREFIX, NUM, TABLE) \
  ASM_OUTPUT_ALIGN ((FILE), 2)
#endif

#undef  ASM_OUTPUT_CASE_LABEL
#define ASM_OUTPUT_CASE_LABEL(FILE, PREFIX, NUM, JUMPTABLE)		\
  do									\
    {									\
      ASM_OUTPUT_BEFORE_CASE_LABEL (FILE, PREFIX, NUM, JUMPTABLE);	\
      (*targetm.asm_out.internal_label) (FILE, PREFIX, NUM);		\
    }									\
  while (0)

/* The standard SVR4 assembler seems to require that certain builtin
   library routines (e.g. .udiv) be explicitly declared as .globl
   in each assembly file where they are referenced.  */

#define ASM_OUTPUT_EXTERNAL_LIBCALL(FILE, FUN)	\
  (*targetm.asm_out.globalize_label) (FILE, XSTR (FUN, 0))

/* This says how to output assembler code to declare an
   uninitialized external linkage data object.  Under SVR4,
   the linker seems to want the alignment of data objects
   to depend on their types.  We do exactly that here.  */

#define COMMON_ASM_OP	"\t.comm\t"

#undef  ASM_OUTPUT_ALIGNED_COMMON
#define ASM_OUTPUT_ALIGNED_COMMON(FILE, NAME, SIZE, ALIGN)		\
  do									\
    {									\
      fprintf ((FILE), "%s", COMMON_ASM_OP);				\
      assemble_name ((FILE), (NAME));					\
      fprintf ((FILE), "," HOST_WIDE_INT_PRINT_UNSIGNED ",%u\n",		\
	       (SIZE), (ALIGN) / BITS_PER_UNIT);			\
    }									\
  while (0)

/* This says how to output assembler code to declare an
   uninitialized internal linkage data object.  Under SVR4,
   the linker seems to want the alignment of data objects
   to depend on their types.  We do exactly that here.  */

#define LOCAL_ASM_OP	"\t.local\t"

#undef  ASM_OUTPUT_ALIGNED_LOCAL
#define ASM_OUTPUT_ALIGNED_LOCAL(FILE, NAME, SIZE, ALIGN)	\
  do								\
    {								\
      fprintf ((FILE), "%s", LOCAL_ASM_OP);			\
      assemble_name ((FILE), (NAME));				\
      fprintf ((FILE), "\n");					\
      ASM_OUTPUT_ALIGNED_COMMON (FILE, NAME, SIZE, ALIGN);	\
    }								\
  while (0)

/* This is the pseudo-op used to generate a contiguous sequence of byte
   values from a double-quoted string WITHOUT HAVING A TERMINATING NUL
   AUTOMATICALLY APPENDED.  This is the same for most svr4 assemblers.  */

#undef  ASCII_DATA_ASM_OP
#define ASCII_DATA_ASM_OP	"\t.ascii\t"

/* Support a read-only data section.  */
#define READONLY_DATA_SECTION_ASM_OP	"\t.section\t.rodata"

/* On svr4, we *do* have support for the .init and .fini sections, and we
   can put stuff in there to be executed before and after `main'.  We let
   crtstuff.c and other files know this by defining the following symbols.
   The definitions say how to change sections to the .init and .fini
   sections.  This is the same for all known svr4 assemblers.  */

#define INIT_SECTION_ASM_OP	"\t.section\t.init"
#define FINI_SECTION_ASM_OP	"\t.section\t.fini"

/* Output assembly directive to move to the beginning of current section.  */
#ifdef HAVE_GAS_SUBSECTION_ORDERING
# define ASM_SECTION_START_OP	"\t.subsection\t-1"
# define ASM_OUTPUT_SECTION_START(FILE)	\
  fprintf ((FILE), "%s\n", ASM_SECTION_START_OP)
#endif

#define MAKE_DECL_ONE_ONLY(DECL) (DECL_WEAK (DECL) = 1)

/* Switch into a generic section.  */
#define TARGET_ASM_NAMED_SECTION  default_elf_asm_named_section

#undef  TARGET_ASM_SELECT_RTX_SECTION
#define TARGET_ASM_SELECT_RTX_SECTION default_elf_select_rtx_section
#undef	TARGET_ASM_SELECT_SECTION
#define TARGET_ASM_SELECT_SECTION default_elf_select_section
#undef  TARGET_HAVE_SWITCHABLE_BSS_SECTIONS
#define TARGET_HAVE_SWITCHABLE_BSS_SECTIONS true

/* Define the strings used for the special svr4 .type and .size directives.
   These strings generally do not vary from one system running svr4 to
   another, but if a given system (e.g. m88k running svr) needs to use
   different pseudo-op names for these, they may be overridden in the
   file which includes this one.  */

#define TYPE_ASM_OP	"\t.type\t"
#define SIZE_ASM_OP	"\t.size\t"

/* This is how we tell the assembler that a symbol is weak.  */

#define ASM_WEAKEN_LABEL(FILE, NAME)	\
  do					\
    {					\
      fputs ("\t.weak\t", (FILE));	\
      assemble_name ((FILE), (NAME));	\
      fputc ('\n', (FILE));		\
    }					\
  while (0)

/* The following macro defines the format used to output the second
   operand of the .type assembler directive.  Different svr4 assemblers
   expect various different forms for this operand.  The one given here
   is just a default.  You may need to override it in your machine-
   specific tm.h file (depending upon the particulars of your assembler).  */

#define TYPE_OPERAND_FMT	"@%s"

/* Write the extra assembler code needed to declare a function's result.
   Most svr4 assemblers don't require any special declaration of the
   result value, but there are exceptions.  */

#ifndef ASM_DECLARE_RESULT
#define ASM_DECLARE_RESULT(FILE, RESULT)
#endif

/* These macros generate the special .type and .size directives which
   are used to set the corresponding fields of the linker symbol table
   entries in an ELF object file under SVR4.  These macros also output
   the starting labels for the relevant functions/objects.  */

/* Write the extra assembler code needed to declare a function properly.
   Some svr4 assemblers need to also have something extra said about the
   function's return value.  We allow for that here.  */

#ifndef ASM_DECLARE_FUNCTION_NAME
#define ASM_DECLARE_FUNCTION_NAME(FILE, NAME, DECL)		\
  do								\
    {								\
      ASM_OUTPUT_TYPE_DIRECTIVE (FILE, NAME, "function");	\
      ASM_DECLARE_RESULT (FILE, DECL_RESULT (DECL));		\
      ASM_OUTPUT_FUNCTION_LABEL (FILE, NAME, DECL);		\
    }								\
  while (0)
#endif

/* Write the extra assembler code needed to declare the name of a
   cold function partition properly. Some svr4 assemblers need to also
   have something extra said about the function's return value.  We
   allow for that here.  */

#ifndef ASM_DECLARE_COLD_FUNCTION_NAME
#define ASM_DECLARE_COLD_FUNCTION_NAME(FILE, NAME, DECL)	\
  do								\
    {								\
      ASM_OUTPUT_TYPE_DIRECTIVE (FILE, NAME, "function");	\
      ASM_DECLARE_RESULT (FILE, DECL_RESULT (DECL));		\
      ASM_OUTPUT_FUNCTION_LABEL (FILE, NAME, DECL);		\
    }								\
  while (0)
#endif

/* Write the extra assembler code needed to declare an object properly.  */

#ifdef HAVE_GAS_GNU_UNIQUE_OBJECT
#define USE_GNU_UNIQUE_OBJECT flag_gnu_unique
#else
#define USE_GNU_UNIQUE_OBJECT 0
#endif

#define ASM_DECLARE_OBJECT_NAME(FILE, NAME, DECL)			\
  do									\
    {									\
      HOST_WIDE_INT size;						\
									\
      /* For template static data member instantiations or		\
	 inline fn local statics and their guard variables, use		\
	 gnu_unique_object so that they will be combined even under	\
	 RTLD_LOCAL.  Don't use gnu_unique_object for typeinfo,		\
	 vtables and other read-only artificial decls.  */		\
      if (USE_GNU_UNIQUE_OBJECT && DECL_ONE_ONLY (DECL)			\
	  && (!DECL_ARTIFICIAL (DECL) || !TREE_READONLY (DECL)))	\
	ASM_OUTPUT_TYPE_DIRECTIVE (FILE, NAME, "gnu_unique_object");	\
      else								\
	ASM_OUTPUT_TYPE_DIRECTIVE (FILE, NAME, "object");		\
									\
      size_directive_output = 0;					\
      if (!flag_inhibit_size_directive					\
	  && (DECL) && DECL_SIZE (DECL))				\
	{								\
	  size_directive_output = 1;					\
	  size = tree_to_uhwi (DECL_SIZE_UNIT (DECL));			\
	  ASM_OUTPUT_SIZE_DIRECTIVE (FILE, NAME, size);			\
	}								\
									\
      ASM_OUTPUT_LABEL (FILE, NAME);					\
    }									\
  while (0)

/* Output the size directive for a decl in rest_of_decl_compilation
   in the case where we did not do so before the initializer.
   Once we find the error_mark_node, we know that the value of
   size_directive_output was set
   by ASM_DECLARE_OBJECT_NAME when it was run for the same decl.  */

#undef ASM_FINISH_DECLARE_OBJECT
#define ASM_FINISH_DECLARE_OBJECT(FILE, DECL, TOP_LEVEL, AT_END)\
  do								\
    {								\
      const char *name = XSTR (XEXP (DECL_RTL (DECL), 0), 0);	\
      HOST_WIDE_INT size;					\
								\
      if (!flag_inhibit_size_directive				\
	  && DECL_SIZE (DECL)					\
	  && ! AT_END && TOP_LEVEL				\
	  && DECL_INITIAL (DECL) == error_mark_node		\
	  && !size_directive_output)				\
	{							\
	  size_directive_output = 1;				\
	  size = tree_to_uhwi (DECL_SIZE_UNIT (DECL));		\
	  ASM_OUTPUT_SIZE_DIRECTIVE (FILE, name, size);		\
	}							\
    }								\
  while (0)

/* This is how to declare the size of a function.  */
#ifndef ASM_DECLARE_FUNCTION_SIZE
#define ASM_DECLARE_FUNCTION_SIZE(FILE, FNAME, DECL)		\
  do								\
    {								\
      if (!flag_inhibit_size_directive)				\
	ASM_OUTPUT_MEASURED_SIZE (FILE, FNAME);			\
    }								\
  while (0)
#endif

/* This is how to declare the size of a cold function partition.  */
#ifndef ASM_DECLARE_COLD_FUNCTION_SIZE
#define ASM_DECLARE_COLD_FUNCTION_SIZE(FILE, FNAME, DECL)	\
  do								\
    {								\
      if (!flag_inhibit_size_directive)				\
	ASM_OUTPUT_MEASURED_SIZE (FILE, FNAME);			\
    }								\
  while (0)
#endif

/* A table of bytes codes used by the ASM_OUTPUT_ASCII and
   ASM_OUTPUT_LIMITED_STRING macros.  Each byte in the table
   corresponds to a particular byte value [0..255].  For any
   given byte value, if the value in the corresponding table
   position is zero, the given character can be output directly.
   If the table value is 1, the byte must be output as a \ooo
   octal escape.  If the tables value is anything else, then the
   byte value should be output as a \ followed by the value
   in the table.  Note that we can use standard UN*X escape
   sequences for many control characters, but we don't use
   \a to represent BEL because some svr4 assemblers (e.g. on
   the i386) don't know about that.  Also, we don't use \v
   since some versions of gas, such as 2.2 did not accept it.  */

#define ELF_ASCII_ESCAPES \
"\1\1\1\1\1\1\1\1btn\1fr\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\
\0\0\"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\\\0\0\0\
\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1\
\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\
\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\
\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\
\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1\1"

/* Some svr4 assemblers have a limit on the number of characters which
   can appear in the operand of a .string directive.  If your assembler
   has such a limitation, you should define STRING_LIMIT to reflect that
   limit.  Note that at least some svr4 assemblers have a limit on the
   actual number of bytes in the double-quoted string, and that they
   count each character in an escape sequence as one byte.  Thus, an
   escape sequence like \377 would count as four bytes.

   If your target assembler doesn't support the .string directive, you
   should define this to zero.
*/

#define ELF_STRING_LIMIT	((unsigned) 256)

#define STRING_ASM_OP	"\t.string\t"

/* The routine used to output NUL terminated strings.  We use a special
   version of this for most svr4 targets because doing so makes the
   generated assembly code more compact (and thus faster to assemble)
   as well as more readable, especially for targets like the i386
   (where the only alternative is to output character sequences as
   comma separated lists of numbers).  */

#define ASM_OUTPUT_LIMITED_STRING(FILE, STR)		\
  default_elf_asm_output_limited_string ((FILE), (STR))

/* The routine used to output sequences of byte values.  We use a special
   version of this for most svr4 targets because doing so makes the
   generated assembly code more compact (and thus faster to assemble)
   as well as more readable.  Note that if we find subparts of the
   character sequence which end with NUL (and which are shorter than
   STRING_LIMIT) we output those using ASM_OUTPUT_LIMITED_STRING.  */

#undef  ASM_OUTPUT_ASCII
#define ASM_OUTPUT_ASCII(FILE, STR, LENGTH)			\
  default_elf_asm_output_ascii ((FILE), (STR), (LENGTH))

/* Allow the use of the -frecord-gcc-switches switch via the
   elf_record_gcc_switches function defined in varasm.c.  */
#undef  TARGET_ASM_RECORD_GCC_SWITCHES
#define TARGET_ASM_RECORD_GCC_SWITCHES elf_record_gcc_switches

/* A C statement (sans semicolon) to output to the stdio stream STREAM
   any text necessary for declaring the name of an external symbol
   named NAME which is referenced in this compilation but not defined.
   It is needed to properly support non-default visibility.  */

#ifndef ASM_OUTPUT_EXTERNAL
#define ASM_OUTPUT_EXTERNAL(FILE, DECL, NAME) \
  default_elf_asm_output_external (FILE, DECL, NAME)
#endif

#undef TARGET_LIBC_HAS_FUNCTION
#define TARGET_LIBC_HAS_FUNCTION no_c99_libc_has_function
