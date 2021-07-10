/* Definitions of various defaults for tm.h macros.
   Copyright (C) 1992-2018 Free Software Foundation, Inc.
   Contributed by Ron Guilmette (rfg@monkeys.com)

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

Under Section 7 of GPL version 3, you are granted additional
permissions described in the GCC Runtime Library Exception, version
3.1, as published by the Free Software Foundation.

You should have received a copy of the GNU General Public License and
a copy of the GCC Runtime Library Exception along with this program;
see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_DEFAULTS_H
#define GCC_DEFAULTS_H

/* How to start an assembler comment.  */
#ifndef ASM_COMMENT_START
#define ASM_COMMENT_START ";#"
#endif

/* Store in OUTPUT a string (made with alloca) containing an
   assembler-name for a local static variable or function named NAME.
   LABELNO is an integer which is different for each call.  */

#ifndef ASM_PN_FORMAT
# ifndef NO_DOT_IN_LABEL
#  define ASM_PN_FORMAT "%s.%lu"
# else
#  ifndef NO_DOLLAR_IN_LABEL
#   define ASM_PN_FORMAT "%s$%lu"
#  else
#   define ASM_PN_FORMAT "__%s_%lu"
#  endif
# endif
#endif /* ! ASM_PN_FORMAT */

#ifndef ASM_FORMAT_PRIVATE_NAME
# define ASM_FORMAT_PRIVATE_NAME(OUTPUT, NAME, LABELNO) \
  do { const char *const name_ = (NAME); \
       char *const output_ = (OUTPUT) = \
	 (char *) alloca (strlen (name_) + 32); \
       sprintf (output_, ASM_PN_FORMAT, name_, (unsigned long)(LABELNO)); \
  } while (0)
#endif

/* Choose a reasonable default for ASM_OUTPUT_ASCII.  */

#ifndef ASM_OUTPUT_ASCII
#define ASM_OUTPUT_ASCII(MYFILE, MYSTRING, MYLENGTH) \
  do {									      \
    FILE *_hide_asm_out_file = (MYFILE);				      \
    const unsigned char *_hide_p = (const unsigned char *) (MYSTRING);	      \
    int _hide_thissize = (MYLENGTH);					      \
    {									      \
      FILE *asm_out_file = _hide_asm_out_file;				      \
      const unsigned char *p = _hide_p;					      \
      int thissize = _hide_thissize;					      \
      int i;								      \
      fprintf (asm_out_file, "\t.ascii \"");				      \
									      \
      for (i = 0; i < thissize; i++)					      \
	{								      \
	  int c = p[i];			   				      \
	  if (c == '\"' || c == '\\')					      \
	    putc ('\\', asm_out_file);					      \
	  if (ISPRINT (c))						      \
	    putc (c, asm_out_file);					      \
	  else								      \
	    {								      \
	      fprintf (asm_out_file, "\\%o", c);			      \
	      /* After an octal-escape, if a digit follows,		      \
		 terminate one string constant and start another.	      \
		 The VAX assembler fails to stop reading the escape	      \
		 after three digits, so this is the only way we		      \
		 can get it to parse the data properly.  */		      \
	      if (i < thissize - 1 && ISDIGIT (p[i + 1]))		      \
		fprintf (asm_out_file, "\"\n\t.ascii \"");		      \
	  }								      \
	}								      \
      fprintf (asm_out_file, "\"\n");					      \
    }									      \
  }									      \
  while (0)
#endif

/* This is how we tell the assembler to equate two values.  */
#ifdef SET_ASM_OP
#ifndef ASM_OUTPUT_DEF
#define ASM_OUTPUT_DEF(FILE,LABEL1,LABEL2)				\
 do {	fprintf ((FILE), "%s", SET_ASM_OP);				\
	assemble_name (FILE, LABEL1);					\
	fprintf (FILE, ",");						\
	assemble_name (FILE, LABEL2);					\
	fprintf (FILE, "\n");						\
  } while (0)
#endif
#endif

#ifndef IFUNC_ASM_TYPE
#define IFUNC_ASM_TYPE "gnu_indirect_function"
#endif

#ifndef TLS_COMMON_ASM_OP
#define TLS_COMMON_ASM_OP ".tls_common"
#endif

#if defined (HAVE_AS_TLS) && !defined (ASM_OUTPUT_TLS_COMMON)
#define ASM_OUTPUT_TLS_COMMON(FILE, DECL, NAME, SIZE)			\
  do									\
    {									\
      fprintf ((FILE), "\t%s\t", TLS_COMMON_ASM_OP);			\
      assemble_name ((FILE), (NAME));					\
      fprintf ((FILE), "," HOST_WIDE_INT_PRINT_UNSIGNED",%u\n",		\
	       (SIZE), DECL_ALIGN (DECL) / BITS_PER_UNIT);		\
    }									\
  while (0)
#endif

/* Decide whether to defer emitting the assembler output for an equate
   of two values.  The default is to not defer output.  */
#ifndef TARGET_DEFERRED_OUTPUT_DEFS
#define TARGET_DEFERRED_OUTPUT_DEFS(DECL,TARGET) false
#endif

/* This is how to output the definition of a user-level label named
   NAME, such as the label on variable NAME.  */

#ifndef ASM_OUTPUT_LABEL
#define ASM_OUTPUT_LABEL(FILE,NAME) \
  do {						\
    assemble_name ((FILE), (NAME));		\
    fputs (":\n", (FILE));			\
  } while (0)
#endif

/* This is how to output the definition of a user-level label named
   NAME, such as the label on a function.  */

#ifndef ASM_OUTPUT_FUNCTION_LABEL
#define ASM_OUTPUT_FUNCTION_LABEL(FILE, NAME, DECL) \
  ASM_OUTPUT_LABEL ((FILE), (NAME))
#endif

/* Output the definition of a compiler-generated label named NAME.  */
#ifndef ASM_OUTPUT_INTERNAL_LABEL
#define ASM_OUTPUT_INTERNAL_LABEL(FILE,NAME)	\
  do {						\
    assemble_name_raw ((FILE), (NAME));		\
    fputs (":\n", (FILE));			\
  } while (0)
#endif

/* This is how to output a reference to a user-level label named NAME.  */

#ifndef ASM_OUTPUT_LABELREF
#define ASM_OUTPUT_LABELREF(FILE,NAME)  \
  do {							\
    fputs (user_label_prefix, (FILE));			\
    fputs ((NAME), (FILE));				\
  } while (0)
#endif

/* Allow target to print debug info labels specially.  This is useful for
   VLIW targets, since debug info labels should go into the middle of
   instruction bundles instead of breaking them.  */

#ifndef ASM_OUTPUT_DEBUG_LABEL
#define ASM_OUTPUT_DEBUG_LABEL(FILE, PREFIX, NUM) \
  (*targetm.asm_out.internal_label) (FILE, PREFIX, NUM)
#endif

/* This is how we tell the assembler that a symbol is weak.  */
#ifndef ASM_OUTPUT_WEAK_ALIAS
#if defined (ASM_WEAKEN_LABEL) && defined (ASM_OUTPUT_DEF)
#define ASM_OUTPUT_WEAK_ALIAS(STREAM, NAME, VALUE)	\
  do							\
    {							\
      ASM_WEAKEN_LABEL (STREAM, NAME);			\
      if (VALUE)					\
        ASM_OUTPUT_DEF (STREAM, NAME, VALUE);		\
    }							\
  while (0)
#endif
#endif

/* This is how we tell the assembler that a symbol is a weak alias to
   another symbol that doesn't require the other symbol to be defined.
   Uses of the former will turn into weak uses of the latter, i.e.,
   uses that, in case the latter is undefined, will not cause errors,
   and will add it to the symbol table as weak undefined.  However, if
   the latter is referenced directly, a strong reference prevails.  */
#ifndef ASM_OUTPUT_WEAKREF
#if defined HAVE_GAS_WEAKREF
#define ASM_OUTPUT_WEAKREF(FILE, DECL, NAME, VALUE)			\
  do									\
    {									\
      fprintf ((FILE), "\t.weakref\t");					\
      assemble_name ((FILE), (NAME));					\
      fprintf ((FILE), ",");						\
      assemble_name ((FILE), (VALUE));					\
      fprintf ((FILE), "\n");						\
    }									\
  while (0)
#endif
#endif

/* How to emit a .type directive.  */
#ifndef ASM_OUTPUT_TYPE_DIRECTIVE
#if defined TYPE_ASM_OP && defined TYPE_OPERAND_FMT
#define ASM_OUTPUT_TYPE_DIRECTIVE(STREAM, NAME, TYPE)	\
  do							\
    {							\
      fputs (TYPE_ASM_OP, STREAM);			\
      assemble_name (STREAM, NAME);			\
      fputs (", ", STREAM);				\
      fprintf (STREAM, TYPE_OPERAND_FMT, TYPE);		\
      putc ('\n', STREAM);				\
    }							\
  while (0)
#endif
#endif

/* How to emit a .size directive.  */
#ifndef ASM_OUTPUT_SIZE_DIRECTIVE
#ifdef SIZE_ASM_OP
#define ASM_OUTPUT_SIZE_DIRECTIVE(STREAM, NAME, SIZE)	\
  do							\
    {							\
      HOST_WIDE_INT size_ = (SIZE);			\
      fputs (SIZE_ASM_OP, STREAM);			\
      assemble_name (STREAM, NAME);			\
      fprintf (STREAM, ", " HOST_WIDE_INT_PRINT_DEC "\n", size_); \
    }							\
  while (0)

#define ASM_OUTPUT_MEASURED_SIZE(STREAM, NAME)		\
  do							\
    {							\
      fputs (SIZE_ASM_OP, STREAM);			\
      assemble_name (STREAM, NAME);			\
      fputs (", .-", STREAM);				\
      assemble_name (STREAM, NAME);			\
      putc ('\n', STREAM);				\
    }							\
  while (0)

#endif
#endif

/* This determines whether or not we support weak symbols.  SUPPORTS_WEAK
   must be a preprocessor constant.  */
#ifndef SUPPORTS_WEAK
#if defined (ASM_WEAKEN_LABEL) || defined (ASM_WEAKEN_DECL)
#define SUPPORTS_WEAK 1
#else
#define SUPPORTS_WEAK 0
#endif
#endif

/* This determines whether or not we support weak symbols during target
   code generation.  TARGET_SUPPORTS_WEAK can be any valid C expression.  */
#ifndef TARGET_SUPPORTS_WEAK
#define TARGET_SUPPORTS_WEAK (SUPPORTS_WEAK)
#endif

/* This determines whether or not we support the discriminator
   attribute in the .loc directive.  */
#ifndef SUPPORTS_DISCRIMINATOR
#ifdef HAVE_GAS_DISCRIMINATOR
#define SUPPORTS_DISCRIMINATOR 1
#else
#define SUPPORTS_DISCRIMINATOR 0
#endif
#endif

/* This determines whether or not we support link-once semantics.  */
#ifndef SUPPORTS_ONE_ONLY
#ifdef MAKE_DECL_ONE_ONLY
#define SUPPORTS_ONE_ONLY 1
#else
#define SUPPORTS_ONE_ONLY 0
#endif
#endif

/* This determines whether weak symbols must be left out of a static
   archive's table of contents.  Defining this macro to be nonzero has
   the consequence that certain symbols will not be made weak that
   otherwise would be.  The C++ ABI requires this macro to be zero;
   see the documentation.  */
#ifndef TARGET_WEAK_NOT_IN_ARCHIVE_TOC
#define TARGET_WEAK_NOT_IN_ARCHIVE_TOC 0
#endif

/* This determines whether or not we need linkonce unwind information.  */
#ifndef TARGET_USES_WEAK_UNWIND_INFO
#define TARGET_USES_WEAK_UNWIND_INFO 0
#endif

/* By default, there is no prefix on user-defined symbols.  */
#ifndef USER_LABEL_PREFIX
#define USER_LABEL_PREFIX ""
#endif

/* If the target supports weak symbols, define TARGET_ATTRIBUTE_WEAK to
   provide a weak attribute.  Else define it to nothing.

   This would normally belong in ansidecl.h, but SUPPORTS_WEAK is
   not available at that time.

   Note, this is only for use by target files which we know are to be
   compiled by GCC.  */
#ifndef TARGET_ATTRIBUTE_WEAK
# if SUPPORTS_WEAK
#  define TARGET_ATTRIBUTE_WEAK __attribute__ ((weak))
# else
#  define TARGET_ATTRIBUTE_WEAK
# endif
#endif

/* By default we can assume that all global symbols are in one namespace,
   across all shared libraries.  */
#ifndef MULTIPLE_SYMBOL_SPACES
# define MULTIPLE_SYMBOL_SPACES 0
#endif

/* If the target supports init_priority C++ attribute, give
   SUPPORTS_INIT_PRIORITY a nonzero value.  */
#ifndef SUPPORTS_INIT_PRIORITY
#define SUPPORTS_INIT_PRIORITY 1
#endif /* SUPPORTS_INIT_PRIORITY */

/* If we have a definition of INCOMING_RETURN_ADDR_RTX, assume that
   the rest of the DWARF 2 frame unwind support is also provided.  */
#if !defined (DWARF2_UNWIND_INFO) && defined (INCOMING_RETURN_ADDR_RTX)
#define DWARF2_UNWIND_INFO 1
#endif

/* If we have named sections, and we're using crtstuff to run ctors,
   use them for registering eh frame information.  */
#if defined (TARGET_ASM_NAMED_SECTION) && DWARF2_UNWIND_INFO \
    && !defined (EH_FRAME_THROUGH_COLLECT2)
#ifndef EH_FRAME_SECTION_NAME
#define EH_FRAME_SECTION_NAME ".eh_frame"
#endif
#endif

/* On many systems, different EH table encodings are used under
   difference circumstances.  Some will require runtime relocations;
   some will not.  For those that do not require runtime relocations,
   we would like to make the table read-only.  However, since the
   read-only tables may need to be combined with read-write tables
   that do require runtime relocation, it is not safe to make the
   tables read-only unless the linker will merge read-only and
   read-write sections into a single read-write section.  If your
   linker does not have this ability, but your system is such that no
   encoding used with non-PIC code will ever require a runtime
   relocation, then you can define EH_TABLES_CAN_BE_READ_ONLY to 1 in
   your target configuration file.  */
#ifndef EH_TABLES_CAN_BE_READ_ONLY
#ifdef HAVE_LD_RO_RW_SECTION_MIXING
#define EH_TABLES_CAN_BE_READ_ONLY 1
#else
#define EH_TABLES_CAN_BE_READ_ONLY 0
#endif
#endif

/* Provide defaults for stuff that may not be defined when using
   sjlj exceptions.  */
#ifndef EH_RETURN_DATA_REGNO
#define EH_RETURN_DATA_REGNO(N) INVALID_REGNUM
#endif

/* Offset between the eh handler address and entry in eh tables.  */
#ifndef RETURN_ADDR_OFFSET
#define RETURN_ADDR_OFFSET 0
#endif

#ifndef MASK_RETURN_ADDR
#define MASK_RETURN_ADDR NULL_RTX
#endif

/* Number of hardware registers that go into the DWARF-2 unwind info.
   If not defined, equals FIRST_PSEUDO_REGISTER  */

#ifndef DWARF_FRAME_REGISTERS
#define DWARF_FRAME_REGISTERS FIRST_PSEUDO_REGISTER
#endif

/* Offsets recorded in opcodes are a multiple of this alignment factor.  */
#ifndef DWARF_CIE_DATA_ALIGNMENT
#ifdef STACK_GROWS_DOWNWARD
#define DWARF_CIE_DATA_ALIGNMENT (-((int) UNITS_PER_WORD))
#else
#define DWARF_CIE_DATA_ALIGNMENT ((int) UNITS_PER_WORD)
#endif
#endif

/* The DWARF 2 CFA column which tracks the return address.  Normally this
   is the column for PC, or the first column after all of the hard
   registers.  */
#ifndef DWARF_FRAME_RETURN_COLUMN
#ifdef PC_REGNUM
#define DWARF_FRAME_RETURN_COLUMN	DWARF_FRAME_REGNUM (PC_REGNUM)
#else
#define DWARF_FRAME_RETURN_COLUMN	DWARF_FRAME_REGISTERS
#endif
#endif

/* How to renumber registers for dbx and gdb.  If not defined, assume
   no renumbering is necessary.  */

#ifndef DBX_REGISTER_NUMBER
#define DBX_REGISTER_NUMBER(REGNO) (REGNO)
#endif

/* The mapping from gcc register number to DWARF 2 CFA column number.
   By default, we just provide columns for all registers.  */
#ifndef DWARF_FRAME_REGNUM
#define DWARF_FRAME_REGNUM(REG) DBX_REGISTER_NUMBER (REG)
#endif

/* The mapping from dwarf CFA reg number to internal dwarf reg numbers.  */
#ifndef DWARF_REG_TO_UNWIND_COLUMN
#define DWARF_REG_TO_UNWIND_COLUMN(REGNO) (REGNO)
#endif

/* Map register numbers held in the call frame info that gcc has
   collected using DWARF_FRAME_REGNUM to those that should be output in
   .debug_frame and .eh_frame.  */
#ifndef DWARF2_FRAME_REG_OUT
#define DWARF2_FRAME_REG_OUT(REGNO, FOR_EH) (REGNO)
#endif

/* The size of addresses as they appear in the Dwarf 2 data.
   Some architectures use word addresses to refer to code locations,
   but Dwarf 2 info always uses byte addresses.  On such machines,
   Dwarf 2 addresses need to be larger than the architecture's
   pointers.  */
#ifndef DWARF2_ADDR_SIZE
#define DWARF2_ADDR_SIZE ((POINTER_SIZE + BITS_PER_UNIT - 1) / BITS_PER_UNIT)
#endif

/* The size in bytes of a DWARF field indicating an offset or length
   relative to a debug info section, specified to be 4 bytes in the
   DWARF-2 specification.  The SGI/MIPS ABI defines it to be the same
   as PTR_SIZE.  */
#ifndef DWARF_OFFSET_SIZE
#define DWARF_OFFSET_SIZE 4
#endif

/* The size in bytes of a DWARF 4 type signature.  */
#ifndef DWARF_TYPE_SIGNATURE_SIZE
#define DWARF_TYPE_SIGNATURE_SIZE 8
#endif

/* Default sizes for base C types.  If the sizes are different for
   your target, you should override these values by defining the
   appropriate symbols in your tm.h file.  */

#ifndef BITS_PER_WORD
#define BITS_PER_WORD (BITS_PER_UNIT * UNITS_PER_WORD)
#endif

#ifndef CHAR_TYPE_SIZE
#define CHAR_TYPE_SIZE BITS_PER_UNIT
#endif

#ifndef BOOL_TYPE_SIZE
/* `bool' has size and alignment `1', on almost all platforms.  */
#define BOOL_TYPE_SIZE CHAR_TYPE_SIZE
#endif

#ifndef SHORT_TYPE_SIZE
#define SHORT_TYPE_SIZE (BITS_PER_UNIT * MIN ((UNITS_PER_WORD + 1) / 2, 2))
#endif

#ifndef INT_TYPE_SIZE
#define INT_TYPE_SIZE BITS_PER_WORD
#endif

#ifndef LONG_TYPE_SIZE
#define LONG_TYPE_SIZE BITS_PER_WORD
#endif

#ifndef LONG_LONG_TYPE_SIZE
#define LONG_LONG_TYPE_SIZE (BITS_PER_WORD * 2)
#endif

#ifndef WCHAR_TYPE_SIZE
#define WCHAR_TYPE_SIZE INT_TYPE_SIZE
#endif

#ifndef FLOAT_TYPE_SIZE
#define FLOAT_TYPE_SIZE BITS_PER_WORD
#endif

#ifndef DOUBLE_TYPE_SIZE
#define DOUBLE_TYPE_SIZE (BITS_PER_WORD * 2)
#endif

#ifndef LONG_DOUBLE_TYPE_SIZE
#define LONG_DOUBLE_TYPE_SIZE (BITS_PER_WORD * 2)
#endif

#ifndef DECIMAL32_TYPE_SIZE
#define DECIMAL32_TYPE_SIZE 32
#endif

#ifndef DECIMAL64_TYPE_SIZE
#define DECIMAL64_TYPE_SIZE 64
#endif

#ifndef DECIMAL128_TYPE_SIZE
#define DECIMAL128_TYPE_SIZE 128
#endif

#ifndef SHORT_FRACT_TYPE_SIZE
#define SHORT_FRACT_TYPE_SIZE BITS_PER_UNIT
#endif

#ifndef FRACT_TYPE_SIZE
#define FRACT_TYPE_SIZE (BITS_PER_UNIT * 2)
#endif

#ifndef LONG_FRACT_TYPE_SIZE
#define LONG_FRACT_TYPE_SIZE (BITS_PER_UNIT * 4)
#endif

#ifndef LONG_LONG_FRACT_TYPE_SIZE
#define LONG_LONG_FRACT_TYPE_SIZE (BITS_PER_UNIT * 8)
#endif

#ifndef SHORT_ACCUM_TYPE_SIZE
#define SHORT_ACCUM_TYPE_SIZE (SHORT_FRACT_TYPE_SIZE * 2)
#endif

#ifndef ACCUM_TYPE_SIZE
#define ACCUM_TYPE_SIZE (FRACT_TYPE_SIZE * 2)
#endif

#ifndef LONG_ACCUM_TYPE_SIZE
#define LONG_ACCUM_TYPE_SIZE (LONG_FRACT_TYPE_SIZE * 2)
#endif

#ifndef LONG_LONG_ACCUM_TYPE_SIZE
#define LONG_LONG_ACCUM_TYPE_SIZE (LONG_LONG_FRACT_TYPE_SIZE * 2)
#endif

/* We let tm.h override the types used here, to handle trivial differences
   such as the choice of unsigned int or long unsigned int for size_t.
   When machines start needing nontrivial differences in the size type,
   it would be best to do something here to figure out automatically
   from other information what type to use.  */

#ifndef SIZE_TYPE
#define SIZE_TYPE "long unsigned int"
#endif

#ifndef SIZETYPE
#define SIZETYPE SIZE_TYPE
#endif

#ifndef PID_TYPE
#define PID_TYPE "int"
#endif

/* If GCC knows the exact uint_least16_t and uint_least32_t types from
   <stdint.h>, use them for char16_t and char32_t.  Otherwise, use
   these guesses; getting the wrong type of a given width will not
   affect C++ name mangling because in C++ these are distinct types
   not typedefs.  */

#ifdef UINT_LEAST16_TYPE
#define CHAR16_TYPE UINT_LEAST16_TYPE
#else
#define CHAR16_TYPE "short unsigned int"
#endif

#ifdef UINT_LEAST32_TYPE
#define CHAR32_TYPE UINT_LEAST32_TYPE
#else
#define CHAR32_TYPE "unsigned int"
#endif

#ifndef WCHAR_TYPE
#define WCHAR_TYPE "int"
#endif

/* WCHAR_TYPE gets overridden by -fshort-wchar.  */
#define MODIFIED_WCHAR_TYPE \
	(flag_short_wchar ? "short unsigned int" : WCHAR_TYPE)

#ifndef PTRDIFF_TYPE
#define PTRDIFF_TYPE "long int"
#endif

#ifndef WINT_TYPE
#define WINT_TYPE "unsigned int"
#endif

#ifndef INTMAX_TYPE
#define INTMAX_TYPE ((INT_TYPE_SIZE == LONG_LONG_TYPE_SIZE)	\
		     ? "int"					\
		     : ((LONG_TYPE_SIZE == LONG_LONG_TYPE_SIZE)	\
			? "long int"				\
			: "long long int"))
#endif

#ifndef UINTMAX_TYPE
#define UINTMAX_TYPE ((INT_TYPE_SIZE == LONG_LONG_TYPE_SIZE)	\
		     ? "unsigned int"				\
		     : ((LONG_TYPE_SIZE == LONG_LONG_TYPE_SIZE)	\
			? "long unsigned int"			\
			: "long long unsigned int"))
#endif


/* There are no default definitions of these <stdint.h> types.  */

#ifndef SIG_ATOMIC_TYPE
#define SIG_ATOMIC_TYPE ((const char *) NULL)
#endif

#ifndef INT8_TYPE
#define INT8_TYPE ((const char *) NULL)
#endif

#ifndef INT16_TYPE
#define INT16_TYPE ((const char *) NULL)
#endif

#ifndef INT32_TYPE
#define INT32_TYPE ((const char *) NULL)
#endif

#ifndef INT64_TYPE
#define INT64_TYPE ((const char *) NULL)
#endif

#ifndef UINT8_TYPE
#define UINT8_TYPE ((const char *) NULL)
#endif

#ifndef UINT16_TYPE
#define UINT16_TYPE ((const char *) NULL)
#endif

#ifndef UINT32_TYPE
#define UINT32_TYPE ((const char *) NULL)
#endif

#ifndef UINT64_TYPE
#define UINT64_TYPE ((const char *) NULL)
#endif

#ifndef INT_LEAST8_TYPE
#define INT_LEAST8_TYPE ((const char *) NULL)
#endif

#ifndef INT_LEAST16_TYPE
#define INT_LEAST16_TYPE ((const char *) NULL)
#endif

#ifndef INT_LEAST32_TYPE
#define INT_LEAST32_TYPE ((const char *) NULL)
#endif

#ifndef INT_LEAST64_TYPE
#define INT_LEAST64_TYPE ((const char *) NULL)
#endif

#ifndef UINT_LEAST8_TYPE
#define UINT_LEAST8_TYPE ((const char *) NULL)
#endif

#ifndef UINT_LEAST16_TYPE
#define UINT_LEAST16_TYPE ((const char *) NULL)
#endif

#ifndef UINT_LEAST32_TYPE
#define UINT_LEAST32_TYPE ((const char *) NULL)
#endif

#ifndef UINT_LEAST64_TYPE
#define UINT_LEAST64_TYPE ((const char *) NULL)
#endif

#ifndef INT_FAST8_TYPE
#define INT_FAST8_TYPE ((const char *) NULL)
#endif

#ifndef INT_FAST16_TYPE
#define INT_FAST16_TYPE ((const char *) NULL)
#endif

#ifndef INT_FAST32_TYPE
#define INT_FAST32_TYPE ((const char *) NULL)
#endif

#ifndef INT_FAST64_TYPE
#define INT_FAST64_TYPE ((const char *) NULL)
#endif

#ifndef UINT_FAST8_TYPE
#define UINT_FAST8_TYPE ((const char *) NULL)
#endif

#ifndef UINT_FAST16_TYPE
#define UINT_FAST16_TYPE ((const char *) NULL)
#endif

#ifndef UINT_FAST32_TYPE
#define UINT_FAST32_TYPE ((const char *) NULL)
#endif

#ifndef UINT_FAST64_TYPE
#define UINT_FAST64_TYPE ((const char *) NULL)
#endif

#ifndef INTPTR_TYPE
#define INTPTR_TYPE ((const char *) NULL)
#endif

#ifndef UINTPTR_TYPE
#define UINTPTR_TYPE ((const char *) NULL)
#endif

/* Width in bits of a pointer.  Mind the value of the macro `Pmode'.  */
#ifndef POINTER_SIZE
#define POINTER_SIZE BITS_PER_WORD
#endif
#ifndef POINTER_SIZE_UNITS
#define POINTER_SIZE_UNITS ((POINTER_SIZE + BITS_PER_UNIT - 1) / BITS_PER_UNIT)
#endif


#ifndef PIC_OFFSET_TABLE_REGNUM
#define PIC_OFFSET_TABLE_REGNUM INVALID_REGNUM
#endif

#ifndef PIC_OFFSET_TABLE_REG_CALL_CLOBBERED
#define PIC_OFFSET_TABLE_REG_CALL_CLOBBERED 0
#endif

#ifndef TARGET_DLLIMPORT_DECL_ATTRIBUTES
#define TARGET_DLLIMPORT_DECL_ATTRIBUTES 0
#endif

#ifndef TARGET_DECLSPEC
#if TARGET_DLLIMPORT_DECL_ATTRIBUTES
/* If the target supports the "dllimport" attribute, users are
   probably used to the "__declspec" syntax.  */
#define TARGET_DECLSPEC 1
#else
#define TARGET_DECLSPEC 0
#endif
#endif

/* By default, the preprocessor should be invoked the same way in C++
   as in C.  */
#ifndef CPLUSPLUS_CPP_SPEC
#ifdef CPP_SPEC
#define CPLUSPLUS_CPP_SPEC CPP_SPEC
#endif
#endif

#ifndef ACCUMULATE_OUTGOING_ARGS
#define ACCUMULATE_OUTGOING_ARGS 0
#endif

/* By default, use the GNU runtime for Objective C.  */
#ifndef NEXT_OBJC_RUNTIME
#define NEXT_OBJC_RUNTIME 0
#endif

/* Supply a default definition for PUSH_ARGS.  */
#ifndef PUSH_ARGS
#ifdef PUSH_ROUNDING
#define PUSH_ARGS	!ACCUMULATE_OUTGOING_ARGS
#else
#define PUSH_ARGS	0
#endif
#endif

/* Decide whether a function's arguments should be processed
   from first to last or from last to first.

   They should if the stack and args grow in opposite directions, but
   only if we have push insns.  */

#ifdef PUSH_ROUNDING

#ifndef PUSH_ARGS_REVERSED
#if defined (STACK_GROWS_DOWNWARD) != defined (ARGS_GROW_DOWNWARD)
#define PUSH_ARGS_REVERSED  PUSH_ARGS
#endif
#endif

#endif

#ifndef PUSH_ARGS_REVERSED
#define PUSH_ARGS_REVERSED 0
#endif

/* Default value for the alignment (in bits) a C conformant malloc has to
   provide. This default is intended to be safe and always correct.  */
#ifndef MALLOC_ABI_ALIGNMENT
#define MALLOC_ABI_ALIGNMENT BITS_PER_WORD
#endif

/* If PREFERRED_STACK_BOUNDARY is not defined, set it to STACK_BOUNDARY.
   STACK_BOUNDARY is required.  */
#ifndef PREFERRED_STACK_BOUNDARY
#define PREFERRED_STACK_BOUNDARY STACK_BOUNDARY
#endif

/* Set INCOMING_STACK_BOUNDARY to PREFERRED_STACK_BOUNDARY if it is not
   defined.  */
#ifndef INCOMING_STACK_BOUNDARY
#define INCOMING_STACK_BOUNDARY PREFERRED_STACK_BOUNDARY
#endif

#ifndef TARGET_DEFAULT_PACK_STRUCT
#define TARGET_DEFAULT_PACK_STRUCT 0
#endif

/* By default, the vtable entries are void pointers, the so the alignment
   is the same as pointer alignment.  The value of this macro specifies
   the alignment of the vtable entry in bits.  It should be defined only
   when special alignment is necessary.  */
#ifndef TARGET_VTABLE_ENTRY_ALIGN
#define TARGET_VTABLE_ENTRY_ALIGN POINTER_SIZE
#endif

/* There are a few non-descriptor entries in the vtable at offsets below
   zero.  If these entries must be padded (say, to preserve the alignment
   specified by TARGET_VTABLE_ENTRY_ALIGN), set this to the number of
   words in each data entry.  */
#ifndef TARGET_VTABLE_DATA_ENTRY_DISTANCE
#define TARGET_VTABLE_DATA_ENTRY_DISTANCE 1
#endif

/* Decide whether it is safe to use a local alias for a virtual function
   when constructing thunks.  */
#ifndef TARGET_USE_LOCAL_THUNK_ALIAS_P
#ifdef ASM_OUTPUT_DEF
#define TARGET_USE_LOCAL_THUNK_ALIAS_P(DECL) 1
#else
#define TARGET_USE_LOCAL_THUNK_ALIAS_P(DECL) 0
#endif
#endif

/* Decide whether target supports aliases.  */
#ifndef TARGET_SUPPORTS_ALIASES
#ifdef ASM_OUTPUT_DEF
#define TARGET_SUPPORTS_ALIASES 1
#else
#define TARGET_SUPPORTS_ALIASES 0
#endif
#endif

/* Select a format to encode pointers in exception handling data.  We
   prefer those that result in fewer dynamic relocations.  Assume no
   special support here and encode direct references.  */
#ifndef ASM_PREFERRED_EH_DATA_FORMAT
#define ASM_PREFERRED_EH_DATA_FORMAT(CODE,GLOBAL)  DW_EH_PE_absptr
#endif

/* By default, the C++ compiler will use the lowest bit of the pointer
   to function to indicate a pointer-to-member-function points to a
   virtual member function.  However, if FUNCTION_BOUNDARY indicates
   function addresses aren't always even, the lowest bit of the delta
   field will be used.  */
#ifndef TARGET_PTRMEMFUNC_VBIT_LOCATION
#define TARGET_PTRMEMFUNC_VBIT_LOCATION \
  (FUNCTION_BOUNDARY >= 2 * BITS_PER_UNIT \
   ? ptrmemfunc_vbit_in_pfn : ptrmemfunc_vbit_in_delta)
#endif

#ifndef DEFAULT_GDB_EXTENSIONS
#define DEFAULT_GDB_EXTENSIONS 1
#endif

/* If more than one debugging type is supported, you must define
   PREFERRED_DEBUGGING_TYPE to choose the default.  */

#if 1 < (defined (DBX_DEBUGGING_INFO) \
         + defined (DWARF2_DEBUGGING_INFO) + defined (XCOFF_DEBUGGING_INFO) \
         + defined (VMS_DEBUGGING_INFO))
#ifndef PREFERRED_DEBUGGING_TYPE
#error You must define PREFERRED_DEBUGGING_TYPE
#endif /* no PREFERRED_DEBUGGING_TYPE */

/* If only one debugging format is supported, define PREFERRED_DEBUGGING_TYPE
   here so other code needn't care.  */
#elif defined DBX_DEBUGGING_INFO
#define PREFERRED_DEBUGGING_TYPE DBX_DEBUG

#elif defined DWARF2_DEBUGGING_INFO || defined DWARF2_LINENO_DEBUGGING_INFO
#define PREFERRED_DEBUGGING_TYPE DWARF2_DEBUG

#elif defined VMS_DEBUGGING_INFO
#define PREFERRED_DEBUGGING_TYPE VMS_AND_DWARF2_DEBUG

#elif defined XCOFF_DEBUGGING_INFO
#define PREFERRED_DEBUGGING_TYPE XCOFF_DEBUG

#else
/* No debugging format is supported by this target.  */
#define PREFERRED_DEBUGGING_TYPE NO_DEBUG
#endif

#ifndef FLOAT_LIB_COMPARE_RETURNS_BOOL
#define FLOAT_LIB_COMPARE_RETURNS_BOOL(MODE, COMPARISON) false
#endif

/* True if the targets integer-comparison functions return { 0, 1, 2
   } to indicate { <, ==, > }.  False if { -1, 0, 1 } is used
   instead.  The libgcc routines are biased.  */
#ifndef TARGET_LIB_INT_CMP_BIASED
#define TARGET_LIB_INT_CMP_BIASED (true)
#endif

/* If FLOAT_WORDS_BIG_ENDIAN is not defined in the header files,
   then the word-endianness is the same as for integers.  */
#ifndef FLOAT_WORDS_BIG_ENDIAN
#define FLOAT_WORDS_BIG_ENDIAN WORDS_BIG_ENDIAN
#endif

#ifndef REG_WORDS_BIG_ENDIAN
#define REG_WORDS_BIG_ENDIAN WORDS_BIG_ENDIAN
#endif


#ifndef TARGET_DEC_EVAL_METHOD
#define TARGET_DEC_EVAL_METHOD 2
#endif

#ifndef HAS_LONG_COND_BRANCH
#define HAS_LONG_COND_BRANCH 0
#endif

#ifndef HAS_LONG_UNCOND_BRANCH
#define HAS_LONG_UNCOND_BRANCH 0
#endif

/* Determine whether __cxa_atexit, rather than atexit, is used to
   register C++ destructors for local statics and global objects.  */
#ifndef DEFAULT_USE_CXA_ATEXIT
#define DEFAULT_USE_CXA_ATEXIT 0
#endif

#if GCC_VERSION >= 3000 && defined IN_GCC
/* These old constraint macros shouldn't appear anywhere in a
   configuration using MD constraint definitions.  */
#endif

/* Determin whether the target runtime library is Bionic */
#ifndef TARGET_HAS_BIONIC
#define TARGET_HAS_BIONIC 0
#endif

/* Indicate that CLZ and CTZ are undefined at zero.  */
#ifndef CLZ_DEFINED_VALUE_AT_ZERO
#define CLZ_DEFINED_VALUE_AT_ZERO(MODE, VALUE)  0
#endif
#ifndef CTZ_DEFINED_VALUE_AT_ZERO
#define CTZ_DEFINED_VALUE_AT_ZERO(MODE, VALUE)  0
#endif

/* Provide a default value for STORE_FLAG_VALUE.  */
#ifndef STORE_FLAG_VALUE
#define STORE_FLAG_VALUE  1
#endif

/* This macro is used to determine what the largest unit size that
   move_by_pieces can use is.  */

/* MOVE_MAX_PIECES is the number of bytes at a time which we can
   move efficiently, as opposed to  MOVE_MAX which is the maximum
   number of bytes we can move with a single instruction.  */

#ifndef MOVE_MAX_PIECES
#define MOVE_MAX_PIECES   MOVE_MAX
#endif

/* STORE_MAX_PIECES is the number of bytes at a time that we can
   store efficiently.  Due to internal GCC limitations, this is
   MOVE_MAX_PIECES limited by the number of bytes GCC can represent
   for an immediate constant.  */

#ifndef STORE_MAX_PIECES
#define STORE_MAX_PIECES  MIN (MOVE_MAX_PIECES, 2 * sizeof (HOST_WIDE_INT))
#endif

/* Likewise for block comparisons.  */
#ifndef COMPARE_MAX_PIECES
#define COMPARE_MAX_PIECES  MOVE_MAX_PIECES
#endif

#ifndef MAX_MOVE_MAX
#define MAX_MOVE_MAX MOVE_MAX
#endif

#ifndef MIN_UNITS_PER_WORD
#define MIN_UNITS_PER_WORD UNITS_PER_WORD
#endif

#ifndef MAX_BITS_PER_WORD
#define MAX_BITS_PER_WORD BITS_PER_WORD
#endif

#ifndef STACK_POINTER_OFFSET
#define STACK_POINTER_OFFSET    0
#endif

#ifndef LOCAL_REGNO
#define LOCAL_REGNO(REGNO)  0
#endif

#ifndef HONOR_REG_ALLOC_ORDER
#define HONOR_REG_ALLOC_ORDER 0
#endif

/* EXIT_IGNORE_STACK should be nonzero if, when returning from a function,
   the stack pointer does not matter.  The value is tested only in
   functions that have frame pointers.  */
#ifndef EXIT_IGNORE_STACK
#define EXIT_IGNORE_STACK 0
#endif

/* Assume that case vectors are not pc-relative.  */
#ifndef CASE_VECTOR_PC_RELATIVE
#define CASE_VECTOR_PC_RELATIVE 0
#endif

/* Force minimum alignment to be able to use the least significant bits
   for distinguishing descriptor addresses from code addresses.  */
#define FUNCTION_ALIGNMENT(ALIGN)					\
  (lang_hooks.custom_function_descriptors				\
   && targetm.calls.custom_function_descriptors > 0			\
   ? MAX ((ALIGN),						\
	  2 * targetm.calls.custom_function_descriptors * BITS_PER_UNIT)\
   : (ALIGN))

/* Assume that trampolines need function alignment.  */
#ifndef TRAMPOLINE_ALIGNMENT
#define TRAMPOLINE_ALIGNMENT FUNCTION_ALIGNMENT (FUNCTION_BOUNDARY)
#endif

/* Register mappings for target machines without register windows.  */
#ifndef INCOMING_REGNO
#define INCOMING_REGNO(N) (N)
#endif

#ifndef OUTGOING_REGNO
#define OUTGOING_REGNO(N) (N)
#endif

#ifndef SHIFT_COUNT_TRUNCATED
#define SHIFT_COUNT_TRUNCATED 0
#endif

#ifndef LEGITIMATE_PIC_OPERAND_P
#define LEGITIMATE_PIC_OPERAND_P(X) 1
#endif

#ifndef TARGET_MEM_CONSTRAINT
#define TARGET_MEM_CONSTRAINT 'm'
#endif

#ifndef REVERSIBLE_CC_MODE
#define REVERSIBLE_CC_MODE(MODE) 0
#endif

/* Biggest alignment supported by the object file format of this machine.  */
#ifndef MAX_OFILE_ALIGNMENT
#define MAX_OFILE_ALIGNMENT BIGGEST_ALIGNMENT
#endif

#ifndef FRAME_GROWS_DOWNWARD
#define FRAME_GROWS_DOWNWARD 0
#endif

#ifndef RETURN_ADDR_IN_PREVIOUS_FRAME
#define RETURN_ADDR_IN_PREVIOUS_FRAME 0
#endif

/* On most machines, the CFA coincides with the first incoming parm.  */
#ifndef ARG_POINTER_CFA_OFFSET
#define ARG_POINTER_CFA_OFFSET(FNDECL) \
  (FIRST_PARM_OFFSET (FNDECL) + crtl->args.pretend_args_size)
#endif

/* On most machines, we use the CFA as DW_AT_frame_base.  */
#ifndef CFA_FRAME_BASE_OFFSET
#define CFA_FRAME_BASE_OFFSET(FNDECL) 0
#endif

/* The offset from the incoming value of %sp to the top of the stack frame
   for the current function.  */
#ifndef INCOMING_FRAME_SP_OFFSET
#define INCOMING_FRAME_SP_OFFSET 0
#endif

#ifndef HARD_REGNO_NREGS_HAS_PADDING
#define HARD_REGNO_NREGS_HAS_PADDING(REGNO, MODE) 0
#define HARD_REGNO_NREGS_WITH_PADDING(REGNO, MODE) -1
#endif

#ifndef OUTGOING_REG_PARM_STACK_SPACE
#define OUTGOING_REG_PARM_STACK_SPACE(FNTYPE) 0
#endif

/* MAX_STACK_ALIGNMENT is the maximum stack alignment guaranteed by
   the backend.  MAX_SUPPORTED_STACK_ALIGNMENT is the maximum best
   effort stack alignment supported by the backend.  If the backend
   supports stack alignment, MAX_SUPPORTED_STACK_ALIGNMENT and
   MAX_STACK_ALIGNMENT are the same.  Otherwise, the incoming stack
   boundary will limit the maximum guaranteed stack alignment.  */
#ifdef MAX_STACK_ALIGNMENT
#define MAX_SUPPORTED_STACK_ALIGNMENT MAX_STACK_ALIGNMENT
#else
#define MAX_STACK_ALIGNMENT STACK_BOUNDARY
#define MAX_SUPPORTED_STACK_ALIGNMENT PREFERRED_STACK_BOUNDARY
#endif

#define SUPPORTS_STACK_ALIGNMENT (MAX_STACK_ALIGNMENT > STACK_BOUNDARY)

#ifndef LOCAL_ALIGNMENT
#define LOCAL_ALIGNMENT(TYPE, ALIGNMENT) ALIGNMENT
#endif

#ifndef STACK_SLOT_ALIGNMENT
#define STACK_SLOT_ALIGNMENT(TYPE,MODE,ALIGN) \
  ((TYPE) ? LOCAL_ALIGNMENT ((TYPE), (ALIGN)) : (ALIGN))
#endif

#ifndef LOCAL_DECL_ALIGNMENT
#define LOCAL_DECL_ALIGNMENT(DECL) \
  LOCAL_ALIGNMENT (TREE_TYPE (DECL), DECL_ALIGN (DECL))
#endif

#ifndef MINIMUM_ALIGNMENT
#define MINIMUM_ALIGNMENT(EXP,MODE,ALIGN) (ALIGN)
#endif

/* Alignment value for attribute ((aligned)).  */
#ifndef ATTRIBUTE_ALIGNED_VALUE
#define ATTRIBUTE_ALIGNED_VALUE BIGGEST_ALIGNMENT
#endif

/* For most ports anything that evaluates to a constant symbolic
   or integer value is acceptable as a constant address.  */
#ifndef CONSTANT_ADDRESS_P
#define CONSTANT_ADDRESS_P(X)   (CONSTANT_P (X) && GET_CODE (X) != CONST_DOUBLE)
#endif

#ifndef MAX_FIXED_MODE_SIZE
#define MAX_FIXED_MODE_SIZE GET_MODE_BITSIZE (DImode)
#endif

/* Nonzero if structures and unions should be returned in memory.

   This should only be defined if compatibility with another compiler or
   with an ABI is needed, because it results in slower code.  */

#ifndef DEFAULT_PCC_STRUCT_RETURN
#define DEFAULT_PCC_STRUCT_RETURN 1
#endif

#ifndef PCC_BITFIELD_TYPE_MATTERS
#define PCC_BITFIELD_TYPE_MATTERS false
#endif

#ifndef INSN_SETS_ARE_DELAYED
#define INSN_SETS_ARE_DELAYED(INSN) false
#endif

#ifndef INSN_REFERENCES_ARE_DELAYED
#define INSN_REFERENCES_ARE_DELAYED(INSN) false
#endif

#ifndef NO_FUNCTION_CSE
#define NO_FUNCTION_CSE false
#endif

#ifndef HARD_REGNO_RENAME_OK
#define HARD_REGNO_RENAME_OK(FROM, TO) true
#endif

#ifndef EPILOGUE_USES
#define EPILOGUE_USES(REG) false
#endif

#ifndef ARGS_GROW_DOWNWARD
#define ARGS_GROW_DOWNWARD 0
#endif

#ifndef STACK_GROWS_DOWNWARD
#define STACK_GROWS_DOWNWARD 0
#endif

#ifndef STACK_PUSH_CODE
#if STACK_GROWS_DOWNWARD
#define STACK_PUSH_CODE PRE_DEC
#else
#define STACK_PUSH_CODE PRE_INC
#endif
#endif

/* Default value for flag_pie when flag_pie is initialized to -1:
   --enable-default-pie: Default flag_pie to -fPIE.
   --disable-default-pie: Default flag_pie to 0.
 */
#ifdef ENABLE_DEFAULT_PIE
# ifndef DEFAULT_FLAG_PIE
#  define DEFAULT_FLAG_PIE 2
# endif
#else
# define DEFAULT_FLAG_PIE 0
#endif

#ifndef SWITCHABLE_TARGET
#define SWITCHABLE_TARGET 0
#endif

/* If the target supports integers that are wider than two
   HOST_WIDE_INTs on the host compiler, then the target should define
   TARGET_SUPPORTS_WIDE_INT and make the appropriate fixups.
   Otherwise the compiler really is not robust.  */
#ifndef TARGET_SUPPORTS_WIDE_INT
#define TARGET_SUPPORTS_WIDE_INT 0
#endif

#ifndef SHORT_IMMEDIATES_SIGN_EXTEND
#define SHORT_IMMEDIATES_SIGN_EXTEND 0
#endif

#ifndef WORD_REGISTER_OPERATIONS
#define WORD_REGISTER_OPERATIONS 0
#endif

#ifndef LOAD_EXTEND_OP
#define LOAD_EXTEND_OP(M) UNKNOWN
#endif

#ifndef INITIAL_FRAME_ADDRESS_RTX
#define INITIAL_FRAME_ADDRESS_RTX NULL
#endif

#ifndef SETUP_FRAME_ADDRESSES
#define SETUP_FRAME_ADDRESSES() do { } while (0)
#endif

#ifndef DYNAMIC_CHAIN_ADDRESS
#define DYNAMIC_CHAIN_ADDRESS(x) (x)
#endif

#ifndef FRAME_ADDR_RTX
#define FRAME_ADDR_RTX(x) (x)
#endif

#ifndef REVERSE_CONDITION
#define REVERSE_CONDITION(code, mode) reverse_condition (code)
#endif

#ifndef TARGET_PECOFF
#define TARGET_PECOFF 0
#endif

#ifndef TARGET_COFF
#define TARGET_COFF 0
#endif

#ifndef EH_RETURN_HANDLER_RTX
#define EH_RETURN_HANDLER_RTX NULL
#endif

#ifdef GCC_INSN_FLAGS_H
/* Dependent default target macro definitions

   This section of defaults.h defines target macros that depend on generated
   headers.  This is a bit awkward:  We want to put all default definitions
   for target macros in defaults.h, but some of the defaults depend on the
   HAVE_* flags defines of insn-flags.h.  But insn-flags.h is not always
   included by files that do include defaults.h.

   Fortunately, the default macro definitions that depend on the HAVE_*
   macros are also the ones that will only be used inside GCC itself, i.e.
   not in the gen* programs or in target objects like libgcc.

   Obviously, it would be best to keep this section of defaults.h as small
   as possible, by converting the macros defined below to target hooks or
   functions.
*/

/* The default branch cost is 1.  */
#ifndef BRANCH_COST
#define BRANCH_COST(speed_p, predictable_p) 1
#endif

/* If a memory-to-memory move would take MOVE_RATIO or more simple
   move-instruction sequences, we will do a movmem or libcall instead.  */

#ifndef MOVE_RATIO
#if defined (HAVE_movmemqi) || defined (HAVE_movmemhi) || defined (HAVE_movmemsi) || defined (HAVE_movmemdi) || defined (HAVE_movmemti)
#define MOVE_RATIO(speed) 2
#else
/* If we are optimizing for space (-Os), cut down the default move ratio.  */
#define MOVE_RATIO(speed) ((speed) ? 15 : 3)
#endif
#endif

/* If a clear memory operation would take CLEAR_RATIO or more simple
   move-instruction sequences, we will do a setmem or libcall instead.  */

#ifndef CLEAR_RATIO
#if defined (HAVE_setmemqi) || defined (HAVE_setmemhi) || defined (HAVE_setmemsi) || defined (HAVE_setmemdi) || defined (HAVE_setmemti)
#define CLEAR_RATIO(speed) 2
#else
/* If we are optimizing for space, cut down the default clear ratio.  */
#define CLEAR_RATIO(speed) ((speed) ? 15 :3)
#endif
#endif

/* If a memory set (to value other than zero) operation would take
   SET_RATIO or more simple move-instruction sequences, we will do a movmem
   or libcall instead.  */
#ifndef SET_RATIO
#define SET_RATIO(speed) MOVE_RATIO (speed)
#endif

/* Supply a default definition of STACK_SAVEAREA_MODE for emit_stack_save.
   Normally move_insn, so Pmode stack pointer.  */

#ifndef STACK_SAVEAREA_MODE
#define STACK_SAVEAREA_MODE(LEVEL) Pmode
#endif

/* Supply a default definition of STACK_SIZE_MODE for
   allocate_dynamic_stack_space.  Normally PLUS/MINUS, so word_mode.  */

#ifndef STACK_SIZE_MODE
#define STACK_SIZE_MODE word_mode
#endif

/* Default value for flag_stack_protect when flag_stack_protect is initialized to -1:
   --enable-default-ssp: Default flag_stack_protect to -fstack-protector-strong.
   --disable-default-ssp: Default flag_stack_protect to 0.
 */
#ifdef ENABLE_DEFAULT_SSP
# ifndef DEFAULT_FLAG_SSP
#  define DEFAULT_FLAG_SSP 3
# endif
#else
# define DEFAULT_FLAG_SSP 0
#endif

/* Provide default values for the macros controlling stack checking.  */

/* The default is neither full builtin stack checking...  */
#ifndef STACK_CHECK_BUILTIN
#define STACK_CHECK_BUILTIN 0
#endif

/* ...nor static builtin stack checking.  */
#ifndef STACK_CHECK_STATIC_BUILTIN
#define STACK_CHECK_STATIC_BUILTIN 0
#endif

/* The default interval is one page (4096 bytes).  */
#ifndef STACK_CHECK_PROBE_INTERVAL_EXP
#define STACK_CHECK_PROBE_INTERVAL_EXP 12
#endif

/* The default is not to move the stack pointer.  */
#ifndef STACK_CHECK_MOVING_SP
#define STACK_CHECK_MOVING_SP 0
#endif

/* This is a kludge to try to capture the discrepancy between the old
   mechanism (generic stack checking) and the new mechanism (static
   builtin stack checking).  STACK_CHECK_PROTECT needs to be bumped
   for the latter because part of the protection area is effectively
   included in STACK_CHECK_MAX_FRAME_SIZE for the former.  */
#ifdef STACK_CHECK_PROTECT
#define STACK_OLD_CHECK_PROTECT STACK_CHECK_PROTECT
#else
#define STACK_OLD_CHECK_PROTECT						\
 (!global_options.x_flag_exceptions					\
  ? 75 * UNITS_PER_WORD							\
  : targetm_common.except_unwind_info (&global_options) == UI_SJLJ	\
    ? 4 * 1024								\
    : 8 * 1024)
#endif

/* Minimum amount of stack required to recover from an anticipated stack
   overflow detection.  The default value conveys an estimate of the amount
   of stack required to propagate an exception.  */
#ifndef STACK_CHECK_PROTECT
#define STACK_CHECK_PROTECT						\
 (!global_options.x_flag_exceptions					\
  ? 4 * 1024								\
  : targetm_common.except_unwind_info (&global_options) == UI_SJLJ	\
    ? 8 * 1024								\
    : 12 * 1024)
#endif

/* Make the maximum frame size be the largest we can and still only need
   one probe per function.  */
#ifndef STACK_CHECK_MAX_FRAME_SIZE
#define STACK_CHECK_MAX_FRAME_SIZE \
  ((1 << STACK_CHECK_PROBE_INTERVAL_EXP) - UNITS_PER_WORD)
#endif

/* This is arbitrary, but should be large enough everywhere.  */
#ifndef STACK_CHECK_FIXED_FRAME_SIZE
#define STACK_CHECK_FIXED_FRAME_SIZE (4 * UNITS_PER_WORD)
#endif

/* Provide a reasonable default for the maximum size of an object to
   allocate in the fixed frame.  We may need to be able to make this
   controllable by the user at some point.  */
#ifndef STACK_CHECK_MAX_VAR_SIZE
#define STACK_CHECK_MAX_VAR_SIZE (STACK_CHECK_MAX_FRAME_SIZE / 100)
#endif

/* By default, the C++ compiler will use function addresses in the
   vtable entries.  Setting this nonzero tells the compiler to use
   function descriptors instead.  The value of this macro says how
   many words wide the descriptor is (normally 2).  It is assumed
   that the address of a function descriptor may be treated as a
   pointer to a function.  */
#ifndef TARGET_VTABLE_USES_DESCRIPTORS
#define TARGET_VTABLE_USES_DESCRIPTORS 0
#endif

#endif /* GCC_INSN_FLAGS_H  */

#ifndef DWARF_GNAT_ENCODINGS_DEFAULT
#define DWARF_GNAT_ENCODINGS_DEFAULT DWARF_GNAT_ENCODINGS_GDB
#endif

#endif  /* ! GCC_DEFAULTS_H */
