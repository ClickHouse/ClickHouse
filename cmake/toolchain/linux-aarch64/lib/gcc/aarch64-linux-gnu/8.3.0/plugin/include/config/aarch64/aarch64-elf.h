/* Machine description for AArch64 architecture.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.
   Contributed by ARM Ltd.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_AARCH64_ELF_H
#define GCC_AARCH64_ELF_H


#define ASM_OUTPUT_LABELREF(FILE, NAME) \
  aarch64_asm_output_labelref (FILE, NAME)

#define TEXT_SECTION_ASM_OP	"\t.text"
#define DATA_SECTION_ASM_OP	"\t.data"
#define BSS_SECTION_ASM_OP	"\t.bss"

#define CTORS_SECTION_ASM_OP "\t.section\t.init_array,\"aw\",%init_array"
#define DTORS_SECTION_ASM_OP "\t.section\t.fini_array,\"aw\",%fini_array"

#undef INIT_SECTION_ASM_OP
#undef FINI_SECTION_ASM_OP
#define INIT_ARRAY_SECTION_ASM_OP CTORS_SECTION_ASM_OP
#define FINI_ARRAY_SECTION_ASM_OP DTORS_SECTION_ASM_OP

/* Since we use .init_array/.fini_array we don't need the markers at
   the start and end of the ctors/dtors arrays.  */
#define CTOR_LIST_BEGIN asm (CTORS_SECTION_ASM_OP)
#define CTOR_LIST_END		/* empty */
#define DTOR_LIST_BEGIN asm (DTORS_SECTION_ASM_OP)
#define DTOR_LIST_END		/* empty */

#undef TARGET_ASM_CONSTRUCTOR
#define TARGET_ASM_CONSTRUCTOR aarch64_elf_asm_constructor

#undef TARGET_ASM_DESTRUCTOR
#define TARGET_ASM_DESTRUCTOR aarch64_elf_asm_destructor

#ifdef HAVE_GAS_MAX_SKIP_P2ALIGN
/* Support for -falign-* switches.  Use .p2align to ensure that code
   sections are padded with NOP instructions, rather than zeros.  */
#define ASM_OUTPUT_MAX_SKIP_ALIGN(FILE, LOG, MAX_SKIP)		\
  do								\
    {								\
      if ((LOG) != 0)						\
	{							\
	  if ((MAX_SKIP) == 0)					\
	    fprintf ((FILE), "\t.p2align %d\n", (int) (LOG));	\
	  else							\
	    fprintf ((FILE), "\t.p2align %d,,%d\n",		\
		     (int) (LOG), (int) (MAX_SKIP));		\
	}							\
    } while (0)

#endif /* HAVE_GAS_MAX_SKIP_P2ALIGN */

#define JUMP_TABLES_IN_TEXT_SECTION 0

#define ASM_OUTPUT_ADDR_DIFF_ELT(STREAM, BODY, VALUE, REL)		\
  do {									\
    switch (GET_MODE (BODY))						\
      {									\
      case E_QImode:							\
	asm_fprintf (STREAM, "\t.byte\t(%LL%d - %LLrtx%d) / 4\n",	\
		     VALUE, REL);					\
	break;								\
      case E_HImode:							\
	asm_fprintf (STREAM, "\t.2byte\t(%LL%d - %LLrtx%d) / 4\n",	\
		     VALUE, REL);					\
	break;								\
      case E_SImode:							\
      case E_DImode: /* See comment in aarch64_output_casesi.  */		\
	asm_fprintf (STREAM, "\t.word\t(%LL%d - %LLrtx%d) / 4\n",	\
		     VALUE, REL);					\
	break;								\
      default:								\
	gcc_unreachable ();						\
      }									\
  } while (0)

#define ASM_OUTPUT_ALIGN(STREAM, POWER)		\
  fprintf(STREAM, "\t.align\t%d\n", (int)POWER)

#define ASM_COMMENT_START "//"

#define LOCAL_LABEL_PREFIX	"."
#define USER_LABEL_PREFIX	""

#define GLOBAL_ASM_OP "\t.global\t"

#ifdef TARGET_BIG_ENDIAN_DEFAULT
#define ENDIAN_SPEC "-mbig-endian"
#else
#define ENDIAN_SPEC "-mlittle-endian"
#endif

#if TARGET_DATA_MODEL == 1
#define ABI_SPEC  "-mabi=lp64"
#define MULTILIB_DEFAULTS { "mabi=lp64" }
#elif TARGET_DATA_MODEL == 2
#define ABI_SPEC  "-mabi=ilp32"
#define MULTILIB_DEFAULTS { "mabi=ilp32" }
#else
#error "Unknown or undefined TARGET_DATA_MODEL!"
#endif

/* Force the default endianness and ABI flags onto the command line
   in order to make the other specs easier to write.  */
#undef DRIVER_SELF_SPECS
#define DRIVER_SELF_SPECS \
  " %{!mbig-endian:%{!mlittle-endian:" ENDIAN_SPEC "}}" \
  " %{!mabi=*:" ABI_SPEC "}" \
  MCPU_MTUNE_NATIVE_SPECS

#ifdef HAVE_AS_MABI_OPTION
#define ASM_MABI_SPEC	"%{mabi=*:-mabi=%*}"
#else
#define ASM_MABI_SPEC	"%{mabi=lp64:}"
#endif

#ifndef ASM_SPEC
#define ASM_SPEC "\
%{mbig-endian:-EB} \
%{mlittle-endian:-EL} \
%{march=*:-march=%*} \
%(asm_cpu_spec)" \
ASM_MABI_SPEC
#endif

#undef TYPE_OPERAND_FMT
#define TYPE_OPERAND_FMT	"%%%s"

/* Stabs debug not required.  */
#undef DBX_DEBUGGING_INFO

#endif /* GCC_AARCH64_ELF_H */
