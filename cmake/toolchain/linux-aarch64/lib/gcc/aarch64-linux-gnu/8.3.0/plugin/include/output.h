/* Declarations for insn-output.c and other code to write to asm_out_file.
   These functions are defined in final.c, and varasm.c.
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

#ifndef GCC_OUTPUT_H
#define GCC_OUTPUT_H

/* Initialize data in final at the beginning of a compilation.  */
extern void init_final (const char *);

/* Enable APP processing of subsequent output.
   Used before the output from an `asm' statement.  */
extern void app_enable (void);

/* Disable APP processing of subsequent output.
   Called from varasm.c before most kinds of output.  */
extern void app_disable (void);

/* Return the number of slots filled in the current
   delayed branch sequence (we don't count the insn needing the
   delay slot).   Zero if not in a delayed branch sequence.  */
extern int dbr_sequence_length (void);

/* Indicate that branch shortening hasn't yet been done.  */
extern void init_insn_lengths (void);

/* Obtain the current length of an insn.  If branch shortening has been done,
   get its actual length.  Otherwise, get its maximum length.  */
extern int get_attr_length (rtx_insn *);

/* Obtain the current length of an insn.  If branch shortening has been done,
   get its actual length.  Otherwise, get its minimum length.  */
extern int get_attr_min_length (rtx_insn *);

/* Make a pass over all insns and compute their actual lengths by shortening
   any branches of variable length if possible.  */
extern void shorten_branches (rtx_insn *);

const char *get_some_local_dynamic_name ();

/* Output assembler code for the start of a function,
   and initialize some of the variables in this file
   for the new function.  The label for the function and associated
   assembler pseudo-ops have already been output in
   `assemble_start_function'.  */
extern void final_start_function (rtx_insn *, FILE *, int);

/* Output assembler code for the end of a function.
   For clarity, args are same as those of `final_start_function'
   even though not all of them are needed.  */
extern void final_end_function (void);

/* Output assembler code for some insns: all or part of a function.  */
extern void final (rtx_insn *, FILE *, int);

/* The final scan for one insn, INSN.  Args are same as in `final', except
   that INSN is the insn being scanned.  Value returned is the next insn to
   be scanned.  */
extern rtx_insn *final_scan_insn (rtx_insn *, FILE *, int, int, int *);

/* Replace a SUBREG with a REG or a MEM, based on the thing it is a
   subreg of.  */
extern rtx alter_subreg (rtx *, bool);

/* Print an operand using machine-dependent assembler syntax.  */
extern void output_operand (rtx, int);

/* Report inconsistency between the assembler template and the operands.
   In an `asm', it's the user's fault; otherwise, the compiler's fault.  */
extern void output_operand_lossage (const char *, ...) ATTRIBUTE_PRINTF_1;

/* Output a string of assembler code, substituting insn operands.
   Defined in final.c.  */
extern void output_asm_insn (const char *, rtx *);

/* Compute a worst-case reference address of a branch so that it
   can be safely used in the presence of aligned labels.
   Defined in final.c.  */
extern int insn_current_reference_address (rtx_insn *);

/* Find the alignment associated with a CODE_LABEL.
   Defined in final.c.  */
extern int label_to_alignment (rtx);

/* Find the alignment maximum skip associated with a CODE_LABEL.
   Defined in final.c.  */
extern int label_to_max_skip (rtx);

/* Output a LABEL_REF, or a bare CODE_LABEL, as an assembler symbol.  */
extern void output_asm_label (rtx);

/* Marks SYMBOL_REFs in x as referenced through use of assemble_external.  */
extern void mark_symbol_refs_as_used (rtx);

/* Print a memory reference operand for address X with access mode MODE
   using machine-dependent assembler syntax.  */
extern void output_address (machine_mode, rtx);

/* Print an integer constant expression in assembler syntax.
   Addition and subtraction are the only arithmetic
   that may appear in these expressions.  */
extern void output_addr_const (FILE *, rtx);

/* Output a string of assembler code, substituting numbers, strings
   and fixed syntactic prefixes.  */
#if GCC_VERSION >= 3004
#define ATTRIBUTE_ASM_FPRINTF(m, n) __attribute__ ((__format__ (__asm_fprintf__, m, n))) ATTRIBUTE_NONNULL(m)
#else
#define ATTRIBUTE_ASM_FPRINTF(m, n) ATTRIBUTE_NONNULL(m)
#endif

extern void fprint_whex (FILE *, unsigned HOST_WIDE_INT);
extern void fprint_ul (FILE *, unsigned long);
extern int sprint_ul (char *, unsigned long);

extern void asm_fprintf (FILE *file, const char *p, ...)
     ATTRIBUTE_ASM_FPRINTF(2, 3);

/* Return nonzero if this function has no function calls.  */
extern int leaf_function_p (void);

/* Return 1 if branch is a forward branch.
   Uses insn_shuid array, so it works only in the final pass.  May be used by
   output templates to add branch prediction hints, for example.  */
extern int final_forward_branch_p (rtx_insn *);

/* Return 1 if this function uses only the registers that can be
   safely renumbered.  */
extern int only_leaf_regs_used (void);

/* Scan IN_RTX and its subexpressions, and renumber all regs into those
   available in leaf functions.  */
extern void leaf_renumber_regs_insn (rtx);

/* Locate the proper template for the given insn-code.  */
extern const char *get_insn_template (int, rtx);

/* Functions in varasm.c.  */

/* Emit any pending weak declarations.  */
extern void weak_finish (void);

/* Decode an `asm' spec for a declaration as a register name.
   Return the register number, or -1 if nothing specified,
   or -2 if the ASMSPEC is not `cc' or `memory' and is not recognized,
   or -3 if ASMSPEC is `cc' and is not recognized,
   or -4 if ASMSPEC is `memory' and is not recognized.
   Accept an exact spelling or a decimal number.
   Prefixes such as % are optional.  */
extern int decode_reg_name (const char *);

/* Similar to decode_reg_name, but takes an extra parameter that is a
   pointer to the number of (internal) registers described by the
   external name.  */
extern int decode_reg_name_and_count (const char *, int *);

extern void do_assemble_alias (tree, tree);

extern void default_assemble_visibility (tree, int);

/* Output a string of literal assembler code
   for an `asm' keyword used between functions.  */
extern void assemble_asm (tree);

/* Get the function's name from a decl, as described by its RTL.  */
extern const char *get_fnname_from_decl (tree);

/* Output assembler code for the constant pool of a function and associated
   with defining the name of the function.  DECL describes the function.
   NAME is the function's name.  For the constant pool, we use the current
   constant pool data.  */
extern void assemble_start_function (tree, const char *);

/* Output assembler code associated with defining the size of the
   function.  DECL describes the function.  NAME is the function's name.  */
extern void assemble_end_function (tree, const char *);

/* Assemble everything that is needed for a variable or function declaration.
   Not used for automatic variables, and not used for function definitions.
   Should not be called for variables of incomplete structure type.

   TOP_LEVEL is nonzero if this variable has file scope.
   AT_END is nonzero if this is the special handling, at end of compilation,
   to define things that have had only tentative definitions.
   DONT_OUTPUT_DATA if nonzero means don't actually output the
   initial value (that will be done by the caller).  */
extern void assemble_variable (tree, int, int, int);

/* Put the vtable verification constructor initialization function
   into the preinit array.  */
extern void assemble_vtv_preinit_initializer (tree);

/* Assemble everything that is needed for a variable declaration that has
   no definition in the current translation unit.  */
extern void assemble_undefined_decl (tree);

/* Compute the alignment of variable specified by DECL.
   DONT_OUTPUT_DATA is from assemble_variable.  */
extern void align_variable (tree decl, bool dont_output_data);

/* Queue for outputting something to declare an external symbol to the
   assembler.  (Most assemblers don't need this, so we normally output
   nothing.)  Do nothing if DECL is not external.  */
extern void assemble_external (tree);

/* Assemble code to leave SIZE bytes of zeros.  */
extern void assemble_zeros (unsigned HOST_WIDE_INT);

/* Assemble an alignment pseudo op for an ALIGN-bit boundary.  */
extern void assemble_align (int);

/* Assemble a string constant with the specified C string as contents.  */
extern void assemble_string (const char *, int);

/* Similar, for calling a library function FUN.  */
extern void assemble_external_libcall (rtx);

/* Assemble a label named NAME.  */
extern void assemble_label (FILE *, const char *);

/* Output to FILE (an assembly file) a reference to NAME.  If NAME
   starts with a *, the rest of NAME is output verbatim.  Otherwise
   NAME is transformed in a target-specific way (usually by the
   addition of an underscore).  */
extern void assemble_name_raw (FILE *, const char *);

/* Like assemble_name_raw, but should be used when NAME might refer to
   an entity that is also represented as a tree (like a function or
   variable).  If NAME does refer to such an entity, that entity will
   be marked as referenced.  */
extern void assemble_name (FILE *, const char *);

/* Return the assembler directive for creating a given kind of integer
   object.  SIZE is the number of bytes in the object and ALIGNED_P
   indicates whether it is known to be aligned.  Return NULL if the
   assembly dialect has no such directive.

   The returned string should be printed at the start of a new line and
   be followed immediately by the object's initial value.  */
extern const char *integer_asm_op (int, int);

/* Use directive OP to assemble an integer object X.  Print OP at the
   start of the line, followed immediately by the value of X.  */
extern void assemble_integer_with_op (const char *, rtx);

/* The default implementation of the asm_out.integer target hook.  */
extern bool default_assemble_integer (rtx, unsigned int, int);

/* Assemble the integer constant X into an object of SIZE bytes.  ALIGN is
   the alignment of the integer in bits.  Return 1 if we were able to output
   the constant, otherwise 0.  If FORCE is nonzero the constant must
   be outputable. */
extern bool assemble_integer (rtx, unsigned, unsigned, int);

/* Return section for TEXT_SECITON_NAME if DECL or DECL_SECTION_NAME (DECL)
   is NULL.  */
extern section *get_named_text_section (tree, const char *, const char *);

/* An interface to assemble_integer for the common case in which a value is
   fully aligned and must be printed.  VALUE is the value of the integer
   object and SIZE is the number of bytes it contains.  */
#define assemble_aligned_integer(SIZE, VALUE) \
  assemble_integer (VALUE, SIZE, (SIZE) * BITS_PER_UNIT, 1)

/* Assemble the floating-point constant D into an object of size MODE.  ALIGN
   is the alignment of the constant in bits.  If REVERSE is true, D is output
   in reverse storage order.  */
extern void assemble_real (REAL_VALUE_TYPE, scalar_float_mode, unsigned,
			   bool = false);

/* Write the address of the entity given by SYMBOL to SEC.  */
extern void assemble_addr_to_section (rtx, section *);

/* Return TRUE if and only if the constant pool has no entries.  Note
   that even entries we might end up choosing not to emit are counted
   here, so there is the potential for missed optimizations.  */
extern bool constant_pool_empty_p (void);

extern rtx_insn *peephole (rtx_insn *);

extern void output_shared_constant_pool (void);

extern void output_object_blocks (void);

extern void output_quoted_string (FILE *, const char *);

/* When outputting delayed branch sequences, this rtx holds the
   sequence being output.  It is null when no delayed branch
   sequence is being output, so it can be used as a test in the
   insn output code.

   This variable is defined  in final.c.  */
extern rtx_sequence *final_sequence;

/* File in which assembler code is being written.  */

#ifdef BUFSIZ
extern FILE *asm_out_file;
#endif

/* The first global object in the file.  */
extern const char *first_global_object_name;

/* The first weak object in the file.  */
extern const char *weak_global_object_name;

/* Nonnull if the insn currently being emitted was a COND_EXEC pattern.  */
extern rtx current_insn_predicate;

/* Last insn processed by final_scan_insn.  */
extern rtx_insn *current_output_insn;

/* Nonzero while outputting an `asm' with operands.
   This means that inconsistencies are the user's fault, so don't die.
   The precise value is the insn being output, to pass to error_for_asm.  */
extern const rtx_insn *this_is_asm_operands;

/* Carry information from ASM_DECLARE_OBJECT_NAME
   to ASM_FINISH_DECLARE_OBJECT.  */
extern int size_directive_output;
extern tree last_assemble_variable_decl;

extern bool first_function_block_is_cold;

/* Decide whether DECL needs to be in a writable section.
   RELOC is the same as for SELECT_SECTION.  */
extern bool decl_readonly_section (const_tree, int);

/* This can be used to compute RELOC for the function above, when
   given a constant expression.  */
extern int compute_reloc_for_constant (tree);

/* User label prefix in effect for this compilation.  */
extern const char *user_label_prefix;

/* Default target function prologue and epilogue assembler output.  */
extern void default_function_pro_epilogue (FILE *);

/* Default target function switched text sections.  */
extern void default_function_switched_text_sections (FILE *, tree, bool);

/* Default target hook that outputs nothing to a stream.  */
extern void no_asm_to_stream (FILE *);

/* Flags controlling properties of a section.  */
#define SECTION_ENTSIZE	 0x000ff	/* entity size in section */
#define SECTION_CODE	 0x00100	/* contains code */
#define SECTION_WRITE	 0x00200	/* data is writable */
#define SECTION_DEBUG	 0x00400	/* contains debug data */
#define SECTION_LINKONCE 0x00800	/* is linkonce */
#define SECTION_SMALL	 0x01000	/* contains "small data" */
#define SECTION_BSS	 0x02000	/* contains zeros only */
#define SECTION_FORGET	 0x04000	/* forget that we've entered the section */
#define SECTION_MERGE	 0x08000	/* contains mergeable data */
#define SECTION_STRINGS  0x10000	/* contains zero terminated strings without
					   embedded zeros */
#define SECTION_OVERRIDE 0x20000	/* allow override of default flags */
#define SECTION_TLS	 0x40000	/* contains thread-local storage */
#define SECTION_NOTYPE	 0x80000	/* don't output @progbits */
#define SECTION_DECLARED 0x100000	/* section has been used */
#define SECTION_STYLE_MASK 0x600000	/* bits used for SECTION_STYLE */
#define SECTION_COMMON   0x800000	/* contains common data */
#define SECTION_RELRO	 0x1000000	/* data is readonly after relocation processing */
#define SECTION_EXCLUDE  0x2000000	/* discarded by the linker */
#define SECTION_MACH_DEP 0x4000000	/* subsequent bits reserved for target */

/* This SECTION_STYLE is used for unnamed sections that we can switch
   to using a special assembler directive.  */
#define SECTION_UNNAMED	 0x000000

/* This SECTION_STYLE is used for named sections that we can switch
   to using a general section directive.  */
#define SECTION_NAMED	 0x200000

/* This SECTION_STYLE is used for sections that we cannot switch to at
   all.  The choice of section is implied by the directive that we use
   to declare the object.  */
#define SECTION_NOSWITCH 0x400000

/* A helper function for default_elf_select_section and
   default_elf_unique_section.  Categorizes the DECL.  */

enum section_category
{
  SECCAT_TEXT,

  SECCAT_RODATA,
  SECCAT_RODATA_MERGE_STR,
  SECCAT_RODATA_MERGE_STR_INIT,
  SECCAT_RODATA_MERGE_CONST,
  SECCAT_SRODATA,

  SECCAT_DATA,

  /* To optimize loading of shared programs, define following subsections
     of data section:
	_REL	Contains data that has relocations, so they get grouped
		together and dynamic linker will visit fewer pages in memory.
	_RO	Contains data that is otherwise read-only.  This is useful
		with prelinking as most relocations won't be dynamically
		linked and thus stay read only.
	_LOCAL	Marks data containing relocations only to local objects.
		These relocations will get fully resolved by prelinking.  */
  SECCAT_DATA_REL,
  SECCAT_DATA_REL_LOCAL,
  SECCAT_DATA_REL_RO,
  SECCAT_DATA_REL_RO_LOCAL,

  SECCAT_SDATA,
  SECCAT_TDATA,

  SECCAT_BSS,
  SECCAT_SBSS,
  SECCAT_TBSS
};

/* Information that is provided by all instances of the section type.  */
struct GTY(()) section_common {
  /* The set of SECTION_* flags that apply to this section.  */
  unsigned int flags;
};

/* Information about a SECTION_NAMED section.  */
struct GTY(()) named_section {
  struct section_common common;

  /* The name of the section.  */
  const char *name;

  /* If nonnull, the VAR_DECL or FUNCTION_DECL with which the
     section is associated.  */
  tree decl;
};

/* A callback that writes the assembly code for switching to an unnamed
   section.  The argument provides callback-specific data.  */
typedef void (*unnamed_section_callback) (const void *);

/* Information about a SECTION_UNNAMED section.  */
struct GTY(()) unnamed_section {
  struct section_common common;

  /* The callback used to switch to the section, and the data that
     should be passed to the callback.  */
  unnamed_section_callback GTY ((skip)) callback;
  const void *GTY ((skip)) data;

  /* The next entry in the chain of unnamed sections.  */
  section *next;
};

/* A callback that writes the assembly code for a decl in a
   SECTION_NOSWITCH section.  DECL is the decl that should be assembled
   and NAME is the name of its SYMBOL_REF.  SIZE is the size of the decl
   in bytes and ROUNDED is that size rounded up to the next
   BIGGEST_ALIGNMENT / BITS_PER_UNIT boundary.

   Return true if the callback used DECL_ALIGN to set the object's
   alignment.  A false return value implies that we are relying
   on the rounded size to align the decl.  */
typedef bool (*noswitch_section_callback) (tree decl, const char *name,
					   unsigned HOST_WIDE_INT size,
					   unsigned HOST_WIDE_INT rounded);

/* Information about a SECTION_NOSWITCH section.  */
struct GTY(()) noswitch_section {
  struct section_common common;

  /* The callback used to assemble decls in this section.  */
  noswitch_section_callback GTY ((skip)) callback;
};

/* Information about a section, which may be named or unnamed.  */
union GTY ((desc ("SECTION_STYLE (&(%h))"), for_user)) section {
  struct section_common GTY ((skip)) common;
  struct named_section GTY ((tag ("SECTION_NAMED"))) named;
  struct unnamed_section GTY ((tag ("SECTION_UNNAMED"))) unnamed;
  struct noswitch_section GTY ((tag ("SECTION_NOSWITCH"))) noswitch;
};

/* Return the style of section SECT.  */
#define SECTION_STYLE(SECT) ((SECT)->common.flags & SECTION_STYLE_MASK)

struct object_block;

/* Special well-known sections.  */
extern GTY(()) section *text_section;
extern GTY(()) section *data_section;
extern GTY(()) section *readonly_data_section;
extern GTY(()) section *sdata_section;
extern GTY(()) section *ctors_section;
extern GTY(()) section *dtors_section;
extern GTY(()) section *bss_section;
extern GTY(()) section *sbss_section;
extern GTY(()) section *exception_section;
extern GTY(()) section *eh_frame_section;
extern GTY(()) section *tls_comm_section;
extern GTY(()) section *comm_section;
extern GTY(()) section *lcomm_section;
extern GTY(()) section *bss_noswitch_section;

extern GTY(()) section *in_section;
extern GTY(()) bool in_cold_section_p;

extern section *get_unnamed_section (unsigned int, void (*) (const void *),
				     const void *);
extern section *get_section (const char *, unsigned int, tree);
extern section *get_named_section (tree, const char *, int);
extern section *get_variable_section (tree, bool);
extern void place_block_symbol (rtx);
extern rtx get_section_anchor (struct object_block *, HOST_WIDE_INT,
			       enum tls_model);
extern section *mergeable_constant_section (machine_mode,
					    unsigned HOST_WIDE_INT,
					    unsigned int);
extern section *function_section (tree);
extern section *unlikely_text_section (void);
extern section *current_function_section (void);
extern void switch_to_other_text_partition (void);

/* Return the numbered .ctors.N (if CONSTRUCTOR_P) or .dtors.N (if
   not) section for PRIORITY.  */
extern section *get_cdtor_priority_section (int, bool);

extern bool unlikely_text_section_p (section *);
extern void switch_to_section (section *);
extern void output_section_asm_op (const void *);

extern void record_tm_clone_pair (tree, tree);
extern void finish_tm_clone_pairs (void);
extern tree get_tm_clone_pair (tree);

extern void default_asm_output_source_filename (FILE *, const char *);
extern void output_file_directive (FILE *, const char *);

extern unsigned int default_section_type_flags (tree, const char *, int);

extern bool have_global_bss_p (void);
extern bool bss_initializer_p (const_tree, bool = false);

extern void default_no_named_section (const char *, unsigned int, tree);
extern void default_elf_asm_named_section (const char *, unsigned int, tree);
extern enum section_category categorize_decl_for_section (const_tree, int);
extern void default_coff_asm_named_section (const char *, unsigned int, tree);
extern void default_pe_asm_named_section (const char *, unsigned int, tree);

extern void default_named_section_asm_out_destructor (rtx, int);
extern void default_dtor_section_asm_out_destructor (rtx, int);
extern void default_named_section_asm_out_constructor (rtx, int);
extern void default_ctor_section_asm_out_constructor (rtx, int);

extern section *default_select_section (tree, int, unsigned HOST_WIDE_INT);
extern section *default_elf_select_section (tree, int, unsigned HOST_WIDE_INT);
extern void default_unique_section (tree, int);
extern section *default_function_rodata_section (tree);
extern section *default_no_function_rodata_section (tree);
extern section *default_clone_table_section (void);
extern section *default_select_rtx_section (machine_mode, rtx,
					    unsigned HOST_WIDE_INT);
extern section *default_elf_select_rtx_section (machine_mode, rtx,
						unsigned HOST_WIDE_INT);
extern void default_encode_section_info (tree, rtx, int);
extern const char *default_strip_name_encoding (const char *);
extern void default_asm_output_anchor (rtx);
extern bool default_use_anchors_for_symbol_p (const_rtx);
extern bool default_binds_local_p (const_tree);
extern bool default_binds_local_p_1 (const_tree, int);
extern bool default_binds_local_p_2 (const_tree);
extern bool default_binds_local_p_3 (const_tree, bool, bool, bool, bool);
extern void default_globalize_label (FILE *, const char *);
extern void default_globalize_decl_name (FILE *, tree);
extern void default_emit_unwind_label (FILE *, tree, int, int);
extern void default_emit_except_table_label (FILE *);
extern void default_generate_internal_label (char *, const char *,
					     unsigned long);
extern void default_internal_label (FILE *, const char *, unsigned long);
extern void default_asm_declare_constant_name (FILE *, const char *,
					       const_tree, HOST_WIDE_INT);
extern void default_file_start (void);
extern void file_end_indicate_exec_stack (void);
extern void file_end_indicate_split_stack (void);

extern void default_elf_asm_output_external (FILE *file, tree,
					     const char *);
extern void default_elf_asm_output_limited_string (FILE *, const char *);
extern void default_elf_asm_output_ascii (FILE *, const char *, unsigned int);
extern void default_elf_internal_label (FILE *, const char *, unsigned long);

extern void default_elf_init_array_asm_out_constructor (rtx, int);
extern void default_elf_fini_array_asm_out_destructor (rtx, int);
extern int maybe_assemble_visibility (tree);

extern int default_address_cost (rtx, machine_mode, addr_space_t, bool);

/* Output stack usage information.  */
extern void output_stack_usage (void);

#endif /* ! GCC_OUTPUT_H */
