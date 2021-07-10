/* HSAIL and BRIG related macros and definitions.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef HSA_H
#define HSA_H

#include "hsa-brig-format.h"
#include "is-a.h"
#include "predict.h"
#include "tree.h"
#include "vec.h"
#include "hash-table.h"
#include "basic-block.h"
#include "bitmap.h"


/* Return true if the compiler should produce HSAIL.  */

static inline bool
hsa_gen_requested_p (void)
{
#ifndef ENABLE_HSA
  return false;
#endif
  return !flag_disable_hsa;
}

/* Standard warning message if we failed to generate HSAIL for a function.  */

#define HSA_SORRY_MSG "could not emit HSAIL for the function"

class hsa_op_immed;
class hsa_op_cst_list;
class hsa_insn_basic;
class hsa_op_address;
class hsa_op_reg;
class hsa_bb;

/* Class representing an input argument, output argument (result) or a
   variable, that will eventually end up being a symbol directive.  */

struct hsa_symbol
{
  /* Constructor.  */
  hsa_symbol (BrigType16_t type, BrigSegment8_t segment,
	      BrigLinkage8_t linkage, bool global_scope_p = false,
	      BrigAllocation allocation = BRIG_ALLOCATION_AUTOMATIC,
	      BrigAlignment8_t align = BRIG_ALIGNMENT_8);

  /* Return total size of the symbol.  */
  unsigned HOST_WIDE_INT total_byte_size ();

  /* Fill in those values into the symbol according to DECL, which are
     determined independently from whether it is parameter, result,
     or a variable, local or global.  */
  void fillup_for_decl (tree decl);

  /* Pointer to the original tree, which is PARM_DECL for input parameters and
     RESULT_DECL for the output parameters.  Also can be CONST_DECL for Fortran
     constants which need to be put into readonly segment.  */
  tree m_decl;

  /* Name of the symbol, that will be written into output and dumps.  Can be
     NULL, see name_number below.  */
  const char *m_name;

  /* If name is NULL, artificial name will be formed from the segment name and
     this number.  */
  int m_name_number;

  /* Once written, this is the offset of the associated symbol directive.  Zero
     means the symbol has not been written yet.  */
  unsigned m_directive_offset;

  /* HSA type of the parameter.  */
  BrigType16_t m_type;

  /* The HSA segment this will eventually end up in.  */
  BrigSegment8_t m_segment;

  /* The HSA kind of linkage.  */
  BrigLinkage8_t m_linkage;

  /* Array dimension, if non-zero.  */
  unsigned HOST_WIDE_INT m_dim;

  /* Constant value, used for string constants.  */
  hsa_op_immed *m_cst_value;

  /* Is in global scope.  */
  bool m_global_scope_p;

  /* True if an error has been seen for the symbol.  */
  bool m_seen_error;

  /* Symbol allocation.  */
  BrigAllocation m_allocation;

  /* Flag used for global variables if a variable is already emitted or not.  */
  bool m_emitted_to_brig;

  /* Alignment of the symbol.  */
  BrigAlignment8_t m_align;

private:
  /* Default constructor.  */
  hsa_symbol ();
};

/* Abstract class for HSA instruction operands.  */

class hsa_op_base
{
public:
  /* Next operand scheduled to be written when writing BRIG operand
     section.  */
  hsa_op_base *m_next;

  /* Offset to which the associated operand structure will be written.  Zero if
     yet not scheduled for writing.  */
  unsigned m_brig_op_offset;

  /* The type of a particular operand.  */
  BrigKind16_t m_kind;

protected:
  hsa_op_base (BrigKind16_t k);
private:
  /* Make the default constructor inaccessible.  */
  hsa_op_base () {}
};

/* Common abstract ancestor for operands which have a type.  */

class hsa_op_with_type : public hsa_op_base
{
public:
  /* The type.  */
  BrigType16_t m_type;

  /* Convert an operand to a destination type DTYPE and attach insns
     to HBB if needed.  */
  hsa_op_with_type *get_in_type (BrigType16_t dtype, hsa_bb *hbb);
  /* If this operand has integer type smaller than 32 bits, extend it to 32
     bits, adding instructions to HBB if needed.  */
  hsa_op_with_type *extend_int_to_32bit (hsa_bb *hbb);

protected:
  hsa_op_with_type (BrigKind16_t k, BrigType16_t t);
private:
  /* Make the default constructor inaccessible.  */
  hsa_op_with_type () : hsa_op_base (BRIG_KIND_NONE) {}
};

/* An immediate HSA operand.  */

class hsa_op_immed : public hsa_op_with_type
{
public:
  hsa_op_immed (tree tree_val, bool min32int = true);
  hsa_op_immed (HOST_WIDE_INT int_value, BrigType16_t type);
  void *operator new (size_t);
  ~hsa_op_immed ();
  void set_type (BrigKind16_t t);

  /* Function returns pointer to a buffer that contains binary representation
     of the immeadiate value.  The buffer has length of BRIG_SIZE and
     a caller is responsible for deallocation of the buffer.  */
  char *emit_to_buffer (unsigned *brig_size);

  /* Value as represented by middle end.  */
  tree m_tree_value;

  /* Integer value representation.  */
  HOST_WIDE_INT m_int_value;

private:
  /* Make the default constructor inaccessible.  */
  hsa_op_immed ();
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
};

/* Report whether or not P is a an immediate operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_immed *>::test (hsa_op_base *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_CONSTANT_BYTES;
}

/* Likewise, but for a more specified base. */

template <>
template <>
inline bool
is_a_helper <hsa_op_immed *>::test (hsa_op_with_type *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_CONSTANT_BYTES;
}


/* HSA register operand.  */

class hsa_op_reg : public hsa_op_with_type
{
  friend class hsa_insn_basic;
  friend class hsa_insn_phi;
public:
  hsa_op_reg (BrigType16_t t);
  void *operator new (size_t);

  /* Verify register operand.  */
  void verify_ssa ();

  /* If NON-NULL, gimple SSA that we come from.  NULL if none.  */
  tree m_gimple_ssa;

  /* Defining instruction while still in the SSA.  */
  hsa_insn_basic *m_def_insn;

  /* If the register allocator decides to spill the register, this is the
     appropriate spill symbol.  */
  hsa_symbol *m_spill_sym;

  /* Number of this register structure in the order in which they were
     allocated.  */
  int m_order;
  int m_lr_begin, m_lr_end;

  /* Zero if the register is not yet allocated.  After, allocation, this must
     be 'c', 's', 'd' or 'q'.  */
  char m_reg_class;
  /* If allocated, the number of the HW register (within its HSA register
     class).  */
  char m_hard_num;

private:
  /* Make the default constructor inaccessible.  */
  hsa_op_reg () : hsa_op_with_type (BRIG_KIND_NONE, BRIG_TYPE_NONE) {}
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
  /* Set definition where the register is defined.  */
  void set_definition (hsa_insn_basic *insn);
  /* Uses of the value while still in SSA.  */
  auto_vec <hsa_insn_basic *> m_uses;
};

/* Report whether or not P is a register operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_reg *>::test (hsa_op_base *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_REGISTER;
}

/* Report whether or not P is a register operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_reg *>::test (hsa_op_with_type *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_REGISTER;
}

/* An address HSA operand.  */

class hsa_op_address : public hsa_op_base
{
public:
  /* set up a new address operand consisting of base symbol SYM, register R and
     immediate OFFSET.  If the machine model is not large and offset is 64 bit,
     the upper, 32 bits have to be zero.  */
  hsa_op_address (hsa_symbol *sym, hsa_op_reg *reg,
		  HOST_WIDE_INT offset = 0);

  void *operator new (size_t);

  /* Set up a new address operand consisting of base symbol SYM and
     immediate OFFSET.  If the machine model is not large and offset is 64 bit,
     the upper, 32 bits have to be zero.  */
  hsa_op_address (hsa_symbol *sym, HOST_WIDE_INT offset = 0);

  /* Set up a new address operand consisting of register R and
     immediate OFFSET.  If the machine model is not large and offset is 64 bit,
     the upper, 32 bits have to be zero.  */
  hsa_op_address (hsa_op_reg *reg, HOST_WIDE_INT offset = 0);

  /* Symbol base of the address.  Can be NULL if there is none.  */
  hsa_symbol *m_symbol;

  /* Register offset.  Can be NULL if there is none.  */
  hsa_op_reg *m_reg;

  /* Immediate byte offset.  */
  HOST_WIDE_INT m_imm_offset;

private:
  /* Make the default constructor inaccessible.  */
  hsa_op_address () : hsa_op_base (BRIG_KIND_NONE) {}
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
};

/* Report whether or not P is an address operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_address *>::test (hsa_op_base *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_ADDRESS;
}

/* A reference to code HSA operand.  It can be either reference
   to a start of a BB or a start of a function.  */

class hsa_op_code_ref : public hsa_op_base
{
public:
  hsa_op_code_ref ();

  /* Offset in the code section that this refers to.  */
  unsigned m_directive_offset;
};

/* Report whether or not P is a code reference operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_code_ref *>::test (hsa_op_base *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_CODE_REF;
}

/* Code list HSA operand.  */

class hsa_op_code_list: public hsa_op_base
{
public:
  hsa_op_code_list (unsigned elements);
  void *operator new (size_t);

  /* Offset to variable-sized array in hsa_data section, where
     are offsets to entries in the hsa_code section.  */
  auto_vec<unsigned> m_offsets;
private:
  /* Make the default constructor inaccessible.  */
  hsa_op_code_list () : hsa_op_base (BRIG_KIND_NONE) {}
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
};

/* Report whether or not P is a code list operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_code_list *>::test (hsa_op_base *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_CODE_LIST;
}

/* Operand list HSA operand.  */

class hsa_op_operand_list: public hsa_op_base
{
public:
  hsa_op_operand_list (unsigned elements);
  ~hsa_op_operand_list ();
  void *operator new (size_t);

  /* Offset to variable-sized array in hsa_data section, where
     are offsets to entries in the hsa_code section.  */
  auto_vec<unsigned> m_offsets;
private:
  /* Make the default constructor inaccessible.  */
  hsa_op_operand_list () : hsa_op_base (BRIG_KIND_NONE) {}
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
};

/* Report whether or not P is a code list operand.  */

template <>
template <>
inline bool
is_a_helper <hsa_op_operand_list *>::test (hsa_op_base *p)
{
  return p->m_kind == BRIG_KIND_OPERAND_OPERAND_LIST;
}

/* Opcodes of instructions that are not part of HSA but that we use to
   represent it nevertheless.  */

#define HSA_OPCODE_PHI (-1)
#define HSA_OPCODE_ARG_BLOCK (-2)

/* The number of operand pointers we can directly in an instruction.  */
#define HSA_BRIG_INT_STORAGE_OPERANDS 5

/* Class representing an HSA instruction.  Unlike typical ancestors for
   specialized classes, this one is also directly used for all instructions
   that are then represented as BrigInstBasic.  */

class hsa_insn_basic
{
public:
  hsa_insn_basic (unsigned nops, int opc);
  hsa_insn_basic (unsigned nops, int opc, BrigType16_t t,
		  hsa_op_base *arg0 = NULL,
		  hsa_op_base *arg1 = NULL,
		  hsa_op_base *arg2 = NULL,
		  hsa_op_base *arg3 = NULL);

  void *operator new (size_t);
  void set_op (int index, hsa_op_base *op);
  hsa_op_base *get_op (int index);
  hsa_op_base **get_op_addr (int index);
  unsigned int operand_count ();
  void verify ();
  unsigned input_count ();
  unsigned num_used_ops ();
  void set_output_in_type (hsa_op_reg *dest, unsigned op_index, hsa_bb *hbb);
  bool op_output_p (unsigned opnum);

  /* The previous and next instruction in the basic block.  */
  hsa_insn_basic *m_prev, *m_next;

  /* Basic block this instruction belongs to.  */
  basic_block m_bb;

  /* Operand code distinguishing different types of instructions.  Eventually
     these should only be BRIG_INST_* values from the BrigOpcode16_t range but
     initially we use negative values for PHI nodes and such.  */
  int m_opcode;

  /* Linearized number assigned to the instruction by HSA RA.  */
  int m_number;

  /* Type of the destination of the operations.  */
  BrigType16_t m_type;

  /* BRIG offset of the instruction in code section.  */
  unsigned int m_brig_offset;

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_basic () {}
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
  /* The individual operands.  All instructions but PHI nodes have five or
     fewer instructions and so will fit the internal storage.  */
  /* TODO: Vast majority of instructions have three or fewer operands, so we
     may actually try reducing it.  */
  auto_vec<hsa_op_base *, HSA_BRIG_INT_STORAGE_OPERANDS> m_operands;
};

/* Class representing a PHI node of the SSA form of HSA virtual
   registers.  */

class hsa_insn_phi : public hsa_insn_basic
{
public:
  hsa_insn_phi (unsigned nops, hsa_op_reg *dst);

  /* Destination.  */
  hsa_op_reg *m_dest;

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_phi () : hsa_insn_basic (1, HSA_OPCODE_PHI) {}
};

/* Report whether or not P is a PHI node.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_phi *>::test (hsa_insn_basic *p)
{
  return p->m_opcode == HSA_OPCODE_PHI;
}

/* HSA instruction for  */
class hsa_insn_br : public hsa_insn_basic
{
public:
  hsa_insn_br (unsigned nops, int opc, BrigType16_t t, BrigWidth8_t width,
	       hsa_op_base *arg0 = NULL, hsa_op_base *arg1 = NULL,
	       hsa_op_base *arg2 = NULL, hsa_op_base *arg3 = NULL);

  /* Number of work-items affected in the same way by the instruction.  */
  BrigWidth8_t m_width;

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_br () : hsa_insn_basic (0, BRIG_OPCODE_BR) {}
};

/* Return true if P is a branching/synchronization instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_br *>::test (hsa_insn_basic *p)
{
  return p->m_opcode == BRIG_OPCODE_BARRIER
    || p->m_opcode == BRIG_OPCODE_BR;
}

/* HSA instruction for conditional branches.  Structurally the same as
   hsa_insn_br but we represent it specially because of inherent control
   flow it represents.  */

class hsa_insn_cbr : public hsa_insn_br
{
public:
  hsa_insn_cbr (hsa_op_reg *ctrl);

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_cbr () : hsa_insn_br (0, BRIG_OPCODE_CBR, BRIG_TYPE_B1,
				 BRIG_WIDTH_1) {}
};

/* Report whether P is a contitional branching instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_cbr *>::test (hsa_insn_basic *p)
{
  return p->m_opcode == BRIG_OPCODE_CBR;
}

/* HSA instruction for switch branches.  */

class hsa_insn_sbr : public hsa_insn_basic
{
public:
  hsa_insn_sbr (hsa_op_reg *index, unsigned jump_count);

  /* Default destructor.  */
  ~hsa_insn_sbr ();

  void replace_all_labels (basic_block old_bb, basic_block new_bb);

  /* Width as described in HSA documentation.  */
  BrigWidth8_t m_width;

  /* Jump table.  */
  vec <basic_block> m_jump_table;

  /* Code list for label references.  */
  hsa_op_code_list *m_label_code_list;

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_sbr () : hsa_insn_basic (1, BRIG_OPCODE_SBR) {}
};

/* Report whether P is a switch branching instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_sbr *>::test (hsa_insn_basic *p)
{
  return p->m_opcode == BRIG_OPCODE_SBR;
}

/* HSA instruction for comparisons.  */

class hsa_insn_cmp : public hsa_insn_basic
{
public:
  hsa_insn_cmp (BrigCompareOperation8_t cmp, BrigType16_t t,
		hsa_op_base *arg0 = NULL, hsa_op_base *arg1 = NULL,
		hsa_op_base *arg2 = NULL);

  /* Source type should be derived from operand types.  */

  /* The comparison operation.  */
  BrigCompareOperation8_t m_compare;

  /* TODO: Modifiers and packing control are missing but so are everywhere
     else.  */
private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_cmp () : hsa_insn_basic (1, BRIG_OPCODE_CMP) {}
};

/* Report whether or not P is a comparison instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_cmp *>::test (hsa_insn_basic *p)
{
  return p->m_opcode == BRIG_OPCODE_CMP;
}

/* HSA instruction for memory operations.  */

class hsa_insn_mem : public hsa_insn_basic
{
public:
  hsa_insn_mem (int opc, BrigType16_t t, hsa_op_base *arg0, hsa_op_base *arg1);

  /* Set alignment to VALUE.  */

  void set_align (BrigAlignment8_t value);

  /* The segment is of the memory access is either the segment of the symbol in
     the address operand or flat address is there is no symbol there.  */

  /* Required alignment of the memory operation.  */
  BrigAlignment8_t m_align;

  /* HSA equiv class, basically an alias set number.  */
  uint8_t m_equiv_class;

  /* TODO:  Add width modifier, perhaps also other things.  */
protected:
  hsa_insn_mem (unsigned nops, int opc, BrigType16_t t,
		hsa_op_base *arg0 = NULL, hsa_op_base *arg1 = NULL,
		hsa_op_base *arg2 = NULL, hsa_op_base *arg3 = NULL);

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_mem () : hsa_insn_basic (1, BRIG_OPCODE_LD) {}
};

/* Report whether or not P is a memory instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_mem *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_LD
	  || p->m_opcode == BRIG_OPCODE_ST);
}

/* HSA instruction for atomic operations.  */

class hsa_insn_atomic : public hsa_insn_mem
{
public:
  hsa_insn_atomic (int nops, int opc, enum BrigAtomicOperation aop,
		   BrigType16_t t, BrigMemoryOrder memorder,
		   hsa_op_base *arg0 = NULL, hsa_op_base *arg1 = NULL,
		   hsa_op_base *arg2 = NULL, hsa_op_base *arg3 = NULL);

  /* The operation itself.  */
  enum BrigAtomicOperation m_atomicop;

  /* Things like acquire/release/aligned.  */
  enum BrigMemoryOrder m_memoryorder;

  /* Scope of the atomic operation.  */
  enum BrigMemoryScope m_memoryscope;

private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_atomic () : hsa_insn_mem (1, BRIG_KIND_NONE, BRIG_TYPE_NONE) {}
};

/* Report whether or not P is an atomic instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_atomic *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_ATOMIC
	  || p->m_opcode == BRIG_OPCODE_ATOMICNORET);
}

/* HSA instruction for signal operations.  */

class hsa_insn_signal : public hsa_insn_basic
{
public:
  hsa_insn_signal (int nops, int opc, enum BrigAtomicOperation sop,
		   BrigType16_t t, BrigMemoryOrder memorder,
		   hsa_op_base *arg0 = NULL, hsa_op_base *arg1 = NULL,
		   hsa_op_base *arg2 = NULL, hsa_op_base *arg3 = NULL);

  /* Things like acquire/release/aligned.  */
  enum BrigMemoryOrder m_memory_order;

  /* The operation itself.  */
  enum BrigAtomicOperation m_signalop;
};

/* Report whether or not P is a signal instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_signal *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_SIGNAL
	  || p->m_opcode == BRIG_OPCODE_SIGNALNORET);
}

/* HSA instruction to convert between flat addressing and segments.  */

class hsa_insn_seg : public hsa_insn_basic
{
public:
  hsa_insn_seg (int opc, BrigType16_t destt, BrigType16_t srct,
		BrigSegment8_t seg, hsa_op_base *arg0, hsa_op_base *arg1);

  /* Source type.  Depends on the source addressing/segment.  */
  BrigType16_t m_src_type;
  /* The segment we are converting from or to.  */
  BrigSegment8_t m_segment;
private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_seg () : hsa_insn_basic (1, BRIG_OPCODE_STOF) {}
};

/* Report whether or not P is a segment conversion instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_seg *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_STOF
	  || p->m_opcode == BRIG_OPCODE_FTOS);
}

/* Class for internal functions for purpose of HSA emission.  */

class hsa_internal_fn
{
public:
  hsa_internal_fn (enum internal_fn fn, unsigned type_bit_size):
    m_fn (fn), m_type_bit_size (type_bit_size), m_offset (0) {}

  hsa_internal_fn (const hsa_internal_fn *f):
    m_fn (f->m_fn), m_type_bit_size (f->m_type_bit_size),
    m_offset (f->m_offset) {}

  /* Return arity of the internal function.  */
  unsigned get_arity ();

  /* Return BRIG type of N-th argument, if -1 is passed, return value type
     is received.  */
  BrigType16_t get_argument_type (int n);

  /* Return function name.  The memory must be released by a caller.  */
  char *name ();

  /* Internal function.  */
  enum internal_fn m_fn;

  /* Bit width of return type.  */
  unsigned m_type_bit_size;

  /* BRIG offset of declaration of the function.  */
  BrigCodeOffset32_t m_offset;
};

/* HSA instruction for function call.  */

class hsa_insn_call : public hsa_insn_basic
{
public:
  hsa_insn_call (tree callee);
  hsa_insn_call (hsa_internal_fn *fn);

  /* Default destructor.  */
  ~hsa_insn_call ();

  /* Called function.  */
  tree m_called_function;

  /* Called internal function.  */
  hsa_internal_fn *m_called_internal_fn;

  /* Input formal arguments.  */
  auto_vec <hsa_symbol *> m_input_args;

  /* Input arguments store instructions.  */
  auto_vec <hsa_insn_mem *> m_input_arg_insns;

  /* Output argument, can be NULL for void functions.  */
  hsa_symbol *m_output_arg;

  /* Called function code reference.  */
  hsa_op_code_ref m_func;

  /* Code list for arguments of the function.  */
  hsa_op_code_list *m_args_code_list;

  /* Code list for result of the function.  */
  hsa_op_code_list *m_result_code_list;
private:
  /* Make the default constructor inaccessible.  */
  hsa_insn_call () : hsa_insn_basic (0, BRIG_OPCODE_CALL) {}
};

/* Report whether or not P is a call instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_call *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_CALL);
}

/* HSA call instruction block encapsulates definition of arguments,
   result type, corresponding loads and a possible store.
   Moreover, it contains a single call instruction.
   Emission of the instruction will produce multiple
   HSAIL instructions.  */

class hsa_insn_arg_block : public hsa_insn_basic
{
public:
  hsa_insn_arg_block (BrigKind brig_kind, hsa_insn_call * call);

  /* Kind of argument block.  */
  BrigKind m_kind;

  /* Call instruction.  */
  hsa_insn_call *m_call_insn;
};

/* Report whether or not P is a call block instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_arg_block *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == HSA_OPCODE_ARG_BLOCK);
}

/* HSA comment instruction.  */

class hsa_insn_comment: public hsa_insn_basic
{
public:
  /* Constructor of class representing the comment in HSAIL.  */
  hsa_insn_comment (const char *s);

  /* Default destructor.  */
  ~hsa_insn_comment ();

  char *m_comment;
};

/* Report whether or not P is a call block instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_comment *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_KIND_DIRECTIVE_COMMENT);
}

/* HSA queue instruction.  */

class hsa_insn_queue: public hsa_insn_basic
{
public:
  hsa_insn_queue (int nops, int opcode, BrigSegment segment,
		  BrigMemoryOrder memory_order,
		  hsa_op_base *arg0 = NULL, hsa_op_base *arg1 = NULL,
		  hsa_op_base *arg2 = NULL, hsa_op_base *arg3 = NULL);

  /* Destructor.  */
  ~hsa_insn_queue ();

  /* Segment used to refer to the queue.  Must be global or flat.  */
  BrigSegment m_segment;
  /* Memory order used to specify synchronization.  */
  BrigMemoryOrder m_memory_order;
};

/* Report whether or not P is a queue instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_queue *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_ADDQUEUEWRITEINDEX
	  || p->m_opcode == BRIG_OPCODE_CASQUEUEWRITEINDEX
	  || p->m_opcode == BRIG_OPCODE_LDQUEUEREADINDEX
	  || p->m_opcode == BRIG_OPCODE_LDQUEUEWRITEINDEX
	  || p->m_opcode == BRIG_OPCODE_STQUEUEREADINDEX
	  || p->m_opcode == BRIG_OPCODE_STQUEUEWRITEINDEX);
}

/* HSA source type instruction.  */

class hsa_insn_srctype: public hsa_insn_basic
{
public:
  hsa_insn_srctype (int nops, BrigOpcode opcode, BrigType16_t destt,
		   BrigType16_t srct, hsa_op_base *arg0, hsa_op_base *arg1,
		   hsa_op_base *arg2);

  /* Source type.  */
  BrigType16_t m_source_type;

  /* Destructor.  */
  ~hsa_insn_srctype ();
};

/* Report whether or not P is a source type instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_srctype *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_POPCOUNT
	  || p->m_opcode == BRIG_OPCODE_FIRSTBIT
	  || p->m_opcode == BRIG_OPCODE_LASTBIT);
}

/* HSA packed instruction.  */

class hsa_insn_packed : public hsa_insn_srctype
{
public:
  hsa_insn_packed (int nops, BrigOpcode opcode, BrigType16_t destt,
		   BrigType16_t srct, hsa_op_base *arg0, hsa_op_base *arg1,
		   hsa_op_base *arg2);

  /* Operand list for an operand of the instruction.  */
  hsa_op_operand_list *m_operand_list;

  /* Destructor.  */
  ~hsa_insn_packed ();
};

/* Report whether or not P is a combine instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_packed *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_COMBINE
	  || p->m_opcode == BRIG_OPCODE_EXPAND);
}

/* HSA convert instruction.  */

class hsa_insn_cvt: public hsa_insn_basic
{
public:
  hsa_insn_cvt (hsa_op_with_type *dest, hsa_op_with_type *src);
};

/* Report whether or not P is a convert instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_cvt *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_CVT);
}

/* HSA alloca instruction.  */

class hsa_insn_alloca: public hsa_insn_basic
{
public:
  hsa_insn_alloca (hsa_op_with_type *dest, hsa_op_with_type *size,
		   unsigned alignment = 0);

  /* Required alignment of the allocation.  */
  BrigAlignment8_t m_align;
};

/* Report whether or not P is an alloca instruction.  */

template <>
template <>
inline bool
is_a_helper <hsa_insn_alloca *>::test (hsa_insn_basic *p)
{
  return (p->m_opcode == BRIG_OPCODE_ALLOCA);
}

/* Basic block of HSA instructions.  */

class hsa_bb
{
public:
  hsa_bb (basic_block cfg_bb);
  hsa_bb (basic_block cfg_bb, int idx);

  /* Append an instruction INSN into the basic block.  */
  void append_insn (hsa_insn_basic *insn);

  /* Add a PHI instruction.  */
  void append_phi (hsa_insn_phi *phi);

  /* The real CFG BB that this HBB belongs to.  */
  basic_block m_bb;

  /* The operand that refers to the label to this BB.  */
  hsa_op_code_ref m_label_ref;

  /* The first and last instruction.  */
  hsa_insn_basic *m_first_insn, *m_last_insn;
  /* The first and last phi node.  */
  hsa_insn_phi *m_first_phi, *m_last_phi;

  /* Just a number to construct names from.  */
  int m_index;

  auto_bitmap m_liveout, m_livein;
private:
  /* Make the default constructor inaccessible.  */
  hsa_bb ();
  /* All objects are deallocated by destroying their pool, so make delete
     inaccessible too.  */
  void operator delete (void *) {}
};

/* Return the corresponding HSA basic block structure for the given control
   flow basic_block BB.  */

static inline hsa_bb *
hsa_bb_for_bb (basic_block bb)
{
  return (struct hsa_bb *) bb->aux;
}

/* Class for hashing local hsa_symbols.  */

struct hsa_noop_symbol_hasher : nofree_ptr_hash <hsa_symbol>
{
  static inline hashval_t hash (const value_type);
  static inline bool equal (const value_type, const compare_type);
};

/* Hash hsa_symbol.  */

inline hashval_t
hsa_noop_symbol_hasher::hash (const value_type item)
{
  return DECL_UID (item->m_decl);
}

/* Return true if the DECL_UIDs of decls both symbols refer to are equal.  */

inline bool
hsa_noop_symbol_hasher::equal (const value_type a, const compare_type b)
{
  return (DECL_UID (a->m_decl) == DECL_UID (b->m_decl));
}

/* Structure that encapsulates intermediate representation of a HSA
   function.  */

class hsa_function_representation
{
public:
  hsa_function_representation (tree fdecl, bool kernel_p,
			       unsigned ssa_names_count,
			       bool modified_cfg = false);
  hsa_function_representation (hsa_internal_fn *fn);
  ~hsa_function_representation ();

  /* Builds a shadow register that is utilized to a kernel dispatch.  */
  hsa_op_reg *get_shadow_reg ();

  /* Return true if we are in a function that has kernel dispatch
     shadow register.  */
  bool has_shadow_reg_p ();

  /* The entry/exit blocks don't contain incoming code,
     but the HSA generator might use them to put code into,
     so we need hsa_bb instances of them.  */
  void init_extra_bbs ();

  /* Update CFG dominators if m_modified_cfg flag is set.  */
  void update_dominance ();

  /* Return linkage of the representation.  */
  BrigLinkage8_t get_linkage ();

  /* Create a private symbol of requested TYPE.  */
  hsa_symbol *create_hsa_temporary (BrigType16_t type);

  /* Lookup or create a HSA pseudo register for a given gimple SSA name.  */
  hsa_op_reg *reg_for_gimple_ssa (tree ssa);

  /* Name of the function.  */
  char *m_name;

  /* Number of allocated register structures.  */
  int m_reg_count;

  /* Input arguments.  */
  vec <hsa_symbol *> m_input_args;

  /* Output argument or NULL if there is none.  */
  hsa_symbol *m_output_arg;

  /* Hash table of local variable symbols.  */
  hash_table <hsa_noop_symbol_hasher> *m_local_symbols;

  /* Hash map for string constants.  */
  hash_map <tree, hsa_symbol *> m_string_constants_map;

  /* Vector of pointers to spill symbols.  */
  vec <struct hsa_symbol *> m_spill_symbols;

  /* Vector of pointers to global variables and transformed string constants
     that are used by the function.  */
  vec <struct hsa_symbol *> m_global_symbols;

  /* Private function artificial variables.  */
  vec <struct hsa_symbol *> m_private_variables;

  /* Vector of called function declarations.  */
  vec <tree> m_called_functions;

  /* Vector of used internal functions.  */
  vec <hsa_internal_fn *> m_called_internal_fns;

  /* Number of HBB BBs.  */
  int m_hbb_count;

  /* Whether or not we could check and enforce SSA properties.  */
  bool m_in_ssa;

  /* True if the function is kernel function.  */
  bool m_kern_p;

  /* True if the function representation is a declaration.  */
  bool m_declaration_p;

  /* Function declaration tree.  */
  tree m_decl;

  /* Internal function info is used for declarations of internal functions.  */
  hsa_internal_fn *m_internal_fn;

  /* Runtime shadow register.  */
  hsa_op_reg *m_shadow_reg;

  /* Number of kernel dispatched which take place in the function.  */
  unsigned m_kernel_dispatch_count;

  /* If the function representation contains a kernel dispatch,
     OMP data size is necessary memory that is used for copying before
     a kernel dispatch.  */
  unsigned m_maximum_omp_data_size;

  /* Return true if there's an HSA-specific warning already seen.  */
  bool m_seen_error;

  /* Counter for temporary symbols created in the function representation.  */
  unsigned m_temp_symbol_count;

  /* SSA names mapping.  */
  vec <hsa_op_reg *> m_ssa_map;

  /* Flag whether a function needs update of dominators before RA.  */
  bool m_modified_cfg;
};

enum hsa_function_kind
{
  HSA_NONE,
  HSA_KERNEL,
  HSA_FUNCTION
};

struct hsa_function_summary
{
  /* Default constructor.  */
  hsa_function_summary ();

  /* Kind of GPU/host function.  */
  hsa_function_kind m_kind;

  /* Pointer to a cgraph node which is a HSA implementation of the function.
     In case of the function is a HSA function, the bound function points
     to the host function.  */
  cgraph_node *m_bound_function;

  /* Identifies if the function is an HSA function or a host function.  */
  bool m_gpu_implementation_p;

  /* True if the function is a gridified kernel.  */
  bool m_gridified_kernel_p;
};

inline
hsa_function_summary::hsa_function_summary (): m_kind (HSA_NONE),
  m_bound_function (NULL), m_gpu_implementation_p (false)
{
}

/* Function summary for HSA functions.  */
class hsa_summary_t: public function_summary <hsa_function_summary *>
{
public:
  hsa_summary_t (symbol_table *table):
    function_summary<hsa_function_summary *> (table) { }

  /* Couple GPU and HOST as gpu-specific and host-specific implementation of
     the same function.  KIND determines whether GPU is a host-invokable kernel
     or gpu-callable function and GRIDIFIED_KERNEL_P is set if the function was
     gridified in OMP.  */

  void link_functions (cgraph_node *gpu, cgraph_node *host,
		       hsa_function_kind kind, bool gridified_kernel_p);

private:
  void process_gpu_implementation_attributes (tree gdecl);
};

/* OMP simple builtin describes behavior that should be done for
   the routine.  */
class omp_simple_builtin
{
public:
  omp_simple_builtin (const char *name, const char *warning_message,
	       bool sorry, hsa_op_immed *return_value = NULL):
    m_name (name), m_warning_message (warning_message), m_sorry (sorry),
    m_return_value (return_value)
  {}

  /* Generate HSAIL instructions for the builtin or produce warning message.  */
  void generate (gimple *stmt, hsa_bb *hbb);

  /* Name of function.  */
  const char *m_name;

  /* Warning message.  */
  const char *m_warning_message;

  /* Flag if we should sorry after the warning message is printed.  */
  bool m_sorry;

  /* Return value of the function.  */
  hsa_op_immed *m_return_value;

  /* Emission function.  */
  void (*m_emit_func) (gimple *stmt, hsa_bb *);
};

/* Class for hashing hsa_internal_fn.  */

struct hsa_internal_fn_hasher: free_ptr_hash <hsa_internal_fn>
{
  static inline hashval_t hash (const value_type);
  static inline bool equal (const value_type, const compare_type);
};

/* Hash hsa_symbol.  */

inline hashval_t
hsa_internal_fn_hasher::hash (const value_type item)
{
  return item->m_fn;
}

/* Return true if the DECL_UIDs of decls both symbols refer to  are equal.  */

inline bool
hsa_internal_fn_hasher::equal (const value_type a, const compare_type b)
{
  return a->m_fn == b->m_fn && a->m_type_bit_size == b->m_type_bit_size;
}

/* in hsa-common.c */
extern struct hsa_function_representation *hsa_cfun;
extern hash_map <tree, vec <const char *> *> *hsa_decl_kernel_dependencies;
extern hsa_summary_t *hsa_summaries;
extern hsa_symbol *hsa_num_threads;
extern unsigned hsa_kernel_calls_counter;
extern hash_set <tree> *hsa_failed_functions;
extern hash_table <hsa_noop_symbol_hasher> *hsa_global_variable_symbols;

bool hsa_callable_function_p (tree fndecl);
void hsa_init_compilation_unit_data (void);
void hsa_deinit_compilation_unit_data (void);
bool hsa_machine_large_p (void);
bool hsa_full_profile_p (void);
bool hsa_opcode_floating_bit_insn_p (BrigOpcode16_t);
unsigned hsa_type_bit_size (BrigType16_t t);
BrigType16_t hsa_bittype_for_bitsize (unsigned bitsize);
BrigType16_t hsa_uint_for_bitsize (unsigned bitsize);
BrigType16_t hsa_float_for_bitsize (unsigned bitsize);
BrigType16_t hsa_bittype_for_type (BrigType16_t t);
BrigType16_t hsa_unsigned_type_for_type (BrigType16_t t);
bool hsa_type_packed_p (BrigType16_t type);
bool hsa_type_float_p (BrigType16_t type);
bool hsa_type_integer_p (BrigType16_t type);
bool hsa_btype_p (BrigType16_t type);
BrigAlignment8_t hsa_alignment_encoding (unsigned n);
BrigAlignment8_t hsa_natural_alignment (BrigType16_t type);
BrigAlignment8_t hsa_object_alignment (tree t);
unsigned hsa_byte_alignment (BrigAlignment8_t alignment);
void hsa_destroy_operand (hsa_op_base *op);
void hsa_destroy_insn (hsa_insn_basic *insn);
void hsa_add_kern_decl_mapping (tree decl, char *name, unsigned, bool);
unsigned hsa_get_number_decl_kernel_mappings (void);
tree hsa_get_decl_kernel_mapping_decl (unsigned i);
char *hsa_get_decl_kernel_mapping_name (unsigned i);
unsigned hsa_get_decl_kernel_mapping_omp_size (unsigned i);
bool hsa_get_decl_kernel_mapping_gridified (unsigned i);
void hsa_free_decl_kernel_mapping (void);
tree *hsa_get_ctor_statements (void);
tree *hsa_get_dtor_statements (void);
tree *hsa_get_kernel_dispatch_type (void);
void hsa_add_kernel_dependency (tree caller, const char *called_function);
void hsa_sanitize_name (char *p);
char *hsa_brig_function_name (const char *p);
const char *hsa_get_declaration_name (tree decl);
void hsa_register_kernel (cgraph_node *host);
void hsa_register_kernel (cgraph_node *gpu, cgraph_node *host);
bool hsa_seen_error (void);
void hsa_fail_cfun (void);

/* In hsa-gen.c.  */
void hsa_build_append_simple_mov (hsa_op_reg *, hsa_op_base *, hsa_bb *);
hsa_symbol *hsa_get_spill_symbol (BrigType16_t);
hsa_symbol *hsa_get_string_cst_symbol (BrigType16_t);
hsa_op_reg *hsa_spill_in (hsa_insn_basic *, hsa_op_reg *, hsa_op_reg **);
hsa_op_reg *hsa_spill_out (hsa_insn_basic *, hsa_op_reg *, hsa_op_reg **);
hsa_bb *hsa_init_new_bb (basic_block);
hsa_function_representation *hsa_generate_function_declaration (tree decl);
hsa_function_representation *hsa_generate_internal_fn_decl (hsa_internal_fn *);
tree hsa_get_host_function (tree decl);

/* In hsa-regalloc.c.  */
void hsa_regalloc (void);

/* In hsa-brig.c.  */
extern hash_table <hsa_internal_fn_hasher> *hsa_emitted_internal_decls;
void hsa_brig_emit_function (void);
void hsa_output_brig (void);
unsigned hsa_get_imm_brig_type_len (BrigType16_t type);
void hsa_brig_emit_omp_symbols (void);

/*  In hsa-dump.c.  */
const char *hsa_seg_name (BrigSegment8_t);
void dump_hsa_insn (FILE *f, hsa_insn_basic *insn);
void dump_hsa_bb (FILE *, hsa_bb *);
void dump_hsa_cfun (FILE *);
DEBUG_FUNCTION void debug_hsa_operand (hsa_op_base *opc);
DEBUG_FUNCTION void debug_hsa_insn (hsa_insn_basic *insn);

union hsa_bytes
{
  uint8_t b8;
  uint16_t b16;
  uint32_t b32;
  uint64_t b64;
};

/* Return true if a function DECL is an HSA implementation.  */

static inline bool
hsa_gpu_implementation_p (tree decl)
{
  if (hsa_summaries == NULL)
    return false;

  hsa_function_summary *s = hsa_summaries->get (cgraph_node::get_create (decl));

  return s->m_gpu_implementation_p;
}

#endif /* HSA_H */
