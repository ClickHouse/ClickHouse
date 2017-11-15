/* libunwind - a platform-independent unwind library
   Copyright (c) 2003, 2005 Hewlett-Packard Development Company, L.P.
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#include "dwarf_i.h"
#include "libunwind_i.h"

/* The "pick" operator provides an index range of 0..255 indicating
   that the stack could at least have a depth of up to 256 elements,
   but the GCC unwinder restricts the depth to 64, which seems
   reasonable so we use the same value here.  */
#define MAX_EXPR_STACK_SIZE     64

#define NUM_OPERANDS(signature) (((signature) >> 6) & 0x3)
#define OPND1_TYPE(signature)   (((signature) >> 3) & 0x7)
#define OPND2_TYPE(signature)   (((signature) >> 0) & 0x7)

#define OPND_SIGNATURE(n, t1, t2) (((n) << 6) | ((t1) << 3) | ((t2) << 0))
#define OPND1(t1)               OPND_SIGNATURE(1, t1, 0)
#define OPND2(t1, t2)           OPND_SIGNATURE(2, t1, t2)

#define VAL8    0x0
#define VAL16   0x1
#define VAL32   0x2
#define VAL64   0x3
#define ULEB128 0x4
#define SLEB128 0x5
#define OFFSET  0x6     /* 32-bit offset for 32-bit DWARF, 64-bit otherwise */
#define ADDR    0x7     /* Machine address.  */

static const uint8_t operands[256] =
  {
    [DW_OP_addr] =              OPND1 (ADDR),
    [DW_OP_const1u] =           OPND1 (VAL8),
    [DW_OP_const1s] =           OPND1 (VAL8),
    [DW_OP_const2u] =           OPND1 (VAL16),
    [DW_OP_const2s] =           OPND1 (VAL16),
    [DW_OP_const4u] =           OPND1 (VAL32),
    [DW_OP_const4s] =           OPND1 (VAL32),
    [DW_OP_const8u] =           OPND1 (VAL64),
    [DW_OP_const8s] =           OPND1 (VAL64),
    [DW_OP_pick] =              OPND1 (VAL8),
    [DW_OP_plus_uconst] =       OPND1 (ULEB128),
    [DW_OP_skip] =              OPND1 (VAL16),
    [DW_OP_bra] =               OPND1 (VAL16),
    [DW_OP_breg0 +  0] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  1] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  2] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  3] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  4] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  5] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  6] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  7] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  8] =        OPND1 (SLEB128),
    [DW_OP_breg0 +  9] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 10] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 11] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 12] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 13] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 14] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 15] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 16] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 17] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 18] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 19] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 20] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 21] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 22] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 23] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 24] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 25] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 26] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 27] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 28] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 29] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 30] =        OPND1 (SLEB128),
    [DW_OP_breg0 + 31] =        OPND1 (SLEB128),
    [DW_OP_regx] =              OPND1 (ULEB128),
    [DW_OP_fbreg] =             OPND1 (SLEB128),
    [DW_OP_bregx] =             OPND2 (ULEB128, SLEB128),
    [DW_OP_piece] =             OPND1 (ULEB128),
    [DW_OP_deref_size] =        OPND1 (VAL8),
    [DW_OP_xderef_size] =       OPND1 (VAL8),
    [DW_OP_call2] =             OPND1 (VAL16),
    [DW_OP_call4] =             OPND1 (VAL32),
    [DW_OP_call_ref] =          OPND1 (OFFSET)
  };

static inline unw_sword_t
sword (unw_addr_space_t as, unw_word_t val)
{
  switch (dwarf_addr_size (as))
    {
    case 1: return (int8_t) val;
    case 2: return (int16_t) val;
    case 4: return (int32_t) val;
    case 8: return (int64_t) val;
    default: abort ();
    }
}

static inline unw_word_t
read_operand (unw_addr_space_t as, unw_accessors_t *a,
              unw_word_t *addr, int operand_type, unw_word_t *val, void *arg)
{
  uint8_t u8;
  uint16_t u16;
  uint32_t u32;
  uint64_t u64;
  int ret;

  if (operand_type == ADDR)
    switch (dwarf_addr_size (as))
      {
      case 1: operand_type = VAL8; break;
      case 2: operand_type = VAL16; break;
      case 4: operand_type = VAL32; break;
      case 8: operand_type = VAL64; break;
      default: abort ();
      }

  switch (operand_type)
    {
    case VAL8:
      ret = dwarf_readu8 (as, a, addr, &u8, arg);
      if (ret < 0)
        return ret;
      *val = u8;
      break;

    case VAL16:
      ret = dwarf_readu16 (as, a, addr, &u16, arg);
      if (ret < 0)
        return ret;
      *val = u16;
      break;

    case VAL32:
      ret = dwarf_readu32 (as, a, addr, &u32, arg);
      if (ret < 0)
        return ret;
      *val = u32;
      break;

    case VAL64:
      ret = dwarf_readu64 (as, a, addr, &u64, arg);
      if (ret < 0)
        return ret;
      *val = u64;
      break;

    case ULEB128:
      ret = dwarf_read_uleb128 (as, a, addr, val, arg);
      break;

    case SLEB128:
      ret = dwarf_read_sleb128 (as, a, addr, val, arg);
      break;

    case OFFSET: /* only used by DW_OP_call_ref, which we don't implement */
    default:
      Debug (1, "Unexpected operand type %d\n", operand_type);
      ret = -UNW_EINVAL;
    }
  return ret;
}

HIDDEN int
dwarf_stack_aligned(struct dwarf_cursor *c, unw_word_t cfa_addr,
                    unw_word_t rbp_addr, unw_word_t *cfa_offset) {
  unw_accessors_t *a;
  int ret;
  void *arg;
  unw_word_t len;
  uint8_t opcode;
  unw_word_t operand1;

  a = unw_get_accessors (c->as);
  arg = c->as_arg;

  ret = dwarf_read_uleb128(c->as, a, &rbp_addr, &len, arg);
  if (len != 2 || ret < 0)
    return 0;

  ret = dwarf_readu8(c->as, a, &rbp_addr, &opcode, arg);
  if (ret < 0 || opcode != DW_OP_breg6)
    return 0;

  ret = read_operand(c->as, a, &rbp_addr,
                     OPND1_TYPE(operands[opcode]), &operand1, arg);

  if (ret < 0 || operand1 != 0)
    return 0;

  ret = dwarf_read_uleb128(c->as, a, &cfa_addr, &len, arg);
  if (ret < 0 || len != 3)
    return 0;

  ret = dwarf_readu8(c->as, a, &cfa_addr, &opcode, arg);
  if (ret < 0 || opcode != DW_OP_breg6)
    return 0;

  ret = read_operand(c->as, a, &cfa_addr,
                     OPND1_TYPE(operands[opcode]), &operand1, arg);
  if (ret < 0)
    return 0;

  ret = dwarf_readu8(c->as, a, &cfa_addr, &opcode, arg);
  if (ret < 0 || opcode != DW_OP_deref)
    return 0;

  *cfa_offset = operand1;
  return 1;
}

HIDDEN int
dwarf_eval_expr (struct dwarf_cursor *c, unw_word_t *addr, unw_word_t len,
                 unw_word_t *valp, int *is_register)
{
  unw_word_t operand1 = 0, operand2 = 0, tmp1, tmp2 = 0, tmp3, end_addr;
  uint8_t opcode, operands_signature, u8;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  unw_word_t stack[MAX_EXPR_STACK_SIZE];
  unsigned int tos = 0;
  uint16_t u16;
  uint32_t u32;
  uint64_t u64;
  int ret;
# define pop()                                  \
({                                              \
  if ((tos - 1) >= MAX_EXPR_STACK_SIZE)         \
    {                                           \
      Debug (1, "Stack underflow\n");           \
      return -UNW_EINVAL;                       \
    }                                           \
  stack[--tos];                                 \
})
# define push(x)                                \
do {                                            \
  unw_word_t _x = (x);                          \
  if (tos >= MAX_EXPR_STACK_SIZE)               \
    {                                           \
      Debug (1, "Stack overflow\n");            \
      return -UNW_EINVAL;                       \
    }                                           \
  stack[tos++] = _x;                            \
} while (0)
# define pick(n)                                \
({                                              \
  unsigned int _index = tos - 1 - (n);          \
  if (_index >= MAX_EXPR_STACK_SIZE)            \
    {                                           \
      Debug (1, "Out-of-stack pick\n");         \
      return -UNW_EINVAL;                       \
    }                                           \
  stack[_index];                                \
})

  as = c->as;
  arg = c->as_arg;
  a = unw_get_accessors (as);
  end_addr = *addr + len;
  *is_register = 0;

  Debug (14, "len=%lu, pushing cfa=0x%lx\n",
         (unsigned long) len, (unsigned long) c->cfa);

  push (c->cfa);        /* push current CFA as required by DWARF spec */

  while (*addr < end_addr)
    {
      if ((ret = dwarf_readu8 (as, a, addr, &opcode, arg)) < 0)
        return ret;

      operands_signature = operands[opcode];

      if (unlikely (NUM_OPERANDS (operands_signature) > 0))
        {
          if ((ret = read_operand (as, a, addr,
                                   OPND1_TYPE (operands_signature),
                                   &operand1, arg)) < 0)
            return ret;
          if (NUM_OPERANDS (operands_signature) > 1)
            if ((ret = read_operand (as, a, addr,
                                     OPND2_TYPE (operands_signature),
                                     &operand2, arg)) < 0)
              return ret;
        }

      switch ((dwarf_expr_op_t) opcode)
        {
        case DW_OP_lit0:  case DW_OP_lit1:  case DW_OP_lit2:
        case DW_OP_lit3:  case DW_OP_lit4:  case DW_OP_lit5:
        case DW_OP_lit6:  case DW_OP_lit7:  case DW_OP_lit8:
        case DW_OP_lit9:  case DW_OP_lit10: case DW_OP_lit11:
        case DW_OP_lit12: case DW_OP_lit13: case DW_OP_lit14:
        case DW_OP_lit15: case DW_OP_lit16: case DW_OP_lit17:
        case DW_OP_lit18: case DW_OP_lit19: case DW_OP_lit20:
        case DW_OP_lit21: case DW_OP_lit22: case DW_OP_lit23:
        case DW_OP_lit24: case DW_OP_lit25: case DW_OP_lit26:
        case DW_OP_lit27: case DW_OP_lit28: case DW_OP_lit29:
        case DW_OP_lit30: case DW_OP_lit31:
          Debug (15, "OP_lit(%d)\n", (int) opcode - DW_OP_lit0);
          push (opcode - DW_OP_lit0);
          break;

        case DW_OP_breg0:  case DW_OP_breg1:  case DW_OP_breg2:
        case DW_OP_breg3:  case DW_OP_breg4:  case DW_OP_breg5:
        case DW_OP_breg6:  case DW_OP_breg7:  case DW_OP_breg8:
        case DW_OP_breg9:  case DW_OP_breg10: case DW_OP_breg11:
        case DW_OP_breg12: case DW_OP_breg13: case DW_OP_breg14:
        case DW_OP_breg15: case DW_OP_breg16: case DW_OP_breg17:
        case DW_OP_breg18: case DW_OP_breg19: case DW_OP_breg20:
        case DW_OP_breg21: case DW_OP_breg22: case DW_OP_breg23:
        case DW_OP_breg24: case DW_OP_breg25: case DW_OP_breg26:
        case DW_OP_breg27: case DW_OP_breg28: case DW_OP_breg29:
        case DW_OP_breg30: case DW_OP_breg31:
          Debug (15, "OP_breg(r%d,0x%lx)\n",
                 (int) opcode - DW_OP_breg0, (unsigned long) operand1);
          if ((ret = unw_get_reg (dwarf_to_cursor (c),
                                  dwarf_to_unw_regnum (opcode - DW_OP_breg0),
                                  &tmp1)) < 0)
            return ret;
          push (tmp1 + operand1);
          break;

        case DW_OP_bregx:
          Debug (15, "OP_bregx(r%d,0x%lx)\n",
                 (int) operand1, (unsigned long) operand2);
          if ((ret = unw_get_reg (dwarf_to_cursor (c),
                                  dwarf_to_unw_regnum (operand1), &tmp1)) < 0)
            return ret;
          push (tmp1 + operand2);
          break;

        case DW_OP_reg0:  case DW_OP_reg1:  case DW_OP_reg2:
        case DW_OP_reg3:  case DW_OP_reg4:  case DW_OP_reg5:
        case DW_OP_reg6:  case DW_OP_reg7:  case DW_OP_reg8:
        case DW_OP_reg9:  case DW_OP_reg10: case DW_OP_reg11:
        case DW_OP_reg12: case DW_OP_reg13: case DW_OP_reg14:
        case DW_OP_reg15: case DW_OP_reg16: case DW_OP_reg17:
        case DW_OP_reg18: case DW_OP_reg19: case DW_OP_reg20:
        case DW_OP_reg21: case DW_OP_reg22: case DW_OP_reg23:
        case DW_OP_reg24: case DW_OP_reg25: case DW_OP_reg26:
        case DW_OP_reg27: case DW_OP_reg28: case DW_OP_reg29:
        case DW_OP_reg30: case DW_OP_reg31:
          Debug (15, "OP_reg(r%d)\n", (int) opcode - DW_OP_reg0);
          *valp = dwarf_to_unw_regnum (opcode - DW_OP_reg0);
          *is_register = 1;
          return 0;

        case DW_OP_regx:
          Debug (15, "OP_regx(r%d)\n", (int) operand1);
          *valp = dwarf_to_unw_regnum (operand1);
          *is_register = 1;
          return 0;

        case DW_OP_addr:
        case DW_OP_const1u:
        case DW_OP_const2u:
        case DW_OP_const4u:
        case DW_OP_const8u:
        case DW_OP_constu:
        case DW_OP_const8s:
        case DW_OP_consts:
          Debug (15, "OP_const(0x%lx)\n", (unsigned long) operand1);
          push (operand1);
          break;

        case DW_OP_const1s:
          if (operand1 & 0x80)
            operand1 |= ((unw_word_t) -1) << 8;
          Debug (15, "OP_const1s(%ld)\n", (long) operand1);
          push (operand1);
          break;

        case DW_OP_const2s:
          if (operand1 & 0x8000)
            operand1 |= ((unw_word_t) -1) << 16;
          Debug (15, "OP_const2s(%ld)\n", (long) operand1);
          push (operand1);
          break;

        case DW_OP_const4s:
          if (operand1 & 0x80000000)
            operand1 |= (((unw_word_t) -1) << 16) << 16;
          Debug (15, "OP_const4s(%ld)\n", (long) operand1);
          push (operand1);
          break;

        case DW_OP_deref:
          Debug (15, "OP_deref\n");
          tmp1 = pop ();
          if ((ret = dwarf_readw (as, a, &tmp1, &tmp2, arg)) < 0)
            return ret;
          push (tmp2);
          break;

        case DW_OP_deref_size:
          Debug (15, "OP_deref_size(%d)\n", (int) operand1);
          tmp1 = pop ();
          switch (operand1)
            {
            default:
              Debug (1, "Unexpected DW_OP_deref_size size %d\n",
                     (int) operand1);
              return -UNW_EINVAL;

            case 1:
              if ((ret = dwarf_readu8 (as, a, &tmp1, &u8, arg)) < 0)
                return ret;
              tmp2 = u8;
              break;

            case 2:
              if ((ret = dwarf_readu16 (as, a, &tmp1, &u16, arg)) < 0)
                return ret;
              tmp2 = u16;
              break;

            case 3:
            case 4:
              if ((ret = dwarf_readu32 (as, a, &tmp1, &u32, arg)) < 0)
                return ret;
              tmp2 = u32;
              if (operand1 == 3)
                {
                  if (dwarf_is_big_endian (as))
                    tmp2 >>= 8;
                  else
                    tmp2 &= 0xffffff;
                }
              break;
            case 5:
            case 6:
            case 7:
            case 8:
              if ((ret = dwarf_readu64 (as, a, &tmp1, &u64, arg)) < 0)
                return ret;
              tmp2 = u64;
              if (operand1 != 8)
                {
                  if (dwarf_is_big_endian (as))
                    tmp2 >>= 64 - 8 * operand1;
                  else
                    tmp2 &= (~ (unw_word_t) 0) << (8 * operand1);
                }
              break;
            }
          push (tmp2);
          break;

        case DW_OP_dup:
          Debug (15, "OP_dup\n");
          push (pick (0));
          break;

        case DW_OP_drop:
          Debug (15, "OP_drop\n");
          (void) pop ();
          break;

        case DW_OP_pick:
          Debug (15, "OP_pick(%d)\n", (int) operand1);
          push (pick (operand1));
          break;

        case DW_OP_over:
          Debug (15, "OP_over\n");
          push (pick (1));
          break;

        case DW_OP_swap:
          Debug (15, "OP_swap\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp1);
          push (tmp2);
          break;

        case DW_OP_rot:
          Debug (15, "OP_rot\n");
          tmp1 = pop ();
          tmp2 = pop ();
          tmp3 = pop ();
          push (tmp1);
          push (tmp3);
          push (tmp2);
          break;

        case DW_OP_abs:
          Debug (15, "OP_abs\n");
          tmp1 = pop ();
          if (tmp1 & ((unw_word_t) 1 << (8 * dwarf_addr_size (as) - 1)))
            tmp1 = -tmp1;
          push (tmp1);
          break;

        case DW_OP_and:
          Debug (15, "OP_and\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp1 & tmp2);
          break;

        case DW_OP_div:
          Debug (15, "OP_div\n");
          tmp1 = pop ();
          tmp2 = pop ();
          if (tmp1)
            tmp1 = sword (as, tmp2) / sword (as, tmp1);
          push (tmp1);
          break;

        case DW_OP_minus:
          Debug (15, "OP_minus\n");
          tmp1 = pop ();
          tmp2 = pop ();
          tmp1 = tmp2 - tmp1;
          push (tmp1);
          break;

        case DW_OP_mod:
          Debug (15, "OP_mod\n");
          tmp1 = pop ();
          tmp2 = pop ();
          if (tmp1)
            tmp1 = tmp2 % tmp1;
          push (tmp1);
          break;

        case DW_OP_mul:
          Debug (15, "OP_mul\n");
          tmp1 = pop ();
          tmp2 = pop ();
          if (tmp1)
            tmp1 = tmp2 * tmp1;
          push (tmp1);
          break;

        case DW_OP_neg:
          Debug (15, "OP_neg\n");
          push (-pop ());
          break;

        case DW_OP_not:
          Debug (15, "OP_not\n");
          push (~pop ());
          break;

        case DW_OP_or:
          Debug (15, "OP_or\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp1 | tmp2);
          break;

        case DW_OP_plus:
          Debug (15, "OP_plus\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp1 + tmp2);
          break;

        case DW_OP_plus_uconst:
          Debug (15, "OP_plus_uconst(%lu)\n", (unsigned long) operand1);
          tmp1 = pop ();
          push (tmp1 + operand1);
          break;

        case DW_OP_shl:
          Debug (15, "OP_shl\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp2 << tmp1);
          break;

        case DW_OP_shr:
          Debug (15, "OP_shr\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp2 >> tmp1);
          break;

        case DW_OP_shra:
          Debug (15, "OP_shra\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) >> tmp1);
          break;

        case DW_OP_xor:
          Debug (15, "OP_xor\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (tmp1 ^ tmp2);
          break;

        case DW_OP_le:
          Debug (15, "OP_le\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) <= sword (as, tmp1));
          break;

        case DW_OP_ge:
          Debug (15, "OP_ge\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) >= sword (as, tmp1));
          break;

        case DW_OP_eq:
          Debug (15, "OP_eq\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) == sword (as, tmp1));
          break;

        case DW_OP_lt:
          Debug (15, "OP_lt\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) < sword (as, tmp1));
          break;

        case DW_OP_gt:
          Debug (15, "OP_gt\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) > sword (as, tmp1));
          break;

        case DW_OP_ne:
          Debug (15, "OP_ne\n");
          tmp1 = pop ();
          tmp2 = pop ();
          push (sword (as, tmp2) != sword (as, tmp1));
          break;

        case DW_OP_skip:
          Debug (15, "OP_skip(%d)\n", (int16_t) operand1);
          *addr += (int16_t) operand1;
          break;

        case DW_OP_bra:
          Debug (15, "OP_skip(%d)\n", (int16_t) operand1);
          tmp1 = pop ();
          if (tmp1)
            *addr += (int16_t) operand1;
          break;

        case DW_OP_nop:
          Debug (15, "OP_nop\n");
          break;

        case DW_OP_call2:
        case DW_OP_call4:
        case DW_OP_call_ref:
        case DW_OP_fbreg:
        case DW_OP_piece:
        case DW_OP_push_object_address:
        case DW_OP_xderef:
        case DW_OP_xderef_size:
        default:
          Debug (1, "Unexpected opcode 0x%x\n", opcode);
          return -UNW_EINVAL;
        }
    }
  *valp = pop ();
  Debug (14, "final value = 0x%lx\n", (unsigned long) *valp);
  return 0;
}
