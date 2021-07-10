/* HSA BRIG (binary representation of HSAIL) 1.0.1 representation description.
   Copyright (C) 2016-2018 Free Software Foundation, Inc.

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
<http://www.gnu.org/licenses/>.

The contents of the file was created by extracting data structures, enum,
typedef and other definitions from HSA Programmer's Reference Manual Version
1.0.1 (http://www.hsafoundation.com/standards/).

HTML version is provided on the following link:
http://www.hsafoundation.com/html/Content/PRM/Topics/PRM_title_page.htm */

#ifndef HSA_BRIG_FORMAT_H
#define HSA_BRIG_FORMAT_H

struct BrigModuleHeader;
typedef uint16_t BrigKind16_t;
typedef uint32_t BrigVersion32_t;

typedef BrigModuleHeader *BrigModule_t;
typedef uint32_t BrigDataOffset32_t;
typedef uint32_t BrigCodeOffset32_t;
typedef uint32_t BrigOperandOffset32_t;
typedef BrigDataOffset32_t BrigDataOffsetString32_t;
typedef BrigDataOffset32_t BrigDataOffsetCodeList32_t;
typedef BrigDataOffset32_t BrigDataOffsetOperandList32_t;
typedef uint8_t BrigAlignment8_t;

enum BrigAlignment
{
  BRIG_ALIGNMENT_NONE = 0,
  BRIG_ALIGNMENT_1 = 1,
  BRIG_ALIGNMENT_2 = 2,
  BRIG_ALIGNMENT_4 = 3,
  BRIG_ALIGNMENT_8 = 4,
  BRIG_ALIGNMENT_16 = 5,
  BRIG_ALIGNMENT_32 = 6,
  BRIG_ALIGNMENT_64 = 7,
  BRIG_ALIGNMENT_128 = 8,
  BRIG_ALIGNMENT_256 = 9
};

typedef uint8_t BrigAllocation8_t;

enum BrigAllocation
{
  BRIG_ALLOCATION_NONE = 0,
  BRIG_ALLOCATION_PROGRAM = 1,
  BRIG_ALLOCATION_AGENT = 2,
  BRIG_ALLOCATION_AUTOMATIC = 3
};

typedef uint8_t BrigAluModifier8_t;

enum BrigAluModifierMask
{
  BRIG_ALU_FTZ = 1
};

typedef uint8_t BrigAtomicOperation8_t;

enum BrigAtomicOperation
{
  BRIG_ATOMIC_ADD = 0,
  BRIG_ATOMIC_AND = 1,
  BRIG_ATOMIC_CAS = 2,
  BRIG_ATOMIC_EXCH = 3,
  BRIG_ATOMIC_LD = 4,
  BRIG_ATOMIC_MAX = 5,
  BRIG_ATOMIC_MIN = 6,
  BRIG_ATOMIC_OR = 7,
  BRIG_ATOMIC_ST = 8,
  BRIG_ATOMIC_SUB = 9,
  BRIG_ATOMIC_WRAPDEC = 10,
  BRIG_ATOMIC_WRAPINC = 11,
  BRIG_ATOMIC_XOR = 12,
  BRIG_ATOMIC_WAIT_EQ = 13,
  BRIG_ATOMIC_WAIT_NE = 14,
  BRIG_ATOMIC_WAIT_LT = 15,
  BRIG_ATOMIC_WAIT_GTE = 16,
  BRIG_ATOMIC_WAITTIMEOUT_EQ = 17,
  BRIG_ATOMIC_WAITTIMEOUT_NE = 18,
  BRIG_ATOMIC_WAITTIMEOUT_LT = 19,
  BRIG_ATOMIC_WAITTIMEOUT_GTE = 20
};

struct BrigBase
{
  uint16_t byteCount;
  BrigKind16_t kind;
};

typedef uint8_t BrigCompareOperation8_t;

enum BrigCompareOperation
{
  BRIG_COMPARE_EQ = 0,
  BRIG_COMPARE_NE = 1,
  BRIG_COMPARE_LT = 2,
  BRIG_COMPARE_LE = 3,
  BRIG_COMPARE_GT = 4,
  BRIG_COMPARE_GE = 5,
  BRIG_COMPARE_EQU = 6,
  BRIG_COMPARE_NEU = 7,
  BRIG_COMPARE_LTU = 8,
  BRIG_COMPARE_LEU = 9,
  BRIG_COMPARE_GTU = 10,
  BRIG_COMPARE_GEU = 11,
  BRIG_COMPARE_NUM = 12,
  BRIG_COMPARE_NAN = 13,
  BRIG_COMPARE_SEQ = 14,
  BRIG_COMPARE_SNE = 15,
  BRIG_COMPARE_SLT = 16,
  BRIG_COMPARE_SLE = 17,
  BRIG_COMPARE_SGT = 18,
  BRIG_COMPARE_SGE = 19,
  BRIG_COMPARE_SGEU = 20,
  BRIG_COMPARE_SEQU = 21,
  BRIG_COMPARE_SNEU = 22,
  BRIG_COMPARE_SLTU = 23,
  BRIG_COMPARE_SLEU = 24,
  BRIG_COMPARE_SNUM = 25,
  BRIG_COMPARE_SNAN = 26,
  BRIG_COMPARE_SGTU = 27
};

typedef uint16_t BrigControlDirective16_t;

enum BrigControlDirective
{
  BRIG_CONTROL_NONE = 0,
  BRIG_CONTROL_ENABLEBREAKEXCEPTIONS = 1,
  BRIG_CONTROL_ENABLEDETECTEXCEPTIONS = 2,
  BRIG_CONTROL_MAXDYNAMICGROUPSIZE = 3,
  BRIG_CONTROL_MAXFLATGRIDSIZE = 4,
  BRIG_CONTROL_MAXFLATWORKGROUPSIZE = 5,
  BRIG_CONTROL_REQUIREDDIM = 6,
  BRIG_CONTROL_REQUIREDGRIDSIZE = 7,
  BRIG_CONTROL_REQUIREDWORKGROUPSIZE = 8,
  BRIG_CONTROL_REQUIRENOPARTIALWORKGROUPS = 9
};

typedef uint32_t BrigExceptions32_t;

enum BrigExceptionsMask
{
  BRIG_EXCEPTIONS_INVALID_OPERATION = 1 << 0,
  BRIG_EXCEPTIONS_DIVIDE_BY_ZERO = 1 << 1,
  BRIG_EXCEPTIONS_OVERFLOW = 1 << 2,
  BRIG_EXCEPTIONS_UNDERFLOW = 1 << 3,
  BRIG_EXCEPTIONS_INEXACT = 1 << 4,
  BRIG_EXCEPTIONS_FIRST_USER_DEFINED = 1 << 16
};

typedef uint8_t BrigExecutableModifier8_t;

enum BrigExecutableModifierMask
{
  BRIG_EXECUTABLE_DEFINITION = 1
};

typedef uint8_t BrigImageChannelOrder8_t;

enum BrigImageChannelOrder
{
  BRIG_CHANNEL_ORDER_A = 0,
  BRIG_CHANNEL_ORDER_R = 1,
  BRIG_CHANNEL_ORDER_RX = 2,
  BRIG_CHANNEL_ORDER_RG = 3,
  BRIG_CHANNEL_ORDER_RGX = 4,
  BRIG_CHANNEL_ORDER_RA = 5,
  BRIG_CHANNEL_ORDER_RGB = 6,
  BRIG_CHANNEL_ORDER_RGBX = 7,
  BRIG_CHANNEL_ORDER_RGBA = 8,
  BRIG_CHANNEL_ORDER_BGRA = 9,
  BRIG_CHANNEL_ORDER_ARGB = 10,
  BRIG_CHANNEL_ORDER_ABGR = 11,
  BRIG_CHANNEL_ORDER_SRGB = 12,
  BRIG_CHANNEL_ORDER_SRGBX = 13,
  BRIG_CHANNEL_ORDER_SRGBA = 14,
  BRIG_CHANNEL_ORDER_SBGRA = 15,
  BRIG_CHANNEL_ORDER_INTENSITY = 16,
  BRIG_CHANNEL_ORDER_LUMINANCE = 17,
  BRIG_CHANNEL_ORDER_DEPTH = 18,
  BRIG_CHANNEL_ORDER_DEPTH_STENCIL = 19,
  BRIG_CHANNEL_ORDER_FIRST_USER_DEFINED = 128
};

typedef uint8_t BrigImageChannelType8_t;

enum BrigImageChannelType
{
  BRIG_CHANNEL_TYPE_SNORM_INT8 = 0,
  BRIG_CHANNEL_TYPE_SNORM_INT16 = 1,
  BRIG_CHANNEL_TYPE_UNORM_INT8 = 2,
  BRIG_CHANNEL_TYPE_UNORM_INT16 = 3,
  BRIG_CHANNEL_TYPE_UNORM_INT24 = 4,
  BRIG_CHANNEL_TYPE_UNORM_SHORT_555 = 5,
  BRIG_CHANNEL_TYPE_UNORM_SHORT_565 = 6,
  BRIG_CHANNEL_TYPE_UNORM_INT_101010 = 7,
  BRIG_CHANNEL_TYPE_SIGNED_INT8 = 8,
  BRIG_CHANNEL_TYPE_SIGNED_INT16 = 9,
  BRIG_CHANNEL_TYPE_SIGNED_INT32 = 10,
  BRIG_CHANNEL_TYPE_UNSIGNED_INT8 = 11,
  BRIG_CHANNEL_TYPE_UNSIGNED_INT16 = 12,
  BRIG_CHANNEL_TYPE_UNSIGNED_INT32 = 13,
  BRIG_CHANNEL_TYPE_HALF_FLOAT = 14,
  BRIG_CHANNEL_TYPE_FLOAT = 15,
  BRIG_CHANNEL_TYPE_FIRST_USER_DEFINED = 128
};

typedef uint8_t BrigImageGeometry8_t;

enum BrigImageGeometry
{
  BRIG_GEOMETRY_1D = 0,
  BRIG_GEOMETRY_2D = 1,
  BRIG_GEOMETRY_3D = 2,
  BRIG_GEOMETRY_1DA = 3,
  BRIG_GEOMETRY_2DA = 4,
  BRIG_GEOMETRY_1DB = 5,
  BRIG_GEOMETRY_2DDEPTH = 6,
  BRIG_GEOMETRY_2DADEPTH = 7,
  BRIG_GEOMETRY_FIRST_USER_DEFINED = 128
};

typedef uint8_t BrigImageQuery8_t;

enum BrigImageQuery
{
  BRIG_IMAGE_QUERY_WIDTH = 0,
  BRIG_IMAGE_QUERY_HEIGHT = 1,
  BRIG_IMAGE_QUERY_DEPTH = 2,
  BRIG_IMAGE_QUERY_ARRAY = 3,
  BRIG_IMAGE_QUERY_CHANNELORDER = 4,
  BRIG_IMAGE_QUERY_CHANNELTYPE = 5
};

enum BrigKind
{
  BRIG_KIND_NONE = 0x0000,
  BRIG_KIND_DIRECTIVE_BEGIN = 0x1000,
  BRIG_KIND_DIRECTIVE_ARG_BLOCK_END = 0x1000,
  BRIG_KIND_DIRECTIVE_ARG_BLOCK_START = 0x1001,
  BRIG_KIND_DIRECTIVE_COMMENT = 0x1002,
  BRIG_KIND_DIRECTIVE_CONTROL = 0x1003,
  BRIG_KIND_DIRECTIVE_EXTENSION = 0x1004,
  BRIG_KIND_DIRECTIVE_FBARRIER = 0x1005,
  BRIG_KIND_DIRECTIVE_FUNCTION = 0x1006,
  BRIG_KIND_DIRECTIVE_INDIRECT_FUNCTION = 0x1007,
  BRIG_KIND_DIRECTIVE_KERNEL = 0x1008,
  BRIG_KIND_DIRECTIVE_LABEL = 0x1009,
  BRIG_KIND_DIRECTIVE_LOC = 0x100a,
  BRIG_KIND_DIRECTIVE_MODULE = 0x100b,
  BRIG_KIND_DIRECTIVE_PRAGMA = 0x100c,
  BRIG_KIND_DIRECTIVE_SIGNATURE = 0x100d,
  BRIG_KIND_DIRECTIVE_VARIABLE = 0x100e,
  BRIG_KIND_DIRECTIVE_END = 0x100f,
  BRIG_KIND_INST_BEGIN = 0x2000,
  BRIG_KIND_INST_ADDR = 0x2000,
  BRIG_KIND_INST_ATOMIC = 0x2001,
  BRIG_KIND_INST_BASIC = 0x2002,
  BRIG_KIND_INST_BR = 0x2003,
  BRIG_KIND_INST_CMP = 0x2004,
  BRIG_KIND_INST_CVT = 0x2005,
  BRIG_KIND_INST_IMAGE = 0x2006,
  BRIG_KIND_INST_LANE = 0x2007,
  BRIG_KIND_INST_MEM = 0x2008,
  BRIG_KIND_INST_MEM_FENCE = 0x2009,
  BRIG_KIND_INST_MOD = 0x200a,
  BRIG_KIND_INST_QUERY_IMAGE = 0x200b,
  BRIG_KIND_INST_QUERY_SAMPLER = 0x200c,
  BRIG_KIND_INST_QUEUE = 0x200d,
  BRIG_KIND_INST_SEG = 0x200e,
  BRIG_KIND_INST_SEG_CVT = 0x200f,
  BRIG_KIND_INST_SIGNAL = 0x2010,
  BRIG_KIND_INST_SOURCE_TYPE = 0x2011,
  BRIG_KIND_INST_END = 0x2012,
  BRIG_KIND_OPERAND_BEGIN = 0x3000,
  BRIG_KIND_OPERAND_ADDRESS = 0x3000,
  BRIG_KIND_OPERAND_ALIGN = 0x3001,
  BRIG_KIND_OPERAND_CODE_LIST = 0x3002,
  BRIG_KIND_OPERAND_CODE_REF = 0x3003,
  BRIG_KIND_OPERAND_CONSTANT_BYTES = 0x3004,
  BRIG_KIND_OPERAND_RESERVED = 0x3005,
  BRIG_KIND_OPERAND_CONSTANT_IMAGE = 0x3006,
  BRIG_KIND_OPERAND_CONSTANT_OPERAND_LIST = 0x3007,
  BRIG_KIND_OPERAND_CONSTANT_SAMPLER = 0x3008,
  BRIG_KIND_OPERAND_OPERAND_LIST = 0x3009,
  BRIG_KIND_OPERAND_REGISTER = 0x300a,
  BRIG_KIND_OPERAND_STRING = 0x300b,
  BRIG_KIND_OPERAND_WAVESIZE = 0x300c,
  BRIG_KIND_OPERAND_END = 0x300d
};

typedef uint8_t BrigLinkage8_t;

enum BrigLinkage
{
  BRIG_LINKAGE_NONE = 0,
  BRIG_LINKAGE_PROGRAM = 1,
  BRIG_LINKAGE_MODULE = 2,
  BRIG_LINKAGE_FUNCTION = 3,
  BRIG_LINKAGE_ARG = 4
};

typedef uint8_t BrigMachineModel8_t;

enum BrigMachineModel
{
  BRIG_MACHINE_SMALL = 0,
  BRIG_MACHINE_LARGE = 1
};

typedef uint8_t BrigMemoryModifier8_t;

enum BrigMemoryModifierMask
{
  BRIG_MEMORY_CONST = 1
};

typedef uint8_t BrigMemoryOrder8_t;

enum BrigMemoryOrder
{
  BRIG_MEMORY_ORDER_NONE = 0,
  BRIG_MEMORY_ORDER_RELAXED = 1,
  BRIG_MEMORY_ORDER_SC_ACQUIRE = 2,
  BRIG_MEMORY_ORDER_SC_RELEASE = 3,
  BRIG_MEMORY_ORDER_SC_ACQUIRE_RELEASE = 4
};

typedef uint8_t BrigMemoryScope8_t;

enum BrigMemoryScope
{
  BRIG_MEMORY_SCOPE_NONE = 0,
  BRIG_MEMORY_SCOPE_WORKITEM = 1,
  BRIG_MEMORY_SCOPE_WAVEFRONT = 2,
  BRIG_MEMORY_SCOPE_WORKGROUP = 3,
  BRIG_MEMORY_SCOPE_AGENT = 4,
  BRIG_MEMORY_SCOPE_SYSTEM = 5
};

struct BrigModuleHeader
{
  char identification[8];
  BrigVersion32_t brigMajor;
  BrigVersion32_t brigMinor;
  uint64_t byteCount;
  uint8_t hash[64];
  uint32_t reserved;
  uint32_t sectionCount;
  uint64_t sectionIndex;
};

typedef uint16_t BrigOpcode16_t;

enum BrigOpcode
{
  BRIG_OPCODE_NOP = 0,
  BRIG_OPCODE_ABS = 1,
  BRIG_OPCODE_ADD = 2,
  BRIG_OPCODE_BORROW = 3,
  BRIG_OPCODE_CARRY = 4,
  BRIG_OPCODE_CEIL = 5,
  BRIG_OPCODE_COPYSIGN = 6,
  BRIG_OPCODE_DIV = 7,
  BRIG_OPCODE_FLOOR = 8,
  BRIG_OPCODE_FMA = 9,
  BRIG_OPCODE_FRACT = 10,
  BRIG_OPCODE_MAD = 11,
  BRIG_OPCODE_MAX = 12,
  BRIG_OPCODE_MIN = 13,
  BRIG_OPCODE_MUL = 14,
  BRIG_OPCODE_MULHI = 15,
  BRIG_OPCODE_NEG = 16,
  BRIG_OPCODE_REM = 17,
  BRIG_OPCODE_RINT = 18,
  BRIG_OPCODE_SQRT = 19,
  BRIG_OPCODE_SUB = 20,
  BRIG_OPCODE_TRUNC = 21,
  BRIG_OPCODE_MAD24 = 22,
  BRIG_OPCODE_MAD24HI = 23,
  BRIG_OPCODE_MUL24 = 24,
  BRIG_OPCODE_MUL24HI = 25,
  BRIG_OPCODE_SHL = 26,
  BRIG_OPCODE_SHR = 27,
  BRIG_OPCODE_AND = 28,
  BRIG_OPCODE_NOT = 29,
  BRIG_OPCODE_OR = 30,
  BRIG_OPCODE_POPCOUNT = 31,
  BRIG_OPCODE_XOR = 32,
  BRIG_OPCODE_BITEXTRACT = 33,
  BRIG_OPCODE_BITINSERT = 34,
  BRIG_OPCODE_BITMASK = 35,
  BRIG_OPCODE_BITREV = 36,
  BRIG_OPCODE_BITSELECT = 37,
  BRIG_OPCODE_FIRSTBIT = 38,
  BRIG_OPCODE_LASTBIT = 39,
  BRIG_OPCODE_COMBINE = 40,
  BRIG_OPCODE_EXPAND = 41,
  BRIG_OPCODE_LDA = 42,
  BRIG_OPCODE_MOV = 43,
  BRIG_OPCODE_SHUFFLE = 44,
  BRIG_OPCODE_UNPACKHI = 45,
  BRIG_OPCODE_UNPACKLO = 46,
  BRIG_OPCODE_PACK = 47,
  BRIG_OPCODE_UNPACK = 48,
  BRIG_OPCODE_CMOV = 49,
  BRIG_OPCODE_CLASS = 50,
  BRIG_OPCODE_NCOS = 51,
  BRIG_OPCODE_NEXP2 = 52,
  BRIG_OPCODE_NFMA = 53,
  BRIG_OPCODE_NLOG2 = 54,
  BRIG_OPCODE_NRCP = 55,
  BRIG_OPCODE_NRSQRT = 56,
  BRIG_OPCODE_NSIN = 57,
  BRIG_OPCODE_NSQRT = 58,
  BRIG_OPCODE_BITALIGN = 59,
  BRIG_OPCODE_BYTEALIGN = 60,
  BRIG_OPCODE_PACKCVT = 61,
  BRIG_OPCODE_UNPACKCVT = 62,
  BRIG_OPCODE_LERP = 63,
  BRIG_OPCODE_SAD = 64,
  BRIG_OPCODE_SADHI = 65,
  BRIG_OPCODE_SEGMENTP = 66,
  BRIG_OPCODE_FTOS = 67,
  BRIG_OPCODE_STOF = 68,
  BRIG_OPCODE_CMP = 69,
  BRIG_OPCODE_CVT = 70,
  BRIG_OPCODE_LD = 71,
  BRIG_OPCODE_ST = 72,
  BRIG_OPCODE_ATOMIC = 73,
  BRIG_OPCODE_ATOMICNORET = 74,
  BRIG_OPCODE_SIGNAL = 75,
  BRIG_OPCODE_SIGNALNORET = 76,
  BRIG_OPCODE_MEMFENCE = 77,
  BRIG_OPCODE_RDIMAGE = 78,
  BRIG_OPCODE_LDIMAGE = 79,
  BRIG_OPCODE_STIMAGE = 80,
  BRIG_OPCODE_IMAGEFENCE = 81,
  BRIG_OPCODE_QUERYIMAGE = 82,
  BRIG_OPCODE_QUERYSAMPLER = 83,
  BRIG_OPCODE_CBR = 84,
  BRIG_OPCODE_BR = 85,
  BRIG_OPCODE_SBR = 86,
  BRIG_OPCODE_BARRIER = 87,
  BRIG_OPCODE_WAVEBARRIER = 88,
  BRIG_OPCODE_ARRIVEFBAR = 89,
  BRIG_OPCODE_INITFBAR = 90,
  BRIG_OPCODE_JOINFBAR = 91,
  BRIG_OPCODE_LEAVEFBAR = 92,
  BRIG_OPCODE_RELEASEFBAR = 93,
  BRIG_OPCODE_WAITFBAR = 94,
  BRIG_OPCODE_LDF = 95,
  BRIG_OPCODE_ACTIVELANECOUNT = 96,
  BRIG_OPCODE_ACTIVELANEID = 97,
  BRIG_OPCODE_ACTIVELANEMASK = 98,
  BRIG_OPCODE_ACTIVELANEPERMUTE = 99,
  BRIG_OPCODE_CALL = 100,
  BRIG_OPCODE_SCALL = 101,
  BRIG_OPCODE_ICALL = 102,
  BRIG_OPCODE_RET = 103,
  BRIG_OPCODE_ALLOCA = 104,
  BRIG_OPCODE_CURRENTWORKGROUPSIZE = 105,
  BRIG_OPCODE_CURRENTWORKITEMFLATID = 106,
  BRIG_OPCODE_DIM = 107,
  BRIG_OPCODE_GRIDGROUPS = 108,
  BRIG_OPCODE_GRIDSIZE = 109,
  BRIG_OPCODE_PACKETCOMPLETIONSIG = 110,
  BRIG_OPCODE_PACKETID = 111,
  BRIG_OPCODE_WORKGROUPID = 112,
  BRIG_OPCODE_WORKGROUPSIZE = 113,
  BRIG_OPCODE_WORKITEMABSID = 114,
  BRIG_OPCODE_WORKITEMFLATABSID = 115,
  BRIG_OPCODE_WORKITEMFLATID = 116,
  BRIG_OPCODE_WORKITEMID = 117,
  BRIG_OPCODE_CLEARDETECTEXCEPT = 118,
  BRIG_OPCODE_GETDETECTEXCEPT = 119,
  BRIG_OPCODE_SETDETECTEXCEPT = 120,
  BRIG_OPCODE_ADDQUEUEWRITEINDEX = 121,
  BRIG_OPCODE_CASQUEUEWRITEINDEX = 122,
  BRIG_OPCODE_LDQUEUEREADINDEX = 123,
  BRIG_OPCODE_LDQUEUEWRITEINDEX = 124,
  BRIG_OPCODE_STQUEUEREADINDEX = 125,
  BRIG_OPCODE_STQUEUEWRITEINDEX = 126,
  BRIG_OPCODE_CLOCK = 127,
  BRIG_OPCODE_CUID = 128,
  BRIG_OPCODE_DEBUGTRAP = 129,
  BRIG_OPCODE_GROUPBASEPTR = 130,
  BRIG_OPCODE_KERNARGBASEPTR = 131,
  BRIG_OPCODE_LANEID = 132,
  BRIG_OPCODE_MAXCUID = 133,
  BRIG_OPCODE_MAXWAVEID = 134,
  BRIG_OPCODE_NULLPTR = 135,
  BRIG_OPCODE_WAVEID = 136,
  BRIG_OPCODE_FIRST_USER_DEFINED = 32768
};

typedef uint8_t BrigPack8_t;

enum BrigPack
{
  BRIG_PACK_NONE = 0,
  BRIG_PACK_PP = 1,
  BRIG_PACK_PS = 2,
  BRIG_PACK_SP = 3,
  BRIG_PACK_SS = 4,
  BRIG_PACK_S = 5,
  BRIG_PACK_P = 6,
  BRIG_PACK_PPSAT = 7,
  BRIG_PACK_PSSAT = 8,
  BRIG_PACK_SPSAT = 9,
  BRIG_PACK_SSSAT = 10,
  BRIG_PACK_SSAT = 11,
  BRIG_PACK_PSAT = 12
};

typedef uint8_t BrigProfile8_t;

enum BrigProfile
{
  BRIG_PROFILE_BASE = 0,
  BRIG_PROFILE_FULL = 1
};

typedef uint16_t BrigRegisterKind16_t;

enum BrigRegisterKind
{
  BRIG_REGISTER_KIND_CONTROL = 0,
  BRIG_REGISTER_KIND_SINGLE = 1,
  BRIG_REGISTER_KIND_DOUBLE = 2,
  BRIG_REGISTER_KIND_QUAD = 3
};

typedef uint8_t BrigRound8_t;

enum BrigRound
{
  BRIG_ROUND_NONE = 0,
  BRIG_ROUND_FLOAT_DEFAULT = 1,
  BRIG_ROUND_FLOAT_NEAR_EVEN = 2,
  BRIG_ROUND_FLOAT_ZERO = 3,
  BRIG_ROUND_FLOAT_PLUS_INFINITY = 4,
  BRIG_ROUND_FLOAT_MINUS_INFINITY = 5,
  BRIG_ROUND_INTEGER_NEAR_EVEN = 6,
  BRIG_ROUND_INTEGER_ZERO = 7,
  BRIG_ROUND_INTEGER_PLUS_INFINITY = 8,
  BRIG_ROUND_INTEGER_MINUS_INFINITY = 9,
  BRIG_ROUND_INTEGER_NEAR_EVEN_SAT = 10,
  BRIG_ROUND_INTEGER_ZERO_SAT = 11,
  BRIG_ROUND_INTEGER_PLUS_INFINITY_SAT = 12,
  BRIG_ROUND_INTEGER_MINUS_INFINITY_SAT = 13,
  BRIG_ROUND_INTEGER_SIGNALING_NEAR_EVEN = 14,
  BRIG_ROUND_INTEGER_SIGNALING_ZERO = 15,
  BRIG_ROUND_INTEGER_SIGNALING_PLUS_INFINITY = 16,
  BRIG_ROUND_INTEGER_SIGNALING_MINUS_INFINITY = 17,
  BRIG_ROUND_INTEGER_SIGNALING_NEAR_EVEN_SAT = 18,
  BRIG_ROUND_INTEGER_SIGNALING_ZERO_SAT = 19,
  BRIG_ROUND_INTEGER_SIGNALING_PLUS_INFINITY_SAT = 20,
  BRIG_ROUND_INTEGER_SIGNALING_MINUS_INFINITY_SAT = 21
};

typedef uint8_t BrigSamplerAddressing8_t;

enum BrigSamplerAddressing
{
  BRIG_ADDRESSING_UNDEFINED = 0,
  BRIG_ADDRESSING_CLAMP_TO_EDGE = 1,
  BRIG_ADDRESSING_CLAMP_TO_BORDER = 2,
  BRIG_ADDRESSING_REPEAT = 3,
  BRIG_ADDRESSING_MIRRORED_REPEAT = 4,
  BRIG_ADDRESSING_FIRST_USER_DEFINED = 128
};

typedef uint8_t BrigSamplerCoordNormalization8_t;

enum BrigSamplerCoordNormalization
{
  BRIG_COORD_UNNORMALIZED = 0,
  BRIG_COORD_NORMALIZED = 1
};

typedef uint8_t BrigSamplerFilter8_t;

enum BrigSamplerFilter
{
  BRIG_FILTER_NEAREST = 0,
  BRIG_FILTER_LINEAR = 1,
  BRIG_FILTER_FIRST_USER_DEFINED = 128
};

typedef uint8_t BrigSamplerQuery8_t;

enum BrigSamplerQuery
{
  BRIG_SAMPLER_QUERY_ADDRESSING = 0,
  BRIG_SAMPLER_QUERY_COORD = 1,
  BRIG_SAMPLER_QUERY_FILTER = 2
};

typedef uint32_t BrigSectionIndex32_t;

enum BrigSectionIndex
{
  BRIG_SECTION_INDEX_DATA = 0,
  BRIG_SECTION_INDEX_CODE = 1,
  BRIG_SECTION_INDEX_OPERAND = 2,
  BRIG_SECTION_INDEX_BEGIN_IMPLEMENTATION_DEFINED = 3
};

struct BrigSectionHeader
{
  uint64_t byteCount;
  uint32_t headerByteCount;
  uint32_t nameLength;
  uint8_t name[1];
};

typedef uint8_t BrigSegCvtModifier8_t;

enum BrigSegCvtModifierMask
{
  BRIG_SEG_CVT_NONULL = 1
};

typedef uint8_t BrigSegment8_t;

enum BrigSegment
{
  BRIG_SEGMENT_NONE = 0,
  BRIG_SEGMENT_FLAT = 1,
  BRIG_SEGMENT_GLOBAL = 2,
  BRIG_SEGMENT_READONLY = 3,
  BRIG_SEGMENT_KERNARG = 4,
  BRIG_SEGMENT_GROUP = 5,
  BRIG_SEGMENT_PRIVATE = 6,
  BRIG_SEGMENT_SPILL = 7,
  BRIG_SEGMENT_ARG = 8,
  BRIG_SEGMENT_FIRST_USER_DEFINED = 128
};

enum
{
  BRIG_TYPE_BASE_SIZE = 5,
  BRIG_TYPE_PACK_SIZE = 2,
  BRIG_TYPE_ARRAY_SIZE = 1,

  BRIG_TYPE_BASE_SHIFT = 0,
  BRIG_TYPE_PACK_SHIFT = BRIG_TYPE_BASE_SHIFT + BRIG_TYPE_BASE_SIZE,
  BRIG_TYPE_ARRAY_SHIFT = BRIG_TYPE_PACK_SHIFT + BRIG_TYPE_PACK_SIZE,

  BRIG_TYPE_BASE_MASK = ((1 << BRIG_TYPE_BASE_SIZE) - 1)
			<< BRIG_TYPE_BASE_SHIFT,
  BRIG_TYPE_PACK_MASK = ((1 << BRIG_TYPE_PACK_SIZE) - 1)
			<< BRIG_TYPE_PACK_SHIFT,
  BRIG_TYPE_ARRAY_MASK = ((1 << BRIG_TYPE_ARRAY_SIZE) - 1)
			 << BRIG_TYPE_ARRAY_SHIFT,

  BRIG_TYPE_PACK_NONE = 0 << BRIG_TYPE_PACK_SHIFT,
  BRIG_TYPE_PACK_32 = 1 << BRIG_TYPE_PACK_SHIFT,
  BRIG_TYPE_PACK_64 = 2 << BRIG_TYPE_PACK_SHIFT,
  BRIG_TYPE_PACK_128 = 3 << BRIG_TYPE_PACK_SHIFT,

  BRIG_TYPE_ARRAY = 1 << BRIG_TYPE_ARRAY_SHIFT
};

typedef uint16_t BrigType16_t;

enum BrigType
{
  BRIG_TYPE_NONE = 0,

  BRIG_TYPE_U8 = 1,
  BRIG_TYPE_U16 = 2,
  BRIG_TYPE_U32 = 3,
  BRIG_TYPE_U64 = 4,

  BRIG_TYPE_S8 = 5,
  BRIG_TYPE_S16 = 6,
  BRIG_TYPE_S32 = 7,
  BRIG_TYPE_S64 = 8,

  BRIG_TYPE_F16 = 9,
  BRIG_TYPE_F32 = 10,
  BRIG_TYPE_F64 = 11,

  BRIG_TYPE_B1 = 12,
  BRIG_TYPE_B8 = 13,
  BRIG_TYPE_B16 = 14,
  BRIG_TYPE_B32 = 15,
  BRIG_TYPE_B64 = 16,
  BRIG_TYPE_B128 = 17,

  BRIG_TYPE_SAMP = 18,
  BRIG_TYPE_ROIMG = 19,
  BRIG_TYPE_WOIMG = 20,
  BRIG_TYPE_RWIMG = 21,

  BRIG_TYPE_SIG32 = 22,
  BRIG_TYPE_SIG64 = 23,

  BRIG_TYPE_U8X4 = BRIG_TYPE_U8 | BRIG_TYPE_PACK_32,
  BRIG_TYPE_U8X8 = BRIG_TYPE_U8 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_U8X16 = BRIG_TYPE_U8 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_U16X2 = BRIG_TYPE_U16 | BRIG_TYPE_PACK_32,
  BRIG_TYPE_U16X4 = BRIG_TYPE_U16 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_U16X8 = BRIG_TYPE_U16 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_U32X2 = BRIG_TYPE_U32 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_U32X4 = BRIG_TYPE_U32 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_U64X2 = BRIG_TYPE_U64 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_S8X4 = BRIG_TYPE_S8 | BRIG_TYPE_PACK_32,
  BRIG_TYPE_S8X8 = BRIG_TYPE_S8 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_S8X16 = BRIG_TYPE_S8 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_S16X2 = BRIG_TYPE_S16 | BRIG_TYPE_PACK_32,
  BRIG_TYPE_S16X4 = BRIG_TYPE_S16 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_S16X8 = BRIG_TYPE_S16 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_S32X2 = BRIG_TYPE_S32 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_S32X4 = BRIG_TYPE_S32 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_S64X2 = BRIG_TYPE_S64 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_F16X2 = BRIG_TYPE_F16 | BRIG_TYPE_PACK_32,
  BRIG_TYPE_F16X4 = BRIG_TYPE_F16 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_F16X8 = BRIG_TYPE_F16 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_F32X2 = BRIG_TYPE_F32 | BRIG_TYPE_PACK_64,
  BRIG_TYPE_F32X4 = BRIG_TYPE_F32 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_F64X2 = BRIG_TYPE_F64 | BRIG_TYPE_PACK_128,

  BRIG_TYPE_U8_ARRAY = BRIG_TYPE_U8 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U16_ARRAY = BRIG_TYPE_U16 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U32_ARRAY = BRIG_TYPE_U32 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U64_ARRAY = BRIG_TYPE_U64 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_S8_ARRAY = BRIG_TYPE_S8 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S16_ARRAY = BRIG_TYPE_S16 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S32_ARRAY = BRIG_TYPE_S32 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S64_ARRAY = BRIG_TYPE_S64 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_F16_ARRAY = BRIG_TYPE_F16 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_F32_ARRAY = BRIG_TYPE_F32 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_F64_ARRAY = BRIG_TYPE_F64 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_B8_ARRAY = BRIG_TYPE_B8 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_B16_ARRAY = BRIG_TYPE_B16 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_B32_ARRAY = BRIG_TYPE_B32 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_B64_ARRAY = BRIG_TYPE_B64 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_B128_ARRAY = BRIG_TYPE_B128 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_SAMP_ARRAY = BRIG_TYPE_SAMP | BRIG_TYPE_ARRAY,
  BRIG_TYPE_ROIMG_ARRAY = BRIG_TYPE_ROIMG | BRIG_TYPE_ARRAY,
  BRIG_TYPE_WOIMG_ARRAY = BRIG_TYPE_WOIMG | BRIG_TYPE_ARRAY,
  BRIG_TYPE_RWIMG_ARRAY = BRIG_TYPE_RWIMG | BRIG_TYPE_ARRAY,

  BRIG_TYPE_SIG32_ARRAY = BRIG_TYPE_SIG32 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_SIG64_ARRAY = BRIG_TYPE_SIG64 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_U8X4_ARRAY = BRIG_TYPE_U8X4 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U8X8_ARRAY = BRIG_TYPE_U8X8 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U8X16_ARRAY = BRIG_TYPE_U8X16 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_U16X2_ARRAY = BRIG_TYPE_U16X2 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U16X4_ARRAY = BRIG_TYPE_U16X4 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U16X8_ARRAY = BRIG_TYPE_U16X8 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_U32X2_ARRAY = BRIG_TYPE_U32X2 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_U32X4_ARRAY = BRIG_TYPE_U32X4 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_U64X2_ARRAY = BRIG_TYPE_U64X2 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_S8X4_ARRAY = BRIG_TYPE_S8X4 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S8X8_ARRAY = BRIG_TYPE_S8X8 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S8X16_ARRAY = BRIG_TYPE_S8X16 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_S16X2_ARRAY = BRIG_TYPE_S16X2 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S16X4_ARRAY = BRIG_TYPE_S16X4 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S16X8_ARRAY = BRIG_TYPE_S16X8 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_S32X2_ARRAY = BRIG_TYPE_S32X2 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_S32X4_ARRAY = BRIG_TYPE_S32X4 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_S64X2_ARRAY = BRIG_TYPE_S64X2 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_F16X2_ARRAY = BRIG_TYPE_F16X2 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_F16X4_ARRAY = BRIG_TYPE_F16X4 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_F16X8_ARRAY = BRIG_TYPE_F16X8 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_F32X2_ARRAY = BRIG_TYPE_F32X2 | BRIG_TYPE_ARRAY,
  BRIG_TYPE_F32X4_ARRAY = BRIG_TYPE_F32X4 | BRIG_TYPE_ARRAY,

  BRIG_TYPE_F64X2_ARRAY = BRIG_TYPE_F64X2 | BRIG_TYPE_ARRAY
};

struct BrigUInt64
{
  uint32_t lo;
  uint32_t hi;
};

typedef uint8_t BrigVariableModifier8_t;

enum BrigVariableModifierMask
{
  BRIG_VARIABLE_DEFINITION = 1,
  BRIG_VARIABLE_CONST = 2
};

enum BrigVersion
{
  BRIG_VERSION_HSAIL_MAJOR = 1,
  BRIG_VERSION_HSAIL_MINOR = 0,
  BRIG_VERSION_BRIG_MAJOR = 1,
  BRIG_VERSION_BRIG_MINOR = 0
};

typedef uint8_t BrigWidth8_t;

enum BrigWidth
{
  BRIG_WIDTH_NONE = 0,
  BRIG_WIDTH_1 = 1,
  BRIG_WIDTH_2 = 2,
  BRIG_WIDTH_4 = 3,
  BRIG_WIDTH_8 = 4,
  BRIG_WIDTH_16 = 5,
  BRIG_WIDTH_32 = 6,
  BRIG_WIDTH_64 = 7,
  BRIG_WIDTH_128 = 8,
  BRIG_WIDTH_256 = 9,
  BRIG_WIDTH_512 = 10,
  BRIG_WIDTH_1024 = 11,
  BRIG_WIDTH_2048 = 12,
  BRIG_WIDTH_4096 = 13,
  BRIG_WIDTH_8192 = 14,
  BRIG_WIDTH_16384 = 15,
  BRIG_WIDTH_32768 = 16,
  BRIG_WIDTH_65536 = 17,
  BRIG_WIDTH_131072 = 18,
  BRIG_WIDTH_262144 = 19,
  BRIG_WIDTH_524288 = 20,
  BRIG_WIDTH_1048576 = 21,
  BRIG_WIDTH_2097152 = 22,
  BRIG_WIDTH_4194304 = 23,
  BRIG_WIDTH_8388608 = 24,
  BRIG_WIDTH_16777216 = 25,
  BRIG_WIDTH_33554432 = 26,
  BRIG_WIDTH_67108864 = 27,
  BRIG_WIDTH_134217728 = 28,
  BRIG_WIDTH_268435456 = 29,
  BRIG_WIDTH_536870912 = 30,
  BRIG_WIDTH_1073741824 = 31,
  BRIG_WIDTH_2147483648 = 32,
  BRIG_WIDTH_WAVESIZE = 33,
  BRIG_WIDTH_ALL = 34
};

struct BrigData
{
  uint32_t byteCount;
  uint8_t bytes[1];
};

struct BrigDirectiveArgBlock
{
  BrigBase base;
};

struct BrigDirectiveComment
{
  BrigBase base;
  BrigDataOffsetString32_t name;
};

struct BrigDirectiveControl
{
  BrigBase base;
  BrigControlDirective16_t control;
  uint16_t reserved;
  BrigDataOffsetOperandList32_t operands;
};

struct BrigDirectiveExecutable
{
  BrigBase base;
  BrigDataOffsetString32_t name;
  uint16_t outArgCount;
  uint16_t inArgCount;
  BrigCodeOffset32_t firstInArg;
  BrigCodeOffset32_t firstCodeBlockEntry;
  BrigCodeOffset32_t nextModuleEntry;
  BrigExecutableModifier8_t modifier;
  BrigLinkage8_t linkage;
  uint16_t reserved;
};

struct BrigDirectiveExtension
{
  BrigBase base;
  BrigDataOffsetString32_t name;
};

struct BrigDirectiveFbarrier
{
  BrigBase base;
  BrigDataOffsetString32_t name;
  BrigVariableModifier8_t modifier;
  BrigLinkage8_t linkage;
  uint16_t reserved;
};

struct BrigDirectiveLabel
{
  BrigBase base;
  BrigDataOffsetString32_t name;
};

struct BrigDirectiveLoc
{
  BrigBase base;
  BrigDataOffsetString32_t filename;
  uint32_t line;
  uint32_t column;
};

struct BrigDirectiveModule
{
  BrigBase base;
  BrigDataOffsetString32_t name;
  BrigVersion32_t hsailMajor;
  BrigVersion32_t hsailMinor;
  BrigProfile8_t profile;
  BrigMachineModel8_t machineModel;
  BrigRound8_t defaultFloatRound;
  uint8_t reserved;
};

struct BrigDirectiveNone
{
  BrigBase base;
};

struct BrigDirectivePragma
{
  BrigBase base;
  BrigDataOffsetOperandList32_t operands;
};

struct BrigDirectiveVariable
{
  BrigBase base;
  BrigDataOffsetString32_t name;
  BrigOperandOffset32_t init;
  BrigType16_t type;
  BrigSegment8_t segment;
  BrigAlignment8_t align;
  BrigUInt64 dim;
  BrigVariableModifier8_t modifier;
  BrigLinkage8_t linkage;
  BrigAllocation8_t allocation;
  uint8_t reserved;
};

struct BrigInstBase
{
  BrigBase base;
  BrigOpcode16_t opcode;
  BrigType16_t type;
  BrigDataOffsetOperandList32_t operands;
};

struct BrigInstAddr
{
  BrigInstBase base;
  BrigSegment8_t segment;
  uint8_t reserved[3];
};

struct BrigInstAtomic
{
  BrigInstBase base;
  BrigSegment8_t segment;
  BrigMemoryOrder8_t memoryOrder;
  BrigMemoryScope8_t memoryScope;
  BrigAtomicOperation8_t atomicOperation;
  uint8_t equivClass;
  uint8_t reserved[3];
};

struct BrigInstBasic
{
  BrigInstBase base;
};

struct BrigInstBr
{
  BrigInstBase base;
  BrigWidth8_t width;
  uint8_t reserved[3];
};

struct BrigInstCmp
{
  BrigInstBase base;
  BrigType16_t sourceType;
  BrigAluModifier8_t modifier;
  BrigCompareOperation8_t compare;
  BrigPack8_t pack;
  uint8_t reserved[3];
};

struct BrigInstCvt
{
  BrigInstBase base;
  BrigType16_t sourceType;
  BrigAluModifier8_t modifier;
  BrigRound8_t round;
};

struct BrigInstImage
{
  BrigInstBase base;
  BrigType16_t imageType;
  BrigType16_t coordType;
  BrigImageGeometry8_t geometry;
  uint8_t equivClass;
  uint16_t reserved;
};

struct BrigInstLane
{
  BrigInstBase base;
  BrigType16_t sourceType;
  BrigWidth8_t width;
  uint8_t reserved;
};

struct BrigInstMem
{
  BrigInstBase base;
  BrigSegment8_t segment;
  BrigAlignment8_t align;
  uint8_t equivClass;
  BrigWidth8_t width;
  BrigMemoryModifier8_t modifier;
  uint8_t reserved[3];
};

struct BrigInstMemFence
{
  BrigInstBase base;
  BrigMemoryOrder8_t memoryOrder;
  BrigMemoryScope8_t globalSegmentMemoryScope;
  BrigMemoryScope8_t groupSegmentMemoryScope;
  BrigMemoryScope8_t imageSegmentMemoryScope;
};

struct BrigInstMod
{
  BrigInstBase base;
  BrigAluModifier8_t modifier;
  BrigRound8_t round;
  BrigPack8_t pack;
  uint8_t reserved;
};

struct BrigInstQueryImage
{
  BrigInstBase base;
  BrigType16_t imageType;
  BrigImageGeometry8_t geometry;
  BrigImageQuery8_t query;
};

struct BrigInstQuerySampler
{
  BrigInstBase base;
  BrigSamplerQuery8_t query;
  uint8_t reserved[3];
};

struct BrigInstQueue
{
  BrigInstBase base;
  BrigSegment8_t segment;
  BrigMemoryOrder8_t memoryOrder;
  uint16_t reserved;
};

struct BrigInstSeg
{
  BrigInstBase base;
  BrigSegment8_t segment;
  uint8_t reserved[3];
};

struct BrigInstSegCvt
{
  BrigInstBase base;
  BrigType16_t sourceType;
  BrigSegment8_t segment;
  BrigSegCvtModifier8_t modifier;
};

struct BrigInstSignal
{
  BrigInstBase base;
  BrigType16_t signalType;
  BrigMemoryOrder8_t memoryOrder;
  BrigAtomicOperation8_t signalOperation;
};

struct BrigInstSourceType
{
  BrigInstBase base;
  BrigType16_t sourceType;
  uint16_t reserved;
};

struct BrigOperandAddress
{
  BrigBase base;
  BrigCodeOffset32_t symbol;
  BrigOperandOffset32_t reg;
  BrigUInt64 offset;
};

struct BrigOperandAlign
{
  BrigBase base;
  BrigAlignment8_t align;
  uint8_t reserved[3];
};

struct BrigOperandCodeList
{
  BrigBase base;
  BrigDataOffsetCodeList32_t elements;
};

struct BrigOperandCodeRef
{
  BrigBase base;
  BrigCodeOffset32_t ref;
};

struct BrigOperandConstantBytes
{
  BrigBase base;
  BrigType16_t type;
  uint16_t reserved;
  BrigDataOffsetString32_t bytes;
};

struct BrigOperandConstantImage
{
  BrigBase base;
  BrigType16_t type;
  BrigImageGeometry8_t geometry;
  BrigImageChannelOrder8_t channelOrder;
  BrigImageChannelType8_t channelType;
  uint8_t reserved[3];
  BrigUInt64 width;
  BrigUInt64 height;
  BrigUInt64 depth;
  BrigUInt64 array;
};

struct BrigOperandConstantOperandList
{
  BrigBase base;
  BrigType16_t type;
  uint16_t reserved;
  BrigDataOffsetOperandList32_t elements;
};

struct BrigOperandConstantSampler
{
  BrigBase base;
  BrigType16_t type;
  BrigSamplerCoordNormalization8_t coord;
  BrigSamplerFilter8_t filter;
  BrigSamplerAddressing8_t addressing;
  uint8_t reserved[3];
};

struct BrigOperandOperandList
{
  BrigBase base;
  BrigDataOffsetOperandList32_t elements;
};

struct BrigOperandRegister
{
  BrigBase base;
  BrigRegisterKind16_t regKind;
  uint16_t regNum;
};

struct BrigOperandString
{
  BrigBase base;
  BrigDataOffsetString32_t string;
};

struct BrigOperandWavesize
{
  BrigBase base;
};

#endif /* HSA_BRIG_FORMAT_H */
