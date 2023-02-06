/* Ppmd.h -- PPMD codec common code
2011-01-27 : Igor Pavlov : Public domain
This code is based on PPMd var.H (2001): Dmitry Shkarin : Public domain */

#ifndef __PPMD_H
#define __PPMD_H

#include "Types.h"
#include "CpuArch.h"

EXTERN_C_BEGIN

#ifdef MY_CPU_32BIT
  #define PPMD_32BIT
#endif

#define PPMD_INT_BITS 7
#define PPMD_PERIOD_BITS 7
#define PPMD_BIN_SCALE (1 << (PPMD_INT_BITS + PPMD_PERIOD_BITS))

#define PPMD_GET_MEAN_SPEC(summ, shift, round) (((summ) + (1 << ((shift) - (round)))) >> (shift))
#define PPMD_GET_MEAN(summ) PPMD_GET_MEAN_SPEC((summ), PPMD_PERIOD_BITS, 2)
#define PPMD_UPDATE_PROB_0(prob) ((prob) + (1 << PPMD_INT_BITS) - PPMD_GET_MEAN(prob))
#define PPMD_UPDATE_PROB_1(prob) ((prob) - PPMD_GET_MEAN(prob))

#define PPMD_N1 4
#define PPMD_N2 4
#define PPMD_N3 4
#define PPMD_N4 ((128 + 3 - 1 * PPMD_N1 - 2 * PPMD_N2 - 3 * PPMD_N3) / 4)
#define PPMD_NUM_INDEXES (PPMD_N1 + PPMD_N2 + PPMD_N3 + PPMD_N4)

#pragma pack(push, 1)
/* Most compilers works OK here even without #pragma pack(push, 1), but some GCC compilers need it. */

/* SEE-contexts for PPM-contexts with masked symbols */
typedef struct
{
  UInt16 Summ; /* Freq */
  Byte Shift;  /* Speed of Freq change; low Shift is for fast change */
  Byte Count;  /* Count to next change of Shift */
} CPpmd_See;

#define Ppmd_See_Update(p)  if ((p)->Shift < PPMD_PERIOD_BITS && --(p)->Count == 0) \
    { (p)->Summ <<= 1; (p)->Count = (Byte)(3 << (p)->Shift++); }

typedef struct
{
  Byte Symbol;
  Byte Freq;
  UInt16 SuccessorLow;
  UInt16 SuccessorHigh;
} CPpmd_State;

#pragma pack(pop)

typedef
  #ifdef PPMD_32BIT
    CPpmd_State *
  #else
    UInt32
  #endif
  CPpmd_State_Ref;

typedef
  #ifdef PPMD_32BIT
    void *
  #else
    UInt32
  #endif
  CPpmd_Void_Ref;

typedef
  #ifdef PPMD_32BIT
    Byte *
  #else
    UInt32
  #endif
  CPpmd_Byte_Ref;

#define PPMD_SetAllBitsIn256Bytes(p) \
  { unsigned i; for (i = 0; i < 256 / sizeof(p[0]); i += 8) { \
  p[i+7] = p[i+6] = p[i+5] = p[i+4] = p[i+3] = p[i+2] = p[i+1] = p[i+0] = ~(size_t)0; }}

EXTERN_C_END
 
#endif
