/* Ppmd7.h -- PPMdH compression codec
2010-03-12 : Igor Pavlov : Public domain
This code is based on PPMd var.H (2001): Dmitry Shkarin : Public domain */

/* This code supports virtual RangeDecoder and includes the implementation
of RangeCoder from 7z, instead of RangeCoder from original PPMd var.H.
If you need the compatibility with original PPMd var.H, you can use external RangeDecoder */

#ifndef __PPMD7_H
#define __PPMD7_H

#include "Ppmd.h"

EXTERN_C_BEGIN

#define PPMD7_MIN_ORDER 2
#define PPMD7_MAX_ORDER 64

#define PPMD7_MIN_MEM_SIZE (1 << 11)
#define PPMD7_MAX_MEM_SIZE (0xFFFFFFFF - 12 * 3)

struct CPpmd7_Context_;

typedef
  #ifdef PPMD_32BIT
    struct CPpmd7_Context_ *
  #else
    UInt32
  #endif
  CPpmd7_Context_Ref;

typedef struct CPpmd7_Context_
{
  UInt16 NumStats;
  UInt16 SummFreq;
  CPpmd_State_Ref Stats;
  CPpmd7_Context_Ref Suffix;
} CPpmd7_Context;

#define Ppmd7Context_OneState(p) ((CPpmd_State *)&(p)->SummFreq)

typedef struct
{
  CPpmd7_Context *MinContext, *MaxContext;
  CPpmd_State *FoundState;
  unsigned OrderFall, InitEsc, PrevSuccess, MaxOrder, HiBitsFlag;
  Int32 RunLength, InitRL; /* must be 32-bit at least */

  UInt32 Size;
  UInt32 GlueCount;
  Byte *Base, *LoUnit, *HiUnit, *Text, *UnitsStart;
  UInt32 AlignOffset;

  Byte Indx2Units[PPMD_NUM_INDEXES];
  Byte Units2Indx[128];
  CPpmd_Void_Ref FreeList[PPMD_NUM_INDEXES];
  Byte NS2Indx[256], NS2BSIndx[256], HB2Flag[256];
  CPpmd_See DummySee, See[25][16];
  UInt16 BinSumm[128][64];
} CPpmd7;

void Ppmd7_Construct(CPpmd7 *p);
Bool Ppmd7_Alloc(CPpmd7 *p, UInt32 size, ISzAlloc *alloc);
void Ppmd7_Free(CPpmd7 *p, ISzAlloc *alloc);
void Ppmd7_Init(CPpmd7 *p, unsigned maxOrder);
#define Ppmd7_WasAllocated(p) ((p)->Base != NULL)


/* ---------- Internal Functions ---------- */

extern const Byte PPMD7_kExpEscape[16];

#ifdef PPMD_32BIT
  #define Ppmd7_GetPtr(p, ptr) (ptr)
  #define Ppmd7_GetContext(p, ptr) (ptr)
  #define Ppmd7_GetStats(p, ctx) ((ctx)->Stats)
#else
  #define Ppmd7_GetPtr(p, offs) ((void *)((p)->Base + (offs)))
  #define Ppmd7_GetContext(p, offs) ((CPpmd7_Context *)Ppmd7_GetPtr((p), (offs)))
  #define Ppmd7_GetStats(p, ctx) ((CPpmd_State *)Ppmd7_GetPtr((p), ((ctx)->Stats)))
#endif

void Ppmd7_Update1(CPpmd7 *p);
void Ppmd7_Update1_0(CPpmd7 *p);
void Ppmd7_Update2(CPpmd7 *p);
void Ppmd7_UpdateBin(CPpmd7 *p);

#define Ppmd7_GetBinSumm(p) \
    &p->BinSumm[Ppmd7Context_OneState(p->MinContext)->Freq - 1][p->PrevSuccess + \
    p->NS2BSIndx[Ppmd7_GetContext(p, p->MinContext->Suffix)->NumStats - 1] + \
    (p->HiBitsFlag = p->HB2Flag[p->FoundState->Symbol]) + \
    2 * p->HB2Flag[Ppmd7Context_OneState(p->MinContext)->Symbol] + \
    ((p->RunLength >> 26) & 0x20)]

CPpmd_See *Ppmd7_MakeEscFreq(CPpmd7 *p, unsigned numMasked, UInt32 *scale);


/* ---------- Decode ---------- */

typedef struct
{
  UInt32 (*GetThreshold)(void *p, UInt32 total);
  void (*Decode)(void *p, UInt32 start, UInt32 size);
  UInt32 (*DecodeBit)(void *p, UInt32 size0);
} IPpmd7_RangeDec;

typedef struct
{
  IPpmd7_RangeDec p;
  UInt32 Range;
  UInt32 Code;
  IByteIn *Stream;
} CPpmd7z_RangeDec;

void Ppmd7z_RangeDec_CreateVTable(CPpmd7z_RangeDec *p);
Bool Ppmd7z_RangeDec_Init(CPpmd7z_RangeDec *p);
#define Ppmd7z_RangeDec_IsFinishedOK(p) ((p)->Code == 0)

int Ppmd7_DecodeSymbol(CPpmd7 *p, IPpmd7_RangeDec *rc);


/* ---------- Encode ---------- */

typedef struct
{
  UInt64 Low;
  UInt32 Range;
  Byte Cache;
  UInt64 CacheSize;
  IByteOut *Stream;
} CPpmd7z_RangeEnc;

void Ppmd7z_RangeEnc_Init(CPpmd7z_RangeEnc *p);
void Ppmd7z_RangeEnc_FlushData(CPpmd7z_RangeEnc *p);

void Ppmd7_EncodeSymbol(CPpmd7 *p, CPpmd7z_RangeEnc *rc, int symbol);

EXTERN_C_END
 
#endif
