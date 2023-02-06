/* 7zDec.c -- Decoding from 7z folder
2010-11-02 : Igor Pavlov : Public domain */

#include <string.h>

/* #define _7ZIP_PPMD_SUPPPORT */

#include "7z.h"

#include "Bcj2.h"
#include "Bra.h"
#include "CpuArch.h"
#include "LzmaDec.h"
#include "Lzma2Dec.h"
#ifdef _7ZIP_PPMD_SUPPPORT
#include "Ppmd7.h"
#endif

#define k_Copy 0
#define k_LZMA2 0x21
#define k_LZMA  0x30101
#define k_BCJ   0x03030103
#define k_PPC   0x03030205
#define k_ARM   0x03030501
#define k_ARMT  0x03030701
#define k_SPARC 0x03030805
#define k_BCJ2  0x0303011B

#ifdef _7ZIP_PPMD_SUPPPORT

#define k_PPMD 0x30401

typedef struct
{
  IByteIn p;
  const Byte *cur;
  const Byte *end;
  const Byte *begin;
  UInt64 processed;
  Bool extra;
  SRes res;
  ILookInStream *inStream;
} CByteInToLook;

static Byte ReadByte(void *pp)
{
  CByteInToLook *p = (CByteInToLook *)pp;
  if (p->cur != p->end)
    return *p->cur++;
  if (p->res == SZ_OK)
  {
    size_t size = p->cur - p->begin;
    p->processed += size;
    p->res = p->inStream->Skip(p->inStream, size);
    size = (1 << 25);
    p->res = p->inStream->Look(p->inStream, (const void **)&p->begin, &size);
    p->cur = p->begin;
    p->end = p->begin + size;
    if (size != 0)
      return *p->cur++;;
  }
  p->extra = True;
  return 0;
}

static SRes SzDecodePpmd(CSzCoderInfo *coder, UInt64 inSize, ILookInStream *inStream,
    Byte *outBuffer, SizeT outSize, ISzAlloc *allocMain)
{
  CPpmd7 ppmd;
  CByteInToLook s;
  SRes res = SZ_OK;

  s.p.Read = ReadByte;
  s.inStream = inStream;
  s.begin = s.end = s.cur = NULL;
  s.extra = False;
  s.res = SZ_OK;
  s.processed = 0;

  if (coder->Props.size != 5)
    return SZ_ERROR_UNSUPPORTED;

  {
    unsigned order = coder->Props.data[0];
    UInt32 memSize = GetUi32(coder->Props.data + 1);
    if (order < PPMD7_MIN_ORDER ||
        order > PPMD7_MAX_ORDER ||
        memSize < PPMD7_MIN_MEM_SIZE ||
        memSize > PPMD7_MAX_MEM_SIZE)
      return SZ_ERROR_UNSUPPORTED;
    Ppmd7_Construct(&ppmd);
    if (!Ppmd7_Alloc(&ppmd, memSize, allocMain))
      return SZ_ERROR_MEM;
    Ppmd7_Init(&ppmd, order);
  }
  {
    CPpmd7z_RangeDec rc;
    Ppmd7z_RangeDec_CreateVTable(&rc);
    rc.Stream = &s.p;
    if (!Ppmd7z_RangeDec_Init(&rc))
      res = SZ_ERROR_DATA;
    else if (s.extra)
      res = (s.res != SZ_OK ? s.res : SZ_ERROR_DATA);
    else
    {
      SizeT i;
      for (i = 0; i < outSize; i++)
      {
        int sym = Ppmd7_DecodeSymbol(&ppmd, &rc.p);
        if (s.extra || sym < 0)
          break;
        outBuffer[i] = (Byte)sym;
      }
      if (i != outSize)
        res = (s.res != SZ_OK ? s.res : SZ_ERROR_DATA);
      else if (s.processed + (s.cur - s.begin) != inSize || !Ppmd7z_RangeDec_IsFinishedOK(&rc))
        res = SZ_ERROR_DATA;
    }
  }
  Ppmd7_Free(&ppmd, allocMain);
  return res;
}

#endif


static SRes SzDecodeLzma(CSzCoderInfo *coder, UInt64 inSize, ILookInStream *inStream,
    Byte *outBuffer, SizeT outSize, ISzAlloc *allocMain)
{
  CLzmaDec state;
  SRes res = SZ_OK;

  LzmaDec_Construct(&state);
  RINOK(LzmaDec_AllocateProbs(&state, coder->Props.data, (unsigned)coder->Props.size, allocMain));
  state.dic = outBuffer;
  state.dicBufSize = outSize;
  LzmaDec_Init(&state);

  for (;;)
  {
    Byte *inBuf = NULL;
    size_t lookahead = (1 << 18);
    if (lookahead > inSize)
      lookahead = (size_t)inSize;
    res = inStream->Look((void *)inStream, (const void **)&inBuf, &lookahead);
    if (res != SZ_OK)
      break;

    {
      SizeT inProcessed = (SizeT)lookahead, dicPos = state.dicPos;
      ELzmaStatus status;
      res = LzmaDec_DecodeToDic(&state, outSize, inBuf, &inProcessed, LZMA_FINISH_END, &status);
      lookahead -= inProcessed;
      inSize -= inProcessed;
      if (res != SZ_OK)
        break;
      if (state.dicPos == state.dicBufSize || (inProcessed == 0 && dicPos == state.dicPos))
      {
        if (state.dicBufSize != outSize || lookahead != 0 ||
            (status != LZMA_STATUS_FINISHED_WITH_MARK &&
             status != LZMA_STATUS_MAYBE_FINISHED_WITHOUT_MARK))
          res = SZ_ERROR_DATA;
        break;
      }
      res = inStream->Skip((void *)inStream, inProcessed);
      if (res != SZ_OK)
        break;
    }
  }

  LzmaDec_FreeProbs(&state, allocMain);
  return res;
}

static SRes SzDecodeLzma2(CSzCoderInfo *coder, UInt64 inSize, ILookInStream *inStream,
    Byte *outBuffer, SizeT outSize, ISzAlloc *allocMain)
{
  CLzma2Dec state;
  SRes res = SZ_OK;

  Lzma2Dec_Construct(&state);
  if (coder->Props.size != 1)
    return SZ_ERROR_DATA;
  RINOK(Lzma2Dec_AllocateProbs(&state, coder->Props.data[0], allocMain));
  state.decoder.dic = outBuffer;
  state.decoder.dicBufSize = outSize;
  Lzma2Dec_Init(&state);

  for (;;)
  {
    Byte *inBuf = NULL;
    size_t lookahead = (1 << 18);
    if (lookahead > inSize)
      lookahead = (size_t)inSize;
    res = inStream->Look((void *)inStream, (const void **)&inBuf, &lookahead);
    if (res != SZ_OK)
      break;

    {
      SizeT inProcessed = (SizeT)lookahead, dicPos = state.decoder.dicPos;
      ELzmaStatus status;
      res = Lzma2Dec_DecodeToDic(&state, outSize, inBuf, &inProcessed, LZMA_FINISH_END, &status);
      lookahead -= inProcessed;
      inSize -= inProcessed;
      if (res != SZ_OK)
        break;
      if (state.decoder.dicPos == state.decoder.dicBufSize || (inProcessed == 0 && dicPos == state.decoder.dicPos))
      {
        if (state.decoder.dicBufSize != outSize || lookahead != 0 ||
            (status != LZMA_STATUS_FINISHED_WITH_MARK))
          res = SZ_ERROR_DATA;
        break;
      }
      res = inStream->Skip((void *)inStream, inProcessed);
      if (res != SZ_OK)
        break;
    }
  }

  Lzma2Dec_FreeProbs(&state, allocMain);
  return res;
}

static SRes SzDecodeCopy(UInt64 inSize, ILookInStream *inStream, Byte *outBuffer)
{
  while (inSize > 0)
  {
    void *inBuf;
    size_t curSize = (1 << 18);
    if (curSize > inSize)
      curSize = (size_t)inSize;
    RINOK(inStream->Look((void *)inStream, (const void **)&inBuf, &curSize));
    if (curSize == 0)
      return SZ_ERROR_INPUT_EOF;
    memcpy(outBuffer, inBuf, curSize);
    outBuffer += curSize;
    inSize -= curSize;
    RINOK(inStream->Skip((void *)inStream, curSize));
  }
  return SZ_OK;
}

static Bool IS_MAIN_METHOD(UInt32 m)
{
  switch(m)
  {
    case k_Copy:
    case k_LZMA:
    case k_LZMA2:
    #ifdef _7ZIP_PPMD_SUPPPORT
    case k_PPMD:
    #endif
      return True;
  }
  return False;
}

static Bool IS_SUPPORTED_CODER(const CSzCoderInfo *c)
{
  return
      c->NumInStreams == 1 &&
      c->NumOutStreams == 1 &&
      c->MethodID <= (UInt32)0xFFFFFFFF &&
      IS_MAIN_METHOD((UInt32)c->MethodID);
}

#define IS_BCJ2(c) ((c)->MethodID == k_BCJ2 && (c)->NumInStreams == 4 && (c)->NumOutStreams == 1)

static SRes CheckSupportedFolder(const CSzFolder *f)
{
  if (f->NumCoders < 1 || f->NumCoders > 4)
    return SZ_ERROR_UNSUPPORTED;
  if (!IS_SUPPORTED_CODER(&f->Coders[0]))
    return SZ_ERROR_UNSUPPORTED;
  if (f->NumCoders == 1)
  {
    if (f->NumPackStreams != 1 || f->PackStreams[0] != 0 || f->NumBindPairs != 0)
      return SZ_ERROR_UNSUPPORTED;
    return SZ_OK;
  }
  if (f->NumCoders == 2)
  {
    CSzCoderInfo *c = &f->Coders[1];
    if (c->MethodID > (UInt32)0xFFFFFFFF ||
        c->NumInStreams != 1 ||
        c->NumOutStreams != 1 ||
        f->NumPackStreams != 1 ||
        f->PackStreams[0] != 0 ||
        f->NumBindPairs != 1 ||
        f->BindPairs[0].InIndex != 1 ||
        f->BindPairs[0].OutIndex != 0)
      return SZ_ERROR_UNSUPPORTED;
    switch ((UInt32)c->MethodID)
    {
      case k_BCJ:
      case k_ARM:
        break;
      default:
        return SZ_ERROR_UNSUPPORTED;
    }
    return SZ_OK;
  }
  if (f->NumCoders == 4)
  {
    if (!IS_SUPPORTED_CODER(&f->Coders[1]) ||
        !IS_SUPPORTED_CODER(&f->Coders[2]) ||
        !IS_BCJ2(&f->Coders[3]))
      return SZ_ERROR_UNSUPPORTED;
    if (f->NumPackStreams != 4 ||
        f->PackStreams[0] != 2 ||
        f->PackStreams[1] != 6 ||
        f->PackStreams[2] != 1 ||
        f->PackStreams[3] != 0 ||
        f->NumBindPairs != 3 ||
        f->BindPairs[0].InIndex != 5 || f->BindPairs[0].OutIndex != 0 ||
        f->BindPairs[1].InIndex != 4 || f->BindPairs[1].OutIndex != 1 ||
        f->BindPairs[2].InIndex != 3 || f->BindPairs[2].OutIndex != 2)
      return SZ_ERROR_UNSUPPORTED;
    return SZ_OK;
  }
  return SZ_ERROR_UNSUPPORTED;
}

static UInt64 GetSum(const UInt64 *values, UInt32 index)
{
  UInt64 sum = 0;
  UInt32 i;
  for (i = 0; i < index; i++)
    sum += values[i];
  return sum;
}

#define CASE_BRA_CONV(isa) case k_ ## isa: isa ## _Convert(outBuffer, outSize, 0, 0); break;

static SRes SzFolder_Decode2(const CSzFolder *folder, const UInt64 *packSizes,
    ILookInStream *inStream, UInt64 startPos,
    Byte *outBuffer, SizeT outSize, ISzAlloc *allocMain,
    Byte *tempBuf[])
{
  UInt32 ci;
  SizeT tempSizes[3] = { 0, 0, 0};
  SizeT tempSize3 = 0;
  Byte *tempBuf3 = 0;

  RINOK(CheckSupportedFolder(folder));

  for (ci = 0; ci < folder->NumCoders; ci++)
  {
    CSzCoderInfo *coder = &folder->Coders[ci];

    if (IS_MAIN_METHOD((UInt32)coder->MethodID))
    {
      UInt32 si = 0;
      UInt64 offset;
      UInt64 inSize;
      Byte *outBufCur = outBuffer;
      SizeT outSizeCur = outSize;
      if (folder->NumCoders == 4)
      {
        UInt32 indices[] = { 3, 2, 0 };
        UInt64 unpackSize = folder->UnpackSizes[ci];
        si = indices[ci];
        if (ci < 2)
        {
          Byte *temp;
          outSizeCur = (SizeT)unpackSize;
          if (outSizeCur != unpackSize)
            return SZ_ERROR_MEM;
          temp = (Byte *)IAlloc_Alloc(allocMain, outSizeCur);
          if (temp == 0 && outSizeCur != 0)
            return SZ_ERROR_MEM;
          outBufCur = tempBuf[1 - ci] = temp;
          tempSizes[1 - ci] = outSizeCur;
        }
        else if (ci == 2)
        {
          if (unpackSize > outSize) /* check it */
            return SZ_ERROR_PARAM;
          tempBuf3 = outBufCur = outBuffer + (outSize - (size_t)unpackSize);
          tempSize3 = outSizeCur = (SizeT)unpackSize;
        }
        else
          return SZ_ERROR_UNSUPPORTED;
      }
      offset = GetSum(packSizes, si);
      inSize = packSizes[si];
      RINOK(LookInStream_SeekTo(inStream, startPos + offset));

      if (coder->MethodID == k_Copy)
      {
        if (inSize != outSizeCur) /* check it */
          return SZ_ERROR_DATA;
        RINOK(SzDecodeCopy(inSize, inStream, outBufCur));
      }
      else if (coder->MethodID == k_LZMA)
      {
        RINOK(SzDecodeLzma(coder, inSize, inStream, outBufCur, outSizeCur, allocMain));
      }
      else if (coder->MethodID == k_LZMA2)
      {
        RINOK(SzDecodeLzma2(coder, inSize, inStream, outBufCur, outSizeCur, allocMain));
      }
      else
      {
        #ifdef _7ZIP_PPMD_SUPPPORT
        RINOK(SzDecodePpmd(coder, inSize, inStream, outBufCur, outSizeCur, allocMain));
        #else
        return SZ_ERROR_UNSUPPORTED;
        #endif
      }
    }
    else if (coder->MethodID == k_BCJ2)
    {
      UInt64 offset = GetSum(packSizes, 1);
      UInt64 s3Size = packSizes[1];
      SRes res;
      if (ci != 3)
        return SZ_ERROR_UNSUPPORTED;
      RINOK(LookInStream_SeekTo(inStream, startPos + offset));
      tempSizes[2] = (SizeT)s3Size;
      if (tempSizes[2] != s3Size)
        return SZ_ERROR_MEM;
      tempBuf[2] = (Byte *)IAlloc_Alloc(allocMain, tempSizes[2]);
      if (tempBuf[2] == 0 && tempSizes[2] != 0)
        return SZ_ERROR_MEM;
      res = SzDecodeCopy(s3Size, inStream, tempBuf[2]);
      RINOK(res)

      res = Bcj2_Decode(
          tempBuf3, tempSize3,
          tempBuf[0], tempSizes[0],
          tempBuf[1], tempSizes[1],
          tempBuf[2], tempSizes[2],
          outBuffer, outSize);
      RINOK(res)
    }
    else
    {
      if (ci != 1)
        return SZ_ERROR_UNSUPPORTED;
      switch(coder->MethodID)
      {
        case k_BCJ:
        {
          UInt32 state;
          x86_Convert_Init(state);
          x86_Convert(outBuffer, outSize, 0, &state, 0);
          break;
        }
        CASE_BRA_CONV(ARM)
        default:
          return SZ_ERROR_UNSUPPORTED;
      }
    }
  }
  return SZ_OK;
}

SRes SzFolder_Decode(const CSzFolder *folder, const UInt64 *packSizes,
    ILookInStream *inStream, UInt64 startPos,
    Byte *outBuffer, size_t outSize, ISzAlloc *allocMain)
{
  Byte *tempBuf[3] = { 0, 0, 0};
  int i;
  SRes res = SzFolder_Decode2(folder, packSizes, inStream, startPos,
      outBuffer, (SizeT)outSize, allocMain, tempBuf);
  for (i = 0; i < 3; i++)
    IAlloc_Free(allocMain, tempBuf[i]);
  return res;
}
