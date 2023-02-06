/* XzDec.c -- Xz Decode
2011-02-07 : Igor Pavlov : Public domain */

/* #define XZ_DUMP */

#ifdef XZ_DUMP
#include <stdio.h>
#endif

#include <stdlib.h>
#include <string.h>

#include "7zCrc.h"
#include "Alloc.h"
#include "Bra.h"
#include "CpuArch.h"
#include "Delta.h"
#include "Lzma2Dec.h"

#ifdef USE_SUBBLOCK
#include "Bcj3Dec.c"
#include "SbDec.c"
#endif

#include "Xz.h"

#define XZ_CHECK_SIZE_MAX 64

#define CODER_BUF_SIZE (1 << 17)

unsigned Xz_ReadVarInt(const Byte *p, size_t maxSize, UInt64 *value)
{
  int i, limit;
  *value = 0;
  limit = (maxSize > 9) ? 9 : (int)maxSize;

  for (i = 0; i < limit;)
  {
    Byte b = p[i];
    *value |= (UInt64)(b & 0x7F) << (7 * i++);
    if ((b & 0x80) == 0)
      return (b == 0 && i != 1) ? 0 : i;
  }
  return 0;
}

/* ---------- BraState ---------- */

#define BRA_BUF_SIZE (1 << 14)

typedef struct
{
  size_t bufPos;
  size_t bufConv;
  size_t bufTotal;

  UInt32 methodId;
  int encodeMode;
  UInt32 delta;
  UInt32 ip;
  UInt32 x86State;
  Byte deltaState[DELTA_STATE_SIZE];

  Byte buf[BRA_BUF_SIZE];
} CBraState;

void BraState_Free(void *pp, ISzAlloc *alloc)
{
  alloc->Free(alloc, pp);
}

SRes BraState_SetProps(void *pp, const Byte *props, size_t propSize, ISzAlloc *alloc)
{
  CBraState *p = ((CBraState *)pp);
  alloc = alloc;
  p->ip = 0;
  if (p->methodId == XZ_ID_Delta)
  {
    if (propSize != 1)
      return SZ_ERROR_UNSUPPORTED;
    p->delta = (unsigned)props[0] + 1;
  }
  else
  {
    if (propSize == 4)
    {
      UInt32 v = GetUi32(props);
      switch(p->methodId)
      {
        case XZ_ID_PPC:
        case XZ_ID_ARM:
        case XZ_ID_SPARC:
          if ((v & 3) != 0)
            return SZ_ERROR_UNSUPPORTED;
          break;
        case XZ_ID_ARMT:
          if ((v & 1) != 0)
            return SZ_ERROR_UNSUPPORTED;
          break;
        case XZ_ID_IA64:
          if ((v & 0xF) != 0)
            return SZ_ERROR_UNSUPPORTED;
          break;
      }
      p->ip = v;
    }
    else if (propSize != 0)
      return SZ_ERROR_UNSUPPORTED;
  }
  return SZ_OK;
}

void BraState_Init(void *pp)
{
  CBraState *p = ((CBraState *)pp);
  p->bufPos = p->bufConv = p->bufTotal = 0;
  x86_Convert_Init(p->x86State);
  if (p->methodId == XZ_ID_Delta)
    Delta_Init(p->deltaState);
}

#define CASE_BRA_CONV(isa) case XZ_ID_ ## isa: p->bufConv = isa ## _Convert(p->buf, p->bufTotal, p->ip, p->encodeMode); break;

static SRes BraState_Code(void *pp, Byte *dest, SizeT *destLen, const Byte *src, SizeT *srcLen,
    int srcWasFinished, ECoderFinishMode finishMode, int *wasFinished)
{
  CBraState *p = ((CBraState *)pp);
  SizeT destLenOrig = *destLen;
  SizeT srcLenOrig = *srcLen;
  *destLen = 0;
  *srcLen = 0;
  finishMode = finishMode;
  *wasFinished = 0;
  while (destLenOrig > 0)
  {
    if (p->bufPos != p->bufConv)
    {
      size_t curSize = p->bufConv - p->bufPos;
      if (curSize > destLenOrig)
        curSize = destLenOrig;
      memcpy(dest, p->buf + p->bufPos, curSize);
      p->bufPos += curSize;
      *destLen += curSize;
      dest += curSize;
      destLenOrig -= curSize;
      continue;
    }
    p->bufTotal -= p->bufPos;
    memmove(p->buf, p->buf + p->bufPos, p->bufTotal);
    p->bufPos = 0;
    p->bufConv = 0;
    {
      size_t curSize = BRA_BUF_SIZE - p->bufTotal;
      if (curSize > srcLenOrig)
        curSize = srcLenOrig;
      memcpy(p->buf + p->bufTotal, src, curSize);
      *srcLen += curSize;
      src += curSize;
      srcLenOrig -= curSize;
      p->bufTotal += curSize;
    }
    if (p->bufTotal == 0)
      break;
    switch(p->methodId)
    {
      case XZ_ID_Delta:
        if (p->encodeMode)
          Delta_Encode(p->deltaState, p->delta, p->buf, p->bufTotal);
        else
          Delta_Decode(p->deltaState, p->delta, p->buf, p->bufTotal);
        p->bufConv = p->bufTotal;
        break;
      case XZ_ID_X86:
        p->bufConv = x86_Convert(p->buf, p->bufTotal, p->ip, &p->x86State, p->encodeMode);
        break;
      CASE_BRA_CONV(PPC)
      CASE_BRA_CONV(IA64)
      CASE_BRA_CONV(ARM)
      CASE_BRA_CONV(ARMT)
      CASE_BRA_CONV(SPARC)
      default:
        return SZ_ERROR_UNSUPPORTED;
    }
    p->ip += (UInt32)p->bufConv;

    if (p->bufConv == 0)
    {
      if (!srcWasFinished)
        break;
      p->bufConv = p->bufTotal;
    }
  }
  if (p->bufTotal == p->bufPos && srcLenOrig == 0 && srcWasFinished)
    *wasFinished = 1;
  return SZ_OK;
}

SRes BraState_SetFromMethod(IStateCoder *p, UInt64 id, int encodeMode, ISzAlloc *alloc)
{
  CBraState *decoder;
  if (id != XZ_ID_Delta &&
      id != XZ_ID_X86 &&
      id != XZ_ID_PPC &&
      id != XZ_ID_IA64 &&
      id != XZ_ID_ARM &&
      id != XZ_ID_ARMT &&
      id != XZ_ID_SPARC)
    return SZ_ERROR_UNSUPPORTED;
  p->p = 0;
  decoder = alloc->Alloc(alloc, sizeof(CBraState));
  if (decoder == 0)
    return SZ_ERROR_MEM;
  decoder->methodId = (UInt32)id;
  decoder->encodeMode = encodeMode;
  p->p = decoder;
  p->Free = BraState_Free;
  p->SetProps = BraState_SetProps;
  p->Init = BraState_Init;
  p->Code = BraState_Code;
  return SZ_OK;
}

/* ---------- SbState ---------- */

#ifdef USE_SUBBLOCK

static void SbState_Free(void *pp, ISzAlloc *alloc)
{
  CSbDec *p = (CSbDec *)pp;
  SbDec_Free(p);
  alloc->Free(alloc, pp);
}

static SRes SbState_SetProps(void *pp, const Byte *props, size_t propSize, ISzAlloc *alloc)
{
  pp = pp;
  props = props;
  alloc = alloc;
  return (propSize == 0) ? SZ_OK : SZ_ERROR_UNSUPPORTED;
}

static void SbState_Init(void *pp)
{
  SbDec_Init((CSbDec *)pp);
}

static SRes SbState_Code(void *pp, Byte *dest, SizeT *destLen, const Byte *src, SizeT *srcLen,
    int srcWasFinished, ECoderFinishMode finishMode, int *wasFinished)
{
  CSbDec *p = (CSbDec *)pp;
  SRes res;
  srcWasFinished = srcWasFinished;
  p->dest = dest;
  p->destLen = *destLen;
  p->src = src;
  p->srcLen = *srcLen;
  p->finish = finishMode; /* change it */
  res = SbDec_Decode((CSbDec *)pp);
  *destLen -= p->destLen;
  *srcLen -= p->srcLen;
  *wasFinished = (*destLen == 0 && *srcLen == 0); /* change it */
  return res;
}

SRes SbState_SetFromMethod(IStateCoder *p, ISzAlloc *alloc)
{
  CSbDec *decoder;
  p->p = 0;
  decoder = alloc->Alloc(alloc, sizeof(CSbDec));
  if (decoder == 0)
    return SZ_ERROR_MEM;
  p->p = decoder;
  p->Free = SbState_Free;
  p->SetProps = SbState_SetProps;
  p->Init = SbState_Init;
  p->Code = SbState_Code;
  SbDec_Construct(decoder);
  SbDec_SetAlloc(decoder, alloc);
  return SZ_OK;
}
#endif

/* ---------- Lzma2State ---------- */

static void Lzma2State_Free(void *pp, ISzAlloc *alloc)
{
  Lzma2Dec_Free((CLzma2Dec *)pp, alloc);
  alloc->Free(alloc, pp);
}

static SRes Lzma2State_SetProps(void *pp, const Byte *props, size_t propSize, ISzAlloc *alloc)
{
  if (propSize != 1)
    return SZ_ERROR_UNSUPPORTED;
  return Lzma2Dec_Allocate((CLzma2Dec *)pp, props[0], alloc);
}

static void Lzma2State_Init(void *pp)
{
  Lzma2Dec_Init((CLzma2Dec *)pp);
}

static SRes Lzma2State_Code(void *pp, Byte *dest, SizeT *destLen, const Byte *src, SizeT *srcLen,
    int srcWasFinished, ECoderFinishMode finishMode, int *wasFinished)
{
  ELzmaStatus status;
  /* ELzmaFinishMode fm = (finishMode == LZMA_FINISH_ANY) ? LZMA_FINISH_ANY : LZMA_FINISH_END; */
  SRes res = Lzma2Dec_DecodeToBuf((CLzma2Dec *)pp, dest, destLen, src, srcLen, finishMode, &status);
  srcWasFinished = srcWasFinished;
  *wasFinished = (status == LZMA_STATUS_FINISHED_WITH_MARK);
  return res;
}

static SRes Lzma2State_SetFromMethod(IStateCoder *p, ISzAlloc *alloc)
{
  CLzma2Dec *decoder = alloc->Alloc(alloc, sizeof(CLzma2Dec));
  p->p = decoder;
  if (decoder == 0)
    return SZ_ERROR_MEM;
  p->Free = Lzma2State_Free;
  p->SetProps = Lzma2State_SetProps;
  p->Init = Lzma2State_Init;
  p->Code = Lzma2State_Code;
  Lzma2Dec_Construct(decoder);
  return SZ_OK;
}


void MixCoder_Construct(CMixCoder *p, ISzAlloc *alloc)
{
  int i;
  p->alloc = alloc;
  p->buf = 0;
  p->numCoders = 0;
  for (i = 0; i < MIXCODER_NUM_FILTERS_MAX; i++)
    p->coders[i].p = NULL;
}

void MixCoder_Free(CMixCoder *p)
{
  int i;
  for (i = 0; i < p->numCoders; i++)
  {
    IStateCoder *sc = &p->coders[i];
    if (p->alloc && sc->p)
      sc->Free(sc->p, p->alloc);
  }
  p->numCoders = 0;
  if (p->buf)
    p->alloc->Free(p->alloc, p->buf);
}

void MixCoder_Init(CMixCoder *p)
{
  int i;
  for (i = 0; i < p->numCoders - 1; i++)
  {
    p->size[i] = 0;
    p->pos[i] = 0;
    p->finished[i] = 0;
  }
  for (i = 0; i < p->numCoders; i++)
  {
    IStateCoder *coder = &p->coders[i];
    coder->Init(coder->p);
  }
}

SRes MixCoder_SetFromMethod(CMixCoder *p, int coderIndex, UInt64 methodId)
{
  IStateCoder *sc = &p->coders[coderIndex];
  p->ids[coderIndex] = methodId;
  switch(methodId)
  {
    case XZ_ID_LZMA2: return Lzma2State_SetFromMethod(sc, p->alloc);
    #ifdef USE_SUBBLOCK
    case XZ_ID_Subblock: return SbState_SetFromMethod(sc, p->alloc);
    #endif
  }
  if (coderIndex == 0)
    return SZ_ERROR_UNSUPPORTED;
  return BraState_SetFromMethod(sc, methodId, 0, p->alloc);
}

SRes MixCoder_Code(CMixCoder *p, Byte *dest, SizeT *destLen,
    const Byte *src, SizeT *srcLen, int srcWasFinished,
    ECoderFinishMode finishMode, ECoderStatus *status)
{
  SizeT destLenOrig = *destLen;
  SizeT srcLenOrig = *srcLen;
  Bool allFinished = True;
  *destLen = 0;
  *srcLen = 0;
  *status = CODER_STATUS_NOT_FINISHED;

  if (p->buf == 0)
  {
    p->buf = p->alloc->Alloc(p->alloc, CODER_BUF_SIZE * (MIXCODER_NUM_FILTERS_MAX - 1));
    if (p->buf == 0)
      return SZ_ERROR_MEM;
  }

  if (p->numCoders != 1)
    finishMode = CODER_FINISH_ANY;

  for (;;)
  {
    Bool processed = False;
    int i;
    /*
    if (p->numCoders == 1 && *destLen == destLenOrig && finishMode == LZMA_FINISH_ANY)
      break;
    */

    for (i = 0; i < p->numCoders; i++)
    {
      SRes res;
      IStateCoder *coder = &p->coders[i];
      Byte *destCur;
      SizeT destLenCur, srcLenCur;
      const Byte *srcCur;
      int srcFinishedCur;
      int encodingWasFinished;
      
      if (i == 0)
      {
        srcCur = src;
        srcLenCur = srcLenOrig - *srcLen;
        srcFinishedCur = srcWasFinished;
      }
      else
      {
        srcCur = p->buf + (CODER_BUF_SIZE * (i - 1)) + p->pos[i - 1];
        srcLenCur = p->size[i - 1] - p->pos[i - 1];
        srcFinishedCur = p->finished[i - 1];
      }
      
      if (i == p->numCoders - 1)
      {
        destCur = dest;
        destLenCur = destLenOrig - *destLen;
      }
      else
      {
        if (p->pos[i] != p->size[i])
          continue;
        destCur = p->buf + (CODER_BUF_SIZE * i);
        destLenCur = CODER_BUF_SIZE;
      }
      
      res = coder->Code(coder->p, destCur, &destLenCur, srcCur, &srcLenCur, srcFinishedCur, finishMode, &encodingWasFinished);

      if (!encodingWasFinished)
        allFinished = False;

      if (i == 0)
      {
        *srcLen += srcLenCur;
        src += srcLenCur;
      }
      else
      {
        p->pos[i - 1] += srcLenCur;
      }

      if (i == p->numCoders - 1)
      {
        *destLen += destLenCur;
        dest += destLenCur;
      }
      else
      {
        p->size[i] = destLenCur;
        p->pos[i] = 0;
        p->finished[i] = encodingWasFinished;
      }
      
      if (res != SZ_OK)
        return res;

      if (destLenCur != 0 || srcLenCur != 0)
        processed = True;
    }
    if (!processed)
      break;
  }
  if (allFinished)
    *status = CODER_STATUS_FINISHED_WITH_MARK;
  return SZ_OK;
}

SRes Xz_ParseHeader(CXzStreamFlags *p, const Byte *buf)
{
  *p = (CXzStreamFlags)GetBe16(buf + XZ_SIG_SIZE);
  if (CrcCalc(buf + XZ_SIG_SIZE, XZ_STREAM_FLAGS_SIZE) !=
      GetUi32(buf + XZ_SIG_SIZE + XZ_STREAM_FLAGS_SIZE))
    return SZ_ERROR_NO_ARCHIVE;
  return XzFlags_IsSupported(*p) ? SZ_OK : SZ_ERROR_UNSUPPORTED;
}

static Bool Xz_CheckFooter(CXzStreamFlags flags, UInt64 indexSize, const Byte *buf)
{
  return
      indexSize == (((UInt64)GetUi32(buf + 4) + 1) << 2) &&
      (GetUi32(buf) == CrcCalc(buf + 4, 6) &&
      flags == GetBe16(buf + 8) &&
      memcmp(buf + 10, XZ_FOOTER_SIG, XZ_FOOTER_SIG_SIZE) == 0);
}

#define READ_VARINT_AND_CHECK(buf, pos, size, res) \
  { unsigned s = Xz_ReadVarInt(buf + pos, size - pos, res); \
  if (s == 0) return SZ_ERROR_ARCHIVE; pos += s; }


SRes XzBlock_Parse(CXzBlock *p, const Byte *header)
{
  unsigned pos;
  int numFilters, i;
  UInt32 headerSize = (UInt32)header[0] << 2;

  if (CrcCalc(header, headerSize) != GetUi32(header + headerSize))
    return SZ_ERROR_ARCHIVE;

  pos = 1;
  if (pos == headerSize)
    return SZ_ERROR_ARCHIVE;
  p->flags = header[pos++];

  if (XzBlock_HasPackSize(p))
  {
    READ_VARINT_AND_CHECK(header, pos, headerSize, &p->packSize);
    if (p->packSize == 0 || p->packSize + headerSize >= (UInt64)1 << 63)
      return SZ_ERROR_ARCHIVE;
  }

  if (XzBlock_HasUnpackSize(p))
    READ_VARINT_AND_CHECK(header, pos, headerSize, &p->unpackSize);

  numFilters = XzBlock_GetNumFilters(p);
  for (i = 0; i < numFilters; i++)
  {
    CXzFilter *filter = p->filters + i;
    UInt64 size;
    READ_VARINT_AND_CHECK(header, pos, headerSize, &filter->id);
    READ_VARINT_AND_CHECK(header, pos, headerSize, &size);
    if (size > headerSize - pos || size > XZ_FILTER_PROPS_SIZE_MAX)
      return SZ_ERROR_ARCHIVE;
    filter->propsSize = (UInt32)size;
    memcpy(filter->props, header + pos, (size_t)size);
    pos += (unsigned)size;

    #ifdef XZ_DUMP
    printf("\nf[%d] = %2X: ", i, filter->id);
    {
      int i;
      for (i = 0; i < size; i++)
        printf(" %2X", filter->props[i]);
    }
    #endif
  }

  while (pos < headerSize)
    if (header[pos++] != 0)
      return SZ_ERROR_ARCHIVE;
  return SZ_OK;
}

SRes XzDec_Init(CMixCoder *p, const CXzBlock *block)
{
  int i;
  Bool needReInit = True;
  int numFilters = XzBlock_GetNumFilters(block);
  if (numFilters == p->numCoders)
  {
    for (i = 0; i < numFilters; i++)
      if (p->ids[i] != block->filters[numFilters - 1 - i].id)
        break;
    needReInit = (i != numFilters);
  }
  if (needReInit)
  {
    MixCoder_Free(p);
    p->numCoders = numFilters;
    for (i = 0; i < numFilters; i++)
    {
      const CXzFilter *f = &block->filters[numFilters - 1 - i];
      RINOK(MixCoder_SetFromMethod(p, i, f->id));
    }
  }
  for (i = 0; i < numFilters; i++)
  {
    const CXzFilter *f = &block->filters[numFilters - 1 - i];
    IStateCoder *sc = &p->coders[i];
    RINOK(sc->SetProps(sc->p, f->props, f->propsSize, p->alloc));
  }
  MixCoder_Init(p);
  return SZ_OK;
}

void XzUnpacker_Init(CXzUnpacker *p)
{
  p->state = XZ_STATE_STREAM_HEADER;
  p->pos = 0;
  p->numStreams = 0;
}

void XzUnpacker_Construct(CXzUnpacker *p, ISzAlloc *alloc)
{
  MixCoder_Construct(&p->decoder, alloc);
  XzUnpacker_Init(p);
}

void XzUnpacker_Free(CXzUnpacker *p)
{
  MixCoder_Free(&p->decoder);
}

SRes XzUnpacker_Code(CXzUnpacker *p, Byte *dest, SizeT *destLen,
    const Byte *src, SizeT *srcLen, int finishMode, ECoderStatus *status)
{
  SizeT destLenOrig = *destLen;
  SizeT srcLenOrig = *srcLen;
  *destLen = 0;
  *srcLen = 0;
  *status = CODER_STATUS_NOT_SPECIFIED;
  for (;;)
  {
    SizeT srcRem = srcLenOrig - *srcLen;

    if (p->state == XZ_STATE_BLOCK)
    {
      SizeT destLen2 = destLenOrig - *destLen;
      SizeT srcLen2 = srcLenOrig - *srcLen;
      SRes res;
      if (srcLen2 == 0 && destLen2 == 0)
      {
        *status = CODER_STATUS_NOT_FINISHED;
        return SZ_OK;
      }
      
      res = MixCoder_Code(&p->decoder, dest, &destLen2, src, &srcLen2, False, finishMode, status);
      XzCheck_Update(&p->check, dest, destLen2);
      
      (*srcLen) += srcLen2;
      src += srcLen2;
      p->packSize += srcLen2;
      
      (*destLen) += destLen2;
      dest += destLen2;
      p->unpackSize += destLen2;
      
      RINOK(res);
      
      if (*status == CODER_STATUS_FINISHED_WITH_MARK)
      {
        Byte temp[32];
        unsigned num = Xz_WriteVarInt(temp, p->packSize + p->blockHeaderSize + XzFlags_GetCheckSize(p->streamFlags));
        num += Xz_WriteVarInt(temp + num, p->unpackSize);
        Sha256_Update(&p->sha, temp, num);
        p->indexSize += num;
        p->numBlocks++;
        
        p->state = XZ_STATE_BLOCK_FOOTER;
        p->pos = 0;
        p->alignPos = 0;
      }
      else if (srcLen2 == 0 && destLen2 == 0)
        return SZ_OK;
      
      continue;
    }

    if (srcRem == 0)
    {
      *status = CODER_STATUS_NEEDS_MORE_INPUT;
      return SZ_OK;
    }

    switch(p->state)
    {
      case XZ_STATE_STREAM_HEADER:
      {
        if (p->pos < XZ_STREAM_HEADER_SIZE)
        {
          if (p->pos < XZ_SIG_SIZE && *src != XZ_SIG[p->pos])
            return SZ_ERROR_NO_ARCHIVE;
          p->buf[p->pos++] = *src++;
          (*srcLen)++;
        }
        else
        {
          RINOK(Xz_ParseHeader(&p->streamFlags, p->buf));
          p->state = XZ_STATE_BLOCK_HEADER;
          Sha256_Init(&p->sha);
          p->indexSize = 0;
          p->numBlocks = 0;
          p->pos = 0;
        }
        break;
      }

      case XZ_STATE_BLOCK_HEADER:
      {
        if (p->pos == 0)
        {
          p->buf[p->pos++] = *src++;
          (*srcLen)++;
          if (p->buf[0] == 0)
          {
            p->indexPreSize = 1 + Xz_WriteVarInt(p->buf + 1, p->numBlocks);
            p->indexPos = p->indexPreSize;
            p->indexSize += p->indexPreSize;
            Sha256_Final(&p->sha, p->shaDigest);
            Sha256_Init(&p->sha);
            p->crc = CrcUpdate(CRC_INIT_VAL, p->buf, p->indexPreSize);
            p->state = XZ_STATE_STREAM_INDEX;
          }
          p->blockHeaderSize = ((UInt32)p->buf[0] << 2) + 4;
        }
        else if (p->pos != p->blockHeaderSize)
        {
          UInt32 cur = p->blockHeaderSize - p->pos;
          if (cur > srcRem)
            cur = (UInt32)srcRem;
          memcpy(p->buf + p->pos, src, cur);
          p->pos += cur;
          (*srcLen) += cur;
          src += cur;
        }
        else
        {
          RINOK(XzBlock_Parse(&p->block, p->buf));
          p->state = XZ_STATE_BLOCK;
          p->packSize = 0;
          p->unpackSize = 0;
          XzCheck_Init(&p->check, XzFlags_GetCheckType(p->streamFlags));
          RINOK(XzDec_Init(&p->decoder, &p->block));
        }
        break;
      }

      case XZ_STATE_BLOCK_FOOTER:
      {
        if (((p->packSize + p->alignPos) & 3) != 0)
        {
          (*srcLen)++;
          p->alignPos++;
          if (*src++ != 0)
            return SZ_ERROR_CRC;
        }
        else
        {
          UInt32 checkSize = XzFlags_GetCheckSize(p->streamFlags);
          UInt32 cur = checkSize - p->pos;
          if (cur != 0)
          {
            if (cur > srcRem)
              cur = (UInt32)srcRem;
            memcpy(p->buf + p->pos, src, cur);
            p->pos += cur;
            (*srcLen) += cur;
            src += cur;
          }
          else
          {
            Byte digest[XZ_CHECK_SIZE_MAX];
            p->state = XZ_STATE_BLOCK_HEADER;
            p->pos = 0;
            if (XzCheck_Final(&p->check, digest) && memcmp(digest, p->buf, checkSize) != 0)
              return SZ_ERROR_CRC;
          }
        }
        break;
      }

      case XZ_STATE_STREAM_INDEX:
      {
        if (p->pos < p->indexPreSize)
        {
          (*srcLen)++;
          if (*src++ != p->buf[p->pos++])
            return SZ_ERROR_CRC;
        }
        else
        {
          if (p->indexPos < p->indexSize)
          {
            UInt64 cur = p->indexSize - p->indexPos;
            if (srcRem > cur)
              srcRem = (SizeT)cur;
            p->crc = CrcUpdate(p->crc, src, srcRem);
            Sha256_Update(&p->sha, src, srcRem);
            (*srcLen) += srcRem;
            src += srcRem;
            p->indexPos += srcRem;
          }
          else if ((p->indexPos & 3) != 0)
          {
            Byte b = *src++;
            p->crc = CRC_UPDATE_BYTE(p->crc, b);
            (*srcLen)++;
            p->indexPos++;
            p->indexSize++;
            if (b != 0)
              return SZ_ERROR_CRC;
          }
          else
          {
            Byte digest[SHA256_DIGEST_SIZE];
            p->state = XZ_STATE_STREAM_INDEX_CRC;
            p->indexSize += 4;
            p->pos = 0;
            Sha256_Final(&p->sha, digest);
            if (memcmp(digest, p->shaDigest, SHA256_DIGEST_SIZE) != 0)
              return SZ_ERROR_CRC;
          }
        }
        break;
      }

      case XZ_STATE_STREAM_INDEX_CRC:
      {
        if (p->pos < 4)
        {
          (*srcLen)++;
          p->buf[p->pos++] = *src++;
        }
        else
        {
          p->state = XZ_STATE_STREAM_FOOTER;
          p->pos = 0;
          if (CRC_GET_DIGEST(p->crc) != GetUi32(p->buf))
            return SZ_ERROR_CRC;
        }
        break;
      }

      case XZ_STATE_STREAM_FOOTER:
      {
        UInt32 cur = XZ_STREAM_FOOTER_SIZE - p->pos;
        if (cur > srcRem)
          cur = (UInt32)srcRem;
        memcpy(p->buf + p->pos, src, cur);
        p->pos += cur;
        (*srcLen) += cur;
        src += cur;
        if (p->pos == XZ_STREAM_FOOTER_SIZE)
        {
          p->state = XZ_STATE_STREAM_PADDING;
          p->numStreams++;
          p->padSize = 0;
          if (!Xz_CheckFooter(p->streamFlags, p->indexSize, p->buf))
            return SZ_ERROR_CRC;
        }
        break;
      }

      case XZ_STATE_STREAM_PADDING:
      {
        if (*src != 0)
        {
          if (((UInt32)p->padSize & 3) != 0)
            return SZ_ERROR_NO_ARCHIVE;
          p->pos = 0;
          p->state = XZ_STATE_STREAM_HEADER;
        }
        else
        {
          (*srcLen)++;
          src++;
          p->padSize++;
        }
        break;
      }
      
      case XZ_STATE_BLOCK: break; /* to disable GCC warning */
    }
  }
  /*
  if (p->state == XZ_STATE_FINISHED)
    *status = CODER_STATUS_FINISHED_WITH_MARK;
  return SZ_OK;
  */
}

Bool XzUnpacker_IsStreamWasFinished(CXzUnpacker *p)
{
  return (p->state == XZ_STATE_STREAM_PADDING) && (((UInt32)p->padSize & 3) == 0);
}
