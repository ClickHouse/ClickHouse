/* XzEnc.c -- Xz Encode
2011-02-07 : Igor Pavlov : Public domain */

#include <stdlib.h>
#include <string.h>

#include "7zCrc.h"
#include "Alloc.h"
#include "Bra.h"
#include "CpuArch.h"
#ifdef USE_SUBBLOCK
#include "Bcj3Enc.c"
#include "SbFind.c"
#include "SbEnc.c"
#endif

#include "XzEnc.h"

static void *SzBigAlloc(void *p, size_t size) { p = p; return BigAlloc(size); }
static void SzBigFree(void *p, void *address) { p = p; BigFree(address); }
static ISzAlloc g_BigAlloc = { SzBigAlloc, SzBigFree };

static void *SzAlloc(void *p, size_t size) { p = p; return MyAlloc(size); }
static void SzFree(void *p, void *address) { p = p; MyFree(address); }
static ISzAlloc g_Alloc = { SzAlloc, SzFree };

#define XzBlock_ClearFlags(p)       (p)->flags = 0;
#define XzBlock_SetNumFilters(p, n) (p)->flags |= ((n) - 1);
#define XzBlock_SetHasPackSize(p)   (p)->flags |= XZ_BF_PACK_SIZE;
#define XzBlock_SetHasUnpackSize(p) (p)->flags |= XZ_BF_UNPACK_SIZE;

static SRes WriteBytes(ISeqOutStream *s, const void *buf, UInt32 size)
{
  return (s->Write(s, buf, size) == size) ? SZ_OK : SZ_ERROR_WRITE;
}

static SRes WriteBytesAndCrc(ISeqOutStream *s, const void *buf, UInt32 size, UInt32 *crc)
{
  *crc = CrcUpdate(*crc, buf, size);
  return WriteBytes(s, buf, size);
}

SRes Xz_WriteHeader(CXzStreamFlags f, ISeqOutStream *s)
{
  UInt32 crc;
  Byte header[XZ_STREAM_HEADER_SIZE];
  memcpy(header, XZ_SIG, XZ_SIG_SIZE);
  header[XZ_SIG_SIZE] = (Byte)(f >> 8);
  header[XZ_SIG_SIZE + 1] = (Byte)(f & 0xFF);
  crc = CrcCalc(header + XZ_SIG_SIZE, XZ_STREAM_FLAGS_SIZE);
  SetUi32(header + XZ_SIG_SIZE + XZ_STREAM_FLAGS_SIZE, crc);
  return WriteBytes(s, header, XZ_STREAM_HEADER_SIZE);
}

SRes XzBlock_WriteHeader(const CXzBlock *p, ISeqOutStream *s)
{
  Byte header[XZ_BLOCK_HEADER_SIZE_MAX];

  unsigned pos = 1;
  int numFilters, i;
  header[pos++] = p->flags;

  if (XzBlock_HasPackSize(p)) pos += Xz_WriteVarInt(header + pos, p->packSize);
  if (XzBlock_HasUnpackSize(p)) pos += Xz_WriteVarInt(header + pos, p->unpackSize);
  numFilters = XzBlock_GetNumFilters(p);
  for (i = 0; i < numFilters; i++)
  {
    const CXzFilter *f = &p->filters[i];
    pos += Xz_WriteVarInt(header + pos, f->id);
    pos += Xz_WriteVarInt(header + pos, f->propsSize);
    memcpy(header + pos, f->props, f->propsSize);
    pos += f->propsSize;
  }
  while((pos & 3) != 0)
    header[pos++] = 0;
  header[0] = (Byte)(pos >> 2);
  SetUi32(header + pos, CrcCalc(header, pos));
  return WriteBytes(s, header, pos + 4);
}

SRes Xz_WriteFooter(CXzStream *p, ISeqOutStream *s)
{
  Byte buf[32];
  UInt64 globalPos;
  {
    UInt32 crc = CRC_INIT_VAL;
    unsigned pos = 1 + Xz_WriteVarInt(buf + 1, p->numBlocks);
    size_t i;

    globalPos = pos;
    buf[0] = 0;
    RINOK(WriteBytesAndCrc(s, buf, pos, &crc));
    for (i = 0; i < p->numBlocks; i++)
    {
      const CXzBlockSizes *block = &p->blocks[i];
      pos = Xz_WriteVarInt(buf, block->totalSize);
      pos += Xz_WriteVarInt(buf + pos, block->unpackSize);
      globalPos += pos;
      RINOK(WriteBytesAndCrc(s, buf, pos, &crc));
    }
    pos = ((unsigned)globalPos & 3);
    if (pos != 0)
    {
      buf[0] = buf[1] = buf[2] = 0;
      RINOK(WriteBytesAndCrc(s, buf, 4 - pos, &crc));
      globalPos += 4 - pos;
    }
    {
      SetUi32(buf, CRC_GET_DIGEST(crc));
      RINOK(WriteBytes(s, buf, 4));
      globalPos += 4;
    }
  }

  {
    UInt32 indexSize = (UInt32)((globalPos >> 2) - 1);
    SetUi32(buf + 4, indexSize);
    buf[8] = (Byte)(p->flags >> 8);
    buf[9] = (Byte)(p->flags & 0xFF);
    SetUi32(buf, CrcCalc(buf + 4, 6));
    memcpy(buf + 10, XZ_FOOTER_SIG, XZ_FOOTER_SIG_SIZE);
    return WriteBytes(s, buf, 12);
  }
}

SRes Xz_AddIndexRecord(CXzStream *p, UInt64 unpackSize, UInt64 totalSize, ISzAlloc *alloc)
{
  if (p->blocks == 0 || p->numBlocksAllocated == p->numBlocks)
  {
    size_t num = (p->numBlocks + 1) * 2;
    size_t newSize = sizeof(CXzBlockSizes) * num;
    CXzBlockSizes *blocks;
    if (newSize / sizeof(CXzBlockSizes) != num)
      return SZ_ERROR_MEM;
    blocks = alloc->Alloc(alloc, newSize);
    if (blocks == 0)
      return SZ_ERROR_MEM;
    if (p->numBlocks != 0)
    {
      memcpy(blocks, p->blocks, p->numBlocks * sizeof(CXzBlockSizes));
      Xz_Free(p, alloc);
    }
    p->blocks = blocks;
    p->numBlocksAllocated = num;
  }
  {
    CXzBlockSizes *block = &p->blocks[p->numBlocks++];
    block->totalSize = totalSize;
    block->unpackSize = unpackSize;
  }
  return SZ_OK;
}

/* ---------- CSeqCheckInStream ---------- */

typedef struct
{
  ISeqInStream p;
  ISeqInStream *realStream;
  UInt64 processed;
  CXzCheck check;
} CSeqCheckInStream;

void SeqCheckInStream_Init(CSeqCheckInStream *p, int mode)
{
  p->processed = 0;
  XzCheck_Init(&p->check, mode);
}

void SeqCheckInStream_GetDigest(CSeqCheckInStream *p, Byte *digest)
{
  XzCheck_Final(&p->check, digest);
}

static SRes SeqCheckInStream_Read(void *pp, void *data, size_t *size)
{
  CSeqCheckInStream *p = (CSeqCheckInStream *)pp;
  SRes res = p->realStream->Read(p->realStream, data, size);
  XzCheck_Update(&p->check, data, *size);
  p->processed += *size;
  return res;
}

/* ---------- CSeqSizeOutStream ---------- */

typedef struct
{
  ISeqOutStream p;
  ISeqOutStream *realStream;
  UInt64 processed;
} CSeqSizeOutStream;

static size_t MyWrite(void *pp, const void *data, size_t size)
{
  CSeqSizeOutStream *p = (CSeqSizeOutStream *)pp;
  size = p->realStream->Write(p->realStream, data, size);
  p->processed += size;
  return size;
}

/* ---------- CSeqInFilter ---------- */

#define FILTER_BUF_SIZE (1 << 20)

typedef struct
{
  ISeqInStream p;
  ISeqInStream *realStream;
  IStateCoder StateCoder;
  Byte *buf;
  size_t curPos;
  size_t endPos;
  int srcWasFinished;
} CSeqInFilter;

static SRes SeqInFilter_Read(void *pp, void *data, size_t *size)
{
  CSeqInFilter *p = (CSeqInFilter *)pp;
  size_t sizeOriginal = *size;
  if (sizeOriginal == 0)
    return S_OK;
  *size = 0;
  for (;;)
  {
    if (!p->srcWasFinished && p->curPos == p->endPos)
    {
      p->curPos = 0;
      p->endPos = FILTER_BUF_SIZE;
      RINOK(p->realStream->Read(p->realStream, p->buf, &p->endPos));
      if (p->endPos == 0)
        p->srcWasFinished = 1;
    }
    {
      SizeT srcLen = p->endPos - p->curPos;
      int wasFinished;
      SRes res;
      *size = sizeOriginal;
      res = p->StateCoder.Code(p->StateCoder.p, data, size, p->buf + p->curPos, &srcLen,
        p->srcWasFinished, CODER_FINISH_ANY, &wasFinished);
      p->curPos += srcLen;
      if (*size != 0 || srcLen == 0 || res != 0)
        return res;
    }
  }
}

static void SeqInFilter_Construct(CSeqInFilter *p)
{
  p->buf = NULL;
  p->p.Read = SeqInFilter_Read;
}

static void SeqInFilter_Free(CSeqInFilter *p)
{
  if (p->buf)
  {
    g_Alloc.Free(&g_Alloc, p->buf);
    p->buf = NULL;
  }
}

SRes BraState_SetFromMethod(IStateCoder *p, UInt64 id, int encodeMode, ISzAlloc *alloc);

static SRes SeqInFilter_Init(CSeqInFilter *p, const CXzFilter *props)
{
  if (!p->buf)
  {
    p->buf = g_Alloc.Alloc(&g_Alloc, FILTER_BUF_SIZE);
    if (!p->buf)
      return SZ_ERROR_MEM;
  }
  p->curPos = p->endPos = 0;
  p->srcWasFinished = 0;
  RINOK(BraState_SetFromMethod(&p->StateCoder, props->id, 1, &g_Alloc));
  RINOK(p->StateCoder.SetProps(p->StateCoder.p, props->props, props->propsSize, &g_Alloc));
  p->StateCoder.Init(p->StateCoder.p);
  return S_OK;
}

/* ---------- CSbEncInStream ---------- */

#ifdef USE_SUBBLOCK

typedef struct
{
  ISeqInStream p;
  ISeqInStream *inStream;
  CSbEnc enc;
} CSbEncInStream;

static SRes SbEncInStream_Read(void *pp, void *data, size_t *size)
{
  CSbEncInStream *p = (CSbEncInStream *)pp;
  size_t sizeOriginal = *size;
  if (sizeOriginal == 0)
    return S_OK;
  for (;;)
  {
    if (p->enc.needRead && !p->enc.readWasFinished)
    {
      size_t processed = p->enc.needReadSizeMax;
      RINOK(p->inStream->Read(p->inStream, p->enc.buf + p->enc.readPos, &processed));
      p->enc.readPos += processed;
      if (processed == 0)
      {
        p->enc.readWasFinished = True;
        p->enc.isFinalFinished = True;
      }
      p->enc.needRead = False;
    }
    *size = sizeOriginal;
    RINOK(SbEnc_Read(&p->enc, data, size));
    if (*size != 0 || !p->enc.needRead)
      return S_OK;
  }
}

void SbEncInStream_Construct(CSbEncInStream *p, ISzAlloc *alloc)
{
  SbEnc_Construct(&p->enc, alloc);
  p->p.Read = SbEncInStream_Read;
}

SRes SbEncInStream_Init(CSbEncInStream *p)
{
  return SbEnc_Init(&p->enc);
}

void SbEncInStream_Free(CSbEncInStream *p)
{
  SbEnc_Free(&p->enc);
}

#endif


typedef struct
{
  CLzma2EncHandle lzma2;
  #ifdef USE_SUBBLOCK
  CSbEncInStream sb;
  #endif
  CSeqInFilter filter;
  ISzAlloc *alloc;
  ISzAlloc *bigAlloc;
} CLzma2WithFilters;


static void Lzma2WithFilters_Construct(CLzma2WithFilters *p, ISzAlloc *alloc, ISzAlloc *bigAlloc)
{
  p->alloc = alloc;
  p->bigAlloc = bigAlloc;
  p->lzma2 = NULL;
  #ifdef USE_SUBBLOCK
  SbEncInStream_Construct(&p->sb, alloc);
  #endif
  SeqInFilter_Construct(&p->filter);
}

static SRes Lzma2WithFilters_Create(CLzma2WithFilters *p)
{
  p->lzma2 = Lzma2Enc_Create(p->alloc, p->bigAlloc);
  if (p->lzma2 == 0)
    return SZ_ERROR_MEM;
  return SZ_OK;
}

static void Lzma2WithFilters_Free(CLzma2WithFilters *p)
{
  SeqInFilter_Free(&p->filter);
  #ifdef USE_SUBBLOCK
  SbEncInStream_Free(&p->sb);
  #endif
  if (p->lzma2)
  {
    Lzma2Enc_Destroy(p->lzma2);
    p->lzma2 = NULL;
  }
}

void XzProps_Init(CXzProps *p)
{
  p->lzma2Props = 0;
  p->filterProps = 0;
  p->checkId = XZ_CHECK_CRC32;
}

void XzFilterProps_Init(CXzFilterProps *p)
{
  p->id = 0;
  p->delta = 0;
  p->ip= 0;
  p->ipDefined = False;
}

static SRes Xz_Compress(CXzStream *xz, CLzma2WithFilters *lzmaf,
    ISeqOutStream *outStream, ISeqInStream *inStream,
    const CXzProps *props, ICompressProgress *progress)
{
  xz->flags = (Byte)props->checkId;

  RINOK(Lzma2Enc_SetProps(lzmaf->lzma2, props->lzma2Props));
  RINOK(Xz_WriteHeader(xz->flags, outStream));

  {
    CSeqCheckInStream checkInStream;
    CSeqSizeOutStream seqSizeOutStream;
    CXzBlock block;
    int filterIndex = 0;
    CXzFilter *filter = NULL;
    const CXzFilterProps *fp = props->filterProps;
    
    XzBlock_ClearFlags(&block);
    XzBlock_SetNumFilters(&block, 1 + (fp ? 1 : 0));
    
    if (fp)
    {
      filter = &block.filters[filterIndex++];
      filter->id = fp->id;
      filter->propsSize = 0;
      if (fp->id == XZ_ID_Delta)
      {
        filter->props[0] = (Byte)(fp->delta - 1);
        filter->propsSize = 1;
      }
      else if (fp->ipDefined)
      {
        SetUi32(filter->props, fp->ip);
        filter->propsSize = 4;
      }
    }

    {
      CXzFilter *f = &block.filters[filterIndex++];
      f->id = XZ_ID_LZMA2;
      f->propsSize = 1;
      f->props[0] = Lzma2Enc_WriteProperties(lzmaf->lzma2);
    }

    seqSizeOutStream.p.Write = MyWrite;
    seqSizeOutStream.realStream = outStream;
    seqSizeOutStream.processed = 0;
    
    RINOK(XzBlock_WriteHeader(&block, &seqSizeOutStream.p));
    
    checkInStream.p.Read = SeqCheckInStream_Read;
    checkInStream.realStream = inStream;
    SeqCheckInStream_Init(&checkInStream, XzFlags_GetCheckType(xz->flags));
    
    if (fp)
    {
      #ifdef USE_SUBBLOCK
      if (fp->id == XZ_ID_Subblock)
      {
        lzmaf->sb.inStream = &checkInStream.p;
        RINOK(SbEncInStream_Init(&lzmaf->sb));
      }
      else
      #endif
      {
        lzmaf->filter.realStream = &checkInStream.p;
        RINOK(SeqInFilter_Init(&lzmaf->filter, filter));
      }
    }

    {
      UInt64 packPos = seqSizeOutStream.processed;
      SRes res = Lzma2Enc_Encode(lzmaf->lzma2, &seqSizeOutStream.p,
        fp ?
        #ifdef USE_SUBBLOCK
        (fp->id == XZ_ID_Subblock) ? &lzmaf->sb.p:
        #endif
        &lzmaf->filter.p:
        &checkInStream.p,
        progress);
      RINOK(res);
      block.unpackSize = checkInStream.processed;
      block.packSize = seqSizeOutStream.processed - packPos;
    }

    {
      unsigned padSize = 0;
      Byte buf[128];
      while((((unsigned)block.packSize + padSize) & 3) != 0)
        buf[padSize++] = 0;
      SeqCheckInStream_GetDigest(&checkInStream, buf + padSize);
      RINOK(WriteBytes(&seqSizeOutStream.p, buf, padSize + XzFlags_GetCheckSize(xz->flags)));
      RINOK(Xz_AddIndexRecord(xz, block.unpackSize, seqSizeOutStream.processed - padSize, &g_Alloc));
    }
  }
  return Xz_WriteFooter(xz, outStream);
}

SRes Xz_Encode(ISeqOutStream *outStream, ISeqInStream *inStream,
    const CXzProps *props, ICompressProgress *progress)
{
  SRes res;
  CXzStream xz;
  CLzma2WithFilters lzmaf;
  Xz_Construct(&xz);
  Lzma2WithFilters_Construct(&lzmaf, &g_Alloc, &g_BigAlloc);
  res = Lzma2WithFilters_Create(&lzmaf);
  if (res == SZ_OK)
    res = Xz_Compress(&xz, &lzmaf, outStream, inStream, props, progress);
  Lzma2WithFilters_Free(&lzmaf);
  Xz_Free(&xz, &g_Alloc);
  return res;
}

SRes Xz_EncodeEmpty(ISeqOutStream *outStream)
{
  SRes res;
  CXzStream xz;
  Xz_Construct(&xz);
  res = Xz_WriteHeader(xz.flags, outStream);
  if (res == SZ_OK)
    res = Xz_WriteFooter(&xz, outStream);
  Xz_Free(&xz, &g_Alloc);
  return res;
}
