/* XzIn.c - Xz input
2011-02-01 : Igor Pavlov : Public domain */

#include <string.h>

#include "7zCrc.h"
#include "CpuArch.h"
#include "Xz.h"

SRes Xz_ReadHeader(CXzStreamFlags *p, ISeqInStream *inStream)
{
  Byte sig[XZ_STREAM_HEADER_SIZE];
  RINOK(SeqInStream_Read2(inStream, sig, XZ_STREAM_HEADER_SIZE, SZ_ERROR_NO_ARCHIVE));
  if (memcmp(sig, XZ_SIG, XZ_SIG_SIZE) != 0)
    return SZ_ERROR_NO_ARCHIVE;
  return Xz_ParseHeader(p, sig);
}

#define READ_VARINT_AND_CHECK(buf, pos, size, res) \
  { unsigned s = Xz_ReadVarInt(buf + pos, size - pos, res); \
  if (s == 0) return SZ_ERROR_ARCHIVE; pos += s; }

SRes XzBlock_ReadHeader(CXzBlock *p, ISeqInStream *inStream, Bool *isIndex, UInt32 *headerSizeRes)
{
  Byte header[XZ_BLOCK_HEADER_SIZE_MAX];
  unsigned headerSize;
  *headerSizeRes = 0;
  RINOK(SeqInStream_ReadByte(inStream, &header[0]));
  headerSize = ((unsigned)header[0] << 2) + 4;
  if (headerSize == 0)
  {
    *headerSizeRes = 1;
    *isIndex = True;
    return SZ_OK;
  }

  *isIndex = False;
  *headerSizeRes = headerSize;
  RINOK(SeqInStream_Read(inStream, header + 1, headerSize - 1));
  return XzBlock_Parse(p, header);
}

#define ADD_SIZE_CHECH(size, val) \
  { UInt64 newSize = size + (val); if (newSize < size) return XZ_SIZE_OVERFLOW; size = newSize; }

UInt64 Xz_GetUnpackSize(const CXzStream *p)
{
  UInt64 size = 0;
  size_t i;
  for (i = 0; i < p->numBlocks; i++)
    ADD_SIZE_CHECH(size, p->blocks[i].unpackSize);
  return size;
}

UInt64 Xz_GetPackSize(const CXzStream *p)
{
  UInt64 size = 0;
  size_t i;
  for (i = 0; i < p->numBlocks; i++)
    ADD_SIZE_CHECH(size, (p->blocks[i].totalSize + 3) & ~(UInt64)3);
  return size;
}

/*
SRes XzBlock_ReadFooter(CXzBlock *p, CXzStreamFlags f, ISeqInStream *inStream)
{
  return SeqInStream_Read(inStream, p->check, XzFlags_GetCheckSize(f));
}
*/

static SRes Xz_ReadIndex2(CXzStream *p, const Byte *buf, size_t size, ISzAlloc *alloc)
{
  size_t i, numBlocks, crcStartPos, pos = 1;
  UInt32 crc;

  if (size < 5 || buf[0] != 0)
    return SZ_ERROR_ARCHIVE;

  size -= 4;
  crc = CrcCalc(buf, size);
  if (crc != GetUi32(buf + size))
    return SZ_ERROR_ARCHIVE;

  {
    UInt64 numBlocks64;
    READ_VARINT_AND_CHECK(buf, pos, size, &numBlocks64);
    numBlocks = (size_t)numBlocks64;
    if (numBlocks != numBlocks64 || numBlocks * 2 > size)
      return SZ_ERROR_ARCHIVE;
  }
  
  crcStartPos = pos;
  Xz_Free(p, alloc);
  if (numBlocks != 0)
  {
    p->numBlocks = numBlocks;
    p->numBlocksAllocated = numBlocks;
    p->blocks = alloc->Alloc(alloc, sizeof(CXzBlockSizes) * numBlocks);
    if (p->blocks == 0)
      return SZ_ERROR_MEM;
    for (i = 0; i < numBlocks; i++)
    {
      CXzBlockSizes *block = &p->blocks[i];
      READ_VARINT_AND_CHECK(buf, pos, size, &block->totalSize);
      READ_VARINT_AND_CHECK(buf, pos, size, &block->unpackSize);
      if (block->totalSize == 0)
        return SZ_ERROR_ARCHIVE;
    }
  }
  while ((pos & 3) != 0)
    if (buf[pos++] != 0)
      return SZ_ERROR_ARCHIVE;
  return (pos == size) ? SZ_OK : SZ_ERROR_ARCHIVE;
}

static SRes Xz_ReadIndex(CXzStream *p, ILookInStream *stream, UInt64 indexSize, ISzAlloc *alloc)
{
  SRes res;
  size_t size;
  Byte *buf;
  if (indexSize > ((UInt32)1 << 31))
    return SZ_ERROR_UNSUPPORTED;
  size = (size_t)indexSize;
  if (size != indexSize)
    return SZ_ERROR_UNSUPPORTED;
  buf = alloc->Alloc(alloc, size);
  if (buf == 0)
    return SZ_ERROR_MEM;
  res = LookInStream_Read2(stream, buf, size, SZ_ERROR_UNSUPPORTED);
  if (res == SZ_OK)
    res = Xz_ReadIndex2(p, buf, size, alloc);
  alloc->Free(alloc, buf);
  return res;
}

static SRes SeekFromCur(ILookInStream *inStream, Int64 *res)
{
  return inStream->Seek(inStream, res, SZ_SEEK_CUR);
}

static SRes Xz_ReadBackward(CXzStream *p, ILookInStream *stream, Int64 *startOffset, ISzAlloc *alloc)
{
  UInt64 indexSize;
  Byte buf[XZ_STREAM_FOOTER_SIZE];

  if ((*startOffset & 3) != 0 || *startOffset < XZ_STREAM_FOOTER_SIZE)
    return SZ_ERROR_NO_ARCHIVE;
  *startOffset = -XZ_STREAM_FOOTER_SIZE;
  RINOK(SeekFromCur(stream, startOffset));

  RINOK(LookInStream_Read2(stream, buf, XZ_STREAM_FOOTER_SIZE, SZ_ERROR_NO_ARCHIVE));
  
  if (memcmp(buf + 10, XZ_FOOTER_SIG, XZ_FOOTER_SIG_SIZE) != 0)
  {
    UInt32 total = 0;
    *startOffset += XZ_STREAM_FOOTER_SIZE;
    for (;;)
    {
      size_t i;
      #define TEMP_BUF_SIZE (1 << 10)
      Byte tempBuf[TEMP_BUF_SIZE];
      if (*startOffset < XZ_STREAM_FOOTER_SIZE || total > (1 << 16))
        return SZ_ERROR_NO_ARCHIVE;
      i = (*startOffset > TEMP_BUF_SIZE) ? TEMP_BUF_SIZE : (size_t)*startOffset;
      total += (UInt32)i;
      *startOffset = -(Int64)i;
      RINOK(SeekFromCur(stream, startOffset));
      RINOK(LookInStream_Read2(stream, tempBuf, i, SZ_ERROR_NO_ARCHIVE));
      for (; i != 0; i--)
        if (tempBuf[i - 1] != 0)
          break;
      if (i != 0)
      {
        if ((i & 3) != 0)
          return SZ_ERROR_NO_ARCHIVE;
        *startOffset += i;
        break;
      }
    }
    if (*startOffset < XZ_STREAM_FOOTER_SIZE)
      return SZ_ERROR_NO_ARCHIVE;
    *startOffset -= XZ_STREAM_FOOTER_SIZE;
    RINOK(stream->Seek(stream, startOffset, SZ_SEEK_SET));
    RINOK(LookInStream_Read2(stream, buf, XZ_STREAM_FOOTER_SIZE, SZ_ERROR_NO_ARCHIVE));
    if (memcmp(buf + 10, XZ_FOOTER_SIG, XZ_FOOTER_SIG_SIZE) != 0)
      return SZ_ERROR_NO_ARCHIVE;
  }
  
  p->flags = (CXzStreamFlags)GetBe16(buf + 8);

  if (!XzFlags_IsSupported(p->flags))
    return SZ_ERROR_UNSUPPORTED;

  if (GetUi32(buf) != CrcCalc(buf + 4, 6))
    return SZ_ERROR_ARCHIVE;

  indexSize = ((UInt64)GetUi32(buf + 4) + 1) << 2;

  *startOffset = -(Int64)(indexSize + XZ_STREAM_FOOTER_SIZE);
  RINOK(SeekFromCur(stream, startOffset));

  RINOK(Xz_ReadIndex(p, stream, indexSize, alloc));

  {
    UInt64 totalSize = Xz_GetPackSize(p);
    UInt64 sum = XZ_STREAM_HEADER_SIZE + totalSize + indexSize;
    if (totalSize == XZ_SIZE_OVERFLOW ||
      sum >= ((UInt64)1 << 63) ||
      totalSize >= ((UInt64)1 << 63))
      return SZ_ERROR_ARCHIVE;
    *startOffset = -(Int64)sum;
    RINOK(SeekFromCur(stream, startOffset));
  }
  {
    CXzStreamFlags headerFlags;
    CSecToRead secToRead;
    SecToRead_CreateVTable(&secToRead);
    secToRead.realStream = stream;

    RINOK(Xz_ReadHeader(&headerFlags, &secToRead.s));
    return (p->flags == headerFlags) ? SZ_OK : SZ_ERROR_ARCHIVE;
  }
}


/* ---------- Xz Streams ---------- */

void Xzs_Construct(CXzs *p)
{
  p->num = p->numAllocated = 0;
  p->streams = 0;
}

void Xzs_Free(CXzs *p, ISzAlloc *alloc)
{
  size_t i;
  for (i = 0; i < p->num; i++)
    Xz_Free(&p->streams[i], alloc);
  alloc->Free(alloc, p->streams);
  p->num = p->numAllocated = 0;
  p->streams = 0;
}

UInt64 Xzs_GetNumBlocks(const CXzs *p)
{
  UInt64 num = 0;
  size_t i;
  for (i = 0; i < p->num; i++)
    num += p->streams[i].numBlocks;
  return num;
}

UInt64 Xzs_GetUnpackSize(const CXzs *p)
{
  UInt64 size = 0;
  size_t i;
  for (i = 0; i < p->num; i++)
    ADD_SIZE_CHECH(size, Xz_GetUnpackSize(&p->streams[i]));
  return size;
}

/*
UInt64 Xzs_GetPackSize(const CXzs *p)
{
  UInt64 size = 0;
  size_t i;
  for (i = 0; i < p->num; i++)
    ADD_SIZE_CHECH(size, Xz_GetTotalSize(&p->streams[i]));
  return size;
}
*/

SRes Xzs_ReadBackward(CXzs *p, ILookInStream *stream, Int64 *startOffset, ICompressProgress *progress, ISzAlloc *alloc)
{
  Int64 endOffset = 0;
  RINOK(stream->Seek(stream, &endOffset, SZ_SEEK_END));
  *startOffset = endOffset;
  for (;;)
  {
    CXzStream st;
    SRes res;
    Xz_Construct(&st);
    res = Xz_ReadBackward(&st, stream, startOffset, alloc);
    st.startOffset = *startOffset;
    RINOK(res);
    if (p->num == p->numAllocated)
    {
      size_t newNum = p->num + p->num / 4 + 1;
      Byte *data = (Byte *)alloc->Alloc(alloc, newNum * sizeof(CXzStream));
      if (data == 0)
        return SZ_ERROR_MEM;
      p->numAllocated = newNum;
      memcpy(data, p->streams, p->num * sizeof(CXzStream));
      alloc->Free(alloc, p->streams);
      p->streams = (CXzStream *)data;
    }
    p->streams[p->num++] = st;
    if (*startOffset == 0)
      break;
    RINOK(stream->Seek(stream, startOffset, SZ_SEEK_SET));
    if (progress && progress->Progress(progress, endOffset - *startOffset, (UInt64)(Int64)-1) != SZ_OK)
      return SZ_ERROR_PROGRESS;
  }
  return SZ_OK;
}
