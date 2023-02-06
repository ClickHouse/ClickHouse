/* Xz.h - Xz interface
2011-01-09 : Igor Pavlov : Public domain */

#ifndef __XZ_H
#define __XZ_H

#include "Sha256.h"

EXTERN_C_BEGIN

#define XZ_ID_Subblock 1
#define XZ_ID_Delta 3
#define XZ_ID_X86 4
#define XZ_ID_PPC 5
#define XZ_ID_IA64 6
#define XZ_ID_ARM 7
#define XZ_ID_ARMT 8
#define XZ_ID_SPARC 9
#define XZ_ID_LZMA2 0x21

unsigned Xz_ReadVarInt(const Byte *p, size_t maxSize, UInt64 *value);
unsigned Xz_WriteVarInt(Byte *buf, UInt64 v);

/* ---------- xz block ---------- */

#define XZ_BLOCK_HEADER_SIZE_MAX 1024

#define XZ_NUM_FILTERS_MAX 4
#define XZ_BF_NUM_FILTERS_MASK 3
#define XZ_BF_PACK_SIZE (1 << 6)
#define XZ_BF_UNPACK_SIZE (1 << 7)

#define XZ_FILTER_PROPS_SIZE_MAX 20

typedef struct
{
  UInt64 id;
  UInt32 propsSize;
  Byte props[XZ_FILTER_PROPS_SIZE_MAX];
} CXzFilter;

typedef struct
{
  UInt64 packSize;
  UInt64 unpackSize;
  Byte flags;
  CXzFilter filters[XZ_NUM_FILTERS_MAX];
} CXzBlock;

#define XzBlock_GetNumFilters(p) (((p)->flags & XZ_BF_NUM_FILTERS_MASK) + 1)
#define XzBlock_HasPackSize(p)   (((p)->flags & XZ_BF_PACK_SIZE) != 0)
#define XzBlock_HasUnpackSize(p) (((p)->flags & XZ_BF_UNPACK_SIZE) != 0)

SRes XzBlock_Parse(CXzBlock *p, const Byte *header);
SRes XzBlock_ReadHeader(CXzBlock *p, ISeqInStream *inStream, Bool *isIndex, UInt32 *headerSizeRes);

/* ---------- xz stream ---------- */

#define XZ_SIG_SIZE 6
#define XZ_FOOTER_SIG_SIZE 2

extern Byte XZ_SIG[XZ_SIG_SIZE];
extern Byte XZ_FOOTER_SIG[XZ_FOOTER_SIG_SIZE];

#define XZ_STREAM_FLAGS_SIZE 2
#define XZ_STREAM_CRC_SIZE 4

#define XZ_STREAM_HEADER_SIZE (XZ_SIG_SIZE + XZ_STREAM_FLAGS_SIZE + XZ_STREAM_CRC_SIZE)
#define XZ_STREAM_FOOTER_SIZE (XZ_FOOTER_SIG_SIZE + XZ_STREAM_FLAGS_SIZE + XZ_STREAM_CRC_SIZE + 4)

#define XZ_CHECK_MASK 0xF
#define XZ_CHECK_NO 0
#define XZ_CHECK_CRC32 1
#define XZ_CHECK_CRC64 4
#define XZ_CHECK_SHA256 10

typedef struct
{
  int mode;
  UInt32 crc;
  UInt64 crc64;
  CSha256 sha;
} CXzCheck;

void XzCheck_Init(CXzCheck *p, int mode);
void XzCheck_Update(CXzCheck *p, const void *data, size_t size);
int XzCheck_Final(CXzCheck *p, Byte *digest);

typedef UInt16 CXzStreamFlags;

#define XzFlags_IsSupported(f) ((f) <= XZ_CHECK_MASK)
#define XzFlags_GetCheckType(f) ((f) & XZ_CHECK_MASK)
#define XzFlags_HasDataCrc32(f) (Xz_GetCheckType(f) == XZ_CHECK_CRC32)
unsigned XzFlags_GetCheckSize(CXzStreamFlags f);

SRes Xz_ParseHeader(CXzStreamFlags *p, const Byte *buf);
SRes Xz_ReadHeader(CXzStreamFlags *p, ISeqInStream *inStream);

typedef struct
{
  UInt64 unpackSize;
  UInt64 totalSize;
} CXzBlockSizes;

typedef struct
{
  CXzStreamFlags flags;
  size_t numBlocks;
  size_t numBlocksAllocated;
  CXzBlockSizes *blocks;
  UInt64 startOffset;
} CXzStream;

void Xz_Construct(CXzStream *p);
void Xz_Free(CXzStream *p, ISzAlloc *alloc);

#define XZ_SIZE_OVERFLOW ((UInt64)(Int64)-1)

UInt64 Xz_GetUnpackSize(const CXzStream *p);
UInt64 Xz_GetPackSize(const CXzStream *p);

typedef struct
{
  size_t num;
  size_t numAllocated;
  CXzStream *streams;
} CXzs;

void Xzs_Construct(CXzs *p);
void Xzs_Free(CXzs *p, ISzAlloc *alloc);
SRes Xzs_ReadBackward(CXzs *p, ILookInStream *inStream, Int64 *startOffset, ICompressProgress *progress, ISzAlloc *alloc);

UInt64 Xzs_GetNumBlocks(const CXzs *p);
UInt64 Xzs_GetUnpackSize(const CXzs *p);

typedef enum
{
  CODER_STATUS_NOT_SPECIFIED,               /* use main error code instead */
  CODER_STATUS_FINISHED_WITH_MARK,          /* stream was finished with end mark. */
  CODER_STATUS_NOT_FINISHED,                /* stream was not finished */
  CODER_STATUS_NEEDS_MORE_INPUT             /* you must provide more input bytes */
} ECoderStatus;

typedef enum
{
  CODER_FINISH_ANY,   /* finish at any point */
  CODER_FINISH_END    /* block must be finished at the end */
} ECoderFinishMode;

typedef struct _IStateCoder
{
  void *p;
  void (*Free)(void *p, ISzAlloc *alloc);
  SRes (*SetProps)(void *p, const Byte *props, size_t propSize, ISzAlloc *alloc);
  void (*Init)(void *p);
  SRes (*Code)(void *p, Byte *dest, SizeT *destLen, const Byte *src, SizeT *srcLen,
      int srcWasFinished, ECoderFinishMode finishMode, int *wasFinished);
} IStateCoder;

#define MIXCODER_NUM_FILTERS_MAX 4

typedef struct
{
  ISzAlloc *alloc;
  Byte *buf;
  int numCoders;
  int finished[MIXCODER_NUM_FILTERS_MAX - 1];
  size_t pos[MIXCODER_NUM_FILTERS_MAX - 1];
  size_t size[MIXCODER_NUM_FILTERS_MAX - 1];
  UInt64 ids[MIXCODER_NUM_FILTERS_MAX];
  IStateCoder coders[MIXCODER_NUM_FILTERS_MAX];
} CMixCoder;

void MixCoder_Construct(CMixCoder *p, ISzAlloc *alloc);
void MixCoder_Free(CMixCoder *p);
void MixCoder_Init(CMixCoder *p);
SRes MixCoder_SetFromMethod(CMixCoder *p, int coderIndex, UInt64 methodId);
SRes MixCoder_Code(CMixCoder *p, Byte *dest, SizeT *destLen,
    const Byte *src, SizeT *srcLen, int srcWasFinished,
    ECoderFinishMode finishMode, ECoderStatus *status);

typedef enum
{
  XZ_STATE_STREAM_HEADER,
  XZ_STATE_STREAM_INDEX,
  XZ_STATE_STREAM_INDEX_CRC,
  XZ_STATE_STREAM_FOOTER,
  XZ_STATE_STREAM_PADDING,
  XZ_STATE_BLOCK_HEADER,
  XZ_STATE_BLOCK,
  XZ_STATE_BLOCK_FOOTER
} EXzState;

typedef struct
{
  EXzState state;
  UInt32 pos;
  unsigned alignPos;
  unsigned indexPreSize;

  CXzStreamFlags streamFlags;
  
  UInt32 blockHeaderSize;
  UInt64 packSize;
  UInt64 unpackSize;

  UInt64 numBlocks;
  UInt64 indexSize;
  UInt64 indexPos;
  UInt64 padSize;

  UInt64 numStreams;

  UInt32 crc;
  CMixCoder decoder;
  CXzBlock block;
  CXzCheck check;
  CSha256 sha;
  Byte shaDigest[SHA256_DIGEST_SIZE];
  Byte buf[XZ_BLOCK_HEADER_SIZE_MAX];
} CXzUnpacker;

void XzUnpacker_Construct(CXzUnpacker *p, ISzAlloc *alloc);
void XzUnpacker_Init(CXzUnpacker *p);
void XzUnpacker_Free(CXzUnpacker *p);

/*
finishMode:
  It has meaning only if the decoding reaches output limit (*destLen).
  LZMA_FINISH_ANY - use smallest number of input bytes
  LZMA_FINISH_END - read EndOfStream marker after decoding

Returns:
  SZ_OK
    status:
      CODER_STATUS_NOT_FINISHED,
      CODER_STATUS_NEEDS_MORE_INPUT - maybe there are more xz streams,
                                      call XzUnpacker_IsStreamWasFinished to check that current stream was finished
  SZ_ERROR_DATA - Data error
  SZ_ERROR_MEM  - Memory allocation error
  SZ_ERROR_UNSUPPORTED - Unsupported properties
  SZ_ERROR_INPUT_EOF - It needs more bytes in input buffer (src).
*/


SRes XzUnpacker_Code(CXzUnpacker *p, Byte *dest, SizeT *destLen,
    const Byte *src, SizeT *srcLen, /* int srcWasFinished, */ int finishMode,
    ECoderStatus *status);

Bool XzUnpacker_IsStreamWasFinished(CXzUnpacker *p);

EXTERN_C_END

#endif
