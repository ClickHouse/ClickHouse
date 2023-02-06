/* XzEnc.h -- Xz Encode
2011-02-07 : Igor Pavlov : Public domain */

#ifndef __XZ_ENC_H
#define __XZ_ENC_H

#include "Lzma2Enc.h"

#include "Xz.h"

EXTERN_C_BEGIN

typedef struct
{
  UInt32 id;
  UInt32 delta;
  UInt32 ip;
  int ipDefined;
} CXzFilterProps;

void XzFilterProps_Init(CXzFilterProps *p);

typedef struct
{
  const CLzma2EncProps *lzma2Props;
  const CXzFilterProps *filterProps;
  unsigned checkId;
} CXzProps;

void XzProps_Init(CXzProps *p);

SRes Xz_Encode(ISeqOutStream *outStream, ISeqInStream *inStream,
    const CXzProps *props, ICompressProgress *progress);

SRes Xz_EncodeEmpty(ISeqOutStream *outStream);

EXTERN_C_END

#endif
