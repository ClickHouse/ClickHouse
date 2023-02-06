/* Lzma86Enc.c -- LZMA + x86 (BCJ) Filter Encoder
2009-08-14 : Igor Pavlov : Public domain */

#include <string.h>

#include "Lzma86.h"

#include "Alloc.h"
#include "Bra.h"
#include "LzmaEnc.h"

#define SZE_OUT_OVERFLOW SZE_DATA_ERROR

static void *SzAlloc(void *p, size_t size) { p = p; return MyAlloc(size); }
static void SzFree(void *p, void *address) { p = p; MyFree(address); }

int Lzma86_Encode(Byte *dest, size_t *destLen, const Byte *src, size_t srcLen,
    int level, UInt32 dictSize, int filterMode)
{
  ISzAlloc g_Alloc = { SzAlloc, SzFree };
  size_t outSize2 = *destLen;
  Byte *filteredStream;
  Bool useFilter;
  int mainResult = SZ_ERROR_OUTPUT_EOF;
  CLzmaEncProps props;
  LzmaEncProps_Init(&props);
  props.level = level;
  props.dictSize = dictSize;
  
  *destLen = 0;
  if (outSize2 < LZMA86_HEADER_SIZE)
    return SZ_ERROR_OUTPUT_EOF;

  {
    int i;
    UInt64 t = srcLen;
    for (i = 0; i < 8; i++, t >>= 8)
      dest[LZMA86_SIZE_OFFSET + i] = (Byte)t;
  }

  filteredStream = 0;
  useFilter = (filterMode != SZ_FILTER_NO);
  if (useFilter)
  {
    if (srcLen != 0)
    {
      filteredStream = (Byte *)MyAlloc(srcLen);
      if (filteredStream == 0)
        return SZ_ERROR_MEM;
      memcpy(filteredStream, src, srcLen);
    }
    {
      UInt32 x86State;
      x86_Convert_Init(x86State);
      x86_Convert(filteredStream, srcLen, 0, &x86State, 1);
    }
  }

  {
    size_t minSize = 0;
    Bool bestIsFiltered = False;

    /* passes for SZ_FILTER_AUTO:
        0 - BCJ + LZMA
        1 - LZMA
        2 - BCJ + LZMA agaian, if pass 0 (BCJ + LZMA) is better.
    */
    int numPasses = (filterMode == SZ_FILTER_AUTO) ? 3 : 1;

    int i;
    for (i = 0; i < numPasses; i++)
    {
      size_t outSizeProcessed = outSize2 - LZMA86_HEADER_SIZE;
      size_t outPropsSize = 5;
      SRes curRes;
      Bool curModeIsFiltered = (numPasses > 1 && i == numPasses - 1);
      if (curModeIsFiltered && !bestIsFiltered)
        break;
      if (useFilter && i == 0)
        curModeIsFiltered = True;
      
      curRes = LzmaEncode(dest + LZMA86_HEADER_SIZE, &outSizeProcessed,
          curModeIsFiltered ? filteredStream : src, srcLen,
          &props, dest + 1, &outPropsSize, 0,
          NULL, &g_Alloc, &g_Alloc);
      
      if (curRes != SZ_ERROR_OUTPUT_EOF)
      {
        if (curRes != SZ_OK)
        {
          mainResult = curRes;
          break;
        }
        if (outSizeProcessed <= minSize || mainResult != SZ_OK)
        {
          minSize = outSizeProcessed;
          bestIsFiltered = curModeIsFiltered;
          mainResult = SZ_OK;
        }
      }
    }
    dest[0] = (bestIsFiltered ? 1 : 0);
    *destLen = LZMA86_HEADER_SIZE + minSize;
  }
  if (useFilter)
    MyFree(filteredStream);
  return mainResult;
}
