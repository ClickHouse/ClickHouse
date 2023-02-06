/* XzCrc64.c -- CRC64 calculation
2010-04-16 : Igor Pavlov : Public domain */

#include "XzCrc64.h"

#define kCrc64Poly UINT64_CONST(0xC96C5795D7870F42)
UInt64 g_Crc64Table[256];

void MY_FAST_CALL Crc64GenerateTable(void)
{
  UInt32 i;
  for (i = 0; i < 256; i++)
  {
    UInt64 r = i;
    int j;
    for (j = 0; j < 8; j++)
      r = (r >> 1) ^ ((UInt64)kCrc64Poly & ~((r & 1) - 1));
    g_Crc64Table[i] = r;
  }
}

UInt64 MY_FAST_CALL Crc64Update(UInt64 v, const void *data, size_t size)
{
  const Byte *p = (const Byte *)data;
  for (; size > 0 ; size--, p++)
    v = CRC64_UPDATE_BYTE(v, *p);
  return v;
}

UInt64 MY_FAST_CALL Crc64Calc(const void *data, size_t size)
{
  return CRC64_GET_DIGEST(Crc64Update(CRC64_INIT_VAL, data, size));
}
