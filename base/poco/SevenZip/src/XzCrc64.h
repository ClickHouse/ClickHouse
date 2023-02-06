/* XzCrc64.h -- CRC64 calculation
2010-04-16 : Igor Pavlov : Public domain */

#ifndef __XZ_CRC64_H
#define __XZ_CRC64_H

#include <stddef.h>

#include "Types.h"

EXTERN_C_BEGIN

extern UInt64 g_Crc64Table[];

void MY_FAST_CALL Crc64GenerateTable(void);

#define CRC64_INIT_VAL UINT64_CONST(0xFFFFFFFFFFFFFFFF)
#define CRC64_GET_DIGEST(crc) ((crc) ^ CRC64_INIT_VAL)
#define CRC64_UPDATE_BYTE(crc, b) (g_Crc64Table[((crc) ^ (b)) & 0xFF] ^ ((crc) >> 8))

UInt64 MY_FAST_CALL Crc64Update(UInt64 crc, const void *data, size_t size);
UInt64 MY_FAST_CALL Crc64Calc(const void *data, size_t size);

EXTERN_C_END

#endif
