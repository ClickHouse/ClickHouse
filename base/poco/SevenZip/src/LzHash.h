/* LzHash.h -- HASH functions for LZ algorithms
2009-02-07 : Igor Pavlov : Public domain */

#ifndef __LZ_HASH_H
#define __LZ_HASH_H

#define kHash2Size (1 << 10)
#define kHash3Size (1 << 16)
#define kHash4Size (1 << 20)

#define kFix3HashSize (kHash2Size)
#define kFix4HashSize (kHash2Size + kHash3Size)
#define kFix5HashSize (kHash2Size + kHash3Size + kHash4Size)

#define HASH2_CALC hashValue = cur[0] | ((UInt32)cur[1] << 8);

#define HASH3_CALC { \
  UInt32 temp = p->crc[cur[0]] ^ cur[1]; \
  hash2Value = temp & (kHash2Size - 1); \
  hashValue = (temp ^ ((UInt32)cur[2] << 8)) & p->hashMask; }

#define HASH4_CALC { \
  UInt32 temp = p->crc[cur[0]] ^ cur[1]; \
  hash2Value = temp & (kHash2Size - 1); \
  hash3Value = (temp ^ ((UInt32)cur[2] << 8)) & (kHash3Size - 1); \
  hashValue = (temp ^ ((UInt32)cur[2] << 8) ^ (p->crc[cur[3]] << 5)) & p->hashMask; }

#define HASH5_CALC { \
  UInt32 temp = p->crc[cur[0]] ^ cur[1]; \
  hash2Value = temp & (kHash2Size - 1); \
  hash3Value = (temp ^ ((UInt32)cur[2] << 8)) & (kHash3Size - 1); \
  hash4Value = (temp ^ ((UInt32)cur[2] << 8) ^ (p->crc[cur[3]] << 5)); \
  hashValue = (hash4Value ^ (p->crc[cur[4]] << 3)) & p->hashMask; \
  hash4Value &= (kHash4Size - 1); }

/* #define HASH_ZIP_CALC hashValue = ((cur[0] | ((UInt32)cur[1] << 8)) ^ p->crc[cur[2]]) & 0xFFFF; */
#define HASH_ZIP_CALC hashValue = ((cur[2] | ((UInt32)cur[0] << 8)) ^ p->crc[cur[1]]) & 0xFFFF;


#define MT_HASH2_CALC \
  hash2Value = (p->crc[cur[0]] ^ cur[1]) & (kHash2Size - 1);

#define MT_HASH3_CALC { \
  UInt32 temp = p->crc[cur[0]] ^ cur[1]; \
  hash2Value = temp & (kHash2Size - 1); \
  hash3Value = (temp ^ ((UInt32)cur[2] << 8)) & (kHash3Size - 1); }

#define MT_HASH4_CALC { \
  UInt32 temp = p->crc[cur[0]] ^ cur[1]; \
  hash2Value = temp & (kHash2Size - 1); \
  hash3Value = (temp ^ ((UInt32)cur[2] << 8)) & (kHash3Size - 1); \
  hash4Value = (temp ^ ((UInt32)cur[2] << 8) ^ (p->crc[cur[3]] << 5)) & (kHash4Size - 1); }

#endif
