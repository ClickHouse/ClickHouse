#pragma once

#include <crc32-s390x.h>

inline uint32_t s390x_crc32c_u8(uint32_t crc, uint8_t v)
{
    return crc32c_le_vx(crc, reinterpret_cast<unsigned char *>(&v), sizeof(v));
}

inline uint32_t s390x_crc32c_u16(uint32_t crc, uint16_t v)
{
    v = __builtin_bswap16(v);
    return crc32c_le_vx(crc, reinterpret_cast<unsigned char *>(&v), sizeof(v));
}

inline uint32_t s390x_crc32c_u32(uint32_t crc, uint32_t v)
{
    v = __builtin_bswap32(v);
    return crc32c_le_vx(crc, reinterpret_cast<unsigned char *>(&v), sizeof(v));
}

inline uint64_t s390x_crc32c(uint64_t crc, uint64_t v)
{
    v = __builtin_bswap64(v);
    return crc32c_le_vx(static_cast<uint32_t>(crc), reinterpret_cast<unsigned char *>(&v), sizeof(uint64_t));
}
