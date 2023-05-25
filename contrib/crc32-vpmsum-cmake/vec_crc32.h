#ifndef VEC_CRC32
#define VEC_CRC32

#if ! ((defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#    error PowerPC architecture is expected
#endif

#ifdef __cplusplus
extern "C" {
#endif

unsigned int crc32_vpmsum(unsigned int crc, const unsigned char *p, unsigned long len);

static inline uint32_t crc32_ppc(uint64_t crc, unsigned char const *buffer, size_t len)
{
    assert(buffer);
    crc = crc32_vpmsum(crc, buffer, (unsigned long)len);

    return crc;
}

#ifdef __cplusplus
}
#endif

#endif
