#ifndef VEC_CRC32
#define VEC_CRC32


#ifdef __cplusplus
extern "C" {
#endif

unsigned int crc32_vpmsum(unsigned int crc, const unsigned char *p, unsigned long len);

static inline uint32_t crc32_ppc(uint64_t crc, unsigned char const *buffer, size_t len)
{
	unsigned char *emptybuffer;
    if (!buffer) {
        emptybuffer = (unsigned char *)malloc(len);
        bzero(emptybuffer, len);
        crc = crc32_vpmsum(crc, emptybuffer, len);
        free(emptybuffer);
    } else {
        crc = crc32_vpmsum(crc, buffer, (unsigned long)len);
    }
	return crc;
}

#ifdef __cplusplus
}
#endif

#endif
