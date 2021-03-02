#ifndef MEMCPY_H_
#define MEMCPY_H_
#if !(__ASSEMBLER__ + __LINKER__ + 0)

/// Note: this header can be included only after libc++ headers.
/// Note: you cannot write std::memcpy if you include this header.

void *memcpy(void *, const void *, size_t);

#ifdef __GNUC__
#define __memcpy_isgoodsize(SIZE)                                    \
  (__builtin_constant_p(SIZE) && ((SIZE) <= __BIGGEST_ALIGNMENT__ && \
                                  __builtin_popcountl((unsigned)(SIZE)) == 1))
#define memcpy(DEST, SRC, SIZE)                                            \
  (__memcpy_isgoodsize(SIZE) ? __builtin_memcpy(DEST, SRC, SIZE) : ({      \
    void *DeSt = (DEST);                                                   \
    const void *SrC = (SRC);                                               \
    size_t SiZe = (SIZE);                                                  \
    asm("call\tMemCpy"                                                     \
        : "=m"(*(char(*)[SiZe])(DeSt))                                     \
        : "D"(DeSt), "S"(SrC), "d"(SiZe), "m"(*(const char(*)[SiZe])(SrC)) \
        : "xmm3", "xmm4", "rcx", "cc");                                    \
    DeSt;                                                                  \
  }))
#endif

#endif /* !(__ASSEMBLER__ + __LINKER__ + 0) */
#endif /* MEMCPY_H_ */
