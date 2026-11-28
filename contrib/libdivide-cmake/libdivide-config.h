#if defined(__SSE2__)
#    define LIBDIVIDE_SSE2
#elif defined(__AVX512F__) || defined(__AVX512BW__) || defined(__AVX512VL__)
#    define LIBDIVIDE_AVX512
#elif defined(__AVX2__)
#    define LIBDIVIDE_AVX2
#elif defined(__aarch64__) && defined(__ARM_NEON)
#    define LIBDIVIDE_NEON
#endif
