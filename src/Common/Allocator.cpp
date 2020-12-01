#include "Allocator.h"

/** Keep definition of this constant in cpp file; otherwise its value
  * is inlined into allocator code making it impossible to override it
  * in third-party code.
  *
  * Note: extern may seem redundant, but is actually needed due to bug in GCC.
  * See also: https://gcc.gnu.org/legacy-ml/gcc-help/2017-12/msg00021.html
  */
#ifdef NDEBUG
    __attribute__((__weak__)) extern const size_t MMAP_THRESHOLD = 64 * (1ULL << 20);
#else
    /**
      * In debug build, use small mmap threshold to reproduce more memory
      * stomping bugs. Along with ASLR it will hopefully detect more issues than
      * ASan. The program may fail due to the limit on number of memory mappings.
      */
    __attribute__((__weak__)) extern const size_t MMAP_THRESHOLD = 4096;
#endif
