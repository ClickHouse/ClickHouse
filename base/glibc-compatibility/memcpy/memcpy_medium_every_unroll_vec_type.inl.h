#define VEC_SIZE 16
#define VZEROUPPER 0
#include "memcpy_medium_every_unroll.inl.h"
#undef VEC_SIZE
#undef VZEROUPPER

#define VEC_SIZE 32
#define VZEROUPPER 1
#include "memcpy_medium_every_unroll.inl.h"
#undef VEC_SIZE
#undef VZEROUPPER

#define VEC_SIZE 64
#define VZEROUPPER 1
#include "memcpy_medium_every_unroll.inl.h"
#undef VEC_SIZE
#undef VZEROUPPER
