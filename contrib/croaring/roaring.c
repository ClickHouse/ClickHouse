/* auto-generated on Tue Dec 18 09:42:59 CST 2018. Do not edit! */
#include "roaring.h"

/* used for http://dmalloc.com/ Dmalloc - Debug Malloc Library */
#ifdef DMALLOC
#include "dmalloc.h"
#endif

/* begin file /opt/bitmap/CRoaring-0.2.57/src/array_util.c */
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern inline int32_t binarySearch(const uint16_t *array, int32_t lenarray,
                                   uint16_t ikey);

#ifdef USESSE4
// used by intersect_vector16
ALIGNED(0x1000)
static const uint8_t shuffle_mask16[] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    6,    7,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,    0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    6,    7,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    6,    7,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    6,    7,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    6,    7,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    8,    9,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    8,    9,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    8,    9,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    8,    9,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    6,    7,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,    8,    9,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,
    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    6,    7,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,    8,    9,    0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    6,    7,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    6,    7,    8,    9,    0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    6,    7,
    8,    9,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 10,   11,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    10,   11,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    6,    7,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    6,    7,    10,   11,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    6,    7,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    6,    7,    10,   11,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    6,    7,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    8,    9,    10,   11,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    8,    9,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    8,    9,    10,   11,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    8,    9,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    8,    9,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    6,    7,    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,    8,    9,    10,   11,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    6,    7,    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    6,    7,    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    6,    7,    8,    9,
    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    6,    7,    8,    9,    10,   11,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    8,    9,    10,   11,
    0xFF, 0xFF, 0xFF, 0xFF, 12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    6,    7,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    6,    7,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    6,    7,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    6,    7,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    6,    7,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 8,    9,    12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    8,    9,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    8,    9,    12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    8,    9,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    8,    9,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,
    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    6,    7,    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    6,    7,    8,    9,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,
    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    6,    7,    8,    9,    12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    6,    7,    8,    9,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    6,    7,    8,    9,    12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    10,   11,   12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    10,   11,   12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    6,    7,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,    10,   11,   12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    6,    7,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    6,    7,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    6,    7,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    6,    7,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    10,   11,   12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 8,    9,    10,   11,   12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    8,    9,
    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    8,    9,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    8,    9,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    8,    9,
    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    8,    9,    10,   11,   12,   13,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    8,    9,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    8,    9,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    6,    7,    8,    9,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,    8,    9,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,
    8,    9,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    6,    7,    8,    9,    10,   11,   12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,    8,    9,    10,   11,
    12,   13,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    6,    7,    8,    9,    10,   11,   12,   13,   0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    6,    7,
    8,    9,    10,   11,   12,   13,   0xFF, 0xFF, 14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    6,    7,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    6,    7,    14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    6,    7,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    6,    7,    14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    6,    7,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    8,    9,    14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    8,    9,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    8,    9,    14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    8,    9,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    8,    9,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    6,    7,    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,    8,    9,    14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    6,    7,    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    6,    7,    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    6,    7,    8,    9,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    6,    7,    8,    9,    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    8,    9,    14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    10,   11,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    10,   11,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    6,    7,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,
    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    6,    7,    10,   11,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    6,    7,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    6,    7,    10,   11,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    6,    7,
    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 8,    9,    10,   11,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    8,    9,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    8,    9,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    8,    9,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    8,    9,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    8,    9,    10,   11,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    8,    9,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    8,    9,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    8,    9,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,
    8,    9,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    6,    7,    8,    9,    10,   11,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    6,    7,    8,    9,
    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,
    8,    9,    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    6,    7,    8,    9,    10,   11,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    6,    7,    8,    9,
    10,   11,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    6,    7,    8,    9,    10,   11,   14,   15,   0xFF, 0xFF,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    12,   13,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    12,   13,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    6,    7,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,    12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    6,    7,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    6,    7,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    6,    7,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    6,    7,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 8,    9,    12,   13,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    8,    9,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    8,    9,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    8,    9,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    8,    9,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    8,    9,    12,   13,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    8,    9,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    8,    9,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    6,    7,    8,    9,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,    8,    9,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,
    8,    9,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    6,    7,    8,    9,    12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,    8,    9,    12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    6,    7,    8,    9,    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    6,    7,    8,    9,    12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    6,    7,
    8,    9,    12,   13,   14,   15,   0xFF, 0xFF, 10,   11,   12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    4,    5,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    10,   11,   12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 6,    7,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    6,    7,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    6,    7,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    6,    7,    10,   11,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    6,    7,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    4,    5,    6,    7,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    4,    5,    6,    7,    10,   11,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    4,    5,    6,    7,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF,
    8,    9,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    8,    9,    10,   11,   12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    8,    9,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    2,    3,    8,    9,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 4,    5,    8,    9,    10,   11,   12,   13,
    14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,
    8,    9,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF,
    2,    3,    4,    5,    8,    9,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,    4,    5,    8,    9,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 6,    7,    8,    9,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0,    1,    6,    7,    8,    9,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 2,    3,    6,    7,    8,    9,    10,   11,
    12,   13,   14,   15,   0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    2,    3,
    6,    7,    8,    9,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF,
    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14,   15,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    1,    4,    5,    6,    7,    8,    9,
    10,   11,   12,   13,   14,   15,   0xFF, 0xFF, 2,    3,    4,    5,
    6,    7,    8,    9,    10,   11,   12,   13,   14,   15,   0xFF, 0xFF,
    0,    1,    2,    3,    4,    5,    6,    7,    8,    9,    10,   11,
    12,   13,   14,   15};

/**
 * From Schlegel et al., Fast Sorted-Set Intersection using SIMD Instructions
 * Optimized by D. Lemire on May 3rd 2013
 */
int32_t intersect_vector16(const uint16_t *__restrict__ A, size_t s_a,
                           const uint16_t *__restrict__ B, size_t s_b,
                           uint16_t *C) {
    size_t count = 0;
    size_t i_a = 0, i_b = 0;
    const int vectorlength = sizeof(__m128i) / sizeof(uint16_t);
    const size_t st_a = (s_a / vectorlength) * vectorlength;
    const size_t st_b = (s_b / vectorlength) * vectorlength;
    __m128i v_a, v_b;
    if ((i_a < st_a) && (i_b < st_b)) {
        v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
        v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
        while ((A[i_a] == 0) || (B[i_b] == 0)) {
            const __m128i res_v = _mm_cmpestrm(
                v_b, vectorlength, v_a, vectorlength,
                _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_BIT_MASK);
            const int r = _mm_extract_epi32(res_v, 0);
            __m128i sm16 = _mm_load_si128((const __m128i *)shuffle_mask16 + r);
            __m128i p = _mm_shuffle_epi8(v_a, sm16);
            _mm_storeu_si128((__m128i *)&C[count], p);  // can overflow
            count += _mm_popcnt_u32(r);
            const uint16_t a_max = A[i_a + vectorlength - 1];
            const uint16_t b_max = B[i_b + vectorlength - 1];
            if (a_max <= b_max) {
                i_a += vectorlength;
                if (i_a == st_a) break;
                v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
            }
            if (b_max <= a_max) {
                i_b += vectorlength;
                if (i_b == st_b) break;
                v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
            }
        }
        if ((i_a < st_a) && (i_b < st_b))
            while (true) {
                const __m128i res_v = _mm_cmpistrm(
                    v_b, v_a,
                    _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_BIT_MASK);
                const int r = _mm_extract_epi32(res_v, 0);
                __m128i sm16 =
                    _mm_load_si128((const __m128i *)shuffle_mask16 + r);
                __m128i p = _mm_shuffle_epi8(v_a, sm16);
                _mm_storeu_si128((__m128i *)&C[count], p);  // can overflow
                count += _mm_popcnt_u32(r);
                const uint16_t a_max = A[i_a + vectorlength - 1];
                const uint16_t b_max = B[i_b + vectorlength - 1];
                if (a_max <= b_max) {
                    i_a += vectorlength;
                    if (i_a == st_a) break;
                    v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
                }
                if (b_max <= a_max) {
                    i_b += vectorlength;
                    if (i_b == st_b) break;
                    v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
                }
            }
    }
    // intersect the tail using scalar intersection
    while (i_a < s_a && i_b < s_b) {
        uint16_t a = A[i_a];
        uint16_t b = B[i_b];
        if (a < b) {
            i_a++;
        } else if (b < a) {
            i_b++;
        } else {
            C[count] = a;  //==b;
            count++;
            i_a++;
            i_b++;
        }
    }
    return (int32_t)count;
}

int32_t intersect_vector16_cardinality(const uint16_t *__restrict__ A,
                                       size_t s_a,
                                       const uint16_t *__restrict__ B,
                                       size_t s_b) {
    size_t count = 0;
    size_t i_a = 0, i_b = 0;
    const int vectorlength = sizeof(__m128i) / sizeof(uint16_t);
    const size_t st_a = (s_a / vectorlength) * vectorlength;
    const size_t st_b = (s_b / vectorlength) * vectorlength;
    __m128i v_a, v_b;
    if ((i_a < st_a) && (i_b < st_b)) {
        v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
        v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
        while ((A[i_a] == 0) || (B[i_b] == 0)) {
            const __m128i res_v = _mm_cmpestrm(
                v_b, vectorlength, v_a, vectorlength,
                _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_BIT_MASK);
            const int r = _mm_extract_epi32(res_v, 0);
            count += _mm_popcnt_u32(r);
            const uint16_t a_max = A[i_a + vectorlength - 1];
            const uint16_t b_max = B[i_b + vectorlength - 1];
            if (a_max <= b_max) {
                i_a += vectorlength;
                if (i_a == st_a) break;
                v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
            }
            if (b_max <= a_max) {
                i_b += vectorlength;
                if (i_b == st_b) break;
                v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
            }
        }
        if ((i_a < st_a) && (i_b < st_b))
            while (true) {
                const __m128i res_v = _mm_cmpistrm(
                    v_b, v_a,
                    _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_BIT_MASK);
                const int r = _mm_extract_epi32(res_v, 0);
                count += _mm_popcnt_u32(r);
                const uint16_t a_max = A[i_a + vectorlength - 1];
                const uint16_t b_max = B[i_b + vectorlength - 1];
                if (a_max <= b_max) {
                    i_a += vectorlength;
                    if (i_a == st_a) break;
                    v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
                }
                if (b_max <= a_max) {
                    i_b += vectorlength;
                    if (i_b == st_b) break;
                    v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
                }
            }
    }
    // intersect the tail using scalar intersection
    while (i_a < s_a && i_b < s_b) {
        uint16_t a = A[i_a];
        uint16_t b = B[i_b];
        if (a < b) {
            i_a++;
        } else if (b < a) {
            i_b++;
        } else {
            count++;
            i_a++;
            i_b++;
        }
    }
    return (int32_t)count;
}

int32_t difference_vector16(const uint16_t *__restrict__ A, size_t s_a,
                            const uint16_t *__restrict__ B, size_t s_b,
                            uint16_t *C) {
    // we handle the degenerate case
    if (s_a == 0) return 0;
    if (s_b == 0) {
        if (A != C) memcpy(C, A, sizeof(uint16_t) * s_a);
        return (int32_t)s_a;
    }
    // handle the leading zeroes, it is messy but it allows us to use the fast
    // _mm_cmpistrm instrinsic safely
    int32_t count = 0;
    if ((A[0] == 0) || (B[0] == 0)) {
        if ((A[0] == 0) && (B[0] == 0)) {
            A++;
            s_a--;
            B++;
            s_b--;
        } else if (A[0] == 0) {
            C[count++] = 0;
            A++;
            s_a--;
        } else {
            B++;
            s_b--;
        }
    }
    // at this point, we have two non-empty arrays, made of non-zero
    // increasing values.
    size_t i_a = 0, i_b = 0;
    const size_t vectorlength = sizeof(__m128i) / sizeof(uint16_t);
    const size_t st_a = (s_a / vectorlength) * vectorlength;
    const size_t st_b = (s_b / vectorlength) * vectorlength;
    if ((i_a < st_a) && (i_b < st_b)) {  // this is the vectorized code path
        __m128i v_a, v_b;                //, v_bmax;
        // we load a vector from A and a vector from B
        v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
        v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
        // we have a runningmask which indicates which values from A have been
        // spotted in B, these don't get written out.
        __m128i runningmask_a_found_in_b = _mm_setzero_si128();
        /****
        * start of the main vectorized loop
        *****/
        while (true) {
            // afoundinb will contain a mask indicate for each entry in A
            // whether it is seen
            // in B
            const __m128i a_found_in_b =
                _mm_cmpistrm(v_b, v_a, _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY |
                                           _SIDD_BIT_MASK);
            runningmask_a_found_in_b =
                _mm_or_si128(runningmask_a_found_in_b, a_found_in_b);
            // we always compare the last values of A and B
            const uint16_t a_max = A[i_a + vectorlength - 1];
            const uint16_t b_max = B[i_b + vectorlength - 1];
            if (a_max <= b_max) {
                // Ok. In this code path, we are ready to write our v_a
                // because there is no need to read more from B, they will
                // all be large values.
                const int bitmask_belongs_to_difference =
                    _mm_extract_epi32(runningmask_a_found_in_b, 0) ^ 0xFF;
                /*** next few lines are probably expensive *****/
                __m128i sm16 = _mm_load_si128((const __m128i *)shuffle_mask16 +
                                              bitmask_belongs_to_difference);
                __m128i p = _mm_shuffle_epi8(v_a, sm16);
                _mm_storeu_si128((__m128i *)&C[count], p);  // can overflow
                count += _mm_popcnt_u32(bitmask_belongs_to_difference);
                // we advance a
                i_a += vectorlength;
                if (i_a == st_a)  // no more
                    break;
                runningmask_a_found_in_b = _mm_setzero_si128();
                v_a = _mm_lddqu_si128((__m128i *)&A[i_a]);
            }
            if (b_max <= a_max) {
                // in this code path, the current v_b has become useless
                i_b += vectorlength;
                if (i_b == st_b) break;
                v_b = _mm_lddqu_si128((__m128i *)&B[i_b]);
            }
        }
        // at this point, either we have i_a == st_a, which is the end of the
        // vectorized processing,
        // or we have i_b == st_b,  and we are not done processing the vector...
        // so we need to finish it off.
        if (i_a < st_a) {        // we have unfinished business...
            uint16_t buffer[8];  // buffer to do a masked load
            memset(buffer, 0, 8 * sizeof(uint16_t));
            memcpy(buffer, B + i_b, (s_b - i_b) * sizeof(uint16_t));
            v_b = _mm_lddqu_si128((__m128i *)buffer);
            const __m128i a_found_in_b =
                _mm_cmpistrm(v_b, v_a, _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY |
                                           _SIDD_BIT_MASK);
            runningmask_a_found_in_b =
                _mm_or_si128(runningmask_a_found_in_b, a_found_in_b);
            const int bitmask_belongs_to_difference =
                _mm_extract_epi32(runningmask_a_found_in_b, 0) ^ 0xFF;
            __m128i sm16 = _mm_load_si128((const __m128i *)shuffle_mask16 +
                                          bitmask_belongs_to_difference);
            __m128i p = _mm_shuffle_epi8(v_a, sm16);
            _mm_storeu_si128((__m128i *)&C[count], p);  // can overflow
            count += _mm_popcnt_u32(bitmask_belongs_to_difference);
            i_a += vectorlength;
        }
        // at this point we should have i_a == st_a and i_b == st_b
    }
    // do the tail using scalar code
    while (i_a < s_a && i_b < s_b) {
        uint16_t a = A[i_a];
        uint16_t b = B[i_b];
        if (b < a) {
            i_b++;
        } else if (a < b) {
            C[count] = a;
            count++;
            i_a++;
        } else {  //==
            i_a++;
            i_b++;
        }
    }
    if (i_a < s_a) {
        memmove(C + count, A + i_a, sizeof(uint16_t) * (s_a - i_a));
        count += (int32_t)(s_a - i_a);
    }
    return count;
}

#endif  // USESSE4



#ifdef USE_OLD_SKEW_INTERSECT
// TODO: given enough experience with the new skew intersect, drop the old one from the code base.


/* Computes the intersection between one small and one large set of uint16_t.
 * Stores the result into buffer and return the number of elements. */
int32_t intersect_skewed_uint16(const uint16_t *small, size_t size_s,
                                const uint16_t *large, size_t size_l,
                                uint16_t *buffer) {
    size_t pos = 0, idx_l = 0, idx_s = 0;

    if (0 == size_s) {
        return 0;
    }

    uint16_t val_l = large[idx_l], val_s = small[idx_s];

    while (true) {
        if (val_l < val_s) {
            idx_l = advanceUntil(large, (int32_t)idx_l, (int32_t)size_l, val_s);
            if (idx_l == size_l) break;
            val_l = large[idx_l];
        } else if (val_s < val_l) {
            idx_s++;
            if (idx_s == size_s) break;
            val_s = small[idx_s];
        } else {
            buffer[pos++] = val_s;
            idx_s++;
            if (idx_s == size_s) break;
            val_s = small[idx_s];
            idx_l = advanceUntil(large, (int32_t)idx_l, (int32_t)size_l, val_s);
            if (idx_l == size_l) break;
            val_l = large[idx_l];
        }
    }

    return (int32_t)pos;
}
#else // USE_OLD_SKEW_INTERSECT


/**
* Branchless binary search going after 4 values at once.
* Assumes that array is sorted.
* You have that array[*index1] >= target1, array[*index12] >= target2, ...
* except when *index1 = n, in which case you know that all values in array are
* smaller than target1, and so forth.
* It has logarithmic complexity.
*/
static void binarySearch4(const uint16_t *array, int32_t n, uint16_t target1,
                   uint16_t target2, uint16_t target3, uint16_t target4,
                   int32_t *index1, int32_t *index2, int32_t *index3,
                   int32_t *index4) {
  const uint16_t *base1 = array;
  const uint16_t *base2 = array;
  const uint16_t *base3 = array;
  const uint16_t *base4 = array;
  if (n == 0)
    return;
  while (n > 1) {
    int32_t half = n >> 1;
    base1 = (base1[half] < target1) ? &base1[half] : base1;
    base2 = (base2[half] < target2) ? &base2[half] : base2;
    base3 = (base3[half] < target3) ? &base3[half] : base3;
    base4 = (base4[half] < target4) ? &base4[half] : base4;
    n -= half;
  }
  *index1 = (int32_t)((*base1 < target1) + base1 - array);
  *index2 = (int32_t)((*base2 < target2) + base2 - array);
  *index3 = (int32_t)((*base3 < target3) + base3 - array);
  *index4 = (int32_t)((*base4 < target4) + base4 - array);
}

/**
* Branchless binary search going after 2 values at once.
* Assumes that array is sorted.
* You have that array[*index1] >= target1, array[*index12] >= target2.
* except when *index1 = n, in which case you know that all values in array are
* smaller than target1, and so forth.
* It has logarithmic complexity.
*/
static void binarySearch2(const uint16_t *array, int32_t n, uint16_t target1,
                   uint16_t target2, int32_t *index1, int32_t *index2) {
  const uint16_t *base1 = array;
  const uint16_t *base2 = array;
  if (n == 0)
    return;
  while (n > 1) {
    int32_t half = n >> 1;
    base1 = (base1[half] < target1) ? &base1[half] : base1;
    base2 = (base2[half] < target2) ? &base2[half] : base2;
    n -= half;
  }
  *index1 = (int32_t)((*base1 < target1) + base1 - array);
  *index2 = (int32_t)((*base2 < target2) + base2 - array);
}

/* Computes the intersection between one small and one large set of uint16_t.
 * Stores the result into buffer and return the number of elements.
 * Processes the small set in blocks of 4 values calling binarySearch4
 * and binarySearch2. This approach can be slightly superior to a conventional
 * galloping search in some instances.
 */
int32_t intersect_skewed_uint16(const uint16_t *small, size_t size_s,
                                         const uint16_t *large, size_t size_l,
                                         uint16_t *buffer) {
  size_t pos = 0, idx_l = 0, idx_s = 0;

  if (0 == size_s) {
    return 0;
  }
  int32_t index1 = 0, index2 = 0, index3 = 0, index4 = 0;
  while ((idx_s + 4 <= size_s) && (idx_l < size_l)) {
    uint16_t target1 = small[idx_s];
    uint16_t target2 = small[idx_s + 1];
    uint16_t target3 = small[idx_s + 2];
    uint16_t target4 = small[idx_s + 3];
    binarySearch4(large + idx_l, (int32_t)(size_l - idx_l), target1, target2, target3,
                  target4, &index1, &index2, &index3, &index4);
    if ((index1 + idx_l < size_l) && (large[idx_l + index1] == target1)) {
      buffer[pos++] = target1;
    }
    if ((index2 + idx_l < size_l) && (large[idx_l + index2] == target2)) {
      buffer[pos++] = target2;
    }
    if ((index3 + idx_l < size_l) && (large[idx_l + index3] == target3)) {
      buffer[pos++] = target3;
    }
    if ((index4 + idx_l < size_l) && (large[idx_l + index4] == target4)) {
      buffer[pos++] = target4;
    }
    idx_s += 4;
    idx_l += index1;
  }
  if ((idx_s + 2 <= size_s) && (idx_l < size_l)) {
    uint16_t target1 = small[idx_s];
    uint16_t target2 = small[idx_s + 1];
    binarySearch2(large + idx_l, (int32_t)(size_l - idx_l), target1, target2, &index1,
                  &index2);
    if ((index1 + idx_l < size_l) && (large[idx_l + index1] == target1)) {
      buffer[pos++] = target1;
    }
    if ((index2 + idx_l < size_l) && (large[idx_l + index2] == target2)) {
      buffer[pos++] = target2;
    }
    idx_s += 2;
    idx_l += index1;
  }
  if ((idx_s < size_s) && (idx_l < size_l)) {
    uint16_t val_s = small[idx_s];
    int32_t index = binarySearch(large + idx_l, (int32_t)(size_l - idx_l), val_s);
    if (index >= 0)
      buffer[pos++] = val_s;
  }
  return (int32_t)pos;
}


#endif //USE_OLD_SKEW_INTERSECT


// TODO: this could be accelerated, possibly, by using binarySearch4 as above.
int32_t intersect_skewed_uint16_cardinality(const uint16_t *small,
                                            size_t size_s,
                                            const uint16_t *large,
                                            size_t size_l) {
    size_t pos = 0, idx_l = 0, idx_s = 0;

    if (0 == size_s) {
        return 0;
    }

    uint16_t val_l = large[idx_l], val_s = small[idx_s];

    while (true) {
        if (val_l < val_s) {
            idx_l = advanceUntil(large, (int32_t)idx_l, (int32_t)size_l, val_s);
            if (idx_l == size_l) break;
            val_l = large[idx_l];
        } else if (val_s < val_l) {
            idx_s++;
            if (idx_s == size_s) break;
            val_s = small[idx_s];
        } else {
            pos++;
            idx_s++;
            if (idx_s == size_s) break;
            val_s = small[idx_s];
            idx_l = advanceUntil(large, (int32_t)idx_l, (int32_t)size_l, val_s);
            if (idx_l == size_l) break;
            val_l = large[idx_l];
        }
    }

    return (int32_t)pos;
}

bool intersect_skewed_uint16_nonempty(const uint16_t *small, size_t size_s,
                                const uint16_t *large, size_t size_l) {
    size_t idx_l = 0, idx_s = 0;

    if (0 == size_s) {
        return false;
    }

    uint16_t val_l = large[idx_l], val_s = small[idx_s];

    while (true) {
        if (val_l < val_s) {
            idx_l = advanceUntil(large, (int32_t)idx_l, (int32_t)size_l, val_s);
            if (idx_l == size_l) break;
            val_l = large[idx_l];
        } else if (val_s < val_l) {
            idx_s++;
            if (idx_s == size_s) break;
            val_s = small[idx_s];
        } else {
            return true;
        }
    }

    return false;
}

/**
 * Generic intersection function.
 */
int32_t intersect_uint16(const uint16_t *A, const size_t lenA,
                         const uint16_t *B, const size_t lenB, uint16_t *out) {
    const uint16_t *initout = out;
    if (lenA == 0 || lenB == 0) return 0;
    const uint16_t *endA = A + lenA;
    const uint16_t *endB = B + lenB;

    while (1) {
        while (*A < *B) {
        SKIP_FIRST_COMPARE:
            if (++A == endA) return (int32_t)(out - initout);
        }
        while (*A > *B) {
            if (++B == endB) return (int32_t)(out - initout);
        }
        if (*A == *B) {
            *out++ = *A;
            if (++A == endA || ++B == endB) return (int32_t)(out - initout);
        } else {
            goto SKIP_FIRST_COMPARE;
        }
    }
    return (int32_t)(out - initout);  // NOTREACHED
}

int32_t intersect_uint16_cardinality(const uint16_t *A, const size_t lenA,
                                     const uint16_t *B, const size_t lenB) {
    int32_t answer = 0;
    if (lenA == 0 || lenB == 0) return 0;
    const uint16_t *endA = A + lenA;
    const uint16_t *endB = B + lenB;

    while (1) {
        while (*A < *B) {
        SKIP_FIRST_COMPARE:
            if (++A == endA) return answer;
        }
        while (*A > *B) {
            if (++B == endB) return answer;
        }
        if (*A == *B) {
            ++answer;
            if (++A == endA || ++B == endB) return answer;
        } else {
            goto SKIP_FIRST_COMPARE;
        }
    }
    return answer;  // NOTREACHED
}


bool intersect_uint16_nonempty(const uint16_t *A, const size_t lenA,
                         const uint16_t *B, const size_t lenB) {
    if (lenA == 0 || lenB == 0) return 0;
    const uint16_t *endA = A + lenA;
    const uint16_t *endB = B + lenB;

    while (1) {
        while (*A < *B) {
        SKIP_FIRST_COMPARE:
            if (++A == endA) return false;
        }
        while (*A > *B) {
            if (++B == endB) return false;
        }
        if (*A == *B) {
            return true;
        } else {
            goto SKIP_FIRST_COMPARE;
        }
    }
    return false;  // NOTREACHED
}



/**
 * Generic intersection function.
 */
size_t intersection_uint32(const uint32_t *A, const size_t lenA,
                           const uint32_t *B, const size_t lenB,
                           uint32_t *out) {
    const uint32_t *initout = out;
    if (lenA == 0 || lenB == 0) return 0;
    const uint32_t *endA = A + lenA;
    const uint32_t *endB = B + lenB;

    while (1) {
        while (*A < *B) {
        SKIP_FIRST_COMPARE:
            if (++A == endA) return (out - initout);
        }
        while (*A > *B) {
            if (++B == endB) return (out - initout);
        }
        if (*A == *B) {
            *out++ = *A;
            if (++A == endA || ++B == endB) return (out - initout);
        } else {
            goto SKIP_FIRST_COMPARE;
        }
    }
    return (out - initout);  // NOTREACHED
}

size_t intersection_uint32_card(const uint32_t *A, const size_t lenA,
                                const uint32_t *B, const size_t lenB) {
    if (lenA == 0 || lenB == 0) return 0;
    size_t card = 0;
    const uint32_t *endA = A + lenA;
    const uint32_t *endB = B + lenB;

    while (1) {
        while (*A < *B) {
        SKIP_FIRST_COMPARE:
            if (++A == endA) return card;
        }
        while (*A > *B) {
            if (++B == endB) return card;
        }
        if (*A == *B) {
            card++;
            if (++A == endA || ++B == endB) return card;
        } else {
            goto SKIP_FIRST_COMPARE;
        }
    }
    return card;  // NOTREACHED
}

// can one vectorize the computation of the union? (Update: Yes! See
// union_vector16).

size_t union_uint16(const uint16_t *set_1, size_t size_1, const uint16_t *set_2,
                    size_t size_2, uint16_t *buffer) {
    size_t pos = 0, idx_1 = 0, idx_2 = 0;

    if (0 == size_2) {
        memmove(buffer, set_1, size_1 * sizeof(uint16_t));
        return size_1;
    }
    if (0 == size_1) {
        memmove(buffer, set_2, size_2 * sizeof(uint16_t));
        return size_2;
    }

    uint16_t val_1 = set_1[idx_1], val_2 = set_2[idx_2];

    while (true) {
        if (val_1 < val_2) {
            buffer[pos++] = val_1;
            ++idx_1;
            if (idx_1 >= size_1) break;
            val_1 = set_1[idx_1];
        } else if (val_2 < val_1) {
            buffer[pos++] = val_2;
            ++idx_2;
            if (idx_2 >= size_2) break;
            val_2 = set_2[idx_2];
        } else {
            buffer[pos++] = val_1;
            ++idx_1;
            ++idx_2;
            if (idx_1 >= size_1 || idx_2 >= size_2) break;
            val_1 = set_1[idx_1];
            val_2 = set_2[idx_2];
        }
    }

    if (idx_1 < size_1) {
        const size_t n_elems = size_1 - idx_1;
        memmove(buffer + pos, set_1 + idx_1, n_elems * sizeof(uint16_t));
        pos += n_elems;
    } else if (idx_2 < size_2) {
        const size_t n_elems = size_2 - idx_2;
        memmove(buffer + pos, set_2 + idx_2, n_elems * sizeof(uint16_t));
        pos += n_elems;
    }

    return pos;
}

int difference_uint16(const uint16_t *a1, int length1, const uint16_t *a2,
                      int length2, uint16_t *a_out) {
    int out_card = 0;
    int k1 = 0, k2 = 0;
    if (length1 == 0) return 0;
    if (length2 == 0) {
        if (a1 != a_out) memcpy(a_out, a1, sizeof(uint16_t) * length1);
        return length1;
    }
    uint16_t s1 = a1[k1];
    uint16_t s2 = a2[k2];
    while (true) {
        if (s1 < s2) {
            a_out[out_card++] = s1;
            ++k1;
            if (k1 >= length1) {
                break;
            }
            s1 = a1[k1];
        } else if (s1 == s2) {
            ++k1;
            ++k2;
            if (k1 >= length1) {
                break;
            }
            if (k2 >= length2) {
                memmove(a_out + out_card, a1 + k1,
                        sizeof(uint16_t) * (length1 - k1));
                return out_card + length1 - k1;
            }
            s1 = a1[k1];
            s2 = a2[k2];
        } else {  // if (val1>val2)
            ++k2;
            if (k2 >= length2) {
                memmove(a_out + out_card, a1 + k1,
                        sizeof(uint16_t) * (length1 - k1));
                return out_card + length1 - k1;
            }
            s2 = a2[k2];
        }
    }
    return out_card;
}

int32_t xor_uint16(const uint16_t *array_1, int32_t card_1,
                   const uint16_t *array_2, int32_t card_2, uint16_t *out) {
    int32_t pos1 = 0, pos2 = 0, pos_out = 0;
    while (pos1 < card_1 && pos2 < card_2) {
        const uint16_t v1 = array_1[pos1];
        const uint16_t v2 = array_2[pos2];
        if (v1 == v2) {
            ++pos1;
            ++pos2;
            continue;
        }
        if (v1 < v2) {
            out[pos_out++] = v1;
            ++pos1;
        } else {
            out[pos_out++] = v2;
            ++pos2;
        }
    }
    if (pos1 < card_1) {
        const size_t n_elems = card_1 - pos1;
        memcpy(out + pos_out, array_1 + pos1, n_elems * sizeof(uint16_t));
        pos_out += (int32_t)n_elems;
    } else if (pos2 < card_2) {
        const size_t n_elems = card_2 - pos2;
        memcpy(out + pos_out, array_2 + pos2, n_elems * sizeof(uint16_t));
        pos_out += (int32_t)n_elems;
    }
    return pos_out;
}

#ifdef USESSE4

/***
 * start of the SIMD 16-bit union code
 *
 */

// Assuming that vInput1 and vInput2 are sorted, produces a sorted output going
// from vecMin all the way to vecMax
// developed originally for merge sort using SIMD instructions.
// Standard merge. See, e.g., Inoue and Taura, SIMD- and Cache-Friendly
// Algorithm for Sorting an Array of Structures
static inline void sse_merge(const __m128i *vInput1,
                             const __m128i *vInput2,              // input 1 & 2
                             __m128i *vecMin, __m128i *vecMax) {  // output
    __m128i vecTmp;
    vecTmp = _mm_min_epu16(*vInput1, *vInput2);
    *vecMax = _mm_max_epu16(*vInput1, *vInput2);
    vecTmp = _mm_alignr_epi8(vecTmp, vecTmp, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    vecTmp = _mm_alignr_epi8(*vecMin, *vecMin, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    vecTmp = _mm_alignr_epi8(*vecMin, *vecMin, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    vecTmp = _mm_alignr_epi8(*vecMin, *vecMin, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    vecTmp = _mm_alignr_epi8(*vecMin, *vecMin, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    vecTmp = _mm_alignr_epi8(*vecMin, *vecMin, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    vecTmp = _mm_alignr_epi8(*vecMin, *vecMin, 2);
    *vecMin = _mm_min_epu16(vecTmp, *vecMax);
    *vecMax = _mm_max_epu16(vecTmp, *vecMax);
    *vecMin = _mm_alignr_epi8(*vecMin, *vecMin, 2);
}

// used by store_unique, generated by simdunion.py
static uint8_t uniqshuf[] = {
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,
    0xc,  0xd,  0xe,  0xf,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,
    0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0x8,  0x9,
    0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,
    0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x8,  0x9,
    0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x8,  0x9,
    0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,
    0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0xa,  0xb,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0xa,  0xb,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0xa,  0xb,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0xa,  0xb,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xa,  0xb,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,
    0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,
    0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x8,  0x9,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x8,  0x9,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x8,  0x9,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x6,  0x7,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x6,  0x7,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x6,  0x7,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0xc,  0xd,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0xc,  0xd,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xc,  0xd,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,
    0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0x8,  0x9,
    0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x8,  0x9,  0xa,  0xb,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x8,  0x9,
    0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x8,  0x9,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0xa,  0xb,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,
    0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,  0xa,  0xb,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,
    0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x6,  0x7,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0xa,  0xb,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0xa,  0xb,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xa,  0xb,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x6,  0x7,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0x8,  0x9,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,
    0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x6,  0x7,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x8,  0x9,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x8,  0x9,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x8,  0x9,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x8,  0x9,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x6,  0x7,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x6,  0x7,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x6,  0x7,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0xe,  0xf,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0xe,  0xf,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xe,  0xf,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,
    0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,
    0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,
    0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x8,  0x9,
    0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x8,  0x9,  0xa,  0xb,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x8,  0x9,  0xa,  0xb,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xa,  0xb,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0xa,  0xb,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,
    0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0xa,  0xb,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0xa,  0xb,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0xa,  0xb,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xa,  0xb,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,
    0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,  0x8,  0x9,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0x8,  0x9,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x8,  0x9,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x8,  0x9,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x8,  0x9,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x6,  0x7,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,  0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x6,  0x7,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0xc,  0xd,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0xc,  0xd,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xc,  0xd,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,
    0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0x8,  0x9,
    0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,
    0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x8,  0x9,  0xa,  0xb,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x8,  0x9,
    0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x8,  0x9,
    0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x8,  0x9,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,
    0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x6,  0x7,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,  0xa,  0xb,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x6,  0x7,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x6,  0x7,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,  0xa,  0xb,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0xa,  0xb,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0xa,  0xb,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0xa,  0xb,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0xa,  0xb,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xa,  0xb,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0x6,  0x7,  0x8,  0x9,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x6,  0x7,
    0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,  0x8,  0x9,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x6,  0x7,
    0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x6,  0x7,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x8,  0x9,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,
    0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x4,  0x5,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x8,  0x9,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x8,  0x9,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x4,  0x5,  0x6,  0x7,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,
    0x6,  0x7,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x4,  0x5,  0x6,  0x7,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,  0x6,  0x7,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0x6,  0x7,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x6,  0x7,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x6,  0x7,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x2,  0x3,
    0x4,  0x5,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x2,  0x3,  0x4,  0x5,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0x4,  0x5,  0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x4,  0x5,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x0,  0x1,  0x2,  0x3,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0x2,  0x3,  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0,  0x1,  0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF};

// write vector new, while omitting repeated values assuming that previously
// written vector was "old"
static inline int store_unique(__m128i old, __m128i newval, uint16_t *output) {
    __m128i vecTmp = _mm_alignr_epi8(newval, old, 16 - 2);
    // lots of high latency instructions follow (optimize?)
    int M = _mm_movemask_epi8(
        _mm_packs_epi16(_mm_cmpeq_epi16(vecTmp, newval), _mm_setzero_si128()));
    int numberofnewvalues = 8 - _mm_popcnt_u32(M);
    __m128i key = _mm_lddqu_si128((const __m128i *)uniqshuf + M);
    __m128i val = _mm_shuffle_epi8(newval, key);
    _mm_storeu_si128((__m128i *)output, val);
    return numberofnewvalues;
}

// working in-place, this function overwrites the repeated values
// could be avoided?
static inline uint32_t unique(uint16_t *out, uint32_t len) {
    uint32_t pos = 1;
    for (uint32_t i = 1; i < len; ++i) {
        if (out[i] != out[i - 1]) {
            out[pos++] = out[i];
        }
    }
    return pos;
}

// use with qsort, could be avoided
static int uint16_compare(const void *a, const void *b) {
    return (*(uint16_t *)a - *(uint16_t *)b);
}

// a one-pass SSE union algorithm
uint32_t union_vector16(const uint16_t *__restrict__ array1, uint32_t length1,
                        const uint16_t *__restrict__ array2, uint32_t length2,
                        uint16_t *__restrict__ output) {
    if ((length1 < 8) || (length2 < 8)) {
        return (uint32_t)union_uint16(array1, length1, array2, length2, output);
    }
    __m128i vA, vB, V, vecMin, vecMax;
    __m128i laststore;
    uint16_t *initoutput = output;
    uint32_t len1 = length1 / 8;
    uint32_t len2 = length2 / 8;
    uint32_t pos1 = 0;
    uint32_t pos2 = 0;
    // we start the machine
    vA = _mm_lddqu_si128((const __m128i *)array1 + pos1);
    pos1++;
    vB = _mm_lddqu_si128((const __m128i *)array2 + pos2);
    pos2++;
    sse_merge(&vA, &vB, &vecMin, &vecMax);
    laststore = _mm_set1_epi16(-1);
    output += store_unique(laststore, vecMin, output);
    laststore = vecMin;
    if ((pos1 < len1) && (pos2 < len2)) {
        uint16_t curA, curB;
        curA = array1[8 * pos1];
        curB = array2[8 * pos2];
        while (true) {
            if (curA <= curB) {
                V = _mm_lddqu_si128((const __m128i *)array1 + pos1);
                pos1++;
                if (pos1 < len1) {
                    curA = array1[8 * pos1];
                } else {
                    break;
                }
            } else {
                V = _mm_lddqu_si128((const __m128i *)array2 + pos2);
                pos2++;
                if (pos2 < len2) {
                    curB = array2[8 * pos2];
                } else {
                    break;
                }
            }
            sse_merge(&V, &vecMax, &vecMin, &vecMax);
            output += store_unique(laststore, vecMin, output);
            laststore = vecMin;
        }
        sse_merge(&V, &vecMax, &vecMin, &vecMax);
        output += store_unique(laststore, vecMin, output);
        laststore = vecMin;
    }
    // we finish the rest off using a scalar algorithm
    // could be improved?
    //
    // copy the small end on a tmp buffer
    uint32_t len = (uint32_t)(output - initoutput);
    uint16_t buffer[16];
    uint32_t leftoversize = store_unique(laststore, vecMax, buffer);
    if (pos1 == len1) {
        memcpy(buffer + leftoversize, array1 + 8 * pos1,
               (length1 - 8 * len1) * sizeof(uint16_t));
        leftoversize += length1 - 8 * len1;
        qsort(buffer, leftoversize, sizeof(uint16_t), uint16_compare);

        leftoversize = unique(buffer, leftoversize);
        len += (uint32_t)union_uint16(buffer, leftoversize, array2 + 8 * pos2,
                                      length2 - 8 * pos2, output);
    } else {
        memcpy(buffer + leftoversize, array2 + 8 * pos2,
               (length2 - 8 * len2) * sizeof(uint16_t));
        leftoversize += length2 - 8 * len2;
        qsort(buffer, leftoversize, sizeof(uint16_t), uint16_compare);
        leftoversize = unique(buffer, leftoversize);
        len += (uint32_t)union_uint16(buffer, leftoversize, array1 + 8 * pos1,
                                      length1 - 8 * pos1, output);
    }
    return len;
}

/**
 * End of the SIMD 16-bit union code
 *
 */

/**
 * Start of SIMD 16-bit XOR code
 */

// write vector new, while omitting repeated values assuming that previously
// written vector was "old"
static inline int store_unique_xor(__m128i old, __m128i newval,
                                   uint16_t *output) {
    __m128i vecTmp1 = _mm_alignr_epi8(newval, old, 16 - 4);
    __m128i vecTmp2 = _mm_alignr_epi8(newval, old, 16 - 2);
    __m128i equalleft = _mm_cmpeq_epi16(vecTmp2, vecTmp1);
    __m128i equalright = _mm_cmpeq_epi16(vecTmp2, newval);
    __m128i equalleftoright = _mm_or_si128(equalleft, equalright);
    int M = _mm_movemask_epi8(
        _mm_packs_epi16(equalleftoright, _mm_setzero_si128()));
    int numberofnewvalues = 8 - _mm_popcnt_u32(M);
    __m128i key = _mm_lddqu_si128((const __m128i *)uniqshuf + M);
    __m128i val = _mm_shuffle_epi8(vecTmp2, key);
    _mm_storeu_si128((__m128i *)output, val);
    return numberofnewvalues;
}

// working in-place, this function overwrites the repeated values
// could be avoided? Warning: assumes len > 0
static inline uint32_t unique_xor(uint16_t *out, uint32_t len) {
    uint32_t pos = 1;
    for (uint32_t i = 1; i < len; ++i) {
        if (out[i] != out[i - 1]) {
            out[pos++] = out[i];
        } else
            pos--;  // if it is identical to previous, delete it
    }
    return pos;
}

// a one-pass SSE xor algorithm
uint32_t xor_vector16(const uint16_t *__restrict__ array1, uint32_t length1,
                      const uint16_t *__restrict__ array2, uint32_t length2,
                      uint16_t *__restrict__ output) {
    if ((length1 < 8) || (length2 < 8)) {
        return xor_uint16(array1, length1, array2, length2, output);
    }
    __m128i vA, vB, V, vecMin, vecMax;
    __m128i laststore;
    uint16_t *initoutput = output;
    uint32_t len1 = length1 / 8;
    uint32_t len2 = length2 / 8;
    uint32_t pos1 = 0;
    uint32_t pos2 = 0;
    // we start the machine
    vA = _mm_lddqu_si128((const __m128i *)array1 + pos1);
    pos1++;
    vB = _mm_lddqu_si128((const __m128i *)array2 + pos2);
    pos2++;
    sse_merge(&vA, &vB, &vecMin, &vecMax);
    laststore = _mm_set1_epi16(-1);
    uint16_t buffer[17];
    output += store_unique_xor(laststore, vecMin, output);

    laststore = vecMin;
    if ((pos1 < len1) && (pos2 < len2)) {
        uint16_t curA, curB;
        curA = array1[8 * pos1];
        curB = array2[8 * pos2];
        while (true) {
            if (curA <= curB) {
                V = _mm_lddqu_si128((const __m128i *)array1 + pos1);
                pos1++;
                if (pos1 < len1) {
                    curA = array1[8 * pos1];
                } else {
                    break;
                }
            } else {
                V = _mm_lddqu_si128((const __m128i *)array2 + pos2);
                pos2++;
                if (pos2 < len2) {
                    curB = array2[8 * pos2];
                } else {
                    break;
                }
            }
            sse_merge(&V, &vecMax, &vecMin, &vecMax);
            // conditionally stores the last value of laststore as well as all
            // but the
            // last value of vecMin
            output += store_unique_xor(laststore, vecMin, output);
            laststore = vecMin;
        }
        sse_merge(&V, &vecMax, &vecMin, &vecMax);
        // conditionally stores the last value of laststore as well as all but
        // the
        // last value of vecMin
        output += store_unique_xor(laststore, vecMin, output);
        laststore = vecMin;
    }
    uint32_t len = (uint32_t)(output - initoutput);

    // we finish the rest off using a scalar algorithm
    // could be improved?
    // conditionally stores the last value of laststore as well as all but the
    // last value of vecMax,
    // we store to "buffer"
    int leftoversize = store_unique_xor(laststore, vecMax, buffer);
    uint16_t vec7 = _mm_extract_epi16(vecMax, 7);
    uint16_t vec6 = _mm_extract_epi16(vecMax, 6);
    if (vec7 != vec6) buffer[leftoversize++] = vec7;
    if (pos1 == len1) {
        memcpy(buffer + leftoversize, array1 + 8 * pos1,
               (length1 - 8 * len1) * sizeof(uint16_t));
        leftoversize += length1 - 8 * len1;
        if (leftoversize == 0) {  // trivial case
            memcpy(output, array2 + 8 * pos2,
                   (length2 - 8 * pos2) * sizeof(uint16_t));
            len += (length2 - 8 * pos2);
        } else {
            qsort(buffer, leftoversize, sizeof(uint16_t), uint16_compare);
            leftoversize = unique_xor(buffer, leftoversize);
            len += xor_uint16(buffer, leftoversize, array2 + 8 * pos2,
                              length2 - 8 * pos2, output);
        }
    } else {
        memcpy(buffer + leftoversize, array2 + 8 * pos2,
               (length2 - 8 * len2) * sizeof(uint16_t));
        leftoversize += length2 - 8 * len2;
        if (leftoversize == 0) {  // trivial case
            memcpy(output, array1 + 8 * pos1,
                   (length1 - 8 * pos1) * sizeof(uint16_t));
            len += (length1 - 8 * pos1);
        } else {
            qsort(buffer, leftoversize, sizeof(uint16_t), uint16_compare);
            leftoversize = unique_xor(buffer, leftoversize);
            len += xor_uint16(buffer, leftoversize, array1 + 8 * pos1,
                              length1 - 8 * pos1, output);
        }
    }
    return len;
}

/**
 * End of SIMD 16-bit XOR code
 */

#endif  // USESSE4

size_t union_uint32(const uint32_t *set_1, size_t size_1, const uint32_t *set_2,
                    size_t size_2, uint32_t *buffer) {
    size_t pos = 0, idx_1 = 0, idx_2 = 0;

    if (0 == size_2) {
        memmove(buffer, set_1, size_1 * sizeof(uint32_t));
        return size_1;
    }
    if (0 == size_1) {
        memmove(buffer, set_2, size_2 * sizeof(uint32_t));
        return size_2;
    }

    uint32_t val_1 = set_1[idx_1], val_2 = set_2[idx_2];

    while (true) {
        if (val_1 < val_2) {
            buffer[pos++] = val_1;
            ++idx_1;
            if (idx_1 >= size_1) break;
            val_1 = set_1[idx_1];
        } else if (val_2 < val_1) {
            buffer[pos++] = val_2;
            ++idx_2;
            if (idx_2 >= size_2) break;
            val_2 = set_2[idx_2];
        } else {
            buffer[pos++] = val_1;
            ++idx_1;
            ++idx_2;
            if (idx_1 >= size_1 || idx_2 >= size_2) break;
            val_1 = set_1[idx_1];
            val_2 = set_2[idx_2];
        }
    }

    if (idx_1 < size_1) {
        const size_t n_elems = size_1 - idx_1;
        memmove(buffer + pos, set_1 + idx_1, n_elems * sizeof(uint32_t));
        pos += n_elems;
    } else if (idx_2 < size_2) {
        const size_t n_elems = size_2 - idx_2;
        memmove(buffer + pos, set_2 + idx_2, n_elems * sizeof(uint32_t));
        pos += n_elems;
    }

    return pos;
}

size_t union_uint32_card(const uint32_t *set_1, size_t size_1,
                         const uint32_t *set_2, size_t size_2) {
    size_t pos = 0, idx_1 = 0, idx_2 = 0;

    if (0 == size_2) {
        return size_1;
    }
    if (0 == size_1) {
        return size_2;
    }

    uint32_t val_1 = set_1[idx_1], val_2 = set_2[idx_2];

    while (true) {
        if (val_1 < val_2) {
            ++idx_1;
            ++pos;
            if (idx_1 >= size_1) break;
            val_1 = set_1[idx_1];
        } else if (val_2 < val_1) {
            ++idx_2;
            ++pos;
            if (idx_2 >= size_2) break;
            val_2 = set_2[idx_2];
        } else {
            ++idx_1;
            ++idx_2;
            ++pos;
            if (idx_1 >= size_1 || idx_2 >= size_2) break;
            val_1 = set_1[idx_1];
            val_2 = set_2[idx_2];
        }
    }

    if (idx_1 < size_1) {
        const size_t n_elems = size_1 - idx_1;
        pos += n_elems;
    } else if (idx_2 < size_2) {
        const size_t n_elems = size_2 - idx_2;
        pos += n_elems;
    }
    return pos;
}



size_t fast_union_uint16(const uint16_t *set_1, size_t size_1, const uint16_t *set_2,
                    size_t size_2, uint16_t *buffer) {
#ifdef ROARING_VECTOR_OPERATIONS_ENABLED
    // compute union with smallest array first
    if (size_1 < size_2) {
        return union_vector16(set_1, (uint32_t)size_1,
                                          set_2, (uint32_t)size_2, buffer);
    } else {
        return union_vector16(set_2, (uint32_t)size_2,
                                          set_1, (uint32_t)size_1, buffer);
    }
#else
    // compute union with smallest array first
    if (size_1 < size_2) {
        return union_uint16(
            set_1, size_1, set_2, size_2, buffer);
    } else {
        return union_uint16(
            set_2, size_2, set_1, size_1, buffer);
    }
#endif
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/array_util.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/bitset_util.c */
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#ifdef IS_X64
static uint8_t lengthTable[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4,
    2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4,
    2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5,
    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
#endif

#ifdef USEAVX
ALIGNED(32)
static uint32_t vecDecodeTable[256][8] = {
    {0, 0, 0, 0, 0, 0, 0, 0}, /* 0x00 (00000000) */
    {1, 0, 0, 0, 0, 0, 0, 0}, /* 0x01 (00000001) */
    {2, 0, 0, 0, 0, 0, 0, 0}, /* 0x02 (00000010) */
    {1, 2, 0, 0, 0, 0, 0, 0}, /* 0x03 (00000011) */
    {3, 0, 0, 0, 0, 0, 0, 0}, /* 0x04 (00000100) */
    {1, 3, 0, 0, 0, 0, 0, 0}, /* 0x05 (00000101) */
    {2, 3, 0, 0, 0, 0, 0, 0}, /* 0x06 (00000110) */
    {1, 2, 3, 0, 0, 0, 0, 0}, /* 0x07 (00000111) */
    {4, 0, 0, 0, 0, 0, 0, 0}, /* 0x08 (00001000) */
    {1, 4, 0, 0, 0, 0, 0, 0}, /* 0x09 (00001001) */
    {2, 4, 0, 0, 0, 0, 0, 0}, /* 0x0A (00001010) */
    {1, 2, 4, 0, 0, 0, 0, 0}, /* 0x0B (00001011) */
    {3, 4, 0, 0, 0, 0, 0, 0}, /* 0x0C (00001100) */
    {1, 3, 4, 0, 0, 0, 0, 0}, /* 0x0D (00001101) */
    {2, 3, 4, 0, 0, 0, 0, 0}, /* 0x0E (00001110) */
    {1, 2, 3, 4, 0, 0, 0, 0}, /* 0x0F (00001111) */
    {5, 0, 0, 0, 0, 0, 0, 0}, /* 0x10 (00010000) */
    {1, 5, 0, 0, 0, 0, 0, 0}, /* 0x11 (00010001) */
    {2, 5, 0, 0, 0, 0, 0, 0}, /* 0x12 (00010010) */
    {1, 2, 5, 0, 0, 0, 0, 0}, /* 0x13 (00010011) */
    {3, 5, 0, 0, 0, 0, 0, 0}, /* 0x14 (00010100) */
    {1, 3, 5, 0, 0, 0, 0, 0}, /* 0x15 (00010101) */
    {2, 3, 5, 0, 0, 0, 0, 0}, /* 0x16 (00010110) */
    {1, 2, 3, 5, 0, 0, 0, 0}, /* 0x17 (00010111) */
    {4, 5, 0, 0, 0, 0, 0, 0}, /* 0x18 (00011000) */
    {1, 4, 5, 0, 0, 0, 0, 0}, /* 0x19 (00011001) */
    {2, 4, 5, 0, 0, 0, 0, 0}, /* 0x1A (00011010) */
    {1, 2, 4, 5, 0, 0, 0, 0}, /* 0x1B (00011011) */
    {3, 4, 5, 0, 0, 0, 0, 0}, /* 0x1C (00011100) */
    {1, 3, 4, 5, 0, 0, 0, 0}, /* 0x1D (00011101) */
    {2, 3, 4, 5, 0, 0, 0, 0}, /* 0x1E (00011110) */
    {1, 2, 3, 4, 5, 0, 0, 0}, /* 0x1F (00011111) */
    {6, 0, 0, 0, 0, 0, 0, 0}, /* 0x20 (00100000) */
    {1, 6, 0, 0, 0, 0, 0, 0}, /* 0x21 (00100001) */
    {2, 6, 0, 0, 0, 0, 0, 0}, /* 0x22 (00100010) */
    {1, 2, 6, 0, 0, 0, 0, 0}, /* 0x23 (00100011) */
    {3, 6, 0, 0, 0, 0, 0, 0}, /* 0x24 (00100100) */
    {1, 3, 6, 0, 0, 0, 0, 0}, /* 0x25 (00100101) */
    {2, 3, 6, 0, 0, 0, 0, 0}, /* 0x26 (00100110) */
    {1, 2, 3, 6, 0, 0, 0, 0}, /* 0x27 (00100111) */
    {4, 6, 0, 0, 0, 0, 0, 0}, /* 0x28 (00101000) */
    {1, 4, 6, 0, 0, 0, 0, 0}, /* 0x29 (00101001) */
    {2, 4, 6, 0, 0, 0, 0, 0}, /* 0x2A (00101010) */
    {1, 2, 4, 6, 0, 0, 0, 0}, /* 0x2B (00101011) */
    {3, 4, 6, 0, 0, 0, 0, 0}, /* 0x2C (00101100) */
    {1, 3, 4, 6, 0, 0, 0, 0}, /* 0x2D (00101101) */
    {2, 3, 4, 6, 0, 0, 0, 0}, /* 0x2E (00101110) */
    {1, 2, 3, 4, 6, 0, 0, 0}, /* 0x2F (00101111) */
    {5, 6, 0, 0, 0, 0, 0, 0}, /* 0x30 (00110000) */
    {1, 5, 6, 0, 0, 0, 0, 0}, /* 0x31 (00110001) */
    {2, 5, 6, 0, 0, 0, 0, 0}, /* 0x32 (00110010) */
    {1, 2, 5, 6, 0, 0, 0, 0}, /* 0x33 (00110011) */
    {3, 5, 6, 0, 0, 0, 0, 0}, /* 0x34 (00110100) */
    {1, 3, 5, 6, 0, 0, 0, 0}, /* 0x35 (00110101) */
    {2, 3, 5, 6, 0, 0, 0, 0}, /* 0x36 (00110110) */
    {1, 2, 3, 5, 6, 0, 0, 0}, /* 0x37 (00110111) */
    {4, 5, 6, 0, 0, 0, 0, 0}, /* 0x38 (00111000) */
    {1, 4, 5, 6, 0, 0, 0, 0}, /* 0x39 (00111001) */
    {2, 4, 5, 6, 0, 0, 0, 0}, /* 0x3A (00111010) */
    {1, 2, 4, 5, 6, 0, 0, 0}, /* 0x3B (00111011) */
    {3, 4, 5, 6, 0, 0, 0, 0}, /* 0x3C (00111100) */
    {1, 3, 4, 5, 6, 0, 0, 0}, /* 0x3D (00111101) */
    {2, 3, 4, 5, 6, 0, 0, 0}, /* 0x3E (00111110) */
    {1, 2, 3, 4, 5, 6, 0, 0}, /* 0x3F (00111111) */
    {7, 0, 0, 0, 0, 0, 0, 0}, /* 0x40 (01000000) */
    {1, 7, 0, 0, 0, 0, 0, 0}, /* 0x41 (01000001) */
    {2, 7, 0, 0, 0, 0, 0, 0}, /* 0x42 (01000010) */
    {1, 2, 7, 0, 0, 0, 0, 0}, /* 0x43 (01000011) */
    {3, 7, 0, 0, 0, 0, 0, 0}, /* 0x44 (01000100) */
    {1, 3, 7, 0, 0, 0, 0, 0}, /* 0x45 (01000101) */
    {2, 3, 7, 0, 0, 0, 0, 0}, /* 0x46 (01000110) */
    {1, 2, 3, 7, 0, 0, 0, 0}, /* 0x47 (01000111) */
    {4, 7, 0, 0, 0, 0, 0, 0}, /* 0x48 (01001000) */
    {1, 4, 7, 0, 0, 0, 0, 0}, /* 0x49 (01001001) */
    {2, 4, 7, 0, 0, 0, 0, 0}, /* 0x4A (01001010) */
    {1, 2, 4, 7, 0, 0, 0, 0}, /* 0x4B (01001011) */
    {3, 4, 7, 0, 0, 0, 0, 0}, /* 0x4C (01001100) */
    {1, 3, 4, 7, 0, 0, 0, 0}, /* 0x4D (01001101) */
    {2, 3, 4, 7, 0, 0, 0, 0}, /* 0x4E (01001110) */
    {1, 2, 3, 4, 7, 0, 0, 0}, /* 0x4F (01001111) */
    {5, 7, 0, 0, 0, 0, 0, 0}, /* 0x50 (01010000) */
    {1, 5, 7, 0, 0, 0, 0, 0}, /* 0x51 (01010001) */
    {2, 5, 7, 0, 0, 0, 0, 0}, /* 0x52 (01010010) */
    {1, 2, 5, 7, 0, 0, 0, 0}, /* 0x53 (01010011) */
    {3, 5, 7, 0, 0, 0, 0, 0}, /* 0x54 (01010100) */
    {1, 3, 5, 7, 0, 0, 0, 0}, /* 0x55 (01010101) */
    {2, 3, 5, 7, 0, 0, 0, 0}, /* 0x56 (01010110) */
    {1, 2, 3, 5, 7, 0, 0, 0}, /* 0x57 (01010111) */
    {4, 5, 7, 0, 0, 0, 0, 0}, /* 0x58 (01011000) */
    {1, 4, 5, 7, 0, 0, 0, 0}, /* 0x59 (01011001) */
    {2, 4, 5, 7, 0, 0, 0, 0}, /* 0x5A (01011010) */
    {1, 2, 4, 5, 7, 0, 0, 0}, /* 0x5B (01011011) */
    {3, 4, 5, 7, 0, 0, 0, 0}, /* 0x5C (01011100) */
    {1, 3, 4, 5, 7, 0, 0, 0}, /* 0x5D (01011101) */
    {2, 3, 4, 5, 7, 0, 0, 0}, /* 0x5E (01011110) */
    {1, 2, 3, 4, 5, 7, 0, 0}, /* 0x5F (01011111) */
    {6, 7, 0, 0, 0, 0, 0, 0}, /* 0x60 (01100000) */
    {1, 6, 7, 0, 0, 0, 0, 0}, /* 0x61 (01100001) */
    {2, 6, 7, 0, 0, 0, 0, 0}, /* 0x62 (01100010) */
    {1, 2, 6, 7, 0, 0, 0, 0}, /* 0x63 (01100011) */
    {3, 6, 7, 0, 0, 0, 0, 0}, /* 0x64 (01100100) */
    {1, 3, 6, 7, 0, 0, 0, 0}, /* 0x65 (01100101) */
    {2, 3, 6, 7, 0, 0, 0, 0}, /* 0x66 (01100110) */
    {1, 2, 3, 6, 7, 0, 0, 0}, /* 0x67 (01100111) */
    {4, 6, 7, 0, 0, 0, 0, 0}, /* 0x68 (01101000) */
    {1, 4, 6, 7, 0, 0, 0, 0}, /* 0x69 (01101001) */
    {2, 4, 6, 7, 0, 0, 0, 0}, /* 0x6A (01101010) */
    {1, 2, 4, 6, 7, 0, 0, 0}, /* 0x6B (01101011) */
    {3, 4, 6, 7, 0, 0, 0, 0}, /* 0x6C (01101100) */
    {1, 3, 4, 6, 7, 0, 0, 0}, /* 0x6D (01101101) */
    {2, 3, 4, 6, 7, 0, 0, 0}, /* 0x6E (01101110) */
    {1, 2, 3, 4, 6, 7, 0, 0}, /* 0x6F (01101111) */
    {5, 6, 7, 0, 0, 0, 0, 0}, /* 0x70 (01110000) */
    {1, 5, 6, 7, 0, 0, 0, 0}, /* 0x71 (01110001) */
    {2, 5, 6, 7, 0, 0, 0, 0}, /* 0x72 (01110010) */
    {1, 2, 5, 6, 7, 0, 0, 0}, /* 0x73 (01110011) */
    {3, 5, 6, 7, 0, 0, 0, 0}, /* 0x74 (01110100) */
    {1, 3, 5, 6, 7, 0, 0, 0}, /* 0x75 (01110101) */
    {2, 3, 5, 6, 7, 0, 0, 0}, /* 0x76 (01110110) */
    {1, 2, 3, 5, 6, 7, 0, 0}, /* 0x77 (01110111) */
    {4, 5, 6, 7, 0, 0, 0, 0}, /* 0x78 (01111000) */
    {1, 4, 5, 6, 7, 0, 0, 0}, /* 0x79 (01111001) */
    {2, 4, 5, 6, 7, 0, 0, 0}, /* 0x7A (01111010) */
    {1, 2, 4, 5, 6, 7, 0, 0}, /* 0x7B (01111011) */
    {3, 4, 5, 6, 7, 0, 0, 0}, /* 0x7C (01111100) */
    {1, 3, 4, 5, 6, 7, 0, 0}, /* 0x7D (01111101) */
    {2, 3, 4, 5, 6, 7, 0, 0}, /* 0x7E (01111110) */
    {1, 2, 3, 4, 5, 6, 7, 0}, /* 0x7F (01111111) */
    {8, 0, 0, 0, 0, 0, 0, 0}, /* 0x80 (10000000) */
    {1, 8, 0, 0, 0, 0, 0, 0}, /* 0x81 (10000001) */
    {2, 8, 0, 0, 0, 0, 0, 0}, /* 0x82 (10000010) */
    {1, 2, 8, 0, 0, 0, 0, 0}, /* 0x83 (10000011) */
    {3, 8, 0, 0, 0, 0, 0, 0}, /* 0x84 (10000100) */
    {1, 3, 8, 0, 0, 0, 0, 0}, /* 0x85 (10000101) */
    {2, 3, 8, 0, 0, 0, 0, 0}, /* 0x86 (10000110) */
    {1, 2, 3, 8, 0, 0, 0, 0}, /* 0x87 (10000111) */
    {4, 8, 0, 0, 0, 0, 0, 0}, /* 0x88 (10001000) */
    {1, 4, 8, 0, 0, 0, 0, 0}, /* 0x89 (10001001) */
    {2, 4, 8, 0, 0, 0, 0, 0}, /* 0x8A (10001010) */
    {1, 2, 4, 8, 0, 0, 0, 0}, /* 0x8B (10001011) */
    {3, 4, 8, 0, 0, 0, 0, 0}, /* 0x8C (10001100) */
    {1, 3, 4, 8, 0, 0, 0, 0}, /* 0x8D (10001101) */
    {2, 3, 4, 8, 0, 0, 0, 0}, /* 0x8E (10001110) */
    {1, 2, 3, 4, 8, 0, 0, 0}, /* 0x8F (10001111) */
    {5, 8, 0, 0, 0, 0, 0, 0}, /* 0x90 (10010000) */
    {1, 5, 8, 0, 0, 0, 0, 0}, /* 0x91 (10010001) */
    {2, 5, 8, 0, 0, 0, 0, 0}, /* 0x92 (10010010) */
    {1, 2, 5, 8, 0, 0, 0, 0}, /* 0x93 (10010011) */
    {3, 5, 8, 0, 0, 0, 0, 0}, /* 0x94 (10010100) */
    {1, 3, 5, 8, 0, 0, 0, 0}, /* 0x95 (10010101) */
    {2, 3, 5, 8, 0, 0, 0, 0}, /* 0x96 (10010110) */
    {1, 2, 3, 5, 8, 0, 0, 0}, /* 0x97 (10010111) */
    {4, 5, 8, 0, 0, 0, 0, 0}, /* 0x98 (10011000) */
    {1, 4, 5, 8, 0, 0, 0, 0}, /* 0x99 (10011001) */
    {2, 4, 5, 8, 0, 0, 0, 0}, /* 0x9A (10011010) */
    {1, 2, 4, 5, 8, 0, 0, 0}, /* 0x9B (10011011) */
    {3, 4, 5, 8, 0, 0, 0, 0}, /* 0x9C (10011100) */
    {1, 3, 4, 5, 8, 0, 0, 0}, /* 0x9D (10011101) */
    {2, 3, 4, 5, 8, 0, 0, 0}, /* 0x9E (10011110) */
    {1, 2, 3, 4, 5, 8, 0, 0}, /* 0x9F (10011111) */
    {6, 8, 0, 0, 0, 0, 0, 0}, /* 0xA0 (10100000) */
    {1, 6, 8, 0, 0, 0, 0, 0}, /* 0xA1 (10100001) */
    {2, 6, 8, 0, 0, 0, 0, 0}, /* 0xA2 (10100010) */
    {1, 2, 6, 8, 0, 0, 0, 0}, /* 0xA3 (10100011) */
    {3, 6, 8, 0, 0, 0, 0, 0}, /* 0xA4 (10100100) */
    {1, 3, 6, 8, 0, 0, 0, 0}, /* 0xA5 (10100101) */
    {2, 3, 6, 8, 0, 0, 0, 0}, /* 0xA6 (10100110) */
    {1, 2, 3, 6, 8, 0, 0, 0}, /* 0xA7 (10100111) */
    {4, 6, 8, 0, 0, 0, 0, 0}, /* 0xA8 (10101000) */
    {1, 4, 6, 8, 0, 0, 0, 0}, /* 0xA9 (10101001) */
    {2, 4, 6, 8, 0, 0, 0, 0}, /* 0xAA (10101010) */
    {1, 2, 4, 6, 8, 0, 0, 0}, /* 0xAB (10101011) */
    {3, 4, 6, 8, 0, 0, 0, 0}, /* 0xAC (10101100) */
    {1, 3, 4, 6, 8, 0, 0, 0}, /* 0xAD (10101101) */
    {2, 3, 4, 6, 8, 0, 0, 0}, /* 0xAE (10101110) */
    {1, 2, 3, 4, 6, 8, 0, 0}, /* 0xAF (10101111) */
    {5, 6, 8, 0, 0, 0, 0, 0}, /* 0xB0 (10110000) */
    {1, 5, 6, 8, 0, 0, 0, 0}, /* 0xB1 (10110001) */
    {2, 5, 6, 8, 0, 0, 0, 0}, /* 0xB2 (10110010) */
    {1, 2, 5, 6, 8, 0, 0, 0}, /* 0xB3 (10110011) */
    {3, 5, 6, 8, 0, 0, 0, 0}, /* 0xB4 (10110100) */
    {1, 3, 5, 6, 8, 0, 0, 0}, /* 0xB5 (10110101) */
    {2, 3, 5, 6, 8, 0, 0, 0}, /* 0xB6 (10110110) */
    {1, 2, 3, 5, 6, 8, 0, 0}, /* 0xB7 (10110111) */
    {4, 5, 6, 8, 0, 0, 0, 0}, /* 0xB8 (10111000) */
    {1, 4, 5, 6, 8, 0, 0, 0}, /* 0xB9 (10111001) */
    {2, 4, 5, 6, 8, 0, 0, 0}, /* 0xBA (10111010) */
    {1, 2, 4, 5, 6, 8, 0, 0}, /* 0xBB (10111011) */
    {3, 4, 5, 6, 8, 0, 0, 0}, /* 0xBC (10111100) */
    {1, 3, 4, 5, 6, 8, 0, 0}, /* 0xBD (10111101) */
    {2, 3, 4, 5, 6, 8, 0, 0}, /* 0xBE (10111110) */
    {1, 2, 3, 4, 5, 6, 8, 0}, /* 0xBF (10111111) */
    {7, 8, 0, 0, 0, 0, 0, 0}, /* 0xC0 (11000000) */
    {1, 7, 8, 0, 0, 0, 0, 0}, /* 0xC1 (11000001) */
    {2, 7, 8, 0, 0, 0, 0, 0}, /* 0xC2 (11000010) */
    {1, 2, 7, 8, 0, 0, 0, 0}, /* 0xC3 (11000011) */
    {3, 7, 8, 0, 0, 0, 0, 0}, /* 0xC4 (11000100) */
    {1, 3, 7, 8, 0, 0, 0, 0}, /* 0xC5 (11000101) */
    {2, 3, 7, 8, 0, 0, 0, 0}, /* 0xC6 (11000110) */
    {1, 2, 3, 7, 8, 0, 0, 0}, /* 0xC7 (11000111) */
    {4, 7, 8, 0, 0, 0, 0, 0}, /* 0xC8 (11001000) */
    {1, 4, 7, 8, 0, 0, 0, 0}, /* 0xC9 (11001001) */
    {2, 4, 7, 8, 0, 0, 0, 0}, /* 0xCA (11001010) */
    {1, 2, 4, 7, 8, 0, 0, 0}, /* 0xCB (11001011) */
    {3, 4, 7, 8, 0, 0, 0, 0}, /* 0xCC (11001100) */
    {1, 3, 4, 7, 8, 0, 0, 0}, /* 0xCD (11001101) */
    {2, 3, 4, 7, 8, 0, 0, 0}, /* 0xCE (11001110) */
    {1, 2, 3, 4, 7, 8, 0, 0}, /* 0xCF (11001111) */
    {5, 7, 8, 0, 0, 0, 0, 0}, /* 0xD0 (11010000) */
    {1, 5, 7, 8, 0, 0, 0, 0}, /* 0xD1 (11010001) */
    {2, 5, 7, 8, 0, 0, 0, 0}, /* 0xD2 (11010010) */
    {1, 2, 5, 7, 8, 0, 0, 0}, /* 0xD3 (11010011) */
    {3, 5, 7, 8, 0, 0, 0, 0}, /* 0xD4 (11010100) */
    {1, 3, 5, 7, 8, 0, 0, 0}, /* 0xD5 (11010101) */
    {2, 3, 5, 7, 8, 0, 0, 0}, /* 0xD6 (11010110) */
    {1, 2, 3, 5, 7, 8, 0, 0}, /* 0xD7 (11010111) */
    {4, 5, 7, 8, 0, 0, 0, 0}, /* 0xD8 (11011000) */
    {1, 4, 5, 7, 8, 0, 0, 0}, /* 0xD9 (11011001) */
    {2, 4, 5, 7, 8, 0, 0, 0}, /* 0xDA (11011010) */
    {1, 2, 4, 5, 7, 8, 0, 0}, /* 0xDB (11011011) */
    {3, 4, 5, 7, 8, 0, 0, 0}, /* 0xDC (11011100) */
    {1, 3, 4, 5, 7, 8, 0, 0}, /* 0xDD (11011101) */
    {2, 3, 4, 5, 7, 8, 0, 0}, /* 0xDE (11011110) */
    {1, 2, 3, 4, 5, 7, 8, 0}, /* 0xDF (11011111) */
    {6, 7, 8, 0, 0, 0, 0, 0}, /* 0xE0 (11100000) */
    {1, 6, 7, 8, 0, 0, 0, 0}, /* 0xE1 (11100001) */
    {2, 6, 7, 8, 0, 0, 0, 0}, /* 0xE2 (11100010) */
    {1, 2, 6, 7, 8, 0, 0, 0}, /* 0xE3 (11100011) */
    {3, 6, 7, 8, 0, 0, 0, 0}, /* 0xE4 (11100100) */
    {1, 3, 6, 7, 8, 0, 0, 0}, /* 0xE5 (11100101) */
    {2, 3, 6, 7, 8, 0, 0, 0}, /* 0xE6 (11100110) */
    {1, 2, 3, 6, 7, 8, 0, 0}, /* 0xE7 (11100111) */
    {4, 6, 7, 8, 0, 0, 0, 0}, /* 0xE8 (11101000) */
    {1, 4, 6, 7, 8, 0, 0, 0}, /* 0xE9 (11101001) */
    {2, 4, 6, 7, 8, 0, 0, 0}, /* 0xEA (11101010) */
    {1, 2, 4, 6, 7, 8, 0, 0}, /* 0xEB (11101011) */
    {3, 4, 6, 7, 8, 0, 0, 0}, /* 0xEC (11101100) */
    {1, 3, 4, 6, 7, 8, 0, 0}, /* 0xED (11101101) */
    {2, 3, 4, 6, 7, 8, 0, 0}, /* 0xEE (11101110) */
    {1, 2, 3, 4, 6, 7, 8, 0}, /* 0xEF (11101111) */
    {5, 6, 7, 8, 0, 0, 0, 0}, /* 0xF0 (11110000) */
    {1, 5, 6, 7, 8, 0, 0, 0}, /* 0xF1 (11110001) */
    {2, 5, 6, 7, 8, 0, 0, 0}, /* 0xF2 (11110010) */
    {1, 2, 5, 6, 7, 8, 0, 0}, /* 0xF3 (11110011) */
    {3, 5, 6, 7, 8, 0, 0, 0}, /* 0xF4 (11110100) */
    {1, 3, 5, 6, 7, 8, 0, 0}, /* 0xF5 (11110101) */
    {2, 3, 5, 6, 7, 8, 0, 0}, /* 0xF6 (11110110) */
    {1, 2, 3, 5, 6, 7, 8, 0}, /* 0xF7 (11110111) */
    {4, 5, 6, 7, 8, 0, 0, 0}, /* 0xF8 (11111000) */
    {1, 4, 5, 6, 7, 8, 0, 0}, /* 0xF9 (11111001) */
    {2, 4, 5, 6, 7, 8, 0, 0}, /* 0xFA (11111010) */
    {1, 2, 4, 5, 6, 7, 8, 0}, /* 0xFB (11111011) */
    {3, 4, 5, 6, 7, 8, 0, 0}, /* 0xFC (11111100) */
    {1, 3, 4, 5, 6, 7, 8, 0}, /* 0xFD (11111101) */
    {2, 3, 4, 5, 6, 7, 8, 0}, /* 0xFE (11111110) */
    {1, 2, 3, 4, 5, 6, 7, 8}  /* 0xFF (11111111) */
};

#endif  // #ifdef USEAVX

#ifdef IS_X64
// same as vecDecodeTable but in 16 bits
ALIGNED(32)
static uint16_t vecDecodeTable_uint16[256][8] = {
    {0, 0, 0, 0, 0, 0, 0, 0}, /* 0x00 (00000000) */
    {1, 0, 0, 0, 0, 0, 0, 0}, /* 0x01 (00000001) */
    {2, 0, 0, 0, 0, 0, 0, 0}, /* 0x02 (00000010) */
    {1, 2, 0, 0, 0, 0, 0, 0}, /* 0x03 (00000011) */
    {3, 0, 0, 0, 0, 0, 0, 0}, /* 0x04 (00000100) */
    {1, 3, 0, 0, 0, 0, 0, 0}, /* 0x05 (00000101) */
    {2, 3, 0, 0, 0, 0, 0, 0}, /* 0x06 (00000110) */
    {1, 2, 3, 0, 0, 0, 0, 0}, /* 0x07 (00000111) */
    {4, 0, 0, 0, 0, 0, 0, 0}, /* 0x08 (00001000) */
    {1, 4, 0, 0, 0, 0, 0, 0}, /* 0x09 (00001001) */
    {2, 4, 0, 0, 0, 0, 0, 0}, /* 0x0A (00001010) */
    {1, 2, 4, 0, 0, 0, 0, 0}, /* 0x0B (00001011) */
    {3, 4, 0, 0, 0, 0, 0, 0}, /* 0x0C (00001100) */
    {1, 3, 4, 0, 0, 0, 0, 0}, /* 0x0D (00001101) */
    {2, 3, 4, 0, 0, 0, 0, 0}, /* 0x0E (00001110) */
    {1, 2, 3, 4, 0, 0, 0, 0}, /* 0x0F (00001111) */
    {5, 0, 0, 0, 0, 0, 0, 0}, /* 0x10 (00010000) */
    {1, 5, 0, 0, 0, 0, 0, 0}, /* 0x11 (00010001) */
    {2, 5, 0, 0, 0, 0, 0, 0}, /* 0x12 (00010010) */
    {1, 2, 5, 0, 0, 0, 0, 0}, /* 0x13 (00010011) */
    {3, 5, 0, 0, 0, 0, 0, 0}, /* 0x14 (00010100) */
    {1, 3, 5, 0, 0, 0, 0, 0}, /* 0x15 (00010101) */
    {2, 3, 5, 0, 0, 0, 0, 0}, /* 0x16 (00010110) */
    {1, 2, 3, 5, 0, 0, 0, 0}, /* 0x17 (00010111) */
    {4, 5, 0, 0, 0, 0, 0, 0}, /* 0x18 (00011000) */
    {1, 4, 5, 0, 0, 0, 0, 0}, /* 0x19 (00011001) */
    {2, 4, 5, 0, 0, 0, 0, 0}, /* 0x1A (00011010) */
    {1, 2, 4, 5, 0, 0, 0, 0}, /* 0x1B (00011011) */
    {3, 4, 5, 0, 0, 0, 0, 0}, /* 0x1C (00011100) */
    {1, 3, 4, 5, 0, 0, 0, 0}, /* 0x1D (00011101) */
    {2, 3, 4, 5, 0, 0, 0, 0}, /* 0x1E (00011110) */
    {1, 2, 3, 4, 5, 0, 0, 0}, /* 0x1F (00011111) */
    {6, 0, 0, 0, 0, 0, 0, 0}, /* 0x20 (00100000) */
    {1, 6, 0, 0, 0, 0, 0, 0}, /* 0x21 (00100001) */
    {2, 6, 0, 0, 0, 0, 0, 0}, /* 0x22 (00100010) */
    {1, 2, 6, 0, 0, 0, 0, 0}, /* 0x23 (00100011) */
    {3, 6, 0, 0, 0, 0, 0, 0}, /* 0x24 (00100100) */
    {1, 3, 6, 0, 0, 0, 0, 0}, /* 0x25 (00100101) */
    {2, 3, 6, 0, 0, 0, 0, 0}, /* 0x26 (00100110) */
    {1, 2, 3, 6, 0, 0, 0, 0}, /* 0x27 (00100111) */
    {4, 6, 0, 0, 0, 0, 0, 0}, /* 0x28 (00101000) */
    {1, 4, 6, 0, 0, 0, 0, 0}, /* 0x29 (00101001) */
    {2, 4, 6, 0, 0, 0, 0, 0}, /* 0x2A (00101010) */
    {1, 2, 4, 6, 0, 0, 0, 0}, /* 0x2B (00101011) */
    {3, 4, 6, 0, 0, 0, 0, 0}, /* 0x2C (00101100) */
    {1, 3, 4, 6, 0, 0, 0, 0}, /* 0x2D (00101101) */
    {2, 3, 4, 6, 0, 0, 0, 0}, /* 0x2E (00101110) */
    {1, 2, 3, 4, 6, 0, 0, 0}, /* 0x2F (00101111) */
    {5, 6, 0, 0, 0, 0, 0, 0}, /* 0x30 (00110000) */
    {1, 5, 6, 0, 0, 0, 0, 0}, /* 0x31 (00110001) */
    {2, 5, 6, 0, 0, 0, 0, 0}, /* 0x32 (00110010) */
    {1, 2, 5, 6, 0, 0, 0, 0}, /* 0x33 (00110011) */
    {3, 5, 6, 0, 0, 0, 0, 0}, /* 0x34 (00110100) */
    {1, 3, 5, 6, 0, 0, 0, 0}, /* 0x35 (00110101) */
    {2, 3, 5, 6, 0, 0, 0, 0}, /* 0x36 (00110110) */
    {1, 2, 3, 5, 6, 0, 0, 0}, /* 0x37 (00110111) */
    {4, 5, 6, 0, 0, 0, 0, 0}, /* 0x38 (00111000) */
    {1, 4, 5, 6, 0, 0, 0, 0}, /* 0x39 (00111001) */
    {2, 4, 5, 6, 0, 0, 0, 0}, /* 0x3A (00111010) */
    {1, 2, 4, 5, 6, 0, 0, 0}, /* 0x3B (00111011) */
    {3, 4, 5, 6, 0, 0, 0, 0}, /* 0x3C (00111100) */
    {1, 3, 4, 5, 6, 0, 0, 0}, /* 0x3D (00111101) */
    {2, 3, 4, 5, 6, 0, 0, 0}, /* 0x3E (00111110) */
    {1, 2, 3, 4, 5, 6, 0, 0}, /* 0x3F (00111111) */
    {7, 0, 0, 0, 0, 0, 0, 0}, /* 0x40 (01000000) */
    {1, 7, 0, 0, 0, 0, 0, 0}, /* 0x41 (01000001) */
    {2, 7, 0, 0, 0, 0, 0, 0}, /* 0x42 (01000010) */
    {1, 2, 7, 0, 0, 0, 0, 0}, /* 0x43 (01000011) */
    {3, 7, 0, 0, 0, 0, 0, 0}, /* 0x44 (01000100) */
    {1, 3, 7, 0, 0, 0, 0, 0}, /* 0x45 (01000101) */
    {2, 3, 7, 0, 0, 0, 0, 0}, /* 0x46 (01000110) */
    {1, 2, 3, 7, 0, 0, 0, 0}, /* 0x47 (01000111) */
    {4, 7, 0, 0, 0, 0, 0, 0}, /* 0x48 (01001000) */
    {1, 4, 7, 0, 0, 0, 0, 0}, /* 0x49 (01001001) */
    {2, 4, 7, 0, 0, 0, 0, 0}, /* 0x4A (01001010) */
    {1, 2, 4, 7, 0, 0, 0, 0}, /* 0x4B (01001011) */
    {3, 4, 7, 0, 0, 0, 0, 0}, /* 0x4C (01001100) */
    {1, 3, 4, 7, 0, 0, 0, 0}, /* 0x4D (01001101) */
    {2, 3, 4, 7, 0, 0, 0, 0}, /* 0x4E (01001110) */
    {1, 2, 3, 4, 7, 0, 0, 0}, /* 0x4F (01001111) */
    {5, 7, 0, 0, 0, 0, 0, 0}, /* 0x50 (01010000) */
    {1, 5, 7, 0, 0, 0, 0, 0}, /* 0x51 (01010001) */
    {2, 5, 7, 0, 0, 0, 0, 0}, /* 0x52 (01010010) */
    {1, 2, 5, 7, 0, 0, 0, 0}, /* 0x53 (01010011) */
    {3, 5, 7, 0, 0, 0, 0, 0}, /* 0x54 (01010100) */
    {1, 3, 5, 7, 0, 0, 0, 0}, /* 0x55 (01010101) */
    {2, 3, 5, 7, 0, 0, 0, 0}, /* 0x56 (01010110) */
    {1, 2, 3, 5, 7, 0, 0, 0}, /* 0x57 (01010111) */
    {4, 5, 7, 0, 0, 0, 0, 0}, /* 0x58 (01011000) */
    {1, 4, 5, 7, 0, 0, 0, 0}, /* 0x59 (01011001) */
    {2, 4, 5, 7, 0, 0, 0, 0}, /* 0x5A (01011010) */
    {1, 2, 4, 5, 7, 0, 0, 0}, /* 0x5B (01011011) */
    {3, 4, 5, 7, 0, 0, 0, 0}, /* 0x5C (01011100) */
    {1, 3, 4, 5, 7, 0, 0, 0}, /* 0x5D (01011101) */
    {2, 3, 4, 5, 7, 0, 0, 0}, /* 0x5E (01011110) */
    {1, 2, 3, 4, 5, 7, 0, 0}, /* 0x5F (01011111) */
    {6, 7, 0, 0, 0, 0, 0, 0}, /* 0x60 (01100000) */
    {1, 6, 7, 0, 0, 0, 0, 0}, /* 0x61 (01100001) */
    {2, 6, 7, 0, 0, 0, 0, 0}, /* 0x62 (01100010) */
    {1, 2, 6, 7, 0, 0, 0, 0}, /* 0x63 (01100011) */
    {3, 6, 7, 0, 0, 0, 0, 0}, /* 0x64 (01100100) */
    {1, 3, 6, 7, 0, 0, 0, 0}, /* 0x65 (01100101) */
    {2, 3, 6, 7, 0, 0, 0, 0}, /* 0x66 (01100110) */
    {1, 2, 3, 6, 7, 0, 0, 0}, /* 0x67 (01100111) */
    {4, 6, 7, 0, 0, 0, 0, 0}, /* 0x68 (01101000) */
    {1, 4, 6, 7, 0, 0, 0, 0}, /* 0x69 (01101001) */
    {2, 4, 6, 7, 0, 0, 0, 0}, /* 0x6A (01101010) */
    {1, 2, 4, 6, 7, 0, 0, 0}, /* 0x6B (01101011) */
    {3, 4, 6, 7, 0, 0, 0, 0}, /* 0x6C (01101100) */
    {1, 3, 4, 6, 7, 0, 0, 0}, /* 0x6D (01101101) */
    {2, 3, 4, 6, 7, 0, 0, 0}, /* 0x6E (01101110) */
    {1, 2, 3, 4, 6, 7, 0, 0}, /* 0x6F (01101111) */
    {5, 6, 7, 0, 0, 0, 0, 0}, /* 0x70 (01110000) */
    {1, 5, 6, 7, 0, 0, 0, 0}, /* 0x71 (01110001) */
    {2, 5, 6, 7, 0, 0, 0, 0}, /* 0x72 (01110010) */
    {1, 2, 5, 6, 7, 0, 0, 0}, /* 0x73 (01110011) */
    {3, 5, 6, 7, 0, 0, 0, 0}, /* 0x74 (01110100) */
    {1, 3, 5, 6, 7, 0, 0, 0}, /* 0x75 (01110101) */
    {2, 3, 5, 6, 7, 0, 0, 0}, /* 0x76 (01110110) */
    {1, 2, 3, 5, 6, 7, 0, 0}, /* 0x77 (01110111) */
    {4, 5, 6, 7, 0, 0, 0, 0}, /* 0x78 (01111000) */
    {1, 4, 5, 6, 7, 0, 0, 0}, /* 0x79 (01111001) */
    {2, 4, 5, 6, 7, 0, 0, 0}, /* 0x7A (01111010) */
    {1, 2, 4, 5, 6, 7, 0, 0}, /* 0x7B (01111011) */
    {3, 4, 5, 6, 7, 0, 0, 0}, /* 0x7C (01111100) */
    {1, 3, 4, 5, 6, 7, 0, 0}, /* 0x7D (01111101) */
    {2, 3, 4, 5, 6, 7, 0, 0}, /* 0x7E (01111110) */
    {1, 2, 3, 4, 5, 6, 7, 0}, /* 0x7F (01111111) */
    {8, 0, 0, 0, 0, 0, 0, 0}, /* 0x80 (10000000) */
    {1, 8, 0, 0, 0, 0, 0, 0}, /* 0x81 (10000001) */
    {2, 8, 0, 0, 0, 0, 0, 0}, /* 0x82 (10000010) */
    {1, 2, 8, 0, 0, 0, 0, 0}, /* 0x83 (10000011) */
    {3, 8, 0, 0, 0, 0, 0, 0}, /* 0x84 (10000100) */
    {1, 3, 8, 0, 0, 0, 0, 0}, /* 0x85 (10000101) */
    {2, 3, 8, 0, 0, 0, 0, 0}, /* 0x86 (10000110) */
    {1, 2, 3, 8, 0, 0, 0, 0}, /* 0x87 (10000111) */
    {4, 8, 0, 0, 0, 0, 0, 0}, /* 0x88 (10001000) */
    {1, 4, 8, 0, 0, 0, 0, 0}, /* 0x89 (10001001) */
    {2, 4, 8, 0, 0, 0, 0, 0}, /* 0x8A (10001010) */
    {1, 2, 4, 8, 0, 0, 0, 0}, /* 0x8B (10001011) */
    {3, 4, 8, 0, 0, 0, 0, 0}, /* 0x8C (10001100) */
    {1, 3, 4, 8, 0, 0, 0, 0}, /* 0x8D (10001101) */
    {2, 3, 4, 8, 0, 0, 0, 0}, /* 0x8E (10001110) */
    {1, 2, 3, 4, 8, 0, 0, 0}, /* 0x8F (10001111) */
    {5, 8, 0, 0, 0, 0, 0, 0}, /* 0x90 (10010000) */
    {1, 5, 8, 0, 0, 0, 0, 0}, /* 0x91 (10010001) */
    {2, 5, 8, 0, 0, 0, 0, 0}, /* 0x92 (10010010) */
    {1, 2, 5, 8, 0, 0, 0, 0}, /* 0x93 (10010011) */
    {3, 5, 8, 0, 0, 0, 0, 0}, /* 0x94 (10010100) */
    {1, 3, 5, 8, 0, 0, 0, 0}, /* 0x95 (10010101) */
    {2, 3, 5, 8, 0, 0, 0, 0}, /* 0x96 (10010110) */
    {1, 2, 3, 5, 8, 0, 0, 0}, /* 0x97 (10010111) */
    {4, 5, 8, 0, 0, 0, 0, 0}, /* 0x98 (10011000) */
    {1, 4, 5, 8, 0, 0, 0, 0}, /* 0x99 (10011001) */
    {2, 4, 5, 8, 0, 0, 0, 0}, /* 0x9A (10011010) */
    {1, 2, 4, 5, 8, 0, 0, 0}, /* 0x9B (10011011) */
    {3, 4, 5, 8, 0, 0, 0, 0}, /* 0x9C (10011100) */
    {1, 3, 4, 5, 8, 0, 0, 0}, /* 0x9D (10011101) */
    {2, 3, 4, 5, 8, 0, 0, 0}, /* 0x9E (10011110) */
    {1, 2, 3, 4, 5, 8, 0, 0}, /* 0x9F (10011111) */
    {6, 8, 0, 0, 0, 0, 0, 0}, /* 0xA0 (10100000) */
    {1, 6, 8, 0, 0, 0, 0, 0}, /* 0xA1 (10100001) */
    {2, 6, 8, 0, 0, 0, 0, 0}, /* 0xA2 (10100010) */
    {1, 2, 6, 8, 0, 0, 0, 0}, /* 0xA3 (10100011) */
    {3, 6, 8, 0, 0, 0, 0, 0}, /* 0xA4 (10100100) */
    {1, 3, 6, 8, 0, 0, 0, 0}, /* 0xA5 (10100101) */
    {2, 3, 6, 8, 0, 0, 0, 0}, /* 0xA6 (10100110) */
    {1, 2, 3, 6, 8, 0, 0, 0}, /* 0xA7 (10100111) */
    {4, 6, 8, 0, 0, 0, 0, 0}, /* 0xA8 (10101000) */
    {1, 4, 6, 8, 0, 0, 0, 0}, /* 0xA9 (10101001) */
    {2, 4, 6, 8, 0, 0, 0, 0}, /* 0xAA (10101010) */
    {1, 2, 4, 6, 8, 0, 0, 0}, /* 0xAB (10101011) */
    {3, 4, 6, 8, 0, 0, 0, 0}, /* 0xAC (10101100) */
    {1, 3, 4, 6, 8, 0, 0, 0}, /* 0xAD (10101101) */
    {2, 3, 4, 6, 8, 0, 0, 0}, /* 0xAE (10101110) */
    {1, 2, 3, 4, 6, 8, 0, 0}, /* 0xAF (10101111) */
    {5, 6, 8, 0, 0, 0, 0, 0}, /* 0xB0 (10110000) */
    {1, 5, 6, 8, 0, 0, 0, 0}, /* 0xB1 (10110001) */
    {2, 5, 6, 8, 0, 0, 0, 0}, /* 0xB2 (10110010) */
    {1, 2, 5, 6, 8, 0, 0, 0}, /* 0xB3 (10110011) */
    {3, 5, 6, 8, 0, 0, 0, 0}, /* 0xB4 (10110100) */
    {1, 3, 5, 6, 8, 0, 0, 0}, /* 0xB5 (10110101) */
    {2, 3, 5, 6, 8, 0, 0, 0}, /* 0xB6 (10110110) */
    {1, 2, 3, 5, 6, 8, 0, 0}, /* 0xB7 (10110111) */
    {4, 5, 6, 8, 0, 0, 0, 0}, /* 0xB8 (10111000) */
    {1, 4, 5, 6, 8, 0, 0, 0}, /* 0xB9 (10111001) */
    {2, 4, 5, 6, 8, 0, 0, 0}, /* 0xBA (10111010) */
    {1, 2, 4, 5, 6, 8, 0, 0}, /* 0xBB (10111011) */
    {3, 4, 5, 6, 8, 0, 0, 0}, /* 0xBC (10111100) */
    {1, 3, 4, 5, 6, 8, 0, 0}, /* 0xBD (10111101) */
    {2, 3, 4, 5, 6, 8, 0, 0}, /* 0xBE (10111110) */
    {1, 2, 3, 4, 5, 6, 8, 0}, /* 0xBF (10111111) */
    {7, 8, 0, 0, 0, 0, 0, 0}, /* 0xC0 (11000000) */
    {1, 7, 8, 0, 0, 0, 0, 0}, /* 0xC1 (11000001) */
    {2, 7, 8, 0, 0, 0, 0, 0}, /* 0xC2 (11000010) */
    {1, 2, 7, 8, 0, 0, 0, 0}, /* 0xC3 (11000011) */
    {3, 7, 8, 0, 0, 0, 0, 0}, /* 0xC4 (11000100) */
    {1, 3, 7, 8, 0, 0, 0, 0}, /* 0xC5 (11000101) */
    {2, 3, 7, 8, 0, 0, 0, 0}, /* 0xC6 (11000110) */
    {1, 2, 3, 7, 8, 0, 0, 0}, /* 0xC7 (11000111) */
    {4, 7, 8, 0, 0, 0, 0, 0}, /* 0xC8 (11001000) */
    {1, 4, 7, 8, 0, 0, 0, 0}, /* 0xC9 (11001001) */
    {2, 4, 7, 8, 0, 0, 0, 0}, /* 0xCA (11001010) */
    {1, 2, 4, 7, 8, 0, 0, 0}, /* 0xCB (11001011) */
    {3, 4, 7, 8, 0, 0, 0, 0}, /* 0xCC (11001100) */
    {1, 3, 4, 7, 8, 0, 0, 0}, /* 0xCD (11001101) */
    {2, 3, 4, 7, 8, 0, 0, 0}, /* 0xCE (11001110) */
    {1, 2, 3, 4, 7, 8, 0, 0}, /* 0xCF (11001111) */
    {5, 7, 8, 0, 0, 0, 0, 0}, /* 0xD0 (11010000) */
    {1, 5, 7, 8, 0, 0, 0, 0}, /* 0xD1 (11010001) */
    {2, 5, 7, 8, 0, 0, 0, 0}, /* 0xD2 (11010010) */
    {1, 2, 5, 7, 8, 0, 0, 0}, /* 0xD3 (11010011) */
    {3, 5, 7, 8, 0, 0, 0, 0}, /* 0xD4 (11010100) */
    {1, 3, 5, 7, 8, 0, 0, 0}, /* 0xD5 (11010101) */
    {2, 3, 5, 7, 8, 0, 0, 0}, /* 0xD6 (11010110) */
    {1, 2, 3, 5, 7, 8, 0, 0}, /* 0xD7 (11010111) */
    {4, 5, 7, 8, 0, 0, 0, 0}, /* 0xD8 (11011000) */
    {1, 4, 5, 7, 8, 0, 0, 0}, /* 0xD9 (11011001) */
    {2, 4, 5, 7, 8, 0, 0, 0}, /* 0xDA (11011010) */
    {1, 2, 4, 5, 7, 8, 0, 0}, /* 0xDB (11011011) */
    {3, 4, 5, 7, 8, 0, 0, 0}, /* 0xDC (11011100) */
    {1, 3, 4, 5, 7, 8, 0, 0}, /* 0xDD (11011101) */
    {2, 3, 4, 5, 7, 8, 0, 0}, /* 0xDE (11011110) */
    {1, 2, 3, 4, 5, 7, 8, 0}, /* 0xDF (11011111) */
    {6, 7, 8, 0, 0, 0, 0, 0}, /* 0xE0 (11100000) */
    {1, 6, 7, 8, 0, 0, 0, 0}, /* 0xE1 (11100001) */
    {2, 6, 7, 8, 0, 0, 0, 0}, /* 0xE2 (11100010) */
    {1, 2, 6, 7, 8, 0, 0, 0}, /* 0xE3 (11100011) */
    {3, 6, 7, 8, 0, 0, 0, 0}, /* 0xE4 (11100100) */
    {1, 3, 6, 7, 8, 0, 0, 0}, /* 0xE5 (11100101) */
    {2, 3, 6, 7, 8, 0, 0, 0}, /* 0xE6 (11100110) */
    {1, 2, 3, 6, 7, 8, 0, 0}, /* 0xE7 (11100111) */
    {4, 6, 7, 8, 0, 0, 0, 0}, /* 0xE8 (11101000) */
    {1, 4, 6, 7, 8, 0, 0, 0}, /* 0xE9 (11101001) */
    {2, 4, 6, 7, 8, 0, 0, 0}, /* 0xEA (11101010) */
    {1, 2, 4, 6, 7, 8, 0, 0}, /* 0xEB (11101011) */
    {3, 4, 6, 7, 8, 0, 0, 0}, /* 0xEC (11101100) */
    {1, 3, 4, 6, 7, 8, 0, 0}, /* 0xED (11101101) */
    {2, 3, 4, 6, 7, 8, 0, 0}, /* 0xEE (11101110) */
    {1, 2, 3, 4, 6, 7, 8, 0}, /* 0xEF (11101111) */
    {5, 6, 7, 8, 0, 0, 0, 0}, /* 0xF0 (11110000) */
    {1, 5, 6, 7, 8, 0, 0, 0}, /* 0xF1 (11110001) */
    {2, 5, 6, 7, 8, 0, 0, 0}, /* 0xF2 (11110010) */
    {1, 2, 5, 6, 7, 8, 0, 0}, /* 0xF3 (11110011) */
    {3, 5, 6, 7, 8, 0, 0, 0}, /* 0xF4 (11110100) */
    {1, 3, 5, 6, 7, 8, 0, 0}, /* 0xF5 (11110101) */
    {2, 3, 5, 6, 7, 8, 0, 0}, /* 0xF6 (11110110) */
    {1, 2, 3, 5, 6, 7, 8, 0}, /* 0xF7 (11110111) */
    {4, 5, 6, 7, 8, 0, 0, 0}, /* 0xF8 (11111000) */
    {1, 4, 5, 6, 7, 8, 0, 0}, /* 0xF9 (11111001) */
    {2, 4, 5, 6, 7, 8, 0, 0}, /* 0xFA (11111010) */
    {1, 2, 4, 5, 6, 7, 8, 0}, /* 0xFB (11111011) */
    {3, 4, 5, 6, 7, 8, 0, 0}, /* 0xFC (11111100) */
    {1, 3, 4, 5, 6, 7, 8, 0}, /* 0xFD (11111101) */
    {2, 3, 4, 5, 6, 7, 8, 0}, /* 0xFE (11111110) */
    {1, 2, 3, 4, 5, 6, 7, 8}  /* 0xFF (11111111) */
};

#endif

#ifdef USEAVX

size_t bitset_extract_setbits_avx2(uint64_t *array, size_t length, void *vout,
                                   size_t outcapacity, uint32_t base) {
    uint32_t *out = (uint32_t *)vout;
    uint32_t *initout = out;
    __m256i baseVec = _mm256_set1_epi32(base - 1);
    __m256i incVec = _mm256_set1_epi32(64);
    __m256i add8 = _mm256_set1_epi32(8);
    uint32_t *safeout = out + outcapacity;
    size_t i = 0;
    for (; (i < length) && (out + 64 <= safeout); ++i) {
        uint64_t w = array[i];
        if (w == 0) {
            baseVec = _mm256_add_epi32(baseVec, incVec);
        } else {
            for (int k = 0; k < 4; ++k) {
                uint8_t byteA = (uint8_t)w;
                uint8_t byteB = (uint8_t)(w >> 8);
                w >>= 16;
                __m256i vecA =
                    _mm256_load_si256((const __m256i *)vecDecodeTable[byteA]);
                __m256i vecB =
                    _mm256_load_si256((const __m256i *)vecDecodeTable[byteB]);
                uint8_t advanceA = lengthTable[byteA];
                uint8_t advanceB = lengthTable[byteB];
                vecA = _mm256_add_epi32(baseVec, vecA);
                baseVec = _mm256_add_epi32(baseVec, add8);
                vecB = _mm256_add_epi32(baseVec, vecB);
                baseVec = _mm256_add_epi32(baseVec, add8);
                _mm256_storeu_si256((__m256i *)out, vecA);
                out += advanceA;
                _mm256_storeu_si256((__m256i *)out, vecB);
                out += advanceB;
            }
        }
    }
    base += i * 64;
    for (; (i < length) && (out < safeout); ++i) {
        uint64_t w = array[i];
        while ((w != 0) && (out < safeout)) {
            uint64_t t = w & (~w + 1); // on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
            int r = __builtin_ctzll(w); // on x64, should compile to TZCNT
            uint32_t val = r + base;
            memcpy(out, &val,
                   sizeof(uint32_t));  // should be compiled as a MOV on x64
            out++;
            w ^= t;
        }
        base += 64;
    }
    return out - initout;
}
#endif  // USEAVX

size_t bitset_extract_setbits(uint64_t *bitset, size_t length, void *vout,
                              uint32_t base) {
    int outpos = 0;
    uint32_t *out = (uint32_t *)vout;
    for (size_t i = 0; i < length; ++i) {
        uint64_t w = bitset[i];
        while (w != 0) {
            uint64_t t = w & (~w + 1); // on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
            int r = __builtin_ctzll(w); // on x64, should compile to TZCNT
            uint32_t val = r + base;
            memcpy(out + outpos, &val,
                   sizeof(uint32_t));  // should be compiled as a MOV on x64
            outpos++;
            w ^= t;
        }
        base += 64;
    }
    return outpos;
}

size_t bitset_extract_intersection_setbits_uint16(const uint64_t * __restrict__ bitset1,
                                                  const uint64_t * __restrict__ bitset2,
                                                  size_t length, uint16_t *out,
                                                  uint16_t base) {
    int outpos = 0;
    for (size_t i = 0; i < length; ++i) {
        uint64_t w = bitset1[i] & bitset2[i];
        while (w != 0) {
            uint64_t t = w & (~w + 1);
            int r = __builtin_ctzll(w);
            out[outpos++] = r + base;
            w ^= t;
        }
        base += 64;
    }
    return outpos;
}

#ifdef IS_X64
/*
 * Given a bitset containing "length" 64-bit words, write out the position
 * of all the set bits to "out" as 16-bit integers, values start at "base" (can
 *be set to zero).
 *
 * The "out" pointer should be sufficient to store the actual number of bits
 *set.
 *
 * Returns how many values were actually decoded.
 *
 * This function uses SSE decoding.
 */
size_t bitset_extract_setbits_sse_uint16(const uint64_t *bitset, size_t length,
                                         uint16_t *out, size_t outcapacity,
                                         uint16_t base) {
    uint16_t *initout = out;
    __m128i baseVec = _mm_set1_epi16(base - 1);
    __m128i incVec = _mm_set1_epi16(64);
    __m128i add8 = _mm_set1_epi16(8);
    uint16_t *safeout = out + outcapacity;
    const int numberofbytes = 2;  // process two bytes at a time
    size_t i = 0;
    for (; (i < length) && (out + numberofbytes * 8 <= safeout); ++i) {
        uint64_t w = bitset[i];
        if (w == 0) {
            baseVec = _mm_add_epi16(baseVec, incVec);
        } else {
            for (int k = 0; k < 4; ++k) {
                uint8_t byteA = (uint8_t)w;
                uint8_t byteB = (uint8_t)(w >> 8);
                w >>= 16;
                __m128i vecA = _mm_load_si128(
                    (const __m128i *)vecDecodeTable_uint16[byteA]);
                __m128i vecB = _mm_load_si128(
                    (const __m128i *)vecDecodeTable_uint16[byteB]);
                uint8_t advanceA = lengthTable[byteA];
                uint8_t advanceB = lengthTable[byteB];
                vecA = _mm_add_epi16(baseVec, vecA);
                baseVec = _mm_add_epi16(baseVec, add8);
                vecB = _mm_add_epi16(baseVec, vecB);
                baseVec = _mm_add_epi16(baseVec, add8);
                _mm_storeu_si128((__m128i *)out, vecA);
                out += advanceA;
                _mm_storeu_si128((__m128i *)out, vecB);
                out += advanceB;
            }
        }
    }
    base += (uint16_t)(i * 64);
    for (; (i < length) && (out < safeout); ++i) {
        uint64_t w = bitset[i];
        while ((w != 0) && (out < safeout)) {
            uint64_t t = w & (~w + 1);
            int r = __builtin_ctzll(w);
            *out = r + base;
            out++;
            w ^= t;
        }
        base += 64;
    }
    return out - initout;
}
#endif

/*
 * Given a bitset containing "length" 64-bit words, write out the position
 * of all the set bits to "out", values start at "base" (can be set to zero).
 *
 * The "out" pointer should be sufficient to store the actual number of bits
 *set.
 *
 * Returns how many values were actually decoded.
 */
size_t bitset_extract_setbits_uint16(const uint64_t *bitset, size_t length,
                                     uint16_t *out, uint16_t base) {
    int outpos = 0;
    for (size_t i = 0; i < length; ++i) {
        uint64_t w = bitset[i];
        while (w != 0) {
            uint64_t t = w & (~w + 1);
            int r = __builtin_ctzll(w);
            out[outpos++] = r + base;
            w ^= t;
        }
        base += 64;
    }
    return outpos;
}

#if defined(ASMBITMANIPOPTIMIZATION)

uint64_t bitset_set_list_withcard(void *bitset, uint64_t card,
                                  const uint16_t *list, uint64_t length) {
    uint64_t offset, load, pos;
    uint64_t shift = 6;
    const uint16_t *end = list + length;
    if (!length) return card;
    // TODO: could unroll for performance, see bitset_set_list
    // bts is not available as an intrinsic in GCC
    __asm volatile(
        "1:\n"
        "movzwq (%[list]), %[pos]\n"
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "bts %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)\n"
        "sbb $-1, %[card]\n"
        "add $2, %[list]\n"
        "cmp %[list], %[end]\n"
        "jnz 1b"
        : [card] "+&r"(card), [list] "+&r"(list), [load] "=&r"(load),
          [pos] "=&r"(pos), [offset] "=&r"(offset)
        : [end] "r"(end), [bitset] "r"(bitset), [shift] "r"(shift));
    return card;
}

void bitset_set_list(void *bitset, const uint16_t *list, uint64_t length) {
    uint64_t pos;
    const uint16_t *end = list + length;

    uint64_t shift = 6;
    uint64_t offset;
    uint64_t load;
    for (; list + 3 < end; list += 4) {
        pos = list[0];
        __asm volatile(
            "shrx %[shift], %[pos], %[offset]\n"
            "mov (%[bitset],%[offset],8), %[load]\n"
            "bts %[pos], %[load]\n"
            "mov %[load], (%[bitset],%[offset],8)"
            : [load] "=&r"(load), [offset] "=&r"(offset)
            : [bitset] "r"(bitset), [shift] "r"(shift), [pos] "r"(pos));
        pos = list[1];
        __asm volatile(
            "shrx %[shift], %[pos], %[offset]\n"
            "mov (%[bitset],%[offset],8), %[load]\n"
            "bts %[pos], %[load]\n"
            "mov %[load], (%[bitset],%[offset],8)"
            : [load] "=&r"(load), [offset] "=&r"(offset)
            : [bitset] "r"(bitset), [shift] "r"(shift), [pos] "r"(pos));
        pos = list[2];
        __asm volatile(
            "shrx %[shift], %[pos], %[offset]\n"
            "mov (%[bitset],%[offset],8), %[load]\n"
            "bts %[pos], %[load]\n"
            "mov %[load], (%[bitset],%[offset],8)"
            : [load] "=&r"(load), [offset] "=&r"(offset)
            : [bitset] "r"(bitset), [shift] "r"(shift), [pos] "r"(pos));
        pos = list[3];
        __asm volatile(
            "shrx %[shift], %[pos], %[offset]\n"
            "mov (%[bitset],%[offset],8), %[load]\n"
            "bts %[pos], %[load]\n"
            "mov %[load], (%[bitset],%[offset],8)"
            : [load] "=&r"(load), [offset] "=&r"(offset)
            : [bitset] "r"(bitset), [shift] "r"(shift), [pos] "r"(pos));
    }

    while (list != end) {
        pos = list[0];
        __asm volatile(
            "shrx %[shift], %[pos], %[offset]\n"
            "mov (%[bitset],%[offset],8), %[load]\n"
            "bts %[pos], %[load]\n"
            "mov %[load], (%[bitset],%[offset],8)"
            : [load] "=&r"(load), [offset] "=&r"(offset)
            : [bitset] "r"(bitset), [shift] "r"(shift), [pos] "r"(pos));
        list++;
    }
}

uint64_t bitset_clear_list(void *bitset, uint64_t card, const uint16_t *list,
                           uint64_t length) {
    uint64_t offset, load, pos;
    uint64_t shift = 6;
    const uint16_t *end = list + length;
    if (!length) return card;
    // btr is not available as an intrinsic in GCC
    __asm volatile(
        "1:\n"
        "movzwq (%[list]), %[pos]\n"
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "btr %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)\n"
        "sbb $0, %[card]\n"
        "add $2, %[list]\n"
        "cmp %[list], %[end]\n"
        "jnz 1b"
        : [card] "+&r"(card), [list] "+&r"(list), [load] "=&r"(load),
          [pos] "=&r"(pos), [offset] "=&r"(offset)
        : [end] "r"(end), [bitset] "r"(bitset), [shift] "r"(shift)
        :
        /* clobbers */ "memory");
    return card;
}

#else
uint64_t bitset_clear_list(void *bitset, uint64_t card, const uint16_t *list,
                           uint64_t length) {
    uint64_t offset, load, newload, pos, index;
    const uint16_t *end = list + length;
    while (list != end) {
        pos = *(const uint16_t *)list;
        offset = pos >> 6;
        index = pos % 64;
        load = ((uint64_t *)bitset)[offset];
        newload = load & ~(UINT64_C(1) << index);
        card -= (load ^ newload) >> index;
        ((uint64_t *)bitset)[offset] = newload;
        list++;
    }
    return card;
}

uint64_t bitset_set_list_withcard(void *bitset, uint64_t card,
                                  const uint16_t *list, uint64_t length) {
    uint64_t offset, load, newload, pos, index;
    const uint16_t *end = list + length;
    while (list != end) {
        pos = *(const uint16_t *)list;
        offset = pos >> 6;
        index = pos % 64;
        load = ((uint64_t *)bitset)[offset];
        newload = load | (UINT64_C(1) << index);
        card += (load ^ newload) >> index;
        ((uint64_t *)bitset)[offset] = newload;
        list++;
    }
    return card;
}

void bitset_set_list(void *bitset, const uint16_t *list, uint64_t length) {
    uint64_t offset, load, newload, pos, index;
    const uint16_t *end = list + length;
    while (list != end) {
        pos = *(const uint16_t *)list;
        offset = pos >> 6;
        index = pos % 64;
        load = ((uint64_t *)bitset)[offset];
        newload = load | (UINT64_C(1) << index);
        ((uint64_t *)bitset)[offset] = newload;
        list++;
    }
}

#endif

/* flip specified bits */
/* TODO: consider whether worthwhile to make an asm version */

uint64_t bitset_flip_list_withcard(void *bitset, uint64_t card,
                                   const uint16_t *list, uint64_t length) {
    uint64_t offset, load, newload, pos, index;
    const uint16_t *end = list + length;
    while (list != end) {
        pos = *(const uint16_t *)list;
        offset = pos >> 6;
        index = pos % 64;
        load = ((uint64_t *)bitset)[offset];
        newload = load ^ (UINT64_C(1) << index);
        // todo: is a branch here all that bad?
        card +=
            (1 - 2 * (((UINT64_C(1) << index) & load) >> index));  // +1 or -1
        ((uint64_t *)bitset)[offset] = newload;
        list++;
    }
    return card;
}

void bitset_flip_list(void *bitset, const uint16_t *list, uint64_t length) {
    uint64_t offset, load, newload, pos, index;
    const uint16_t *end = list + length;
    while (list != end) {
        pos = *(const uint16_t *)list;
        offset = pos >> 6;
        index = pos % 64;
        load = ((uint64_t *)bitset)[offset];
        newload = load ^ (UINT64_C(1) << index);
        ((uint64_t *)bitset)[offset] = newload;
        list++;
    }
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/bitset_util.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/array.c */
/*
 * array.c
 *
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

extern inline uint16_t array_container_minimum(const array_container_t *arr);
extern inline uint16_t array_container_maximum(const array_container_t *arr);
extern inline int array_container_index_equalorlarger(const array_container_t *arr, uint16_t x);

extern inline int array_container_rank(const array_container_t *arr,
                                       uint16_t x);
extern inline bool array_container_contains(const array_container_t *arr,
                                            uint16_t pos);
extern int array_container_cardinality(const array_container_t *array);
extern bool array_container_nonzero_cardinality(const array_container_t *array);
extern void array_container_clear(array_container_t *array);
extern int32_t array_container_serialized_size_in_bytes(int32_t card);
extern bool array_container_empty(const array_container_t *array);
extern bool array_container_full(const array_container_t *array);

/* Create a new array with capacity size. Return NULL in case of failure. */
array_container_t *array_container_create_given_capacity(int32_t size) {
    array_container_t *container;

    if ((container = (array_container_t *)malloc(sizeof(array_container_t))) ==
        NULL) {
        return NULL;
    }

    if( size <= 0 ) { // we don't want to rely on malloc(0)
        container->array = NULL;
    } else if ((container->array = (uint16_t *)malloc(sizeof(uint16_t) * size)) ==
        NULL) {
        free(container);
        return NULL;
    }

    container->capacity = size;
    container->cardinality = 0;

    return container;
}

/* Create a new array. Return NULL in case of failure. */
array_container_t *array_container_create() {
    return array_container_create_given_capacity(ARRAY_DEFAULT_INIT_SIZE);
}

/* Create a new array containing all values in [min,max). */
array_container_t * array_container_create_range(uint32_t min, uint32_t max) {
    array_container_t * answer = array_container_create_given_capacity(max - min + 1);
    if(answer == NULL) return answer;
    answer->cardinality = 0;
    for(uint32_t k = min; k < max; k++) {
      answer->array[answer->cardinality++] = k;
    }
    return answer;
}

/* Duplicate container */
array_container_t *array_container_clone(const array_container_t *src) {
    array_container_t *newcontainer =
        array_container_create_given_capacity(src->capacity);
    if (newcontainer == NULL) return NULL;

    newcontainer->cardinality = src->cardinality;

    memcpy(newcontainer->array, src->array,
           src->cardinality * sizeof(uint16_t));

    return newcontainer;
}

int array_container_shrink_to_fit(array_container_t *src) {
    if (src->cardinality == src->capacity) return 0;  // nothing to do
    int savings = src->capacity - src->cardinality;
    src->capacity = src->cardinality;
    if( src->capacity == 0) { // we do not want to rely on realloc for zero allocs
      free(src->array);
      src->array = NULL;
    } else {
      uint16_t *oldarray = src->array;
      src->array =
        (uint16_t *)realloc(oldarray, src->capacity * sizeof(uint16_t));
      if (src->array == NULL) free(oldarray);  // should never happen?
    }
    return savings;
}

/* Free memory. */
void array_container_free(array_container_t *arr) {
    if(arr->array != NULL) {// Jon Strabala reports that some tools complain otherwise
      free(arr->array);
      arr->array = NULL; // pedantic
    }
    free(arr);
}

static inline int32_t grow_capacity(int32_t capacity) {
    return (capacity <= 0) ? ARRAY_DEFAULT_INIT_SIZE
                           : capacity < 64 ? capacity * 2
                                           : capacity < 1024 ? capacity * 3 / 2
                                                             : capacity * 5 / 4;
}

static inline int32_t clamp(int32_t val, int32_t min, int32_t max) {
    return ((val < min) ? min : (val > max) ? max : val);
}

void array_container_grow(array_container_t *container, int32_t min,
                          bool preserve) {

    int32_t max = (min <= DEFAULT_MAX_SIZE ? DEFAULT_MAX_SIZE : 65536);
    int32_t new_capacity = clamp(grow_capacity(container->capacity), min, max);

    container->capacity = new_capacity;
    uint16_t *array = container->array;

    if (preserve) {
        container->array =
            (uint16_t *)realloc(array, new_capacity * sizeof(uint16_t));
        if (container->array == NULL) free(array);
    } else {
        // Jon Strabala reports that some tools complain otherwise
        if (array != NULL) {
          free(array);
        }
        container->array = (uint16_t *)malloc(new_capacity * sizeof(uint16_t));
    }

    //  handle the case where realloc fails
    if (container->array == NULL) {
      fprintf(stderr, "could not allocate memory\n");
    }
    assert(container->array != NULL);
}

/* Copy one container into another. We assume that they are distinct. */
void array_container_copy(const array_container_t *src,
                          array_container_t *dst) {
    const int32_t cardinality = src->cardinality;
    if (cardinality > dst->capacity) {
        array_container_grow(dst, cardinality, false);
    }

    dst->cardinality = cardinality;
    memcpy(dst->array, src->array, cardinality * sizeof(uint16_t));
}

void array_container_add_from_range(array_container_t *arr, uint32_t min,
                                    uint32_t max, uint16_t step) {
    for (uint32_t value = min; value < max; value += step) {
        array_container_append(arr, value);
    }
}

/* Computes the union of array1 and array2 and write the result to arrayout.
 * It is assumed that arrayout is distinct from both array1 and array2.
 */
void array_container_union(const array_container_t *array_1,
                           const array_container_t *array_2,
                           array_container_t *out) {
    const int32_t card_1 = array_1->cardinality, card_2 = array_2->cardinality;
    const int32_t max_cardinality = card_1 + card_2;

    if (out->capacity < max_cardinality) {
      array_container_grow(out, max_cardinality, false);
    }
    out->cardinality = (int32_t)fast_union_uint16(array_1->array, card_1,
                                      array_2->array, card_2, out->array);

}

/* Computes the  difference of array1 and array2 and write the result
 * to array out.
 * Array out does not need to be distinct from array_1
 */
void array_container_andnot(const array_container_t *array_1,
                            const array_container_t *array_2,
                            array_container_t *out) {
    if (out->capacity < array_1->cardinality)
        array_container_grow(out, array_1->cardinality, false);
#ifdef ROARING_VECTOR_OPERATIONS_ENABLED
    out->cardinality =
        difference_vector16(array_1->array, array_1->cardinality,
                            array_2->array, array_2->cardinality, out->array);
#else
    out->cardinality =
        difference_uint16(array_1->array, array_1->cardinality, array_2->array,
                          array_2->cardinality, out->array);
#endif
}

/* Computes the symmetric difference of array1 and array2 and write the
 * result
 * to arrayout.
 * It is assumed that arrayout is distinct from both array1 and array2.
 */
void array_container_xor(const array_container_t *array_1,
                         const array_container_t *array_2,
                         array_container_t *out) {
    const int32_t card_1 = array_1->cardinality, card_2 = array_2->cardinality;
    const int32_t max_cardinality = card_1 + card_2;
    if (out->capacity < max_cardinality) {
        array_container_grow(out, max_cardinality, false);
    }

#ifdef ROARING_VECTOR_OPERATIONS_ENABLED
    out->cardinality =
        xor_vector16(array_1->array, array_1->cardinality, array_2->array,
                     array_2->cardinality, out->array);
#else
    out->cardinality =
        xor_uint16(array_1->array, array_1->cardinality, array_2->array,
                   array_2->cardinality, out->array);
#endif
}

static inline int32_t minimum_int32(int32_t a, int32_t b) {
    return (a < b) ? a : b;
}

/* computes the intersection of array1 and array2 and write the result to
 * arrayout.
 * It is assumed that arrayout is distinct from both array1 and array2.
 * */
void array_container_intersection(const array_container_t *array1,
                                  const array_container_t *array2,
                                  array_container_t *out) {
    int32_t card_1 = array1->cardinality, card_2 = array2->cardinality,
            min_card = minimum_int32(card_1, card_2);
    const int threshold = 64;  // subject to tuning
#ifdef USEAVX
    if (out->capacity < min_card) {
      array_container_grow(out, min_card + sizeof(__m128i) / sizeof(uint16_t),
        false);
    }
#else
    if (out->capacity < min_card) {
      array_container_grow(out, min_card, false);
    }
#endif

    if (card_1 * threshold < card_2) {
        out->cardinality = intersect_skewed_uint16(
            array1->array, card_1, array2->array, card_2, out->array);
    } else if (card_2 * threshold < card_1) {
        out->cardinality = intersect_skewed_uint16(
            array2->array, card_2, array1->array, card_1, out->array);
    } else {
#ifdef USEAVX
        out->cardinality = intersect_vector16(
            array1->array, card_1, array2->array, card_2, out->array);
#else
        out->cardinality = intersect_uint16(array1->array, card_1,
                                            array2->array, card_2, out->array);
#endif
    }
}

/* computes the size of the intersection of array1 and array2
 * */
int array_container_intersection_cardinality(const array_container_t *array1,
                                             const array_container_t *array2) {
    int32_t card_1 = array1->cardinality, card_2 = array2->cardinality;
    const int threshold = 64;  // subject to tuning
    if (card_1 * threshold < card_2) {
        return intersect_skewed_uint16_cardinality(array1->array, card_1,
                                                   array2->array, card_2);
    } else if (card_2 * threshold < card_1) {
        return intersect_skewed_uint16_cardinality(array2->array, card_2,
                                                   array1->array, card_1);
    } else {
#ifdef USEAVX
        return intersect_vector16_cardinality(array1->array, card_1,
                                              array2->array, card_2);
#else
        return intersect_uint16_cardinality(array1->array, card_1,
                                            array2->array, card_2);
#endif
    }
}

bool array_container_intersect(const array_container_t *array1,
                                  const array_container_t *array2) {
    int32_t card_1 = array1->cardinality, card_2 = array2->cardinality;
    const int threshold = 64;  // subject to tuning
    if (card_1 * threshold < card_2) {
        return intersect_skewed_uint16_nonempty(
            array1->array, card_1, array2->array, card_2);
    } else if (card_2 * threshold < card_1) {
    	return intersect_skewed_uint16_nonempty(
            array2->array, card_2, array1->array, card_1);
    } else {
    	// we do not bother vectorizing
        return intersect_uint16_nonempty(array1->array, card_1,
                                            array2->array, card_2);
    }
}

/* computes the intersection of array1 and array2 and write the result to
 * array1.
 * */
void array_container_intersection_inplace(array_container_t *src_1,
                                          const array_container_t *src_2) {
    // todo: can any of this be vectorized?
    int32_t card_1 = src_1->cardinality, card_2 = src_2->cardinality;
    const int threshold = 64;  // subject to tuning
    if (card_1 * threshold < card_2) {
        src_1->cardinality = intersect_skewed_uint16(
            src_1->array, card_1, src_2->array, card_2, src_1->array);
    } else if (card_2 * threshold < card_1) {
        src_1->cardinality = intersect_skewed_uint16(
            src_2->array, card_2, src_1->array, card_1, src_1->array);
    } else {
        src_1->cardinality = intersect_uint16(
            src_1->array, card_1, src_2->array, card_2, src_1->array);
    }
}

int array_container_to_uint32_array(void *vout, const array_container_t *cont,
                                    uint32_t base) {
    int outpos = 0;
    uint32_t *out = (uint32_t *)vout;
    for (int i = 0; i < cont->cardinality; ++i) {
        const uint32_t val = base + cont->array[i];
        memcpy(out + outpos, &val,
               sizeof(uint32_t));  // should be compiled as a MOV on x64
        outpos++;
    }
    return outpos;
}

void array_container_printf(const array_container_t *v) {
    if (v->cardinality == 0) {
        printf("{}");
        return;
    }
    printf("{");
    printf("%d", v->array[0]);
    for (int i = 1; i < v->cardinality; ++i) {
        printf(",%d", v->array[i]);
    }
    printf("}");
}

void array_container_printf_as_uint32_array(const array_container_t *v,
                                            uint32_t base) {
    if (v->cardinality == 0) {
        return;
    }
    printf("%u", v->array[0] + base);
    for (int i = 1; i < v->cardinality; ++i) {
        printf(",%u", v->array[i] + base);
    }
}

/* Compute the number of runs */
int32_t array_container_number_of_runs(const array_container_t *a) {
    // Can SIMD work here?
    int32_t nr_runs = 0;
    int32_t prev = -2;
    for (const uint16_t *p = a->array; p != a->array + a->cardinality; ++p) {
        if (*p != prev + 1) nr_runs++;
        prev = *p;
    }
    return nr_runs;
}

int32_t array_container_serialize(const array_container_t *container, char *buf) {
    int32_t l, off;
    uint16_t cardinality = (uint16_t)container->cardinality;

    memcpy(buf, &cardinality, off = sizeof(cardinality));
    l = sizeof(uint16_t) * container->cardinality;
    if (l) memcpy(&buf[off], container->array, l);

    return (off + l);
}

/**
 * Writes the underlying array to buf, outputs how many bytes were written.
 * The number of bytes written should be
 * array_container_size_in_bytes(container).
 *
 */
int32_t array_container_write(const array_container_t *container, char *buf) {
    memcpy(buf, container->array, container->cardinality * sizeof(uint16_t));
    return array_container_size_in_bytes(container);
}

bool array_container_equals(const array_container_t *container1,
                            const array_container_t *container2) {
    if (container1->cardinality != container2->cardinality) {
        return false;
    }
    // could be vectorized:
    for (int32_t i = 0; i < container1->cardinality; ++i) {
        if (container1->array[i] != container2->array[i]) return false;
    }
    return true;
}

bool array_container_is_subset(const array_container_t *container1,
                               const array_container_t *container2) {
    if (container1->cardinality > container2->cardinality) {
        return false;
    }
    int i1 = 0, i2 = 0;
    while (i1 < container1->cardinality && i2 < container2->cardinality) {
        if (container1->array[i1] == container2->array[i2]) {
            i1++;
            i2++;
        } else if (container1->array[i1] > container2->array[i2]) {
            i2++;
        } else {  // container1->array[i1] < container2->array[i2]
            return false;
        }
    }
    if (i1 == container1->cardinality) {
        return true;
    } else {
        return false;
    }
}

int32_t array_container_read(int32_t cardinality, array_container_t *container,
                             const char *buf) {
    if (container->capacity < cardinality) {
        array_container_grow(container, cardinality, false);
    }
    container->cardinality = cardinality;
    memcpy(container->array, buf, container->cardinality * sizeof(uint16_t));

    return array_container_size_in_bytes(container);
}

uint32_t array_container_serialization_len(const array_container_t *container) {
    return (sizeof(uint16_t) /* container->cardinality converted to 16 bit */ +
            (sizeof(uint16_t) * container->cardinality));
}

void *array_container_deserialize(const char *buf, size_t buf_len) {
    array_container_t *ptr;

    if (buf_len < 2) /* capacity converted to 16 bit */
        return (NULL);
    else
        buf_len -= 2;

    if ((ptr = (array_container_t *)malloc(sizeof(array_container_t))) !=
        NULL) {
        size_t len;
        int32_t off;
        uint16_t cardinality;

        memcpy(&cardinality, buf, off = sizeof(cardinality));

        ptr->capacity = ptr->cardinality = (uint32_t)cardinality;
        len = sizeof(uint16_t) * ptr->cardinality;

        if (len != buf_len) {
            free(ptr);
            return (NULL);
        }

        if ((ptr->array = (uint16_t *)malloc(sizeof(uint16_t) *
                                             ptr->capacity)) == NULL) {
            free(ptr);
            return (NULL);
        }

        if (len) memcpy(ptr->array, &buf[off], len);

        /* Check if returned values are monotonically increasing */
        for (int32_t i = 0, j = 0; i < ptr->cardinality; i++) {
            if (ptr->array[i] < j) {
                free(ptr->array);
                free(ptr);
                return (NULL);
            } else
                j = ptr->array[i];
        }
    }

    return (ptr);
}

bool array_container_iterate(const array_container_t *cont, uint32_t base,
                             roaring_iterator iterator, void *ptr) {
    for (int i = 0; i < cont->cardinality; i++)
        if (!iterator(cont->array[i] + base, ptr)) return false;
    return true;
}

bool array_container_iterate64(const array_container_t *cont, uint32_t base,
                               roaring_iterator64 iterator, uint64_t high_bits,
                               void *ptr) {
    for (int i = 0; i < cont->cardinality; i++)
        if (!iterator(high_bits | (uint64_t)(cont->array[i] + base), ptr))
            return false;
    return true;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/array.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/bitset.c */
/*
 * bitset.c
 *
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


extern int bitset_container_cardinality(const bitset_container_t *bitset);
extern bool bitset_container_nonzero_cardinality(bitset_container_t *bitset);
extern void bitset_container_set(bitset_container_t *bitset, uint16_t pos);
extern void bitset_container_unset(bitset_container_t *bitset, uint16_t pos);
extern inline bool bitset_container_get(const bitset_container_t *bitset,
                                        uint16_t pos);
extern int32_t bitset_container_serialized_size_in_bytes();
extern bool bitset_container_add(bitset_container_t *bitset, uint16_t pos);
extern bool bitset_container_remove(bitset_container_t *bitset, uint16_t pos);
extern inline bool bitset_container_contains(const bitset_container_t *bitset,
                                             uint16_t pos);

void bitset_container_clear(bitset_container_t *bitset) {
    memset(bitset->array, 0, sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
    bitset->cardinality = 0;
}

void bitset_container_set_all(bitset_container_t *bitset) {
    memset(bitset->array, INT64_C(-1),
           sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
    bitset->cardinality = (1 << 16);
}



/* Create a new bitset. Return NULL in case of failure. */
bitset_container_t *bitset_container_create(void) {
    bitset_container_t *bitset =
        (bitset_container_t *)malloc(sizeof(bitset_container_t));

    if (!bitset) {
        return NULL;
    }
    // sizeof(__m256i) == 32
    bitset->array = (uint64_t *)aligned_malloc(
        32, sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
    if (!bitset->array) {
        free(bitset);
        return NULL;
    }
    bitset_container_clear(bitset);
    return bitset;
}

/* Copy one container into another. We assume that they are distinct. */
void bitset_container_copy(const bitset_container_t *source,
                           bitset_container_t *dest) {
    dest->cardinality = source->cardinality;
    memcpy(dest->array, source->array,
           sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
}

void bitset_container_add_from_range(bitset_container_t *bitset, uint32_t min,
                                     uint32_t max, uint16_t step) {
    if (step == 0) return;   // refuse to crash
    if ((64 % step) == 0) {  // step divides 64
        uint64_t mask = 0;   // construct the repeated mask
        for (uint32_t value = (min % step); value < 64; value += step) {
            mask |= ((uint64_t)1 << value);
        }
        uint32_t firstword = min / 64;
        uint32_t endword = (max - 1) / 64;
        bitset->cardinality = (max - min + step - 1) / step;
        if (firstword == endword) {
            bitset->array[firstword] |=
                mask & (((~UINT64_C(0)) << (min % 64)) &
                        ((~UINT64_C(0)) >> ((~max + 1) % 64)));
            return;
        }
        bitset->array[firstword] = mask & ((~UINT64_C(0)) << (min % 64));
        for (uint32_t i = firstword + 1; i < endword; i++)
            bitset->array[i] = mask;
        bitset->array[endword] = mask & ((~UINT64_C(0)) >> ((~max + 1) % 64));
    } else {
        for (uint32_t value = min; value < max; value += step) {
            bitset_container_add(bitset, value);
        }
    }
}

/* Free memory. */
void bitset_container_free(bitset_container_t *bitset) {
    if(bitset->array != NULL) {// Jon Strabala reports that some tools complain otherwise
      aligned_free(bitset->array);
      bitset->array = NULL; // pedantic
    }
    free(bitset);
}

/* duplicate container. */
bitset_container_t *bitset_container_clone(const bitset_container_t *src) {
    bitset_container_t *bitset =
        (bitset_container_t *)malloc(sizeof(bitset_container_t));

    if (!bitset) {
        return NULL;
    }
    // sizeof(__m256i) == 32
    bitset->array = (uint64_t *)aligned_malloc(
        32, sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
    if (!bitset->array) {
        free(bitset);
        return NULL;
    }
    bitset->cardinality = src->cardinality;
    memcpy(bitset->array, src->array,
           sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
    return bitset;
}

void bitset_container_set_range(bitset_container_t *bitset, uint32_t begin,
                                uint32_t end) {
    bitset_set_range(bitset->array, begin, end);
    bitset->cardinality =
        bitset_container_compute_cardinality(bitset);  // could be smarter
}


bool bitset_container_intersect(const bitset_container_t *src_1,
                                  const bitset_container_t *src_2) {
	// could vectorize, but this is probably already quite fast in practice
    const uint64_t * __restrict__ array_1 = src_1->array;
    const uint64_t * __restrict__ array_2 = src_2->array;
	for (int i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; i ++) {
        if((array_1[i] & array_2[i]) != 0) return true;
    }
    return false;
}


#ifdef USEAVX
#ifndef WORDS_IN_AVX2_REG
#define WORDS_IN_AVX2_REG sizeof(__m256i) / sizeof(uint64_t)
#endif
/* Get the number of bits set (force computation) */
int bitset_container_compute_cardinality(const bitset_container_t *bitset) {
    return (int) avx2_harley_seal_popcount256(
        (const __m256i *)bitset->array,
        BITSET_CONTAINER_SIZE_IN_WORDS / (WORDS_IN_AVX2_REG));
}
#else

/* Get the number of bits set (force computation) */
int bitset_container_compute_cardinality(const bitset_container_t *bitset) {
    const uint64_t *array = bitset->array;
    int32_t sum = 0;
    for (int i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; i += 4) {
        sum += hamming(array[i]);
        sum += hamming(array[i + 1]);
        sum += hamming(array[i + 2]);
        sum += hamming(array[i + 3]);
    }
    return sum;
}

#endif

#ifdef USEAVX

#define BITSET_CONTAINER_FN_REPEAT 8
#ifndef WORDS_IN_AVX2_REG
#define WORDS_IN_AVX2_REG sizeof(__m256i) / sizeof(uint64_t)
#endif
#define LOOP_SIZE                    \
    BITSET_CONTAINER_SIZE_IN_WORDS / \
        ((WORDS_IN_AVX2_REG)*BITSET_CONTAINER_FN_REPEAT)

/* Computes a binary operation (eg union) on bitset1 and bitset2 and write the
   result to bitsetout */
// clang-format off
#define BITSET_CONTAINER_FN(opname, opsymbol, avx_intrinsic)            \
int bitset_container_##opname##_nocard(const bitset_container_t *src_1, \
                                       const bitset_container_t *src_2, \
                                       bitset_container_t *dst) {       \
    const uint8_t * __restrict__ array_1 = (const uint8_t *)src_1->array; \
    const uint8_t * __restrict__ array_2 = (const uint8_t *)src_2->array; \
    /* not using the blocking optimization for some reason*/            \
    uint8_t *out = (uint8_t*)dst->array;                                \
    const int innerloop = 8;                                            \
    for (size_t i = 0;                                                  \
        i < BITSET_CONTAINER_SIZE_IN_WORDS / (WORDS_IN_AVX2_REG);       \
                                                         i+=innerloop) {\
        __m256i A1, A2, AO;                                             \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1));                  \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2));                  \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)out, AO);                        \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 32));             \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 32));             \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+32), AO);                   \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 64));             \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 64));             \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+64), AO);                   \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 96));             \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 96));             \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+96), AO);                   \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 128));            \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 128));            \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+128), AO);                  \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 160));            \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 160));            \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+160), AO);                  \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 192));            \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 192));            \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+192), AO);                  \
        A1 = _mm256_lddqu_si256((const __m256i *)(array_1 + 224));            \
        A2 = _mm256_lddqu_si256((const __m256i *)(array_2 + 224));            \
        AO = avx_intrinsic(A2, A1);                                     \
        _mm256_storeu_si256((__m256i *)(out+224), AO);                  \
        out+=256;                                                       \
        array_1 += 256;                                                 \
        array_2 += 256;                                                 \
    }                                                                   \
    dst->cardinality = BITSET_UNKNOWN_CARDINALITY;                      \
    return dst->cardinality;                                            \
}                                                                       \
/* next, a version that updates cardinality*/                           \
int bitset_container_##opname(const bitset_container_t *src_1,          \
                              const bitset_container_t *src_2,          \
                              bitset_container_t *dst) {                \
    const __m256i * __restrict__ array_1 = (const __m256i *) src_1->array; \
    const __m256i * __restrict__ array_2 = (const __m256i *) src_2->array; \
    __m256i *out = (__m256i *) dst->array;                              \
    dst->cardinality = (int32_t)avx2_harley_seal_popcount256andstore_##opname(array_2,\
    		array_1, out,BITSET_CONTAINER_SIZE_IN_WORDS / (WORDS_IN_AVX2_REG));\
    return dst->cardinality;                                            \
}                                                                       \
/* next, a version that just computes the cardinality*/                 \
int bitset_container_##opname##_justcard(const bitset_container_t *src_1, \
                              const bitset_container_t *src_2) {        \
    const __m256i * __restrict__ data1 = (const __m256i *) src_1->array; \
    const __m256i * __restrict__ data2 = (const __m256i *) src_2->array; \
    return (int)avx2_harley_seal_popcount256_##opname(data2,                \
    		data1, BITSET_CONTAINER_SIZE_IN_WORDS / (WORDS_IN_AVX2_REG));\
}



#else /* not USEAVX  */

#define BITSET_CONTAINER_FN(opname, opsymbol, avxintrinsic)               \
int bitset_container_##opname(const bitset_container_t *src_1,            \
                              const bitset_container_t *src_2,            \
                              bitset_container_t *dst) {                  \
    const uint64_t * __restrict__ array_1 = src_1->array;                 \
    const uint64_t * __restrict__ array_2 = src_2->array;                 \
    uint64_t *out = dst->array;                                           \
    int32_t sum = 0;                                                      \
    for (size_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; i += 2) {      \
        const uint64_t word_1 = (array_1[i])opsymbol(array_2[i]),         \
                       word_2 = (array_1[i + 1])opsymbol(array_2[i + 1]); \
        out[i] = word_1;                                                  \
        out[i + 1] = word_2;                                              \
        sum += hamming(word_1);                                    \
        sum += hamming(word_2);                                    \
    }                                                                     \
    dst->cardinality = sum;                                               \
    return dst->cardinality;                                              \
}                                                                         \
int bitset_container_##opname##_nocard(const bitset_container_t *src_1,   \
                                       const bitset_container_t *src_2,   \
                                       bitset_container_t *dst) {         \
    const uint64_t * __restrict__ array_1 = src_1->array;                 \
    const uint64_t * __restrict__ array_2 = src_2->array;                 \
    uint64_t *out = dst->array;                                           \
    for (size_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; i++) {         \
        out[i] = (array_1[i])opsymbol(array_2[i]);                        \
    }                                                                     \
    dst->cardinality = BITSET_UNKNOWN_CARDINALITY;                        \
    return dst->cardinality;                                              \
}                                                                         \
int bitset_container_##opname##_justcard(const bitset_container_t *src_1, \
                              const bitset_container_t *src_2) {          \
    const uint64_t * __restrict__ array_1 = src_1->array;                 \
    const uint64_t * __restrict__ array_2 = src_2->array;                 \
    int32_t sum = 0;                                                      \
    for (size_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; i += 2) {      \
        const uint64_t word_1 = (array_1[i])opsymbol(array_2[i]),         \
                       word_2 = (array_1[i + 1])opsymbol(array_2[i + 1]); \
        sum += hamming(word_1);                                    \
        sum += hamming(word_2);                                    \
    }                                                                     \
    return sum;                                                           \
}

#endif

// we duplicate the function because other containers use the "or" term, makes API more consistent
BITSET_CONTAINER_FN(or, |, _mm256_or_si256)
BITSET_CONTAINER_FN(union, |, _mm256_or_si256)

// we duplicate the function because other containers use the "intersection" term, makes API more consistent
BITSET_CONTAINER_FN(and, &, _mm256_and_si256)
BITSET_CONTAINER_FN(intersection, &, _mm256_and_si256)

BITSET_CONTAINER_FN(xor, ^, _mm256_xor_si256)
BITSET_CONTAINER_FN(andnot, &~, _mm256_andnot_si256)
// clang-format On



int bitset_container_to_uint32_array( void *vout, const bitset_container_t *cont, uint32_t base) {
#ifdef USEAVX2FORDECODING
	if(cont->cardinality >= 8192)// heuristic
		return (int) bitset_extract_setbits_avx2(cont->array, BITSET_CONTAINER_SIZE_IN_WORDS, vout,cont->cardinality,base);
	else
		return (int) bitset_extract_setbits(cont->array, BITSET_CONTAINER_SIZE_IN_WORDS, vout,base);
#else
	return (int) bitset_extract_setbits(cont->array, BITSET_CONTAINER_SIZE_IN_WORDS, vout,base);
#endif
}

/*
 * Print this container using printf (useful for debugging).
 */
void bitset_container_printf(const bitset_container_t * v) {
	printf("{");
	uint32_t base = 0;
	bool iamfirst = true;// TODO: rework so that this is not necessary yet still readable
	for (int i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i) {
		uint64_t w = v->array[i];
		while (w != 0) {
			uint64_t t = w & (~w + 1);
			int r = __builtin_ctzll(w);
			if(iamfirst) {// predicted to be false
				printf("%u",base + r);
				iamfirst = false;
			} else {
				printf(",%u",base + r);
			}
			w ^= t;
		}
		base += 64;
	}
	printf("}");
}


/*
 * Print this container using printf as a comma-separated list of 32-bit integers starting at base.
 */
void bitset_container_printf_as_uint32_array(const bitset_container_t * v, uint32_t base) {
	bool iamfirst = true;// TODO: rework so that this is not necessary yet still readable
	for (int i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i) {
		uint64_t w = v->array[i];
		while (w != 0) {
			uint64_t t = w & (~w + 1);
			int r = __builtin_ctzll(w);
			if(iamfirst) {// predicted to be false
				printf("%u", r + base);
				iamfirst = false;
			} else {
				printf(",%u",r + base);
			}
			w ^= t;
		}
		base += 64;
	}
}


// TODO: use the fast lower bound, also
int bitset_container_number_of_runs(bitset_container_t *b) {
  int num_runs = 0;
  uint64_t next_word = b->array[0];

  for (int i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS-1; ++i) {
    uint64_t word = next_word;
    next_word = b->array[i+1];
    num_runs += hamming((~word) & (word << 1)) + ( (word >> 63) & ~next_word);
  }

  uint64_t word = next_word;
  num_runs += hamming((~word) & (word << 1));
  if((word & 0x8000000000000000ULL) != 0)
    num_runs++;
  return num_runs;
}

int32_t bitset_container_serialize(const bitset_container_t *container, char *buf) {
  int32_t l = sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS;
  memcpy(buf, container->array, l);
  return(l);
}



int32_t bitset_container_write(const bitset_container_t *container,
                                  char *buf) {
	memcpy(buf, container->array, BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t));
	return bitset_container_size_in_bytes(container);
}


int32_t bitset_container_read(int32_t cardinality, bitset_container_t *container,
		const char *buf)  {
	container->cardinality = cardinality;
	memcpy(container->array, buf, BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t));
	return bitset_container_size_in_bytes(container);
}

uint32_t bitset_container_serialization_len() {
  return(sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS);
}

void* bitset_container_deserialize(const char *buf, size_t buf_len) {
  bitset_container_t *ptr;
  size_t l = sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS;

  if(l != buf_len)
    return(NULL);

  if((ptr = (bitset_container_t *)malloc(sizeof(bitset_container_t))) != NULL) {
    memcpy(ptr, buf, sizeof(bitset_container_t));
    // sizeof(__m256i) == 32
    ptr->array = (uint64_t *) aligned_malloc(32, l);
    if (! ptr->array) {
        free(ptr);
        return NULL;
    }
    memcpy(ptr->array, buf, l);
    ptr->cardinality = bitset_container_compute_cardinality(ptr);
  }

  return((void*)ptr);
}

bool bitset_container_iterate(const bitset_container_t *cont, uint32_t base, roaring_iterator iterator, void *ptr) {
  for (int32_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i ) {
    uint64_t w = cont->array[i];
    while (w != 0) {
      uint64_t t = w & (~w + 1);
      int r = __builtin_ctzll(w);
      if(!iterator(r + base, ptr)) return false;
      w ^= t;
    }
    base += 64;
  }
  return true;
}

bool bitset_container_iterate64(const bitset_container_t *cont, uint32_t base, roaring_iterator64 iterator, uint64_t high_bits, void *ptr) {
  for (int32_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i ) {
    uint64_t w = cont->array[i];
    while (w != 0) {
      uint64_t t = w & (~w + 1);
      int r = __builtin_ctzll(w);
      if(!iterator(high_bits | (uint64_t)(r + base), ptr)) return false;
      w ^= t;
    }
    base += 64;
  }
  return true;
}


bool bitset_container_equals(const bitset_container_t *container1, const bitset_container_t *container2) {
	if((container1->cardinality != BITSET_UNKNOWN_CARDINALITY) && (container2->cardinality != BITSET_UNKNOWN_CARDINALITY)) {
		if(container1->cardinality != container2->cardinality) {
			return false;
		}
	}
	for(int32_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i ) {
		if(container1->array[i] != container2->array[i]) {
			return false;
		}
	}
	return true;
}

bool bitset_container_is_subset(const bitset_container_t *container1,
                          const bitset_container_t *container2) {
    if((container1->cardinality != BITSET_UNKNOWN_CARDINALITY) && (container2->cardinality != BITSET_UNKNOWN_CARDINALITY)) {
        if(container1->cardinality > container2->cardinality) {
            return false;
        }
    }
    for(int32_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i ) {
		if((container1->array[i] & container2->array[i]) != container1->array[i]) {
			return false;
		}
	}
	return true;
}

bool bitset_container_select(const bitset_container_t *container, uint32_t *start_rank, uint32_t rank, uint32_t *element) {
    int card = bitset_container_cardinality(container);
    if(rank >= *start_rank + card) {
        *start_rank += card;
        return false;
    }
    const uint64_t *array = container->array;
    int32_t size;
    for (int i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; i += 1) {
        size = hamming(array[i]);
        if(rank <= *start_rank + size) {
            uint64_t w = container->array[i];
            uint16_t base = i*64;
            while (w != 0) {
                uint64_t t = w & (~w + 1);
                int r = __builtin_ctzll(w);
                if(*start_rank == rank) {
                    *element = r+base;
                    return true;
                }
                w ^= t;
                *start_rank += 1;
            }
        }
        else
            *start_rank += size;
    }
    assert(false);
    __builtin_unreachable();
}


/* Returns the smallest value (assumes not empty) */
uint16_t bitset_container_minimum(const bitset_container_t *container) {
  for (int32_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i ) {
    uint64_t w = container->array[i];
    if (w != 0) {
      int r = __builtin_ctzll(w);
      return r + i * 64;
    }
  }
  return UINT16_MAX;
}

/* Returns the largest value (assumes not empty) */
uint16_t bitset_container_maximum(const bitset_container_t *container) {
  for (int32_t i = BITSET_CONTAINER_SIZE_IN_WORDS - 1; i > 0; --i ) {
    uint64_t w = container->array[i];
    if (w != 0) {
      int r = __builtin_clzll(w);
      return i * 64 + 63  - r;
    }
  }
  return 0;
}

/* Returns the number of values equal or smaller than x */
int bitset_container_rank(const bitset_container_t *container, uint16_t x) {
  uint32_t x32 = x;
  int sum = 0;
  uint32_t k = 0;
  for (; k + 63 <= x32; k += 64)  {
    sum += hamming(container->array[k / 64]);
  }
  // at this point, we have covered everything up to k, k not included.
  // we have that k < x, but not so large that k+63<=x
  // k is a power of 64
  int bitsleft = x32 - k + 1;// will be in [0,64)
  uint64_t leftoverword = container->array[k / 64];// k / 64 should be within scope
  leftoverword = leftoverword & ((UINT64_C(1) << bitsleft) - 1);
  sum += hamming(leftoverword);
  return sum;
}

/* Returns the index of the first value equal or larger than x, or -1 */
int bitset_container_index_equalorlarger(const bitset_container_t *container, uint16_t x) {
  uint32_t x32 = x;
  uint32_t k = x32 / 64;
  uint64_t word = container->array[k];
  const int diff = x32 - k * 64; // in [0,64)
  word = (word >> diff) << diff; // a mask is faster, but we don't care
  while(word == 0) {
    k++;
    if(k == BITSET_CONTAINER_SIZE_IN_WORDS) return -1;
    word = container->array[k];
  }
  return k * 64 + __builtin_ctzll(word);
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/bitset.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/containers.c */


extern inline const void *container_unwrap_shared(
    const void *candidate_shared_container, uint8_t *type);
extern inline void *container_mutable_unwrap_shared(
    void *candidate_shared_container, uint8_t *type);

extern const char *get_container_name(uint8_t typecode);

extern int container_get_cardinality(const void *container, uint8_t typecode);

extern void *container_iand(void *c1, uint8_t type1, const void *c2,
                            uint8_t type2, uint8_t *result_type);

extern void *container_ior(void *c1, uint8_t type1, const void *c2,
                           uint8_t type2, uint8_t *result_type);

extern void *container_ixor(void *c1, uint8_t type1, const void *c2,
                            uint8_t type2, uint8_t *result_type);

extern void *container_iandnot(void *c1, uint8_t type1, const void *c2,
                               uint8_t type2, uint8_t *result_type);

void container_free(void *container, uint8_t typecode) {
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            bitset_container_free((bitset_container_t *)container);
            break;
        case ARRAY_CONTAINER_TYPE_CODE:
            array_container_free((array_container_t *)container);
            break;
        case RUN_CONTAINER_TYPE_CODE:
            run_container_free((run_container_t *)container);
            break;
        case SHARED_CONTAINER_TYPE_CODE:
            shared_container_free((shared_container_t *)container);
            break;
        default:
            assert(false);
            __builtin_unreachable();
    }
}

void container_printf(const void *container, uint8_t typecode) {
    container = container_unwrap_shared(container, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            bitset_container_printf((const bitset_container_t *)container);
            return;
        case ARRAY_CONTAINER_TYPE_CODE:
            array_container_printf((const array_container_t *)container);
            return;
        case RUN_CONTAINER_TYPE_CODE:
            run_container_printf((const run_container_t *)container);
            return;
        default:
            __builtin_unreachable();
    }
}

void container_printf_as_uint32_array(const void *container, uint8_t typecode,
                                      uint32_t base) {
    container = container_unwrap_shared(container, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            bitset_container_printf_as_uint32_array(
                (const bitset_container_t *)container, base);
            return;
        case ARRAY_CONTAINER_TYPE_CODE:
            array_container_printf_as_uint32_array(
                (const array_container_t *)container, base);
            return;
        case RUN_CONTAINER_TYPE_CODE:
            run_container_printf_as_uint32_array(
                (const run_container_t *)container, base);
            return;
            return;
        default:
            __builtin_unreachable();
    }
}

int32_t container_serialize(const void *container, uint8_t typecode,
                            char *buf) {
    container = container_unwrap_shared(container, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            return (bitset_container_serialize((const bitset_container_t *)container,
                                               buf));
        case ARRAY_CONTAINER_TYPE_CODE:
            return (
                array_container_serialize((const array_container_t *)container, buf));
        case RUN_CONTAINER_TYPE_CODE:
            return (run_container_serialize((const run_container_t *)container, buf));
        default:
            assert(0);
            __builtin_unreachable();
            return (-1);
    }
}

uint32_t container_serialization_len(const void *container, uint8_t typecode) {
    container = container_unwrap_shared(container, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            return bitset_container_serialization_len();
        case ARRAY_CONTAINER_TYPE_CODE:
            return array_container_serialization_len(
                (const array_container_t *)container);
        case RUN_CONTAINER_TYPE_CODE:
            return run_container_serialization_len(
                (const run_container_t *)container);
        default:
            assert(0);
            __builtin_unreachable();
            return (0);
    }
}

void *container_deserialize(uint8_t typecode, const char *buf, size_t buf_len) {
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            return (bitset_container_deserialize(buf, buf_len));
        case ARRAY_CONTAINER_TYPE_CODE:
            return (array_container_deserialize(buf, buf_len));
        case RUN_CONTAINER_TYPE_CODE:
            return (run_container_deserialize(buf, buf_len));
        case SHARED_CONTAINER_TYPE_CODE:
            printf("this should never happen.\n");
            assert(0);
            __builtin_unreachable();
            return (NULL);
        default:
            assert(0);
            __builtin_unreachable();
            return (NULL);
    }
}

extern bool container_nonzero_cardinality(const void *container,
                                          uint8_t typecode);

extern void container_free(void *container, uint8_t typecode);

extern int container_to_uint32_array(uint32_t *output, const void *container,
                                     uint8_t typecode, uint32_t base);

extern void *container_add(void *container, uint16_t val, uint8_t typecode,
                           uint8_t *new_typecode);

extern inline bool container_contains(const void *container, uint16_t val,
                                      uint8_t typecode);

extern void *container_clone(const void *container, uint8_t typecode);

extern void *container_and(const void *c1, uint8_t type1, const void *c2,
                           uint8_t type2, uint8_t *result_type);

extern void *container_or(const void *c1, uint8_t type1, const void *c2,
                          uint8_t type2, uint8_t *result_type);

extern void *container_xor(const void *c1, uint8_t type1, const void *c2,
                           uint8_t type2, uint8_t *result_type);

void *get_copy_of_container(void *container, uint8_t *typecode,
                            bool copy_on_write) {
    if (copy_on_write) {
        shared_container_t *shared_container;
        if (*typecode == SHARED_CONTAINER_TYPE_CODE) {
            shared_container = (shared_container_t *)container;
            shared_container->counter += 1;
            return shared_container;
        }
        assert(*typecode != SHARED_CONTAINER_TYPE_CODE);

        if ((shared_container = (shared_container_t *)malloc(
                 sizeof(shared_container_t))) == NULL) {
            return NULL;
        }

        shared_container->container = container;
        shared_container->typecode = *typecode;

        shared_container->counter = 2;
        *typecode = SHARED_CONTAINER_TYPE_CODE;

        return shared_container;
    }  // copy_on_write
    // otherwise, no copy on write...
    const void *actualcontainer =
        container_unwrap_shared((const void *)container, typecode);
    assert(*typecode != SHARED_CONTAINER_TYPE_CODE);
    return container_clone(actualcontainer, *typecode);
}
/**
 * Copies a container, requires a typecode. This allocates new memory, caller
 * is responsible for deallocation.
 */
void *container_clone(const void *container, uint8_t typecode) {
    container = container_unwrap_shared(container, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            return bitset_container_clone((const bitset_container_t *)container);
        case ARRAY_CONTAINER_TYPE_CODE:
            return array_container_clone((const array_container_t *)container);
        case RUN_CONTAINER_TYPE_CODE:
            return run_container_clone((const run_container_t *)container);
        case SHARED_CONTAINER_TYPE_CODE:
            printf("shared containers are not cloneable\n");
            assert(false);
            return NULL;
        default:
            assert(false);
            __builtin_unreachable();
            return NULL;
    }
}

void *shared_container_extract_copy(shared_container_t *container,
                                    uint8_t *typecode) {
    assert(container->counter > 0);
    assert(container->typecode != SHARED_CONTAINER_TYPE_CODE);
    container->counter--;
    *typecode = container->typecode;
    void *answer;
    if (container->counter == 0) {
        answer = container->container;
        container->container = NULL;  // paranoid
        free(container);
    } else {
        answer = container_clone(container->container, *typecode);
    }
    assert(*typecode != SHARED_CONTAINER_TYPE_CODE);
    return answer;
}

void shared_container_free(shared_container_t *container) {
    assert(container->counter > 0);
    container->counter--;
    if (container->counter == 0) {
        assert(container->typecode != SHARED_CONTAINER_TYPE_CODE);
        container_free(container->container, container->typecode);
        container->container = NULL;  // paranoid
        free(container);
    }
}

extern void *container_not(const void *c1, uint8_t type1, uint8_t *result_type);

extern void *container_not_range(const void *c1, uint8_t type1,
                                 uint32_t range_start, uint32_t range_end,
                                 uint8_t *result_type);

extern void *container_inot(void *c1, uint8_t type1, uint8_t *result_type);

extern void *container_inot_range(void *c1, uint8_t type1, uint32_t range_start,
                                  uint32_t range_end, uint8_t *result_type);

extern void *container_range_of_ones(uint32_t range_start, uint32_t range_end,
                                     uint8_t *result_type);

// where are the correponding things for union and intersection??
extern void *container_lazy_xor(const void *c1, uint8_t type1, const void *c2,
                                uint8_t type2, uint8_t *result_type);

extern void *container_lazy_ixor(void *c1, uint8_t type1, const void *c2,
                                 uint8_t type2, uint8_t *result_type);

extern void *container_andnot(const void *c1, uint8_t type1, const void *c2,
                              uint8_t type2, uint8_t *result_type);
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/containers.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/convert.c */
#include <stdio.h>


// file contains grubby stuff that must know impl. details of all container
// types.
bitset_container_t *bitset_container_from_array(const array_container_t *a) {
    bitset_container_t *ans = bitset_container_create();
    int limit = array_container_cardinality(a);
    for (int i = 0; i < limit; ++i) bitset_container_set(ans, a->array[i]);
    return ans;
}

bitset_container_t *bitset_container_from_run(const run_container_t *arr) {
    int card = run_container_cardinality(arr);
    bitset_container_t *answer = bitset_container_create();
    for (int rlepos = 0; rlepos < arr->n_runs; ++rlepos) {
        rle16_t vl = arr->runs[rlepos];
        bitset_set_lenrange(answer->array, vl.value, vl.length);
    }
    answer->cardinality = card;
    return answer;
}

array_container_t *array_container_from_run(const run_container_t *arr) {
    array_container_t *answer =
        array_container_create_given_capacity(run_container_cardinality(arr));
    answer->cardinality = 0;
    for (int rlepos = 0; rlepos < arr->n_runs; ++rlepos) {
        int run_start = arr->runs[rlepos].value;
        int run_end = run_start + arr->runs[rlepos].length;

        for (int run_value = run_start; run_value <= run_end; ++run_value) {
            answer->array[answer->cardinality++] = (uint16_t)run_value;
        }
    }
    return answer;
}

array_container_t *array_container_from_bitset(const bitset_container_t *bits) {
    array_container_t *result =
        array_container_create_given_capacity(bits->cardinality);
    result->cardinality = bits->cardinality;
    //  sse version ends up being slower here
    // (bitset_extract_setbits_sse_uint16)
    // because of the sparsity of the data
    bitset_extract_setbits_uint16(bits->array, BITSET_CONTAINER_SIZE_IN_WORDS,
                                  result->array, 0);
    return result;
}

/* assumes that container has adequate space.  Run from [s,e] (inclusive) */
static void add_run(run_container_t *r, int s, int e) {
    r->runs[r->n_runs].value = s;
    r->runs[r->n_runs].length = e - s;
    r->n_runs++;
}

run_container_t *run_container_from_array(const array_container_t *c) {
    int32_t n_runs = array_container_number_of_runs(c);
    run_container_t *answer = run_container_create_given_capacity(n_runs);
    int prev = -2;
    int run_start = -1;
    int32_t card = c->cardinality;
    if (card == 0) return answer;
    for (int i = 0; i < card; ++i) {
        const uint16_t cur_val = c->array[i];
        if (cur_val != prev + 1) {
            // new run starts; flush old one, if any
            if (run_start != -1) add_run(answer, run_start, prev);
            run_start = cur_val;
        }
        prev = c->array[i];
    }
    // now prev is the last seen value
    add_run(answer, run_start, prev);
    // assert(run_container_cardinality(answer) == c->cardinality);
    return answer;
}

/**
 * Convert the runcontainer to either a Bitmap or an Array Container, depending
 * on the cardinality.  Frees the container.
 * Allocates and returns new container, which caller is responsible for freeing
 */

void *convert_to_bitset_or_array_container(run_container_t *r, int32_t card,
                                           uint8_t *resulttype) {
    if (card <= DEFAULT_MAX_SIZE) {
        array_container_t *answer = array_container_create_given_capacity(card);
        answer->cardinality = 0;
        for (int rlepos = 0; rlepos < r->n_runs; ++rlepos) {
            uint16_t run_start = r->runs[rlepos].value;
            uint16_t run_end = run_start + r->runs[rlepos].length;
            for (uint16_t run_value = run_start; run_value <= run_end;
                 ++run_value) {
                answer->array[answer->cardinality++] = run_value;
            }
        }
        assert(card == answer->cardinality);
        *resulttype = ARRAY_CONTAINER_TYPE_CODE;
        run_container_free(r);
        return answer;
    }
    bitset_container_t *answer = bitset_container_create();
    for (int rlepos = 0; rlepos < r->n_runs; ++rlepos) {
        uint16_t run_start = r->runs[rlepos].value;
        bitset_set_lenrange(answer->array, run_start, r->runs[rlepos].length);
    }
    answer->cardinality = card;
    *resulttype = BITSET_CONTAINER_TYPE_CODE;
    run_container_free(r);
    return answer;
}

/* Converts a run container to either an array or a bitset, IF it saves space.
 */
/* If a conversion occurs, the caller is responsible to free the original
 * container and
 * he becomes responsible to free the new one. */
void *convert_run_to_efficient_container(run_container_t *c,
                                         uint8_t *typecode_after) {
    int32_t size_as_run_container =
        run_container_serialized_size_in_bytes(c->n_runs);

    int32_t size_as_bitset_container =
        bitset_container_serialized_size_in_bytes();
    int32_t card = run_container_cardinality(c);
    int32_t size_as_array_container =
        array_container_serialized_size_in_bytes(card);

    int32_t min_size_non_run =
        size_as_bitset_container < size_as_array_container
            ? size_as_bitset_container
            : size_as_array_container;
    if (size_as_run_container <= min_size_non_run) {  // no conversion
        *typecode_after = RUN_CONTAINER_TYPE_CODE;
        return c;
    }
    if (card <= DEFAULT_MAX_SIZE) {
        // to array
        array_container_t *answer = array_container_create_given_capacity(card);
        answer->cardinality = 0;
        for (int rlepos = 0; rlepos < c->n_runs; ++rlepos) {
            int run_start = c->runs[rlepos].value;
            int run_end = run_start + c->runs[rlepos].length;

            for (int run_value = run_start; run_value <= run_end; ++run_value) {
                answer->array[answer->cardinality++] = (uint16_t)run_value;
            }
        }
        *typecode_after = ARRAY_CONTAINER_TYPE_CODE;
        return answer;
    }

    // else to bitset
    bitset_container_t *answer = bitset_container_create();

    for (int rlepos = 0; rlepos < c->n_runs; ++rlepos) {
        int start = c->runs[rlepos].value;
        int end = start + c->runs[rlepos].length;
        bitset_set_range(answer->array, start, end + 1);
    }
    answer->cardinality = card;
    *typecode_after = BITSET_CONTAINER_TYPE_CODE;
    return answer;
}

// like convert_run_to_efficient_container but frees the old result if needed
void *convert_run_to_efficient_container_and_free(run_container_t *c,
                                                  uint8_t *typecode_after) {
    void *answer = convert_run_to_efficient_container(c, typecode_after);
    if (answer != c) run_container_free(c);
    return answer;
}

/* once converted, the original container is disposed here, rather than
   in roaring_array
*/

// TODO: split into run-  array-  and bitset-  subfunctions for sanity;
// a few function calls won't really matter.

void *convert_run_optimize(void *c, uint8_t typecode_original,
                           uint8_t *typecode_after) {
    if (typecode_original == RUN_CONTAINER_TYPE_CODE) {
        void *newc = convert_run_to_efficient_container((run_container_t *)c,
                                                        typecode_after);
        if (newc != c) {
            container_free(c, typecode_original);
        }
        return newc;
    } else if (typecode_original == ARRAY_CONTAINER_TYPE_CODE) {
        // it might need to be converted to a run container.
        array_container_t *c_qua_array = (array_container_t *)c;
        int32_t n_runs = array_container_number_of_runs(c_qua_array);
        int32_t size_as_run_container =
            run_container_serialized_size_in_bytes(n_runs);
        int32_t card = array_container_cardinality(c_qua_array);
        int32_t size_as_array_container =
            array_container_serialized_size_in_bytes(card);

        if (size_as_run_container >= size_as_array_container) {
            *typecode_after = ARRAY_CONTAINER_TYPE_CODE;
            return c;
        }
        // else convert array to run container
        run_container_t *answer = run_container_create_given_capacity(n_runs);
        int prev = -2;
        int run_start = -1;

        assert(card > 0);
        for (int i = 0; i < card; ++i) {
            uint16_t cur_val = c_qua_array->array[i];
            if (cur_val != prev + 1) {
                // new run starts; flush old one, if any
                if (run_start != -1) add_run(answer, run_start, prev);
                run_start = cur_val;
            }
            prev = c_qua_array->array[i];
        }
        assert(run_start >= 0);
        // now prev is the last seen value
        add_run(answer, run_start, prev);
        *typecode_after = RUN_CONTAINER_TYPE_CODE;
        array_container_free(c_qua_array);
        return answer;
    } else if (typecode_original ==
               BITSET_CONTAINER_TYPE_CODE) {  // run conversions on bitset
        // does bitset need conversion to run?
        bitset_container_t *c_qua_bitset = (bitset_container_t *)c;
        int32_t n_runs = bitset_container_number_of_runs(c_qua_bitset);
        int32_t size_as_run_container =
            run_container_serialized_size_in_bytes(n_runs);
        int32_t size_as_bitset_container =
            bitset_container_serialized_size_in_bytes();

        if (size_as_bitset_container <= size_as_run_container) {
            // no conversion needed.
            *typecode_after = BITSET_CONTAINER_TYPE_CODE;
            return c;
        }
        // bitset to runcontainer (ported from Java  RunContainer(
        // BitmapContainer bc, int nbrRuns))
        assert(n_runs > 0);  // no empty bitmaps
        run_container_t *answer = run_container_create_given_capacity(n_runs);

        int long_ctr = 0;
        uint64_t cur_word = c_qua_bitset->array[0];
        int run_count = 0;
        while (true) {
            while (cur_word == UINT64_C(0) &&
                   long_ctr < BITSET_CONTAINER_SIZE_IN_WORDS - 1)
                cur_word = c_qua_bitset->array[++long_ctr];

            if (cur_word == UINT64_C(0)) {
                bitset_container_free(c_qua_bitset);
                *typecode_after = RUN_CONTAINER_TYPE_CODE;
                return answer;
            }

            int local_run_start = __builtin_ctzll(cur_word);
            int run_start = local_run_start + 64 * long_ctr;
            uint64_t cur_word_with_1s = cur_word | (cur_word - 1);

            int run_end = 0;
            while (cur_word_with_1s == UINT64_C(0xFFFFFFFFFFFFFFFF) &&
                   long_ctr < BITSET_CONTAINER_SIZE_IN_WORDS - 1)
                cur_word_with_1s = c_qua_bitset->array[++long_ctr];

            if (cur_word_with_1s == UINT64_C(0xFFFFFFFFFFFFFFFF)) {
                run_end = 64 + long_ctr * 64;  // exclusive, I guess
                add_run(answer, run_start, run_end - 1);
                bitset_container_free(c_qua_bitset);
                *typecode_after = RUN_CONTAINER_TYPE_CODE;
                return answer;
            }
            int local_run_end = __builtin_ctzll(~cur_word_with_1s);
            run_end = local_run_end + long_ctr * 64;
            add_run(answer, run_start, run_end - 1);
            run_count++;
            cur_word = cur_word_with_1s & (cur_word_with_1s + 1);
        }
        return answer;
    } else {
        assert(false);
        __builtin_unreachable();
        return NULL;
    }
}

bitset_container_t *bitset_container_from_run_range(const run_container_t *run,
                                                    uint32_t min, uint32_t max) {
    bitset_container_t *bitset = bitset_container_create();
    int32_t union_cardinality = 0;
    for (int32_t i = 0; i < run->n_runs; ++i) {
        uint32_t rle_min = run->runs[i].value;
        uint32_t rle_max = rle_min + run->runs[i].length;
        bitset_set_lenrange(bitset->array, rle_min, rle_max - rle_min);
        union_cardinality += run->runs[i].length + 1;
    }
    union_cardinality += max - min + 1;
    union_cardinality -= bitset_lenrange_cardinality(bitset->array, min, max-min);
    bitset_set_lenrange(bitset->array, min, max - min);
    bitset->cardinality = union_cardinality;
    return bitset;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/convert.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_andnot.c */
/*
 * mixed_andnot.c.  More methods since operation is not symmetric,
 * except no "wide" andnot , so no lazy options motivated.
 */

#include <assert.h>
#include <string.h>


/* Compute the andnot of src_1 and src_2 and write the result to
 * dst, a valid array container that could be the same as dst.*/
void array_bitset_container_andnot(const array_container_t *src_1,
                                   const bitset_container_t *src_2,
                                   array_container_t *dst) {
    // follows Java implementation as of June 2016
    if (dst->capacity < src_1->cardinality) {
        array_container_grow(dst, src_1->cardinality, false);
    }
    int32_t newcard = 0;
    const int32_t origcard = src_1->cardinality;
    for (int i = 0; i < origcard; ++i) {
        uint16_t key = src_1->array[i];
        dst->array[newcard] = key;
        newcard += 1 - bitset_container_contains(src_2, key);
    }
    dst->cardinality = newcard;
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * src_1 */

void array_bitset_container_iandnot(array_container_t *src_1,
                                    const bitset_container_t *src_2) {
    array_bitset_container_andnot(src_1, src_2, src_1);
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst, which does not initially have a valid container.
 * Return true for a bitset result; false for array
 */

bool bitset_array_container_andnot(const bitset_container_t *src_1,
                                   const array_container_t *src_2, void **dst) {
    // Java did this directly, but we have option of asm or avx
    bitset_container_t *result = bitset_container_create();
    bitset_container_copy(src_1, result);
    result->cardinality =
        (int32_t)bitset_clear_list(result->array, (uint64_t)result->cardinality,
                                   src_2->array, (uint64_t)src_2->cardinality);

    // do required type conversions.
    if (result->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(result);
        bitset_container_free(result);
        return false;
    }
    *dst = result;
    return true;
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst (which has no container initially).  It will modify src_1
 * to be dst if the result is a bitset.  Otherwise, it will
 * free src_1 and dst will be a new array container.  In both
 * cases, the caller is responsible for deallocating dst.
 * Returns true iff dst is a bitset  */

bool bitset_array_container_iandnot(bitset_container_t *src_1,
                                    const array_container_t *src_2,
                                    void **dst) {
    *dst = src_1;
    src_1->cardinality =
        (int32_t)bitset_clear_list(src_1->array, (uint64_t)src_1->cardinality,
                                   src_2->array, (uint64_t)src_2->cardinality);

    if (src_1->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(src_1);
        bitset_container_free(src_1);
        return false;  // not bitset
    } else
        return true;
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst. Result may be either a bitset or an array container
 * (returns "result is bitset"). dst does not initially have
 * any container, but becomes either a bitset container (return
 * result true) or an array container.
 */

bool run_bitset_container_andnot(const run_container_t *src_1,
                                 const bitset_container_t *src_2, void **dst) {
    // follows the Java implementation as of June 2016
    int card = run_container_cardinality(src_1);
    if (card <= DEFAULT_MAX_SIZE) {
        // must be an array
        array_container_t *answer = array_container_create_given_capacity(card);
        answer->cardinality = 0;
        for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
            rle16_t rle = src_1->runs[rlepos];
            for (int run_value = rle.value; run_value <= rle.value + rle.length;
                 ++run_value) {
                if (!bitset_container_get(src_2, (uint16_t)run_value)) {
                    answer->array[answer->cardinality++] = (uint16_t)run_value;
                }
            }
        }
        *dst = answer;
        return false;
    } else {  // we guess it will be a bitset, though have to check guess when
              // done
        bitset_container_t *answer = bitset_container_clone(src_2);

        uint32_t last_pos = 0;
        for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
            rle16_t rle = src_1->runs[rlepos];

            uint32_t start = rle.value;
            uint32_t end = start + rle.length + 1;
            bitset_reset_range(answer->array, last_pos, start);
            bitset_flip_range(answer->array, start, end);
            last_pos = end;
        }
        bitset_reset_range(answer->array, last_pos, (uint32_t)(1 << 16));

        answer->cardinality = bitset_container_compute_cardinality(answer);

        if (answer->cardinality <= DEFAULT_MAX_SIZE) {
            *dst = array_container_from_bitset(answer);
            bitset_container_free(answer);
            return false;  // not bitset
        }
        *dst = answer;
        return true;  // bitset
    }
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst. Result may be either a bitset or an array container
 * (returns "result is bitset"). dst does not initially have
 * any container, but becomes either a bitset container (return
 * result true) or an array container.
 */

bool run_bitset_container_iandnot(run_container_t *src_1,
                                  const bitset_container_t *src_2, void **dst) {
    // dummy implementation
    bool ans = run_bitset_container_andnot(src_1, src_2, dst);
    run_container_free(src_1);
    return ans;
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst. Result may be either a bitset or an array container
 * (returns "result is bitset").  dst does not initially have
 * any container, but becomes either a bitset container (return
 * result true) or an array container.
 */

bool bitset_run_container_andnot(const bitset_container_t *src_1,
                                 const run_container_t *src_2, void **dst) {
    // follows Java implementation
    bitset_container_t *result = bitset_container_create();

    bitset_container_copy(src_1, result);
    for (int32_t rlepos = 0; rlepos < src_2->n_runs; ++rlepos) {
        rle16_t rle = src_2->runs[rlepos];
        bitset_reset_range(result->array, rle.value,
                           rle.value + rle.length + UINT32_C(1));
    }
    result->cardinality = bitset_container_compute_cardinality(result);

    if (result->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(result);
        bitset_container_free(result);
        return false;  // not bitset
    }
    *dst = result;
    return true;  // bitset
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst (which has no container initially).  It will modify src_1
 * to be dst if the result is a bitset.  Otherwise, it will
 * free src_1 and dst will be a new array container.  In both
 * cases, the caller is responsible for deallocating dst.
 * Returns true iff dst is a bitset  */

bool bitset_run_container_iandnot(bitset_container_t *src_1,
                                  const run_container_t *src_2, void **dst) {
    *dst = src_1;

    for (int32_t rlepos = 0; rlepos < src_2->n_runs; ++rlepos) {
        rle16_t rle = src_2->runs[rlepos];
        bitset_reset_range(src_1->array, rle.value,
                           rle.value + rle.length + UINT32_C(1));
    }
    src_1->cardinality = bitset_container_compute_cardinality(src_1);

    if (src_1->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(src_1);
        bitset_container_free(src_1);
        return false;  // not bitset
    } else
        return true;
}

/* helper. a_out must be a valid array container with adequate capacity.
 * Returns the cardinality of the output container. Partly Based on Java
 * implementation Util.unsignedDifference.
 *
 * TODO: Util.unsignedDifference does not use advanceUntil.  Is it cheaper
 * to avoid advanceUntil?
 */

static int run_array_array_subtract(const run_container_t *r,
                                    const array_container_t *a_in,
                                    array_container_t *a_out) {
    int out_card = 0;
    int32_t in_array_pos =
        -1;  // since advanceUntil always assumes we start the search AFTER this

    for (int rlepos = 0; rlepos < r->n_runs; rlepos++) {
        int32_t start = r->runs[rlepos].value;
        int32_t end = start + r->runs[rlepos].length + 1;

        in_array_pos = advanceUntil(a_in->array, in_array_pos,
                                    a_in->cardinality, (uint16_t)start);

        if (in_array_pos >= a_in->cardinality) {  // run has no items subtracted
            for (int32_t i = start; i < end; ++i)
                a_out->array[out_card++] = (uint16_t)i;
        } else {
            uint16_t next_nonincluded = a_in->array[in_array_pos];
            if (next_nonincluded >= end) {
                // another case when run goes unaltered
                for (int32_t i = start; i < end; ++i)
                    a_out->array[out_card++] = (uint16_t)i;
                in_array_pos--;  // ensure we see this item again if necessary
            } else {
                for (int32_t i = start; i < end; ++i)
                    if (i != next_nonincluded)
                        a_out->array[out_card++] = (uint16_t)i;
                    else  // 0 should ensure  we don't match
                        next_nonincluded =
                            (in_array_pos + 1 >= a_in->cardinality)
                                ? 0
                                : a_in->array[++in_array_pos];
                in_array_pos--;  // see again
            }
        }
    }
    return out_card;
}

/* dst does not indicate a valid container initially.  Eventually it
 * can become any type of container.
 */

int run_array_container_andnot(const run_container_t *src_1,
                               const array_container_t *src_2, void **dst) {
    // follows the Java impl as of June 2016

    int card = run_container_cardinality(src_1);
    const int arbitrary_threshold = 32;

    if (card <= arbitrary_threshold) {
        if (src_2->cardinality == 0) {
            *dst = run_container_clone(src_1);
            return RUN_CONTAINER_TYPE_CODE;
        }
        // Java's "lazyandNot.toEfficientContainer" thing
        run_container_t *answer = run_container_create_given_capacity(
            card + array_container_cardinality(src_2));

        int rlepos = 0;
        int xrlepos = 0;  // "x" is src_2
        rle16_t rle = src_1->runs[rlepos];
        int32_t start = rle.value;
        int32_t end = start + rle.length + 1;
        int32_t xstart = src_2->array[xrlepos];

        while ((rlepos < src_1->n_runs) && (xrlepos < src_2->cardinality)) {
            if (end <= xstart) {
                // output the first run
                answer->runs[answer->n_runs++] =
                    (rle16_t){.value = (uint16_t)start,
                              .length = (uint16_t)(end - start - 1)};
                rlepos++;
                if (rlepos < src_1->n_runs) {
                    start = src_1->runs[rlepos].value;
                    end = start + src_1->runs[rlepos].length + 1;
                }
            } else if (xstart + 1 <= start) {
                // exit the second run
                xrlepos++;
                if (xrlepos < src_2->cardinality) {
                    xstart = src_2->array[xrlepos];
                }
            } else {
                if (start < xstart) {
                    answer->runs[answer->n_runs++] =
                        (rle16_t){.value = (uint16_t)start,
                                  .length = (uint16_t)(xstart - start - 1)};
                }
                if (xstart + 1 < end) {
                    start = xstart + 1;
                } else {
                    rlepos++;
                    if (rlepos < src_1->n_runs) {
                        start = src_1->runs[rlepos].value;
                        end = start + src_1->runs[rlepos].length + 1;
                    }
                }
            }
        }
        if (rlepos < src_1->n_runs) {
            answer->runs[answer->n_runs++] =
                (rle16_t){.value = (uint16_t)start,
                          .length = (uint16_t)(end - start - 1)};
            rlepos++;
            if (rlepos < src_1->n_runs) {
                memcpy(answer->runs + answer->n_runs, src_1->runs + rlepos,
                       (src_1->n_runs - rlepos) * sizeof(rle16_t));
                answer->n_runs += (src_1->n_runs - rlepos);
            }
        }
        uint8_t return_type;
        *dst = convert_run_to_efficient_container(answer, &return_type);
        if (answer != *dst) run_container_free(answer);
        return return_type;
    }
    // else it's a bitmap or array

    if (card <= DEFAULT_MAX_SIZE) {
        array_container_t *ac = array_container_create_given_capacity(card);
        // nb Java code used a generic iterator-based merge to compute
        // difference
        ac->cardinality = run_array_array_subtract(src_1, src_2, ac);
        *dst = ac;
        return ARRAY_CONTAINER_TYPE_CODE;
    }
    bitset_container_t *ans = bitset_container_from_run(src_1);
    bool result_is_bitset = bitset_array_container_iandnot(ans, src_2, dst);
    return (result_is_bitset ? BITSET_CONTAINER_TYPE_CODE
                             : ARRAY_CONTAINER_TYPE_CODE);
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst (which has no container initially).  It will modify src_1
 * to be dst if the result is a bitset.  Otherwise, it will
 * free src_1 and dst will be a new array container.  In both
 * cases, the caller is responsible for deallocating dst.
 * Returns true iff dst is a bitset  */

int run_array_container_iandnot(run_container_t *src_1,
                                const array_container_t *src_2, void **dst) {
    // dummy implementation same as June 2016 Java
    int ans = run_array_container_andnot(src_1, src_2, dst);
    run_container_free(src_1);
    return ans;
}

/* dst must be a valid array container, allowed to be src_1 */

void array_run_container_andnot(const array_container_t *src_1,
                                const run_container_t *src_2,
                                array_container_t *dst) {
    // basically following Java impl as of June 2016
    if (src_1->cardinality > dst->capacity) {
        array_container_grow(dst, src_1->cardinality, false);
    }

    if (src_2->n_runs == 0) {
        memmove(dst->array, src_1->array,
                sizeof(uint16_t) * src_1->cardinality);
        dst->cardinality = src_1->cardinality;
        return;
    }
    int32_t run_start = src_2->runs[0].value;
    int32_t run_end = run_start + src_2->runs[0].length;
    int which_run = 0;

    uint16_t val = 0;
    int dest_card = 0;
    for (int i = 0; i < src_1->cardinality; ++i) {
        val = src_1->array[i];
        if (val < run_start)
            dst->array[dest_card++] = val;
        else if (val <= run_end) {
            ;  // omitted item
        } else {
            do {
                if (which_run + 1 < src_2->n_runs) {
                    ++which_run;
                    run_start = src_2->runs[which_run].value;
                    run_end = run_start + src_2->runs[which_run].length;

                } else
                    run_start = run_end = (1 << 16) + 1;
            } while (val > run_end);
            --i;
        }
    }
    dst->cardinality = dest_card;
}

/* dst does not indicate a valid container initially.  Eventually it
 * can become any kind of container.
 */

void array_run_container_iandnot(array_container_t *src_1,
                                 const run_container_t *src_2) {
    array_run_container_andnot(src_1, src_2, src_1);
}

/* dst does not indicate a valid container initially.  Eventually it
 * can become any kind of container.
 */

int run_run_container_andnot(const run_container_t *src_1,
                             const run_container_t *src_2, void **dst) {
    run_container_t *ans = run_container_create();
    run_container_andnot(src_1, src_2, ans);
    uint8_t typecode_after;
    *dst = convert_run_to_efficient_container_and_free(ans, &typecode_after);
    return typecode_after;
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst (which has no container initially).  It will modify src_1
 * to be dst if the result is a bitset.  Otherwise, it will
 * free src_1 and dst will be a new array container.  In both
 * cases, the caller is responsible for deallocating dst.
 * Returns true iff dst is a bitset  */

int run_run_container_iandnot(run_container_t *src_1,
                              const run_container_t *src_2, void **dst) {
    // following Java impl as of June 2016 (dummy)
    int ans = run_run_container_andnot(src_1, src_2, dst);
    run_container_free(src_1);
    return ans;
}

/*
 * dst is a valid array container and may be the same as src_1
 */

void array_array_container_andnot(const array_container_t *src_1,
                                  const array_container_t *src_2,
                                  array_container_t *dst) {
    array_container_andnot(src_1, src_2, dst);
}

/* inplace array-array andnot will always be able to reuse the space of
 * src_1 */
void array_array_container_iandnot(array_container_t *src_1,
                                   const array_container_t *src_2) {
    array_container_andnot(src_1, src_2, src_1);
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst (which has no container initially). Return value is
 * "dst is a bitset"
 */

bool bitset_bitset_container_andnot(const bitset_container_t *src_1,
                                    const bitset_container_t *src_2,
                                    void **dst) {
    bitset_container_t *ans = bitset_container_create();
    int card = bitset_container_andnot(src_1, src_2, ans);
    if (card <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(ans);
        bitset_container_free(ans);
        return false;  // not bitset
    } else {
        *dst = ans;
        return true;
    }
}

/* Compute the andnot of src_1 and src_2 and write the result to
 * dst (which has no container initially).  It will modify src_1
 * to be dst if the result is a bitset.  Otherwise, it will
 * free src_1 and dst will be a new array container.  In both
 * cases, the caller is responsible for deallocating dst.
 * Returns true iff dst is a bitset  */

bool bitset_bitset_container_iandnot(bitset_container_t *src_1,
                                     const bitset_container_t *src_2,
                                     void **dst) {
    int card = bitset_container_andnot(src_1, src_2, src_1);
    if (card <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(src_1);
        bitset_container_free(src_1);
        return false;  // not bitset
    } else {
        *dst = src_1;
        return true;
    }
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_andnot.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_equal.c */

bool array_container_equal_bitset(const array_container_t* container1,
                                  const bitset_container_t* container2) {
    if (container2->cardinality != BITSET_UNKNOWN_CARDINALITY) {
        if (container2->cardinality != container1->cardinality) {
            return false;
        }
    }
    int32_t pos = 0;
    for (int32_t i = 0; i < BITSET_CONTAINER_SIZE_IN_WORDS; ++i) {
        uint64_t w = container2->array[i];
        while (w != 0) {
            uint64_t t = w & (~w + 1);
            uint16_t r = i * 64 + __builtin_ctzll(w);
            if (pos >= container1->cardinality) {
                return false;
            }
            if (container1->array[pos] != r) {
                return false;
            }
            ++pos;
            w ^= t;
        }
    }
    return (pos == container1->cardinality);
}

bool run_container_equals_array(const run_container_t* container1,
                                const array_container_t* container2) {
    if (run_container_cardinality(container1) != container2->cardinality)
        return false;
    int32_t pos = 0;
    for (int i = 0; i < container1->n_runs; ++i) {
        const uint32_t run_start = container1->runs[i].value;
        const uint32_t le = container1->runs[i].length;

        if (container2->array[pos] != run_start) {
            return false;
        }

        if (container2->array[pos + le] != run_start + le) {
            return false;
        }

        pos += le + 1;
    }
    return true;
}

bool run_container_equals_bitset(const run_container_t* container1,
                                 const bitset_container_t* container2) {
    if (container2->cardinality != BITSET_UNKNOWN_CARDINALITY) {
        if (container2->cardinality != run_container_cardinality(container1)) {
            return false;
        }
    } else {
        int32_t card = bitset_container_compute_cardinality(
            container2);  // modify container2?
        if (card != run_container_cardinality(container1)) {
            return false;
        }
    }
    for (int i = 0; i < container1->n_runs; ++i) {
        uint32_t run_start = container1->runs[i].value;
        uint32_t le = container1->runs[i].length;
        for (uint32_t j = run_start; j <= run_start + le; ++j) {
            // todo: this code could be much faster
            if (!bitset_container_contains(container2, j)) {
                return false;
            }
        }
    }
    return true;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_equal.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_intersection.c */
/*
 * mixed_intersection.c
 *
 */


/* Compute the intersection of src_1 and src_2 and write the result to
 * dst.  */
void array_bitset_container_intersection(const array_container_t *src_1,
                                         const bitset_container_t *src_2,
                                         array_container_t *dst) {
    if (dst->capacity < src_1->cardinality) {
        array_container_grow(dst, src_1->cardinality, false);
    }
    int32_t newcard = 0;  // dst could be src_1
    const int32_t origcard = src_1->cardinality;
    for (int i = 0; i < origcard; ++i) {
        uint16_t key = src_1->array[i];
        // this branchless approach is much faster...
        dst->array[newcard] = key;
        newcard += bitset_container_contains(src_2, key);
        /**
         * we could do it this way instead...
         * if (bitset_container_contains(src_2, key)) {
         * dst->array[newcard++] = key;
         * }
         * but if the result is unpredictible, the processor generates
         * many mispredicted branches.
         * Difference can be huge (from 3 cycles when predictible all the way
         * to 16 cycles when unpredictible.
         * See
         * https://github.com/lemire/Code-used-on-Daniel-Lemire-s-blog/blob/master/extra/bitset/c/arraybitsetintersection.c
         */
    }
    dst->cardinality = newcard;
}

/* Compute the size of the intersection of src_1 and src_2. */
int array_bitset_container_intersection_cardinality(
    const array_container_t *src_1, const bitset_container_t *src_2) {
    int32_t newcard = 0;
    const int32_t origcard = src_1->cardinality;
    for (int i = 0; i < origcard; ++i) {
        uint16_t key = src_1->array[i];
        newcard += bitset_container_contains(src_2, key);
    }
    return newcard;
}


bool array_bitset_container_intersect(const array_container_t *src_1,
                                         const bitset_container_t *src_2) {
	const int32_t origcard = src_1->cardinality;
	for (int i = 0; i < origcard; ++i) {
	        uint16_t key = src_1->array[i];
	        if(bitset_container_contains(src_2, key)) return true;
	}
	return false;
}

/* Compute the intersection of src_1 and src_2 and write the result to
 * dst. It is allowed for dst to be equal to src_1. We assume that dst is a
 * valid container. */
void array_run_container_intersection(const array_container_t *src_1,
                                      const run_container_t *src_2,
                                      array_container_t *dst) {
    if (run_container_is_full(src_2)) {
        if (dst != src_1) array_container_copy(src_1, dst);
        return;
    }
    if (dst->capacity < src_1->cardinality) {
        array_container_grow(dst, src_1->cardinality, false);
    }
    if (src_2->n_runs == 0) {
        return;
    }
    int32_t rlepos = 0;
    int32_t arraypos = 0;
    rle16_t rle = src_2->runs[rlepos];
    int32_t newcard = 0;
    while (arraypos < src_1->cardinality) {
        const uint16_t arrayval = src_1->array[arraypos];
        while (rle.value + rle.length <
               arrayval) {  // this will frequently be false
            ++rlepos;
            if (rlepos == src_2->n_runs) {
                dst->cardinality = newcard;
                return;  // we are done
            }
            rle = src_2->runs[rlepos];
        }
        if (rle.value > arrayval) {
            arraypos = advanceUntil(src_1->array, arraypos, src_1->cardinality,
                                    rle.value);
        } else {
            dst->array[newcard] = arrayval;
            newcard++;
            arraypos++;
        }
    }
    dst->cardinality = newcard;
}

/* Compute the intersection of src_1 and src_2 and write the result to
 * *dst. If the result is true then the result is a bitset_container_t
 * otherwise is a array_container_t. If *dst ==  src_2, an in-place processing
 * is attempted.*/
bool run_bitset_container_intersection(const run_container_t *src_1,
                                       const bitset_container_t *src_2,
                                       void **dst) {
    if (run_container_is_full(src_1)) {
        if (*dst != src_2) *dst = bitset_container_clone(src_2);
        return true;
    }
    int32_t card = run_container_cardinality(src_1);
    if (card <= DEFAULT_MAX_SIZE) {
        // result can only be an array (assuming that we never make a
        // RunContainer)
        if (card > src_2->cardinality) {
            card = src_2->cardinality;
        }
        array_container_t *answer = array_container_create_given_capacity(card);
        *dst = answer;
        if (*dst == NULL) {
            return false;
        }
        for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
            rle16_t rle = src_1->runs[rlepos];
            uint32_t endofrun = (uint32_t)rle.value + rle.length;
            for (uint32_t runValue = rle.value; runValue <= endofrun;
                 ++runValue) {
                answer->array[answer->cardinality] = (uint16_t)runValue;
                answer->cardinality +=
                    bitset_container_contains(src_2, runValue);
            }
        }
        return false;
    }
    if (*dst == src_2) {  // we attempt in-place
        bitset_container_t *answer = (bitset_container_t *)*dst;
        uint32_t start = 0;
        for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
            const rle16_t rle = src_1->runs[rlepos];
            uint32_t end = rle.value;
            bitset_reset_range(src_2->array, start, end);

            start = end + rle.length + 1;
        }
        bitset_reset_range(src_2->array, start, UINT32_C(1) << 16);
        answer->cardinality = bitset_container_compute_cardinality(answer);
        if (src_2->cardinality > DEFAULT_MAX_SIZE) {
            return true;
        } else {
            array_container_t *newanswer = array_container_from_bitset(src_2);
            if (newanswer == NULL) {
                *dst = NULL;
                return false;
            }
            *dst = newanswer;
            return false;
        }
    } else {  // no inplace
        // we expect the answer to be a bitmap (if we are lucky)
        bitset_container_t *answer = bitset_container_clone(src_2);

        *dst = answer;
        if (answer == NULL) {
            return true;
        }
        uint32_t start = 0;
        for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
            const rle16_t rle = src_1->runs[rlepos];
            uint32_t end = rle.value;
            bitset_reset_range(answer->array, start, end);
            start = end + rle.length + 1;
        }
        bitset_reset_range(answer->array, start, UINT32_C(1) << 16);
        answer->cardinality = bitset_container_compute_cardinality(answer);

        if (answer->cardinality > DEFAULT_MAX_SIZE) {
            return true;
        } else {
            array_container_t *newanswer = array_container_from_bitset(answer);
            bitset_container_free((bitset_container_t *)*dst);
            if (newanswer == NULL) {
                *dst = NULL;
                return false;
            }
            *dst = newanswer;
            return false;
        }
    }
}

/* Compute the size of the intersection between src_1 and src_2 . */
int array_run_container_intersection_cardinality(const array_container_t *src_1,
                                                 const run_container_t *src_2) {
    if (run_container_is_full(src_2)) {
        return src_1->cardinality;
    }
    if (src_2->n_runs == 0) {
        return 0;
    }
    int32_t rlepos = 0;
    int32_t arraypos = 0;
    rle16_t rle = src_2->runs[rlepos];
    int32_t newcard = 0;
    while (arraypos < src_1->cardinality) {
        const uint16_t arrayval = src_1->array[arraypos];
        while (rle.value + rle.length <
               arrayval) {  // this will frequently be false
            ++rlepos;
            if (rlepos == src_2->n_runs) {
                return newcard;  // we are done
            }
            rle = src_2->runs[rlepos];
        }
        if (rle.value > arrayval) {
            arraypos = advanceUntil(src_1->array, arraypos, src_1->cardinality,
                                    rle.value);
        } else {
            newcard++;
            arraypos++;
        }
    }
    return newcard;
}

/* Compute the intersection  between src_1 and src_2
 **/
int run_bitset_container_intersection_cardinality(
    const run_container_t *src_1, const bitset_container_t *src_2) {
    if (run_container_is_full(src_1)) {
        return bitset_container_cardinality(src_2);
    }
    int answer = 0;
    for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
        rle16_t rle = src_1->runs[rlepos];
        answer +=
            bitset_lenrange_cardinality(src_2->array, rle.value, rle.length);
    }
    return answer;
}


bool array_run_container_intersect(const array_container_t *src_1,
                                      const run_container_t *src_2) {
	if( run_container_is_full(src_2) ) {
	    return !array_container_empty(src_1);
	}
	if (src_2->n_runs == 0) {
        return false;
    }
    int32_t rlepos = 0;
    int32_t arraypos = 0;
    rle16_t rle = src_2->runs[rlepos];
    while (arraypos < src_1->cardinality) {
        const uint16_t arrayval = src_1->array[arraypos];
        while (rle.value + rle.length <
               arrayval) {  // this will frequently be false
            ++rlepos;
            if (rlepos == src_2->n_runs) {
                return false;  // we are done
            }
            rle = src_2->runs[rlepos];
        }
        if (rle.value > arrayval) {
            arraypos = advanceUntil(src_1->array, arraypos, src_1->cardinality,
                                    rle.value);
        } else {
            return true;
        }
    }
    return false;
}

/* Compute the intersection  between src_1 and src_2
 **/
bool run_bitset_container_intersect(const run_container_t *src_1,
                                       const bitset_container_t *src_2) {
	   if( run_container_is_full(src_1) ) {
		   return !bitset_container_empty(src_2);
	   }
       for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
           rle16_t rle = src_1->runs[rlepos];
           if(!bitset_lenrange_empty(src_2->array, rle.value,rle.length)) return true;
       }
       return false;
}

/*
 * Compute the intersection between src_1 and src_2 and write the result
 * to *dst. If the return function is true, the result is a bitset_container_t
 * otherwise is a array_container_t.
 */
bool bitset_bitset_container_intersection(const bitset_container_t *src_1,
                                          const bitset_container_t *src_2,
                                          void **dst) {
    const int newCardinality = bitset_container_and_justcard(src_1, src_2);
    if (newCardinality > DEFAULT_MAX_SIZE) {
        *dst = bitset_container_create();
        if (*dst != NULL) {
            bitset_container_and_nocard(src_1, src_2,
                                        (bitset_container_t *)*dst);
            ((bitset_container_t *)*dst)->cardinality = newCardinality;
        }
        return true;  // it is a bitset
    }
    *dst = array_container_create_given_capacity(newCardinality);
    if (*dst != NULL) {
        ((array_container_t *)*dst)->cardinality = newCardinality;
        bitset_extract_intersection_setbits_uint16(
            ((const bitset_container_t *)src_1)->array,
            ((const bitset_container_t *)src_2)->array,
            BITSET_CONTAINER_SIZE_IN_WORDS, ((array_container_t *)*dst)->array,
            0);
    }
    return false;  // not a bitset
}

bool bitset_bitset_container_intersection_inplace(
    bitset_container_t *src_1, const bitset_container_t *src_2, void **dst) {
    const int newCardinality = bitset_container_and_justcard(src_1, src_2);
    if (newCardinality > DEFAULT_MAX_SIZE) {
        *dst = src_1;
        bitset_container_and_nocard(src_1, src_2, src_1);
        ((bitset_container_t *)*dst)->cardinality = newCardinality;
        return true;  // it is a bitset
    }
    *dst = array_container_create_given_capacity(newCardinality);
    if (*dst != NULL) {
        ((array_container_t *)*dst)->cardinality = newCardinality;
        bitset_extract_intersection_setbits_uint16(
            ((const bitset_container_t *)src_1)->array,
            ((const bitset_container_t *)src_2)->array,
            BITSET_CONTAINER_SIZE_IN_WORDS, ((array_container_t *)*dst)->array,
            0);
    }
    return false;  // not a bitset
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_intersection.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_negation.c */
/*
 * mixed_negation.c
 *
 */

#include <assert.h>
#include <string.h>


// TODO: make simplified and optimized negation code across
// the full range.

/* Negation across the entire range of the container.
 * Compute the  negation of src  and write the result
 * to *dst. The complement of a
 * sufficiently sparse set will always be dense and a hence a bitmap
' * We assume that dst is pre-allocated and a valid bitset container
 * There can be no in-place version.
 */
void array_container_negation(const array_container_t *src,
                              bitset_container_t *dst) {
    uint64_t card = UINT64_C(1 << 16);
    bitset_container_set_all(dst);

    dst->cardinality = (int32_t)bitset_clear_list(dst->array, card, src->array,
                                                  (uint64_t)src->cardinality);
}

/* Negation across the entire range of the container
 * Compute the  negation of src  and write the result
 * to *dst.  A true return value indicates a bitset result,
 * otherwise the result is an array container.
 *  We assume that dst is not pre-allocated. In
 * case of failure, *dst will be NULL.
 */
bool bitset_container_negation(const bitset_container_t *src, void **dst) {
    return bitset_container_negation_range(src, 0, (1 << 16), dst);
}

/* inplace version */
/*
 * Same as bitset_container_negation except that if the output is to
 * be a
 * bitset_container_t, then src is modified and no allocation is made.
 * If the output is to be an array_container_t, then caller is responsible
 * to free the container.
 * In all cases, the result is in *dst.
 */
bool bitset_container_negation_inplace(bitset_container_t *src, void **dst) {
    return bitset_container_negation_range_inplace(src, 0, (1 << 16), dst);
}

/* Negation across the entire range of container
 * Compute the  negation of src  and write the result
 * to *dst.  Return values are the *_TYPECODES as defined * in containers.h
 *  We assume that dst is not pre-allocated. In
 * case of failure, *dst will be NULL.
 */
int run_container_negation(const run_container_t *src, void **dst) {
    return run_container_negation_range(src, 0, (1 << 16), dst);
}

/*
 * Same as run_container_negation except that if the output is to
 * be a
 * run_container_t, and has the capacity to hold the result,
 * then src is modified and no allocation is made.
 * In all cases, the result is in *dst.
 */
int run_container_negation_inplace(run_container_t *src, void **dst) {
    return run_container_negation_range_inplace(src, 0, (1 << 16), dst);
}

/* Negation across a range of the container.
 * Compute the  negation of src  and write the result
 * to *dst. Returns true if the result is a bitset container
 * and false for an array container.  *dst is not preallocated.
 */
bool array_container_negation_range(const array_container_t *src,
                                    const int range_start, const int range_end,
                                    void **dst) {
    /* close port of the Java implementation */
    if (range_start >= range_end) {
        *dst = array_container_clone(src);
        return false;
    }

    int32_t start_index =
        binarySearch(src->array, src->cardinality, (uint16_t)range_start);
    if (start_index < 0) start_index = -start_index - 1;

    int32_t last_index =
        binarySearch(src->array, src->cardinality, (uint16_t)(range_end - 1));
    if (last_index < 0) last_index = -last_index - 2;

    const int32_t current_values_in_range = last_index - start_index + 1;
    const int32_t span_to_be_flipped = range_end - range_start;
    const int32_t new_values_in_range =
        span_to_be_flipped - current_values_in_range;
    const int32_t cardinality_change =
        new_values_in_range - current_values_in_range;
    const int32_t new_cardinality = src->cardinality + cardinality_change;

    if (new_cardinality > DEFAULT_MAX_SIZE) {
        bitset_container_t *temp = bitset_container_from_array(src);
        bitset_flip_range(temp->array, (uint32_t)range_start,
                          (uint32_t)range_end);
        temp->cardinality = new_cardinality;
        *dst = temp;
        return true;
    }

    array_container_t *arr =
        array_container_create_given_capacity(new_cardinality);
    *dst = (void *)arr;
    if(new_cardinality == 0) {
      arr->cardinality = new_cardinality;
      return false; // we are done.
    }
    // copy stuff before the active area
    memcpy(arr->array, src->array, start_index * sizeof(uint16_t));

    // work on the range
    int32_t out_pos = start_index, in_pos = start_index;
    int32_t val_in_range = range_start;
    for (; val_in_range < range_end && in_pos <= last_index; ++val_in_range) {
        if ((uint16_t)val_in_range != src->array[in_pos]) {
            arr->array[out_pos++] = (uint16_t)val_in_range;
        } else {
            ++in_pos;
        }
    }
    for (; val_in_range < range_end; ++val_in_range)
        arr->array[out_pos++] = (uint16_t)val_in_range;

    // content after the active range
    memcpy(arr->array + out_pos, src->array + (last_index + 1),
           (src->cardinality - (last_index + 1)) * sizeof(uint16_t));
    arr->cardinality = new_cardinality;
    return false;
}

/* Even when the result would fit, it is unclear how to make an
 * inplace version without inefficient copying.
 */

bool array_container_negation_range_inplace(array_container_t *src,
                                            const int range_start,
                                            const int range_end, void **dst) {
    bool ans = array_container_negation_range(src, range_start, range_end, dst);
    // TODO : try a real inplace version
    array_container_free(src);
    return ans;
}

/* Negation across a range of the container
 * Compute the  negation of src  and write the result
 * to *dst.  A true return value indicates a bitset result,
 * otherwise the result is an array container.
 *  We assume that dst is not pre-allocated. In
 * case of failure, *dst will be NULL.
 */
bool bitset_container_negation_range(const bitset_container_t *src,
                                     const int range_start, const int range_end,
                                     void **dst) {
    // TODO maybe consider density-based estimate
    // and sometimes build result directly as array, with
    // conversion back to bitset if wrong.  Or determine
    // actual result cardinality, then go directly for the known final cont.

    // keep computation using bitsets as long as possible.
    bitset_container_t *t = bitset_container_clone(src);
    bitset_flip_range(t->array, (uint32_t)range_start, (uint32_t)range_end);
    t->cardinality = bitset_container_compute_cardinality(t);

    if (t->cardinality > DEFAULT_MAX_SIZE) {
        *dst = t;
        return true;
    } else {
        *dst = array_container_from_bitset(t);
        bitset_container_free(t);
        return false;
    }
}

/* inplace version */
/*
 * Same as bitset_container_negation except that if the output is to
 * be a
 * bitset_container_t, then src is modified and no allocation is made.
 * If the output is to be an array_container_t, then caller is responsible
 * to free the container.
 * In all cases, the result is in *dst.
 */
bool bitset_container_negation_range_inplace(bitset_container_t *src,
                                             const int range_start,
                                             const int range_end, void **dst) {
    bitset_flip_range(src->array, (uint32_t)range_start, (uint32_t)range_end);
    src->cardinality = bitset_container_compute_cardinality(src);
    if (src->cardinality > DEFAULT_MAX_SIZE) {
        *dst = src;
        return true;
    }
    *dst = array_container_from_bitset(src);
    bitset_container_free(src);
    return false;
}

/* Negation across a range of container
 * Compute the  negation of src  and write the result
 * to *dst. Return values are the *_TYPECODES as defined * in containers.h
 *  We assume that dst is not pre-allocated. In
 * case of failure, *dst will be NULL.
 */
int run_container_negation_range(const run_container_t *src,
                                 const int range_start, const int range_end,
                                 void **dst) {
    uint8_t return_typecode;

    // follows the Java implementation
    if (range_end <= range_start) {
        *dst = run_container_clone(src);
        return RUN_CONTAINER_TYPE_CODE;
    }

    run_container_t *ans = run_container_create_given_capacity(
        src->n_runs + 1);  // src->n_runs + 1);
    int k = 0;
    for (; k < src->n_runs && src->runs[k].value < range_start; ++k) {
        ans->runs[k] = src->runs[k];
        ans->n_runs++;
    }

    run_container_smart_append_exclusive(
        ans, (uint16_t)range_start, (uint16_t)(range_end - range_start - 1));

    for (; k < src->n_runs; ++k) {
        run_container_smart_append_exclusive(ans, src->runs[k].value,
                                             src->runs[k].length);
    }

    *dst = convert_run_to_efficient_container(ans, &return_typecode);
    if (return_typecode != RUN_CONTAINER_TYPE_CODE) run_container_free(ans);

    return return_typecode;
}

/*
 * Same as run_container_negation except that if the output is to
 * be a
 * run_container_t, and has the capacity to hold the result,
 * then src is modified and no allocation is made.
 * In all cases, the result is in *dst.
 */
int run_container_negation_range_inplace(run_container_t *src,
                                         const int range_start,
                                         const int range_end, void **dst) {
    uint8_t return_typecode;

    if (range_end <= range_start) {
        *dst = src;
        return RUN_CONTAINER_TYPE_CODE;
    }

    // TODO: efficient special case when range is 0 to 65535 inclusive

    if (src->capacity == src->n_runs) {
        // no excess room.  More checking to see if result can fit
        bool last_val_before_range = false;
        bool first_val_in_range = false;
        bool last_val_in_range = false;
        bool first_val_past_range = false;

        if (range_start > 0)
            last_val_before_range =
                run_container_contains(src, (uint16_t)(range_start - 1));
        first_val_in_range = run_container_contains(src, (uint16_t)range_start);

        if (last_val_before_range == first_val_in_range) {
            last_val_in_range =
                run_container_contains(src, (uint16_t)(range_end - 1));
            if (range_end != 0x10000)
                first_val_past_range =
                    run_container_contains(src, (uint16_t)range_end);

            if (last_val_in_range ==
                first_val_past_range) {  // no space for inplace
                int ans = run_container_negation_range(src, range_start,
                                                       range_end, dst);
                run_container_free(src);
                return ans;
            }
        }
    }
    // all other cases: result will fit

    run_container_t *ans = src;
    int my_nbr_runs = src->n_runs;

    ans->n_runs = 0;
    int k = 0;
    for (; (k < my_nbr_runs) && (src->runs[k].value < range_start); ++k) {
        // ans->runs[k] = src->runs[k]; (would be self-copy)
        ans->n_runs++;
    }

    // as with Java implementation, use locals to give self a buffer of depth 1
    rle16_t buffered = (rle16_t){.value = (uint16_t)0, .length = (uint16_t)0};
    rle16_t next = buffered;
    if (k < my_nbr_runs) buffered = src->runs[k];

    run_container_smart_append_exclusive(
        ans, (uint16_t)range_start, (uint16_t)(range_end - range_start - 1));

    for (; k < my_nbr_runs; ++k) {
        if (k + 1 < my_nbr_runs) next = src->runs[k + 1];

        run_container_smart_append_exclusive(ans, buffered.value,
                                             buffered.length);
        buffered = next;
    }

    *dst = convert_run_to_efficient_container(ans, &return_typecode);
    if (return_typecode != RUN_CONTAINER_TYPE_CODE) run_container_free(ans);

    return return_typecode;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_negation.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_subset.c */

bool array_container_is_subset_bitset(const array_container_t* container1,
                                      const bitset_container_t* container2) {
    if (container2->cardinality != BITSET_UNKNOWN_CARDINALITY) {
        if (container2->cardinality < container1->cardinality) {
            return false;
        }
    }
    for (int i = 0; i < container1->cardinality; ++i) {
        if (!bitset_container_contains(container2, container1->array[i])) {
            return false;
        }
    }
    return true;
}

bool run_container_is_subset_array(const run_container_t* container1,
                                   const array_container_t* container2) {
    if (run_container_cardinality(container1) > container2->cardinality)
        return false;
    int32_t start_pos = -1, stop_pos = -1;
    for (int i = 0; i < container1->n_runs; ++i) {
        int32_t start = container1->runs[i].value;
        int32_t stop = start + container1->runs[i].length;
        start_pos = advanceUntil(container2->array, stop_pos,
                                 container2->cardinality, start);
        stop_pos = advanceUntil(container2->array, stop_pos,
                                container2->cardinality, stop);
        if (start_pos == container2->cardinality) {
            return false;
        } else if (stop_pos - start_pos != stop - start ||
                   container2->array[start_pos] != start ||
                   container2->array[stop_pos] != stop) {
            return false;
        }
    }
    return true;
}

bool array_container_is_subset_run(const array_container_t* container1,
                                   const run_container_t* container2) {
    if (container1->cardinality > run_container_cardinality(container2))
        return false;
    int i_array = 0, i_run = 0;
    while (i_array < container1->cardinality && i_run < container2->n_runs) {
        uint32_t start = container2->runs[i_run].value;
        uint32_t stop = start + container2->runs[i_run].length;
        if (container1->array[i_array] < start) {
            return false;
        } else if (container1->array[i_array] > stop) {
            i_run++;
        } else {  // the value of the array is in the run
            i_array++;
        }
    }
    if (i_array == container1->cardinality) {
        return true;
    } else {
        return false;
    }
}

bool run_container_is_subset_bitset(const run_container_t* container1,
                                    const bitset_container_t* container2) {
    // todo: this code could be much faster
    if (container2->cardinality != BITSET_UNKNOWN_CARDINALITY) {
        if (container2->cardinality < run_container_cardinality(container1)) {
            return false;
        }
    } else {
        int32_t card = bitset_container_compute_cardinality(
            container2);  // modify container2?
        if (card < run_container_cardinality(container1)) {
            return false;
        }
    }
    for (int i = 0; i < container1->n_runs; ++i) {
        uint32_t run_start = container1->runs[i].value;
        uint32_t le = container1->runs[i].length;
        for (uint32_t j = run_start; j <= run_start + le; ++j) {
            if (!bitset_container_contains(container2, j)) {
                return false;
            }
        }
    }
    return true;
}

bool bitset_container_is_subset_run(const bitset_container_t* container1,
                                    const run_container_t* container2) {
    // todo: this code could be much faster
    if (container1->cardinality != BITSET_UNKNOWN_CARDINALITY) {
        if (container1->cardinality > run_container_cardinality(container2)) {
            return false;
        }
    }
    int32_t i_bitset = 0, i_run = 0;
    while (i_bitset < BITSET_CONTAINER_SIZE_IN_WORDS &&
           i_run < container2->n_runs) {
        uint64_t w = container1->array[i_bitset];
        while (w != 0 && i_run < container2->n_runs) {
            uint32_t start = container2->runs[i_run].value;
            uint32_t stop = start + container2->runs[i_run].length;
            uint64_t t = w & (~w + 1);
            uint16_t r = i_bitset * 64 + __builtin_ctzll(w);
            if (r < start) {
                return false;
            } else if (r > stop) {
                i_run++;
                continue;
            } else {
                w ^= t;
            }
        }
        if (w == 0) {
            i_bitset++;
        } else {
            return false;
        }
    }
    if (i_bitset < BITSET_CONTAINER_SIZE_IN_WORDS) {
        // terminated iterating on the run containers, check that rest of bitset
        // is empty
        for (; i_bitset < BITSET_CONTAINER_SIZE_IN_WORDS; i_bitset++) {
            if (container1->array[i_bitset] != 0) {
                return false;
            }
        }
    }
    return true;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_subset.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_union.c */
/*
 * mixed_union.c
 *
 */

#include <assert.h>
#include <string.h>


/* Compute the union of src_1 and src_2 and write the result to
 * dst.  */
void array_bitset_container_union(const array_container_t *src_1,
                                  const bitset_container_t *src_2,
                                  bitset_container_t *dst) {
    if (src_2 != dst) bitset_container_copy(src_2, dst);
    dst->cardinality = (int32_t)bitset_set_list_withcard(
        dst->array, dst->cardinality, src_1->array, src_1->cardinality);
}

/* Compute the union of src_1 and src_2 and write the result to
 * dst. It is allowed for src_2 to be dst.  This version does not
 * update the cardinality of dst (it is set to BITSET_UNKNOWN_CARDINALITY). */
void array_bitset_container_lazy_union(const array_container_t *src_1,
                                       const bitset_container_t *src_2,
                                       bitset_container_t *dst) {
    if (src_2 != dst) bitset_container_copy(src_2, dst);
    bitset_set_list(dst->array, src_1->array, src_1->cardinality);
    dst->cardinality = BITSET_UNKNOWN_CARDINALITY;
}

void run_bitset_container_union(const run_container_t *src_1,
                                const bitset_container_t *src_2,
                                bitset_container_t *dst) {
    assert(!run_container_is_full(src_1));  // catch this case upstream
    if (src_2 != dst) bitset_container_copy(src_2, dst);
    for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
        rle16_t rle = src_1->runs[rlepos];
        bitset_set_lenrange(dst->array, rle.value, rle.length);
    }
    dst->cardinality = bitset_container_compute_cardinality(dst);
}

void run_bitset_container_lazy_union(const run_container_t *src_1,
                                     const bitset_container_t *src_2,
                                     bitset_container_t *dst) {
    assert(!run_container_is_full(src_1));  // catch this case upstream
    if (src_2 != dst) bitset_container_copy(src_2, dst);
    for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
        rle16_t rle = src_1->runs[rlepos];
        bitset_set_lenrange(dst->array, rle.value, rle.length);
    }
    dst->cardinality = BITSET_UNKNOWN_CARDINALITY;
}

// why do we leave the result as a run container??
void array_run_container_union(const array_container_t *src_1,
                               const run_container_t *src_2,
                               run_container_t *dst) {
    if (run_container_is_full(src_2)) {
        run_container_copy(src_2, dst);
        return;
    }
    // TODO: see whether the "2*" is spurious
    run_container_grow(dst, 2 * (src_1->cardinality + src_2->n_runs), false);
    int32_t rlepos = 0;
    int32_t arraypos = 0;
    rle16_t previousrle;
    if (src_2->runs[rlepos].value <= src_1->array[arraypos]) {
        previousrle = run_container_append_first(dst, src_2->runs[rlepos]);
        rlepos++;
    } else {
        previousrle =
            run_container_append_value_first(dst, src_1->array[arraypos]);
        arraypos++;
    }
    while ((rlepos < src_2->n_runs) && (arraypos < src_1->cardinality)) {
        if (src_2->runs[rlepos].value <= src_1->array[arraypos]) {
            run_container_append(dst, src_2->runs[rlepos], &previousrle);
            rlepos++;
        } else {
            run_container_append_value(dst, src_1->array[arraypos],
                                       &previousrle);
            arraypos++;
        }
    }
    if (arraypos < src_1->cardinality) {
        while (arraypos < src_1->cardinality) {
            run_container_append_value(dst, src_1->array[arraypos],
                                       &previousrle);
            arraypos++;
        }
    } else {
        while (rlepos < src_2->n_runs) {
            run_container_append(dst, src_2->runs[rlepos], &previousrle);
            rlepos++;
        }
    }
}

void array_run_container_inplace_union(const array_container_t *src_1,
                                       run_container_t *src_2) {
    if (run_container_is_full(src_2)) {
        return;
    }
    const int32_t maxoutput = src_1->cardinality + src_2->n_runs;
    const int32_t neededcapacity = maxoutput + src_2->n_runs;
    if (src_2->capacity < neededcapacity)
        run_container_grow(src_2, neededcapacity, true);
    memmove(src_2->runs + maxoutput, src_2->runs,
            src_2->n_runs * sizeof(rle16_t));
    rle16_t *inputsrc2 = src_2->runs + maxoutput;
    int32_t rlepos = 0;
    int32_t arraypos = 0;
    int src2nruns = src_2->n_runs;
    src_2->n_runs = 0;

    rle16_t previousrle;

    if (inputsrc2[rlepos].value <= src_1->array[arraypos]) {
        previousrle = run_container_append_first(src_2, inputsrc2[rlepos]);
        rlepos++;
    } else {
        previousrle =
            run_container_append_value_first(src_2, src_1->array[arraypos]);
        arraypos++;
    }

    while ((rlepos < src2nruns) && (arraypos < src_1->cardinality)) {
        if (inputsrc2[rlepos].value <= src_1->array[arraypos]) {
            run_container_append(src_2, inputsrc2[rlepos], &previousrle);
            rlepos++;
        } else {
            run_container_append_value(src_2, src_1->array[arraypos],
                                       &previousrle);
            arraypos++;
        }
    }
    if (arraypos < src_1->cardinality) {
        while (arraypos < src_1->cardinality) {
            run_container_append_value(src_2, src_1->array[arraypos],
                                       &previousrle);
            arraypos++;
        }
    } else {
        while (rlepos < src2nruns) {
            run_container_append(src_2, inputsrc2[rlepos], &previousrle);
            rlepos++;
        }
    }
}

bool array_array_container_union(const array_container_t *src_1,
                                 const array_container_t *src_2, void **dst) {
    int totalCardinality = src_1->cardinality + src_2->cardinality;
    if (totalCardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_create_given_capacity(totalCardinality);
        if (*dst != NULL) {
            array_container_union(src_1, src_2, (array_container_t *)*dst);
        } else {
            return true; // otherwise failure won't be caught
        }
        return false;  // not a bitset
    }
    *dst = bitset_container_create();
    bool returnval = true;  // expect a bitset
    if (*dst != NULL) {
        bitset_container_t *ourbitset = (bitset_container_t *)*dst;
        bitset_set_list(ourbitset->array, src_1->array, src_1->cardinality);
        ourbitset->cardinality = (int32_t)bitset_set_list_withcard(
            ourbitset->array, src_1->cardinality, src_2->array,
            src_2->cardinality);
        if (ourbitset->cardinality <= DEFAULT_MAX_SIZE) {
            // need to convert!
            *dst = array_container_from_bitset(ourbitset);
            bitset_container_free(ourbitset);
            returnval = false;  // not going to be a bitset
        }
    }
    return returnval;
}

bool array_array_container_inplace_union(array_container_t *src_1,
                                 const array_container_t *src_2, void **dst) {
    int totalCardinality = src_1->cardinality + src_2->cardinality;
    *dst = NULL;
    if (totalCardinality <= DEFAULT_MAX_SIZE) {
        if(src_1->capacity < totalCardinality) {
          *dst = array_container_create_given_capacity(2  * totalCardinality); // be purposefully generous
          if (*dst != NULL) {
              array_container_union(src_1, src_2, (array_container_t *)*dst);
          } else {
              return true; // otherwise failure won't be caught
          }
          return false;  // not a bitset
        } else {
          memmove(src_1->array + src_2->cardinality, src_1->array, src_1->cardinality * sizeof(uint16_t));
          src_1->cardinality = (int32_t)fast_union_uint16(src_1->array + src_2->cardinality, src_1->cardinality,
                                  src_2->array, src_2->cardinality, src_1->array);
          return false; // not a bitset
        }
    }
    *dst = bitset_container_create();
    bool returnval = true;  // expect a bitset
    if (*dst != NULL) {
        bitset_container_t *ourbitset = (bitset_container_t *)*dst;
        bitset_set_list(ourbitset->array, src_1->array, src_1->cardinality);
        ourbitset->cardinality = (int32_t)bitset_set_list_withcard(
            ourbitset->array, src_1->cardinality, src_2->array,
            src_2->cardinality);
        if (ourbitset->cardinality <= DEFAULT_MAX_SIZE) {
            // need to convert!
            if(src_1->capacity < ourbitset->cardinality) {
              array_container_grow(src_1, ourbitset->cardinality, false);
            }

            bitset_extract_setbits_uint16(ourbitset->array, BITSET_CONTAINER_SIZE_IN_WORDS,
                                  src_1->array, 0);
            src_1->cardinality =  ourbitset->cardinality;
            *dst = src_1;
            bitset_container_free(ourbitset);
            returnval = false;  // not going to be a bitset
        }
    }
    return returnval;
}


bool array_array_container_lazy_union(const array_container_t *src_1,
                                      const array_container_t *src_2,
                                      void **dst) {
    int totalCardinality = src_1->cardinality + src_2->cardinality;
    if (totalCardinality <= ARRAY_LAZY_LOWERBOUND) {
        *dst = array_container_create_given_capacity(totalCardinality);
        if (*dst != NULL) {
            array_container_union(src_1, src_2, (array_container_t *)*dst);
        } else {
              return true; // otherwise failure won't be caught
        }
        return false;  // not a bitset
    }
    *dst = bitset_container_create();
    bool returnval = true;  // expect a bitset
    if (*dst != NULL) {
        bitset_container_t *ourbitset = (bitset_container_t *)*dst;
        bitset_set_list(ourbitset->array, src_1->array, src_1->cardinality);
        bitset_set_list(ourbitset->array, src_2->array, src_2->cardinality);
        ourbitset->cardinality = BITSET_UNKNOWN_CARDINALITY;
    }
    return returnval;
}


bool array_array_container_lazy_inplace_union(array_container_t *src_1,
                                      const array_container_t *src_2,
                                      void **dst) {
    int totalCardinality = src_1->cardinality + src_2->cardinality;
    *dst = NULL;
    if (totalCardinality <= ARRAY_LAZY_LOWERBOUND) {
        if(src_1->capacity < totalCardinality) {
          *dst = array_container_create_given_capacity(2  * totalCardinality); // be purposefully generous
          if (*dst != NULL) {
              array_container_union(src_1, src_2, (array_container_t *)*dst);
          } else {
            return true; // otherwise failure won't be caught
          }
          return false;  // not a bitset
        } else {
          memmove(src_1->array + src_2->cardinality, src_1->array, src_1->cardinality * sizeof(uint16_t));
          src_1->cardinality = (int32_t)fast_union_uint16(src_1->array + src_2->cardinality, src_1->cardinality,
                                  src_2->array, src_2->cardinality, src_1->array);
          return false; // not a bitset
        }
    }
    *dst = bitset_container_create();
    bool returnval = true;  // expect a bitset
    if (*dst != NULL) {
        bitset_container_t *ourbitset = (bitset_container_t *)*dst;
        bitset_set_list(ourbitset->array, src_1->array, src_1->cardinality);
        bitset_set_list(ourbitset->array, src_2->array, src_2->cardinality);
        ourbitset->cardinality = BITSET_UNKNOWN_CARDINALITY;
    }
    return returnval;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_union.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_xor.c */
/*
 * mixed_xor.c
 */

#include <assert.h>
#include <string.h>


/* Compute the xor of src_1 and src_2 and write the result to
 * dst (which has no container initially).
 * Result is true iff dst is a bitset  */
bool array_bitset_container_xor(const array_container_t *src_1,
                                const bitset_container_t *src_2, void **dst) {
    bitset_container_t *result = bitset_container_create();
    bitset_container_copy(src_2, result);
    result->cardinality = (int32_t)bitset_flip_list_withcard(
        result->array, result->cardinality, src_1->array, src_1->cardinality);

    // do required type conversions.
    if (result->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(result);
        bitset_container_free(result);
        return false;  // not bitset
    }
    *dst = result;
    return true;  // bitset
}

/* Compute the xor of src_1 and src_2 and write the result to
 * dst. It is allowed for src_2 to be dst.  This version does not
 * update the cardinality of dst (it is set to BITSET_UNKNOWN_CARDINALITY).
 */

void array_bitset_container_lazy_xor(const array_container_t *src_1,
                                     const bitset_container_t *src_2,
                                     bitset_container_t *dst) {
    if (src_2 != dst) bitset_container_copy(src_2, dst);
    bitset_flip_list(dst->array, src_1->array, src_1->cardinality);
    dst->cardinality = BITSET_UNKNOWN_CARDINALITY;
}

/* Compute the xor of src_1 and src_2 and write the result to
 * dst. Result may be either a bitset or an array container
 * (returns "result is bitset"). dst does not initially have
 * any container, but becomes either a bitset container (return
 * result true) or an array container.
 */

bool run_bitset_container_xor(const run_container_t *src_1,
                              const bitset_container_t *src_2, void **dst) {
    bitset_container_t *result = bitset_container_create();

    bitset_container_copy(src_2, result);
    for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
        rle16_t rle = src_1->runs[rlepos];
        bitset_flip_range(result->array, rle.value,
                          rle.value + rle.length + UINT32_C(1));
    }
    result->cardinality = bitset_container_compute_cardinality(result);

    if (result->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(result);
        bitset_container_free(result);
        return false;  // not bitset
    }
    *dst = result;
    return true;  // bitset
}

/* lazy xor.  Dst is initialized and may be equal to src_2.
 *  Result is left as a bitset container, even if actual
 *  cardinality would dictate an array container.
 */

void run_bitset_container_lazy_xor(const run_container_t *src_1,
                                   const bitset_container_t *src_2,
                                   bitset_container_t *dst) {
    if (src_2 != dst) bitset_container_copy(src_2, dst);
    for (int32_t rlepos = 0; rlepos < src_1->n_runs; ++rlepos) {
        rle16_t rle = src_1->runs[rlepos];
        bitset_flip_range(dst->array, rle.value,
                          rle.value + rle.length + UINT32_C(1));
    }
    dst->cardinality = BITSET_UNKNOWN_CARDINALITY;
}

/* dst does not indicate a valid container initially.  Eventually it
 * can become any kind of container.
 */

int array_run_container_xor(const array_container_t *src_1,
                            const run_container_t *src_2, void **dst) {
    // semi following Java XOR implementation as of May 2016
    // the C OR implementation works quite differently and can return a run
    // container
    // TODO could optimize for full run containers.

    // use of lazy following Java impl.
    const int arbitrary_threshold = 32;
    if (src_1->cardinality < arbitrary_threshold) {
        run_container_t *ans = run_container_create();
        array_run_container_lazy_xor(src_1, src_2, ans);  // keeps runs.
        uint8_t typecode_after;
        *dst =
            convert_run_to_efficient_container_and_free(ans, &typecode_after);
        return typecode_after;
    }

    int card = run_container_cardinality(src_2);
    if (card <= DEFAULT_MAX_SIZE) {
        // Java implementation works with the array, xoring the run elements via
        // iterator
        array_container_t *temp = array_container_from_run(src_2);
        bool ret_is_bitset = array_array_container_xor(temp, src_1, dst);
        array_container_free(temp);
        return ret_is_bitset ? BITSET_CONTAINER_TYPE_CODE
                             : ARRAY_CONTAINER_TYPE_CODE;

    } else {  // guess that it will end up as a bitset
        bitset_container_t *result = bitset_container_from_run(src_2);
        bool is_bitset = bitset_array_container_ixor(result, src_1, dst);
        // any necessary type conversion has been done by the ixor
        int retval = (is_bitset ? BITSET_CONTAINER_TYPE_CODE
                                : ARRAY_CONTAINER_TYPE_CODE);
        return retval;
    }
}

/* Dst is a valid run container. (Can it be src_2? Let's say not.)
 * Leaves result as run container, even if other options are
 * smaller.
 */

void array_run_container_lazy_xor(const array_container_t *src_1,
                                  const run_container_t *src_2,
                                  run_container_t *dst) {
    run_container_grow(dst, src_1->cardinality + src_2->n_runs, false);
    int32_t rlepos = 0;
    int32_t arraypos = 0;
    dst->n_runs = 0;

    while ((rlepos < src_2->n_runs) && (arraypos < src_1->cardinality)) {
        if (src_2->runs[rlepos].value <= src_1->array[arraypos]) {
            run_container_smart_append_exclusive(dst, src_2->runs[rlepos].value,
                                                 src_2->runs[rlepos].length);
            rlepos++;
        } else {
            run_container_smart_append_exclusive(dst, src_1->array[arraypos],
                                                 0);
            arraypos++;
        }
    }
    while (arraypos < src_1->cardinality) {
        run_container_smart_append_exclusive(dst, src_1->array[arraypos], 0);
        arraypos++;
    }
    while (rlepos < src_2->n_runs) {
        run_container_smart_append_exclusive(dst, src_2->runs[rlepos].value,
                                             src_2->runs[rlepos].length);
        rlepos++;
    }
}

/* dst does not indicate a valid container initially.  Eventually it
 * can become any kind of container.
 */

int run_run_container_xor(const run_container_t *src_1,
                          const run_container_t *src_2, void **dst) {
    run_container_t *ans = run_container_create();
    run_container_xor(src_1, src_2, ans);
    uint8_t typecode_after;
    *dst = convert_run_to_efficient_container_and_free(ans, &typecode_after);
    return typecode_after;
}

/*
 * Java implementation (as of May 2016) for array_run, run_run
 * and  bitset_run don't do anything different for inplace.
 * Could adopt the mixed_union.c approach instead (ie, using
 * smart_append_exclusive)
 *
 */

bool array_array_container_xor(const array_container_t *src_1,
                               const array_container_t *src_2, void **dst) {
    int totalCardinality =
        src_1->cardinality + src_2->cardinality;  // upper bound
    if (totalCardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_create_given_capacity(totalCardinality);
        array_container_xor(src_1, src_2, (array_container_t *)*dst);
        return false;  // not a bitset
    }
    *dst = bitset_container_from_array(src_1);
    bool returnval = true;  // expect a bitset
    bitset_container_t *ourbitset = (bitset_container_t *)*dst;
    ourbitset->cardinality = (uint32_t)bitset_flip_list_withcard(
        ourbitset->array, src_1->cardinality, src_2->array, src_2->cardinality);
    if (ourbitset->cardinality <= DEFAULT_MAX_SIZE) {
        // need to convert!
        *dst = array_container_from_bitset(ourbitset);
        bitset_container_free(ourbitset);
        returnval = false;  // not going to be a bitset
    }

    return returnval;
}

bool array_array_container_lazy_xor(const array_container_t *src_1,
                                    const array_container_t *src_2,
                                    void **dst) {
    int totalCardinality = src_1->cardinality + src_2->cardinality;
    // upper bound, but probably poor estimate for xor
    if (totalCardinality <= ARRAY_LAZY_LOWERBOUND) {
        *dst = array_container_create_given_capacity(totalCardinality);
        if (*dst != NULL)
            array_container_xor(src_1, src_2, (array_container_t *)*dst);
        return false;  // not a bitset
    }
    *dst = bitset_container_from_array(src_1);
    bool returnval = true;  // expect a bitset (maybe, for XOR??)
    if (*dst != NULL) {
        bitset_container_t *ourbitset = (bitset_container_t *)*dst;
        bitset_flip_list(ourbitset->array, src_2->array, src_2->cardinality);
        ourbitset->cardinality = BITSET_UNKNOWN_CARDINALITY;
    }
    return returnval;
}

/* Compute the xor of src_1 and src_2 and write the result to
 * dst (which has no container initially). Return value is
 * "dst is a bitset"
 */

bool bitset_bitset_container_xor(const bitset_container_t *src_1,
                                 const bitset_container_t *src_2, void **dst) {
    bitset_container_t *ans = bitset_container_create();
    int card = bitset_container_xor(src_1, src_2, ans);
    if (card <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(ans);
        bitset_container_free(ans);
        return false;  // not bitset
    } else {
        *dst = ans;
        return true;
    }
}

/* Compute the xor of src_1 and src_2 and write the result to
 * dst (which has no container initially).  It will modify src_1
 * to be dst if the result is a bitset.  Otherwise, it will
 * free src_1 and dst will be a new array container.  In both
 * cases, the caller is responsible for deallocating dst.
 * Returns true iff dst is a bitset  */

bool bitset_array_container_ixor(bitset_container_t *src_1,
                                 const array_container_t *src_2, void **dst) {
    *dst = src_1;
    src_1->cardinality = (uint32_t)bitset_flip_list_withcard(
        src_1->array, src_1->cardinality, src_2->array, src_2->cardinality);

    if (src_1->cardinality <= DEFAULT_MAX_SIZE) {
        *dst = array_container_from_bitset(src_1);
        bitset_container_free(src_1);
        return false;  // not bitset
    } else
        return true;
}

/* a bunch of in-place, some of which may not *really* be inplace.
 * TODO: write actual inplace routine if efficiency warrants it
 * Anything inplace with a bitset is a good candidate
 */

bool bitset_bitset_container_ixor(bitset_container_t *src_1,
                                  const bitset_container_t *src_2, void **dst) {
    bool ans = bitset_bitset_container_xor(src_1, src_2, dst);
    bitset_container_free(src_1);
    return ans;
}

bool array_bitset_container_ixor(array_container_t *src_1,
                                 const bitset_container_t *src_2, void **dst) {
    bool ans = array_bitset_container_xor(src_1, src_2, dst);
    array_container_free(src_1);
    return ans;
}

/* Compute the xor of src_1 and src_2 and write the result to
 * dst. Result may be either a bitset or an array container
 * (returns "result is bitset"). dst does not initially have
 * any container, but becomes either a bitset container (return
 * result true) or an array container.
 */

bool run_bitset_container_ixor(run_container_t *src_1,
                               const bitset_container_t *src_2, void **dst) {
    bool ans = run_bitset_container_xor(src_1, src_2, dst);
    run_container_free(src_1);
    return ans;
}

bool bitset_run_container_ixor(bitset_container_t *src_1,
                               const run_container_t *src_2, void **dst) {
    bool ans = run_bitset_container_xor(src_2, src_1, dst);
    bitset_container_free(src_1);
    return ans;
}

/* dst does not indicate a valid container initially.  Eventually it
 * can become any kind of container.
 */

int array_run_container_ixor(array_container_t *src_1,
                             const run_container_t *src_2, void **dst) {
    int ans = array_run_container_xor(src_1, src_2, dst);
    array_container_free(src_1);
    return ans;
}

int run_array_container_ixor(run_container_t *src_1,
                             const array_container_t *src_2, void **dst) {
    int ans = array_run_container_xor(src_2, src_1, dst);
    run_container_free(src_1);
    return ans;
}

bool array_array_container_ixor(array_container_t *src_1,
                                const array_container_t *src_2, void **dst) {
    bool ans = array_array_container_xor(src_1, src_2, dst);
    array_container_free(src_1);
    return ans;
}

int run_run_container_ixor(run_container_t *src_1, const run_container_t *src_2,
                           void **dst) {
    int ans = run_run_container_xor(src_1, src_2, dst);
    run_container_free(src_1);
    return ans;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/mixed_xor.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/containers/run.c */
#include <stdio.h>
#include <stdlib.h>


extern inline uint16_t run_container_minimum(const run_container_t *run);
extern inline uint16_t run_container_maximum(const run_container_t *run);
extern inline int32_t interleavedBinarySearch(const rle16_t *array,
                                              int32_t lenarray, uint16_t ikey);
extern inline bool run_container_contains(const run_container_t *run,
                                          uint16_t pos);
extern inline int run_container_index_equalorlarger(const run_container_t *arr, uint16_t x);
extern bool run_container_is_full(const run_container_t *run);
extern bool run_container_nonzero_cardinality(const run_container_t *r);
extern void run_container_clear(run_container_t *run);
extern int32_t run_container_serialized_size_in_bytes(int32_t num_runs);
extern run_container_t *run_container_create_range(uint32_t start,
                                                   uint32_t stop);

bool run_container_add(run_container_t *run, uint16_t pos) {
    int32_t index = interleavedBinarySearch(run->runs, run->n_runs, pos);
    if (index >= 0) return false;  // already there
    index = -index - 2;            // points to preceding value, possibly -1
    if (index >= 0) {              // possible match
        int32_t offset = pos - run->runs[index].value;
        int32_t le = run->runs[index].length;
        if (offset <= le) return false;  // already there
        if (offset == le + 1) {
            // we may need to fuse
            if (index + 1 < run->n_runs) {
                if (run->runs[index + 1].value == pos + 1) {
                    // indeed fusion is needed
                    run->runs[index].length = run->runs[index + 1].value +
                                              run->runs[index + 1].length -
                                              run->runs[index].value;
                    recoverRoomAtIndex(run, (uint16_t)(index + 1));
                    return true;
                }
            }
            run->runs[index].length++;
            return true;
        }
        if (index + 1 < run->n_runs) {
            // we may need to fuse
            if (run->runs[index + 1].value == pos + 1) {
                // indeed fusion is needed
                run->runs[index + 1].value = pos;
                run->runs[index + 1].length = run->runs[index + 1].length + 1;
                return true;
            }
        }
    }
    if (index == -1) {
        // we may need to extend the first run
        if (0 < run->n_runs) {
            if (run->runs[0].value == pos + 1) {
                run->runs[0].length++;
                run->runs[0].value--;
                return true;
            }
        }
    }
    makeRoomAtIndex(run, (uint16_t)(index + 1));
    run->runs[index + 1].value = pos;
    run->runs[index + 1].length = 0;
    return true;
}

/* Create a new run container. Return NULL in case of failure. */
run_container_t *run_container_create_given_capacity(int32_t size) {
    run_container_t *run;
    /* Allocate the run container itself. */
    if ((run = (run_container_t *)malloc(sizeof(run_container_t))) == NULL) {
        return NULL;
    }
    if (size <= 0 ) { // we don't want to rely on malloc(0)
        run->runs = NULL;
    } else if ((run->runs = (rle16_t *)malloc(sizeof(rle16_t) * size)) == NULL) {
        free(run);
        return NULL;
    }
    run->capacity = size;
    run->n_runs = 0;
    return run;
}

int run_container_shrink_to_fit(run_container_t *src) {
    if (src->n_runs == src->capacity) return 0;  // nothing to do
    int savings = src->capacity - src->n_runs;
    src->capacity = src->n_runs;
    rle16_t *oldruns = src->runs;
    src->runs = (rle16_t *)realloc(oldruns, src->capacity * sizeof(rle16_t));
    if (src->runs == NULL) free(oldruns);  // should never happen?
    return savings;
}
/* Create a new run container. Return NULL in case of failure. */
run_container_t *run_container_create(void) {
    return run_container_create_given_capacity(RUN_DEFAULT_INIT_SIZE);
}

run_container_t *run_container_clone(const run_container_t *src) {
    run_container_t *run = run_container_create_given_capacity(src->capacity);
    if (run == NULL) return NULL;
    run->capacity = src->capacity;
    run->n_runs = src->n_runs;
    memcpy(run->runs, src->runs, src->n_runs * sizeof(rle16_t));
    return run;
}

/* Free memory. */
void run_container_free(run_container_t *run) {
    if(run->runs != NULL) {// Jon Strabala reports that some tools complain otherwise
      free(run->runs);
      run->runs = NULL;  // pedantic
    }
    free(run);
}

void run_container_grow(run_container_t *run, int32_t min, bool copy) {
    int32_t newCapacity =
        (run->capacity == 0)
            ? RUN_DEFAULT_INIT_SIZE
            : run->capacity < 64 ? run->capacity * 2
                                 : run->capacity < 1024 ? run->capacity * 3 / 2
                                                        : run->capacity * 5 / 4;
    if (newCapacity < min) newCapacity = min;
    run->capacity = newCapacity;
    assert(run->capacity >= min);
    if (copy) {
        rle16_t *oldruns = run->runs;
        run->runs =
            (rle16_t *)realloc(oldruns, run->capacity * sizeof(rle16_t));
        if (run->runs == NULL) free(oldruns);
    } else {
        // Jon Strabala reports that some tools complain otherwise
        if (run->runs != NULL) {
          free(run->runs);
        }
        run->runs = (rle16_t *)malloc(run->capacity * sizeof(rle16_t));
    }
    // handle the case where realloc fails
    if (run->runs == NULL) {
      fprintf(stderr, "could not allocate memory\n");
    }
    assert(run->runs != NULL);
}

/* copy one container into another */
void run_container_copy(const run_container_t *src, run_container_t *dst) {
    const int32_t n_runs = src->n_runs;
    if (src->n_runs > dst->capacity) {
        run_container_grow(dst, n_runs, false);
    }
    dst->n_runs = n_runs;
    memcpy(dst->runs, src->runs, sizeof(rle16_t) * n_runs);
}

/* Compute the union of `src_1' and `src_2' and write the result to `dst'
 * It is assumed that `dst' is distinct from both `src_1' and `src_2'. */
void run_container_union(const run_container_t *src_1,
                         const run_container_t *src_2, run_container_t *dst) {
    // TODO: this could be a lot more efficient

    // we start out with inexpensive checks
    const bool if1 = run_container_is_full(src_1);
    const bool if2 = run_container_is_full(src_2);
    if (if1 || if2) {
        if (if1) {
            run_container_copy(src_1, dst);
            return;
        }
        if (if2) {
            run_container_copy(src_2, dst);
            return;
        }
    }
    const int32_t neededcapacity = src_1->n_runs + src_2->n_runs;
    if (dst->capacity < neededcapacity)
        run_container_grow(dst, neededcapacity, false);
    dst->n_runs = 0;
    int32_t rlepos = 0;
    int32_t xrlepos = 0;

    rle16_t previousrle;
    if (src_1->runs[rlepos].value <= src_2->runs[xrlepos].value) {
        previousrle = run_container_append_first(dst, src_1->runs[rlepos]);
        rlepos++;
    } else {
        previousrle = run_container_append_first(dst, src_2->runs[xrlepos]);
        xrlepos++;
    }

    while ((xrlepos < src_2->n_runs) && (rlepos < src_1->n_runs)) {
        rle16_t newrl;
        if (src_1->runs[rlepos].value <= src_2->runs[xrlepos].value) {
            newrl = src_1->runs[rlepos];
            rlepos++;
        } else {
            newrl = src_2->runs[xrlepos];
            xrlepos++;
        }
        run_container_append(dst, newrl, &previousrle);
    }
    while (xrlepos < src_2->n_runs) {
        run_container_append(dst, src_2->runs[xrlepos], &previousrle);
        xrlepos++;
    }
    while (rlepos < src_1->n_runs) {
        run_container_append(dst, src_1->runs[rlepos], &previousrle);
        rlepos++;
    }
}

/* Compute the union of `src_1' and `src_2' and write the result to `src_1'
 */
void run_container_union_inplace(run_container_t *src_1,
                                 const run_container_t *src_2) {
    // TODO: this could be a lot more efficient

    // we start out with inexpensive checks
    const bool if1 = run_container_is_full(src_1);
    const bool if2 = run_container_is_full(src_2);
    if (if1 || if2) {
        if (if1) {
            return;
        }
        if (if2) {
            run_container_copy(src_2, src_1);
            return;
        }
    }
    // we move the data to the end of the current array
    const int32_t maxoutput = src_1->n_runs + src_2->n_runs;
    const int32_t neededcapacity = maxoutput + src_1->n_runs;
    if (src_1->capacity < neededcapacity)
        run_container_grow(src_1, neededcapacity, true);
    memmove(src_1->runs + maxoutput, src_1->runs,
            src_1->n_runs * sizeof(rle16_t));
    rle16_t *inputsrc1 = src_1->runs + maxoutput;
    const int32_t input1nruns = src_1->n_runs;
    src_1->n_runs = 0;
    int32_t rlepos = 0;
    int32_t xrlepos = 0;

    rle16_t previousrle;
    if (inputsrc1[rlepos].value <= src_2->runs[xrlepos].value) {
        previousrle = run_container_append_first(src_1, inputsrc1[rlepos]);
        rlepos++;
    } else {
        previousrle = run_container_append_first(src_1, src_2->runs[xrlepos]);
        xrlepos++;
    }
    while ((xrlepos < src_2->n_runs) && (rlepos < input1nruns)) {
        rle16_t newrl;
        if (inputsrc1[rlepos].value <= src_2->runs[xrlepos].value) {
            newrl = inputsrc1[rlepos];
            rlepos++;
        } else {
            newrl = src_2->runs[xrlepos];
            xrlepos++;
        }
        run_container_append(src_1, newrl, &previousrle);
    }
    while (xrlepos < src_2->n_runs) {
        run_container_append(src_1, src_2->runs[xrlepos], &previousrle);
        xrlepos++;
    }
    while (rlepos < input1nruns) {
        run_container_append(src_1, inputsrc1[rlepos], &previousrle);
        rlepos++;
    }
}

/* Compute the symmetric difference of `src_1' and `src_2' and write the result
 * to `dst'
 * It is assumed that `dst' is distinct from both `src_1' and `src_2'. */
void run_container_xor(const run_container_t *src_1,
                       const run_container_t *src_2, run_container_t *dst) {
    // don't bother to convert xor with full range into negation
    // since negation is implemented similarly

    const int32_t neededcapacity = src_1->n_runs + src_2->n_runs;
    if (dst->capacity < neededcapacity)
        run_container_grow(dst, neededcapacity, false);

    int32_t pos1 = 0;
    int32_t pos2 = 0;
    dst->n_runs = 0;

    while ((pos1 < src_1->n_runs) && (pos2 < src_2->n_runs)) {
        if (src_1->runs[pos1].value <= src_2->runs[pos2].value) {
            run_container_smart_append_exclusive(dst, src_1->runs[pos1].value,
                                                 src_1->runs[pos1].length);
            pos1++;
        } else {
            run_container_smart_append_exclusive(dst, src_2->runs[pos2].value,
                                                 src_2->runs[pos2].length);
            pos2++;
        }
    }
    while (pos1 < src_1->n_runs) {
        run_container_smart_append_exclusive(dst, src_1->runs[pos1].value,
                                             src_1->runs[pos1].length);
        pos1++;
    }

    while (pos2 < src_2->n_runs) {
        run_container_smart_append_exclusive(dst, src_2->runs[pos2].value,
                                             src_2->runs[pos2].length);
        pos2++;
    }
}

/* Compute the intersection of src_1 and src_2 and write the result to
 * dst. It is assumed that dst is distinct from both src_1 and src_2. */
void run_container_intersection(const run_container_t *src_1,
                                const run_container_t *src_2,
                                run_container_t *dst) {
    const bool if1 = run_container_is_full(src_1);
    const bool if2 = run_container_is_full(src_2);
    if (if1 || if2) {
        if (if1) {
            run_container_copy(src_2, dst);
            return;
        }
        if (if2) {
            run_container_copy(src_1, dst);
            return;
        }
    }
    // TODO: this could be a lot more efficient, could use SIMD optimizations
    const int32_t neededcapacity = src_1->n_runs + src_2->n_runs;
    if (dst->capacity < neededcapacity)
        run_container_grow(dst, neededcapacity, false);
    dst->n_runs = 0;
    int32_t rlepos = 0;
    int32_t xrlepos = 0;
    int32_t start = src_1->runs[rlepos].value;
    int32_t end = start + src_1->runs[rlepos].length + 1;
    int32_t xstart = src_2->runs[xrlepos].value;
    int32_t xend = xstart + src_2->runs[xrlepos].length + 1;
    while ((rlepos < src_1->n_runs) && (xrlepos < src_2->n_runs)) {
        if (end <= xstart) {
            ++rlepos;
            if (rlepos < src_1->n_runs) {
                start = src_1->runs[rlepos].value;
                end = start + src_1->runs[rlepos].length + 1;
            }
        } else if (xend <= start) {
            ++xrlepos;
            if (xrlepos < src_2->n_runs) {
                xstart = src_2->runs[xrlepos].value;
                xend = xstart + src_2->runs[xrlepos].length + 1;
            }
        } else {  // they overlap
            const int32_t lateststart = start > xstart ? start : xstart;
            int32_t earliestend;
            if (end == xend) {  // improbable
                earliestend = end;
                rlepos++;
                xrlepos++;
                if (rlepos < src_1->n_runs) {
                    start = src_1->runs[rlepos].value;
                    end = start + src_1->runs[rlepos].length + 1;
                }
                if (xrlepos < src_2->n_runs) {
                    xstart = src_2->runs[xrlepos].value;
                    xend = xstart + src_2->runs[xrlepos].length + 1;
                }
            } else if (end < xend) {
                earliestend = end;
                rlepos++;
                if (rlepos < src_1->n_runs) {
                    start = src_1->runs[rlepos].value;
                    end = start + src_1->runs[rlepos].length + 1;
                }

            } else {  // end > xend
                earliestend = xend;
                xrlepos++;
                if (xrlepos < src_2->n_runs) {
                    xstart = src_2->runs[xrlepos].value;
                    xend = xstart + src_2->runs[xrlepos].length + 1;
                }
            }
            dst->runs[dst->n_runs].value = (uint16_t)lateststart;
            dst->runs[dst->n_runs].length =
                (uint16_t)(earliestend - lateststart - 1);
            dst->n_runs++;
        }
    }
}

/* Compute the size of the intersection of src_1 and src_2 . */
int run_container_intersection_cardinality(const run_container_t *src_1,
                                           const run_container_t *src_2) {
    const bool if1 = run_container_is_full(src_1);
    const bool if2 = run_container_is_full(src_2);
    if (if1 || if2) {
        if (if1) {
            return run_container_cardinality(src_2);
        }
        if (if2) {
            return run_container_cardinality(src_1);
        }
    }
    int answer = 0;
    int32_t rlepos = 0;
    int32_t xrlepos = 0;
    int32_t start = src_1->runs[rlepos].value;
    int32_t end = start + src_1->runs[rlepos].length + 1;
    int32_t xstart = src_2->runs[xrlepos].value;
    int32_t xend = xstart + src_2->runs[xrlepos].length + 1;
    while ((rlepos < src_1->n_runs) && (xrlepos < src_2->n_runs)) {
        if (end <= xstart) {
            ++rlepos;
            if (rlepos < src_1->n_runs) {
                start = src_1->runs[rlepos].value;
                end = start + src_1->runs[rlepos].length + 1;
            }
        } else if (xend <= start) {
            ++xrlepos;
            if (xrlepos < src_2->n_runs) {
                xstart = src_2->runs[xrlepos].value;
                xend = xstart + src_2->runs[xrlepos].length + 1;
            }
        } else {  // they overlap
            const int32_t lateststart = start > xstart ? start : xstart;
            int32_t earliestend;
            if (end == xend) {  // improbable
                earliestend = end;
                rlepos++;
                xrlepos++;
                if (rlepos < src_1->n_runs) {
                    start = src_1->runs[rlepos].value;
                    end = start + src_1->runs[rlepos].length + 1;
                }
                if (xrlepos < src_2->n_runs) {
                    xstart = src_2->runs[xrlepos].value;
                    xend = xstart + src_2->runs[xrlepos].length + 1;
                }
            } else if (end < xend) {
                earliestend = end;
                rlepos++;
                if (rlepos < src_1->n_runs) {
                    start = src_1->runs[rlepos].value;
                    end = start + src_1->runs[rlepos].length + 1;
                }

            } else {  // end > xend
                earliestend = xend;
                xrlepos++;
                if (xrlepos < src_2->n_runs) {
                    xstart = src_2->runs[xrlepos].value;
                    xend = xstart + src_2->runs[xrlepos].length + 1;
                }
            }
            answer += earliestend - lateststart;
        }
    }
    return answer;
}

bool run_container_intersect(const run_container_t *src_1,
                                const run_container_t *src_2) {
    const bool if1 = run_container_is_full(src_1);
    const bool if2 = run_container_is_full(src_2);
    if (if1 || if2) {
        if (if1) {
            return !run_container_empty(src_2);
        }
        if (if2) {
        	return !run_container_empty(src_1);
        }
    }
    int32_t rlepos = 0;
    int32_t xrlepos = 0;
    int32_t start = src_1->runs[rlepos].value;
    int32_t end = start + src_1->runs[rlepos].length + 1;
    int32_t xstart = src_2->runs[xrlepos].value;
    int32_t xend = xstart + src_2->runs[xrlepos].length + 1;
    while ((rlepos < src_1->n_runs) && (xrlepos < src_2->n_runs)) {
        if (end <= xstart) {
            ++rlepos;
            if (rlepos < src_1->n_runs) {
                start = src_1->runs[rlepos].value;
                end = start + src_1->runs[rlepos].length + 1;
            }
        } else if (xend <= start) {
            ++xrlepos;
            if (xrlepos < src_2->n_runs) {
                xstart = src_2->runs[xrlepos].value;
                xend = xstart + src_2->runs[xrlepos].length + 1;
            }
        } else {  // they overlap
            return true;
        }
    }
    return false;
}


/* Compute the difference of src_1 and src_2 and write the result to
 * dst. It is assumed that dst is distinct from both src_1 and src_2. */
void run_container_andnot(const run_container_t *src_1,
                          const run_container_t *src_2, run_container_t *dst) {
    // following Java implementation as of June 2016

    if (dst->capacity < src_1->n_runs + src_2->n_runs)
        run_container_grow(dst, src_1->n_runs + src_2->n_runs, false);

    dst->n_runs = 0;

    int rlepos1 = 0;
    int rlepos2 = 0;
    int32_t start = src_1->runs[rlepos1].value;
    int32_t end = start + src_1->runs[rlepos1].length + 1;
    int32_t start2 = src_2->runs[rlepos2].value;
    int32_t end2 = start2 + src_2->runs[rlepos2].length + 1;

    while ((rlepos1 < src_1->n_runs) && (rlepos2 < src_2->n_runs)) {
        if (end <= start2) {
            // output the first run
            dst->runs[dst->n_runs++] =
                (rle16_t){.value = (uint16_t)start,
                          .length = (uint16_t)(end - start - 1)};
            rlepos1++;
            if (rlepos1 < src_1->n_runs) {
                start = src_1->runs[rlepos1].value;
                end = start + src_1->runs[rlepos1].length + 1;
            }
        } else if (end2 <= start) {
            // exit the second run
            rlepos2++;
            if (rlepos2 < src_2->n_runs) {
                start2 = src_2->runs[rlepos2].value;
                end2 = start2 + src_2->runs[rlepos2].length + 1;
            }
        } else {
            if (start < start2) {
                dst->runs[dst->n_runs++] =
                    (rle16_t){.value = (uint16_t)start,
                              .length = (uint16_t)(start2 - start - 1)};
            }
            if (end2 < end) {
                start = end2;
            } else {
                rlepos1++;
                if (rlepos1 < src_1->n_runs) {
                    start = src_1->runs[rlepos1].value;
                    end = start + src_1->runs[rlepos1].length + 1;
                }
            }
        }
    }
    if (rlepos1 < src_1->n_runs) {
        dst->runs[dst->n_runs++] = (rle16_t){
            .value = (uint16_t)start, .length = (uint16_t)(end - start - 1)};
        rlepos1++;
        if (rlepos1 < src_1->n_runs) {
            memcpy(dst->runs + dst->n_runs, src_1->runs + rlepos1,
                   sizeof(rle16_t) * (src_1->n_runs - rlepos1));
            dst->n_runs += src_1->n_runs - rlepos1;
        }
    }
}

int run_container_to_uint32_array(void *vout, const run_container_t *cont,
                                  uint32_t base) {
    int outpos = 0;
    uint32_t *out = (uint32_t *)vout;
    for (int i = 0; i < cont->n_runs; ++i) {
        uint32_t run_start = base + cont->runs[i].value;
        uint16_t le = cont->runs[i].length;
        for (int j = 0; j <= le; ++j) {
            uint32_t val = run_start + j;
            memcpy(out + outpos, &val,
                   sizeof(uint32_t));  // should be compiled as a MOV on x64
            outpos++;
        }
    }
    return outpos;
}

/*
 * Print this container using printf (useful for debugging).
 */
void run_container_printf(const run_container_t *cont) {
    for (int i = 0; i < cont->n_runs; ++i) {
        uint16_t run_start = cont->runs[i].value;
        uint16_t le = cont->runs[i].length;
        printf("[%d,%d]", run_start, run_start + le);
    }
}

/*
 * Print this container using printf as a comma-separated list of 32-bit
 * integers starting at base.
 */
void run_container_printf_as_uint32_array(const run_container_t *cont,
                                          uint32_t base) {
    if (cont->n_runs == 0) return;
    {
        uint32_t run_start = base + cont->runs[0].value;
        uint16_t le = cont->runs[0].length;
        printf("%u", run_start);
        for (uint32_t j = 1; j <= le; ++j) printf(",%u", run_start + j);
    }
    for (int32_t i = 1; i < cont->n_runs; ++i) {
        uint32_t run_start = base + cont->runs[i].value;
        uint16_t le = cont->runs[i].length;
        for (uint32_t j = 0; j <= le; ++j) printf(",%u", run_start + j);
    }
}

int32_t run_container_serialize(const run_container_t *container, char *buf) {
    int32_t l, off;

    memcpy(buf, &container->n_runs, off = sizeof(container->n_runs));
    memcpy(&buf[off], &container->capacity, sizeof(container->capacity));
    off += sizeof(container->capacity);

    l = sizeof(rle16_t) * container->n_runs;
    memcpy(&buf[off], container->runs, l);
    return (off + l);
}

int32_t run_container_write(const run_container_t *container, char *buf) {
    memcpy(buf, &container->n_runs, sizeof(uint16_t));
    memcpy(buf + sizeof(uint16_t), container->runs,
           container->n_runs * sizeof(rle16_t));
    return run_container_size_in_bytes(container);
}

int32_t run_container_read(int32_t cardinality, run_container_t *container,
                           const char *buf) {
    (void)cardinality;
    memcpy(&container->n_runs, buf, sizeof(uint16_t));
    if (container->n_runs > container->capacity)
        run_container_grow(container, container->n_runs, false);
    if(container->n_runs > 0) {
      memcpy(container->runs, buf + sizeof(uint16_t),
           container->n_runs * sizeof(rle16_t));
    }
    return run_container_size_in_bytes(container);
}

uint32_t run_container_serialization_len(const run_container_t *container) {
    return (sizeof(container->n_runs) + sizeof(container->capacity) +
            sizeof(rle16_t) * container->n_runs);
}

void *run_container_deserialize(const char *buf, size_t buf_len) {
    run_container_t *ptr;

    if (buf_len < 8 /* n_runs + capacity */)
        return (NULL);
    else
        buf_len -= 8;

    if ((ptr = (run_container_t *)malloc(sizeof(run_container_t))) != NULL) {
        size_t len;
        int32_t off;

        memcpy(&ptr->n_runs, buf, off = 4);
        memcpy(&ptr->capacity, &buf[off], 4);
        off += 4;

        len = sizeof(rle16_t) * ptr->n_runs;

        if (len != buf_len) {
            free(ptr);
            return (NULL);
        }

        if ((ptr->runs = (rle16_t *)malloc(len)) == NULL) {
            free(ptr);
            return (NULL);
        }

        memcpy(ptr->runs, &buf[off], len);

        /* Check if returned values are monotonically increasing */
        for (int32_t i = 0, j = 0; i < ptr->n_runs; i++) {
            if (ptr->runs[i].value < j) {
                free(ptr->runs);
                free(ptr);
                return (NULL);
            } else
                j = ptr->runs[i].value;
        }
    }

    return (ptr);
}

bool run_container_iterate(const run_container_t *cont, uint32_t base,
                           roaring_iterator iterator, void *ptr) {
    for (int i = 0; i < cont->n_runs; ++i) {
        uint32_t run_start = base + cont->runs[i].value;
        uint16_t le = cont->runs[i].length;

        for (int j = 0; j <= le; ++j)
            if (!iterator(run_start + j, ptr)) return false;
    }
    return true;
}

bool run_container_iterate64(const run_container_t *cont, uint32_t base,
                             roaring_iterator64 iterator, uint64_t high_bits,
                             void *ptr) {
    for (int i = 0; i < cont->n_runs; ++i) {
        uint32_t run_start = base + cont->runs[i].value;
        uint16_t le = cont->runs[i].length;

        for (int j = 0; j <= le; ++j)
            if (!iterator(high_bits | (uint64_t)(run_start + j), ptr))
                return false;
    }
    return true;
}

bool run_container_equals(const run_container_t *container1,
                          const run_container_t *container2) {
    if (container1->n_runs != container2->n_runs) {
        return false;
    }
    for (int32_t i = 0; i < container1->n_runs; ++i) {
        if ((container1->runs[i].value != container2->runs[i].value) ||
            (container1->runs[i].length != container2->runs[i].length))
            return false;
    }
    return true;
}

bool run_container_is_subset(const run_container_t *container1,
                             const run_container_t *container2) {
    int i1 = 0, i2 = 0;
    while (i1 < container1->n_runs && i2 < container2->n_runs) {
        int start1 = container1->runs[i1].value;
        int stop1 = start1 + container1->runs[i1].length;
        int start2 = container2->runs[i2].value;
        int stop2 = start2 + container2->runs[i2].length;
        if (start1 < start2) {
            return false;
        } else {  // start1 >= start2
            if (stop1 < stop2) {
                i1++;
            } else if (stop1 == stop2) {
                i1++;
                i2++;
            } else {  // stop1 > stop2
                i2++;
            }
        }
    }
    if (i1 == container1->n_runs) {
        return true;
    } else {
        return false;
    }
}

// TODO: write smart_append_exclusive version to match the overloaded 1 param
// Java version (or  is it even used?)

// follows the Java implementation closely
// length is the rle-value.  Ie, run [10,12) uses a length value 1.
void run_container_smart_append_exclusive(run_container_t *src,
                                          const uint16_t start,
                                          const uint16_t length) {
    int old_end;
    rle16_t *last_run = src->n_runs ? src->runs + (src->n_runs - 1) : NULL;
    rle16_t *appended_last_run = src->runs + src->n_runs;

    if (!src->n_runs ||
        (start > (old_end = last_run->value + last_run->length + 1))) {
        *appended_last_run = (rle16_t){.value = start, .length = length};
        src->n_runs++;
        return;
    }
    if (old_end == start) {
        // we merge
        last_run->length += (length + 1);
        return;
    }
    int new_end = start + length + 1;

    if (start == last_run->value) {
        // wipe out previous
        if (new_end < old_end) {
            *last_run = (rle16_t){.value = (uint16_t)new_end,
                                  .length = (uint16_t)(old_end - new_end - 1)};
            return;
        } else if (new_end > old_end) {
            *last_run = (rle16_t){.value = (uint16_t)old_end,
                                  .length = (uint16_t)(new_end - old_end - 1)};
            return;
        } else {
            src->n_runs--;
            return;
        }
    }
    last_run->length = start - last_run->value - 1;
    if (new_end < old_end) {
        *appended_last_run =
            (rle16_t){.value = (uint16_t)new_end,
                      .length = (uint16_t)(old_end - new_end - 1)};
        src->n_runs++;
    } else if (new_end > old_end) {
        *appended_last_run =
            (rle16_t){.value = (uint16_t)old_end,
                      .length = (uint16_t)(new_end - old_end - 1)};
        src->n_runs++;
    }
}

bool run_container_select(const run_container_t *container,
                          uint32_t *start_rank, uint32_t rank,
                          uint32_t *element) {
    for (int i = 0; i < container->n_runs; i++) {
        uint16_t length = container->runs[i].length;
        if (rank <= *start_rank + length) {
            uint16_t value = container->runs[i].value;
            *element = value + rank - (*start_rank);
            return true;
        } else
            *start_rank += length + 1;
    }
    return false;
}

int run_container_rank(const run_container_t *container, uint16_t x) {
    int sum = 0;
    uint32_t x32 = x;
    for (int i = 0; i < container->n_runs; i++) {
        uint32_t startpoint = container->runs[i].value;
        uint32_t length = container->runs[i].length;
        uint32_t endpoint = length + startpoint;
        if (x <= endpoint) {
            if (x < startpoint) break;
            return sum + (x32 - startpoint) + 1;
        } else {
            sum += length + 1;
        }
    }
    return sum;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/containers/run.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/roaring.c */
#include <assert.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

extern inline bool roaring_bitmap_contains(const roaring_bitmap_t *r,
                                           uint32_t val);

// this is like roaring_bitmap_add, but it populates pointer arguments in such a
// way
// that we can recover the container touched, which, in turn can be used to
// accelerate some functions (when you repeatedly need to add to the same
// container)
void *containerptr_roaring_bitmap_add(roaring_bitmap_t *r,
                                                    uint32_t val,
                                                    uint8_t *typecode,
                                                    int *index) {
    uint16_t hb = val >> 16;
    const int i = ra_get_index(&r->high_low_container, hb);
    if (i >= 0) {
        ra_unshare_container_at_index(&r->high_low_container, i);
        void *container =
            ra_get_container_at_index(&r->high_low_container, i, typecode);
        uint8_t newtypecode = *typecode;
        void *container2 =
            container_add(container, val & 0xFFFF, *typecode, &newtypecode);
        *index = i;
        if (container2 != container) {
            container_free(container, *typecode);
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
            *typecode = newtypecode;
            return container2;
        } else {
            return container;
        }
    } else {
        array_container_t *newac = array_container_create();
        void *container = container_add(newac, val & 0xFFFF,
                                        ARRAY_CONTAINER_TYPE_CODE, typecode);
        // we could just assume that it stays an array container
        ra_insert_new_key_value_at(&r->high_low_container, -i - 1, hb,
                                   container, *typecode);
        *index = -i - 1;
        return container;
    }
}

roaring_bitmap_t *roaring_bitmap_create() {
    roaring_bitmap_t *ans =
        (roaring_bitmap_t *)malloc(sizeof(roaring_bitmap_t));
    if (!ans) {
        return NULL;
    }
    bool is_ok = ra_init(&ans->high_low_container);
    if (!is_ok) {
        free(ans);
        return NULL;
    }
    ans->copy_on_write = false;
    return ans;
}

roaring_bitmap_t *roaring_bitmap_create_with_capacity(uint32_t cap) {
    roaring_bitmap_t *ans =
        (roaring_bitmap_t *)malloc(sizeof(roaring_bitmap_t));
    if (!ans) {
        return NULL;
    }
    bool is_ok = ra_init_with_capacity(&ans->high_low_container, cap);
    if (!is_ok) {
        free(ans);
        return NULL;
    }
    ans->copy_on_write = false;
    return ans;
}

void roaring_bitmap_add_many(roaring_bitmap_t *r, size_t n_args,
                             const uint32_t *vals) {
    void *container = NULL;  // hold value of last container touched
    uint8_t typecode = 0;    // typecode of last container touched
    uint32_t prev = 0;       // previous valued inserted
    size_t i = 0;            // index of value
    int containerindex = 0;
    if (n_args == 0) return;
    uint32_t val;
    memcpy(&val, vals + i, sizeof(val));
    container =
        containerptr_roaring_bitmap_add(r, val, &typecode, &containerindex);
    prev = val;
    i++;
    for (; i < n_args; i++) {
        memcpy(&val, vals + i, sizeof(val));
        if (((prev ^ val) >> 16) ==
            0) {  // no need to seek the container, it is at hand
            // because we already have the container at hand, we can do the
            // insertion
            // automatically, bypassing the roaring_bitmap_add call
            uint8_t newtypecode = typecode;
            void *container2 =
                container_add(container, val & 0xFFFF, typecode, &newtypecode);
            if (container2 != container) {  // rare instance when we need to
                                            // change the container type
                container_free(container, typecode);
                ra_set_container_at_index(&r->high_low_container,
                                          containerindex, container2,
                                          newtypecode);
                typecode = newtypecode;
                container = container2;
            }
        } else {
            container = containerptr_roaring_bitmap_add(r, val, &typecode,
                                                        &containerindex);
        }
        prev = val;
    }
}

roaring_bitmap_t *roaring_bitmap_of_ptr(size_t n_args, const uint32_t *vals) {
    roaring_bitmap_t *answer = roaring_bitmap_create();
    roaring_bitmap_add_many(answer, n_args, vals);
    return answer;
}

roaring_bitmap_t *roaring_bitmap_of(size_t n_args, ...) {
    // todo: could be greatly optimized but we do not expect this call to ever
    // include long lists
    roaring_bitmap_t *answer = roaring_bitmap_create();
    va_list ap;
    va_start(ap, n_args);
    for (size_t i = 1; i <= n_args; i++) {
        uint32_t val = va_arg(ap, uint32_t);
        roaring_bitmap_add(answer, val);
    }
    va_end(ap);
    return answer;
}

static inline uint32_t minimum_uint32(uint32_t a, uint32_t b) {
    return (a < b) ? a : b;
}

static inline uint64_t minimum_uint64(uint64_t a, uint64_t b) {
    return (a < b) ? a : b;
}

roaring_bitmap_t *roaring_bitmap_from_range(uint64_t min, uint64_t max,
                                            uint32_t step) {
    if(max >= UINT64_C(0x100000000)) {
        max = UINT64_C(0x100000000);
    }
    if (step == 0) return NULL;
    if (max <= min) return NULL;
    roaring_bitmap_t *answer = roaring_bitmap_create();
    if (step >= (1 << 16)) {
        for (uint32_t value = (uint32_t)min; value < max; value += step) {
            roaring_bitmap_add(answer, value);
        }
        return answer;
    }
    uint64_t min_tmp = min;
    do {
        uint32_t key = (uint32_t)min_tmp >> 16;
        uint32_t container_min = min_tmp & 0xFFFF;
        uint32_t container_max = (uint32_t)minimum_uint64(max - (key << 16), 1 << 16);
        uint8_t type;
        void *container = container_from_range(&type, container_min,
                                               container_max, (uint16_t)step);
        ra_append(&answer->high_low_container, key, container, type);
        uint32_t gap = container_max - container_min + step - 1;
        min_tmp += gap - (gap % step);
    } while (min_tmp < max);
    // cardinality of bitmap will be ((uint64_t) max - min + step - 1 ) / step
    return answer;
}

void roaring_bitmap_add_range_closed(roaring_bitmap_t *ra, uint32_t min, uint32_t max) {
    if (min > max) {
        return;
    }

    uint32_t min_key = min >> 16;
    uint32_t max_key = max >> 16;

    int32_t num_required_containers = max_key - min_key + 1;
    int32_t suffix_length = count_greater(ra->high_low_container.keys,
                                          ra->high_low_container.size,
                                          max_key);
    int32_t prefix_length = count_less(ra->high_low_container.keys,
                                       ra->high_low_container.size - suffix_length,
                                       min_key);
    int32_t common_length = ra->high_low_container.size - prefix_length - suffix_length;

    if (num_required_containers > common_length) {
        ra_shift_tail(&ra->high_low_container, suffix_length,
                      num_required_containers - common_length);
    }

    int32_t src = prefix_length + common_length - 1;
    int32_t dst = ra->high_low_container.size - suffix_length - 1;
    for (uint32_t key = max_key; key != min_key-1; key--) { // beware of min_key==0
        uint32_t container_min = (min_key == key) ? (min & 0xffff) : 0;
        uint32_t container_max = (max_key == key) ? (max & 0xffff) : 0xffff;
        void* new_container;
        uint8_t new_type;

        if (src >= 0 && ra->high_low_container.keys[src] == key) {
            ra_unshare_container_at_index(&ra->high_low_container, src);
            new_container = container_add_range(ra->high_low_container.containers[src],
                                                ra->high_low_container.typecodes[src],
                                                container_min, container_max, &new_type);
            if (new_container != ra->high_low_container.containers[src]) {
                container_free(ra->high_low_container.containers[src],
                               ra->high_low_container.typecodes[src]);
            }
            src--;
        } else {
            new_container = container_from_range(&new_type, container_min,
                                                 container_max+1, 1);
        }
        ra_replace_key_and_container_at_index(&ra->high_low_container, dst,
                                              key, new_container, new_type);
        dst--;
    }
}

void roaring_bitmap_remove_range_closed(roaring_bitmap_t *ra, uint32_t min, uint32_t max) {
    if (min > max) {
        return;
    }

    uint32_t min_key = min >> 16;
    uint32_t max_key = max >> 16;

    int32_t src = count_less(ra->high_low_container.keys, ra->high_low_container.size, min_key);
    int32_t dst = src;
    while (src < ra->high_low_container.size && ra->high_low_container.keys[src] <= max_key) {
        uint32_t container_min = (min_key == ra->high_low_container.keys[src]) ? (min & 0xffff) : 0;
        uint32_t container_max = (max_key == ra->high_low_container.keys[src]) ? (max & 0xffff) : 0xffff;
        ra_unshare_container_at_index(&ra->high_low_container, src);
        void *new_container;
        uint8_t new_type;
        new_container = container_remove_range(ra->high_low_container.containers[src],
                                               ra->high_low_container.typecodes[src],
                                               container_min, container_max,
                                               &new_type);
        if (new_container != ra->high_low_container.containers[src]) {
            container_free(ra->high_low_container.containers[src],
                           ra->high_low_container.typecodes[src]);
        }
        if (new_container) {
            ra_replace_key_and_container_at_index(&ra->high_low_container, dst,
                                                  ra->high_low_container.keys[src],
                                                  new_container, new_type);
            dst++;
        }
        src++;
    }
    if (src > dst) {
        ra_shift_tail(&ra->high_low_container, ra->high_low_container.size - src, dst - src);
    }
}

void roaring_bitmap_add_range(roaring_bitmap_t *ra, uint64_t min, uint64_t max);
void roaring_bitmap_remove_range(roaring_bitmap_t *ra, uint64_t min, uint64_t max);

void roaring_bitmap_printf(const roaring_bitmap_t *ra) {
    printf("{");
    for (int i = 0; i < ra->high_low_container.size; ++i) {
        container_printf_as_uint32_array(
            ra->high_low_container.containers[i],
            ra->high_low_container.typecodes[i],
            ((uint32_t)ra->high_low_container.keys[i]) << 16);
        if (i + 1 < ra->high_low_container.size) printf(",");
    }
    printf("}");
}

void roaring_bitmap_printf_describe(const roaring_bitmap_t *ra) {
    printf("{");
    for (int i = 0; i < ra->high_low_container.size; ++i) {
        printf("%d: %s (%d)", ra->high_low_container.keys[i],
               get_full_container_name(ra->high_low_container.containers[i],
                                       ra->high_low_container.typecodes[i]),
               container_get_cardinality(ra->high_low_container.containers[i],
                                         ra->high_low_container.typecodes[i]));
        if (ra->high_low_container.typecodes[i] == SHARED_CONTAINER_TYPE_CODE) {
            printf(
                "(shared count = %" PRIu32 " )",
                ((shared_container_t *)(ra->high_low_container.containers[i]))
                    ->counter);
        }

        if (i + 1 < ra->high_low_container.size) printf(", ");
    }
    printf("}");
}

typedef struct min_max_sum_s {
    uint32_t min;
    uint32_t max;
    uint64_t sum;
} min_max_sum_t;

static bool min_max_sum_fnc(uint32_t value, void *param) {
    min_max_sum_t *mms = (min_max_sum_t *)param;
    if (value > mms->max) mms->max = value;
    if (value < mms->min) mms->min = value;
    mms->sum += value;
    return true;  // we always process all data points
}

/**
*  (For advanced users.)
* Collect statistics about the bitmap
*/
void roaring_bitmap_statistics(const roaring_bitmap_t *ra,
                               roaring_statistics_t *stat) {
    memset(stat, 0, sizeof(*stat));
    stat->n_containers = ra->high_low_container.size;
    stat->cardinality = roaring_bitmap_get_cardinality(ra);
    min_max_sum_t mms;
    mms.min = UINT32_C(0xFFFFFFFF);
    mms.max = UINT32_C(0);
    mms.sum = 0;
    roaring_iterate(ra, &min_max_sum_fnc, &mms);
    stat->min_value = mms.min;
    stat->max_value = mms.max;
    stat->sum_value = mms.sum;

    for (int i = 0; i < ra->high_low_container.size; ++i) {
        uint8_t truetype =
            get_container_type(ra->high_low_container.containers[i],
                               ra->high_low_container.typecodes[i]);
        uint32_t card =
            container_get_cardinality(ra->high_low_container.containers[i],
                                      ra->high_low_container.typecodes[i]);
        uint32_t sbytes =
            container_size_in_bytes(ra->high_low_container.containers[i],
                                    ra->high_low_container.typecodes[i]);
        switch (truetype) {
            case BITSET_CONTAINER_TYPE_CODE:
                stat->n_bitset_containers++;
                stat->n_values_bitset_containers += card;
                stat->n_bytes_bitset_containers += sbytes;
                break;
            case ARRAY_CONTAINER_TYPE_CODE:
                stat->n_array_containers++;
                stat->n_values_array_containers += card;
                stat->n_bytes_array_containers += sbytes;
                break;
            case RUN_CONTAINER_TYPE_CODE:
                stat->n_run_containers++;
                stat->n_values_run_containers += card;
                stat->n_bytes_run_containers += sbytes;
                break;
            default:
                assert(false);
                __builtin_unreachable();
        }
    }
}

roaring_bitmap_t *roaring_bitmap_copy(const roaring_bitmap_t *r) {
    roaring_bitmap_t *ans =
        (roaring_bitmap_t *)malloc(sizeof(roaring_bitmap_t));
    if (!ans) {
        return NULL;
    }
    bool is_ok = ra_copy(&r->high_low_container, &ans->high_low_container,
                         r->copy_on_write);
    if (!is_ok) {
        free(ans);
        return NULL;
    }
    ans->copy_on_write = r->copy_on_write;
    return ans;
}

bool roaring_bitmap_overwrite(roaring_bitmap_t *dest,
                                     const roaring_bitmap_t *src) {
    return ra_overwrite(&src->high_low_container, &dest->high_low_container,
                        src->copy_on_write);
}

void roaring_bitmap_free(roaring_bitmap_t *r) {
    ra_clear(&r->high_low_container);
    free(r);
}

void roaring_bitmap_clear(roaring_bitmap_t *r) {
  ra_reset(&r->high_low_container);
}

void roaring_bitmap_add(roaring_bitmap_t *r, uint32_t val) {
    const uint16_t hb = val >> 16;
    const int i = ra_get_index(&r->high_low_container, hb);
    uint8_t typecode;
    if (i >= 0) {
        ra_unshare_container_at_index(&r->high_low_container, i);
        void *container =
            ra_get_container_at_index(&r->high_low_container, i, &typecode);
        uint8_t newtypecode = typecode;
        void *container2 =
            container_add(container, val & 0xFFFF, typecode, &newtypecode);
        if (container2 != container) {
            container_free(container, typecode);
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
        }
    } else {
        array_container_t *newac = array_container_create();
        void *container = container_add(newac, val & 0xFFFF,
                                        ARRAY_CONTAINER_TYPE_CODE, &typecode);
        // we could just assume that it stays an array container
        ra_insert_new_key_value_at(&r->high_low_container, -i - 1, hb,
                                   container, typecode);
    }
}

bool roaring_bitmap_add_checked(roaring_bitmap_t *r, uint32_t val) {
    const uint16_t hb = val >> 16;
    const int i = ra_get_index(&r->high_low_container, hb);
    uint8_t typecode;
    bool result = false;
    if (i >= 0) {
        ra_unshare_container_at_index(&r->high_low_container, i);
        void *container =
            ra_get_container_at_index(&r->high_low_container, i, &typecode);

        const int oldCardinality =
            container_get_cardinality(container, typecode);

        uint8_t newtypecode = typecode;
        void *container2 =
            container_add(container, val & 0xFFFF, typecode, &newtypecode);
        if (container2 != container) {
            container_free(container, typecode);
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
            result = true;
        } else {
            const int newCardinality =
                container_get_cardinality(container, newtypecode);

            result = oldCardinality != newCardinality;
        }
    } else {
        array_container_t *newac = array_container_create();
        void *container = container_add(newac, val & 0xFFFF,
                                        ARRAY_CONTAINER_TYPE_CODE, &typecode);
        // we could just assume that it stays an array container
        ra_insert_new_key_value_at(&r->high_low_container, -i - 1, hb,
                                   container, typecode);
        result = true;
    }

    return result;
}

void roaring_bitmap_remove(roaring_bitmap_t *r, uint32_t val) {
    const uint16_t hb = val >> 16;
    const int i = ra_get_index(&r->high_low_container, hb);
    uint8_t typecode;
    if (i >= 0) {
        ra_unshare_container_at_index(&r->high_low_container, i);
        void *container =
            ra_get_container_at_index(&r->high_low_container, i, &typecode);
        uint8_t newtypecode = typecode;
        void *container2 =
            container_remove(container, val & 0xFFFF, typecode, &newtypecode);
        if (container2 != container) {
            container_free(container, typecode);
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
        }
        if (container_get_cardinality(container2, newtypecode) != 0) {
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
        } else {
            ra_remove_at_index_and_free(&r->high_low_container, i);
        }
    }
}

bool roaring_bitmap_remove_checked(roaring_bitmap_t *r, uint32_t val) {
    const uint16_t hb = val >> 16;
    const int i = ra_get_index(&r->high_low_container, hb);
    uint8_t typecode;
    bool result = false;
    if (i >= 0) {
        ra_unshare_container_at_index(&r->high_low_container, i);
        void *container =
            ra_get_container_at_index(&r->high_low_container, i, &typecode);

        const int oldCardinality =
            container_get_cardinality(container, typecode);

        uint8_t newtypecode = typecode;
        void *container2 =
            container_remove(container, val & 0xFFFF, typecode, &newtypecode);
        if (container2 != container) {
            container_free(container, typecode);
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
        }

        const int newCardinality =
            container_get_cardinality(container2, newtypecode);

        if (newCardinality != 0) {
            ra_set_container_at_index(&r->high_low_container, i, container2,
                                      newtypecode);
        } else {
            ra_remove_at_index_and_free(&r->high_low_container, i);
        }

        result = oldCardinality != newCardinality;
    }
    return result;
}

void roaring_bitmap_remove_many(roaring_bitmap_t *r, size_t n_args,
                                const uint32_t *vals) {
    if (n_args == 0 || r->high_low_container.size == 0) {
        return;
    }
    int32_t pos = -1; // position of the container used in the previous iteration
    for (size_t i = 0; i < n_args; i++) {
        uint16_t key = (uint16_t)(vals[i] >> 16);
        if (pos < 0 || key != r->high_low_container.keys[pos]) {
            pos = ra_get_index(&r->high_low_container, key);
        }
        if (pos >= 0) {
            uint8_t new_typecode;
            void *new_container;
            new_container = container_remove(r->high_low_container.containers[pos],
                                             vals[i] & 0xffff,
                                             r->high_low_container.typecodes[pos],
                                             &new_typecode);
            if (new_container != r->high_low_container.containers[pos]) {
                container_free(r->high_low_container.containers[pos],
                               r->high_low_container.typecodes[pos]);
                ra_replace_key_and_container_at_index(&r->high_low_container,
                                                      pos, key, new_container,
                                                      new_typecode);
            }
            if (!container_nonzero_cardinality(new_container, new_typecode)) {
                container_free(new_container, new_typecode);
                ra_remove_at_index(&r->high_low_container, pos);
                pos = -1;
            }
        }
    }
}

// there should be some SIMD optimizations possible here
roaring_bitmap_t *roaring_bitmap_and(const roaring_bitmap_t *x1,
                                     const roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    uint32_t neededcap = length1 > length2 ? length2 : length1;
    roaring_bitmap_t *answer = roaring_bitmap_create_with_capacity(neededcap);
    answer->copy_on_write = x1->copy_on_write && x2->copy_on_write;

    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
        const uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
        const uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        if (s1 == s2) {
            uint8_t container_type_1, container_type_2;
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c = container_and(c1, container_type_1, c2, container_type_2,
                                    &container_result_type);
            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_append(&answer->high_low_container, s1, c,
                          container_result_type);
            } else {
                container_free(
                    c, container_result_type);  // otherwise:memory leak!
            }
            ++pos1;
            ++pos2;
        } else if (s1 < s2) {  // s1 < s2
            pos1 = ra_advance_until(&x1->high_low_container, s2, pos1);
        } else {  // s1 > s2
            pos2 = ra_advance_until(&x2->high_low_container, s1, pos2);
        }
    }
    return answer;
}

/**
 * Compute the union of 'number' bitmaps.
 */
roaring_bitmap_t *roaring_bitmap_or_many(size_t number,
                                         const roaring_bitmap_t **x) {
    if (number == 0) {
        return roaring_bitmap_create();
    }
    if (number == 1) {
        return roaring_bitmap_copy(x[0]);
    }
    roaring_bitmap_t *answer =
        roaring_bitmap_lazy_or(x[0], x[1], LAZY_OR_BITSET_CONVERSION);
    for (size_t i = 2; i < number; i++) {
        roaring_bitmap_lazy_or_inplace(answer, x[i], LAZY_OR_BITSET_CONVERSION);
    }
    roaring_bitmap_repair_after_lazy(answer);
    return answer;
}

/**
 * Compute the xor of 'number' bitmaps.
 */
roaring_bitmap_t *roaring_bitmap_xor_many(size_t number,
                                          const roaring_bitmap_t **x) {
    if (number == 0) {
        return roaring_bitmap_create();
    }
    if (number == 1) {
        return roaring_bitmap_copy(x[0]);
    }
    roaring_bitmap_t *answer = roaring_bitmap_lazy_xor(x[0], x[1]);
    for (size_t i = 2; i < number; i++) {
        roaring_bitmap_lazy_xor_inplace(answer, x[i]);
    }
    roaring_bitmap_repair_after_lazy(answer);
    return answer;
}

// inplace and (modifies its first argument).
void roaring_bitmap_and_inplace(roaring_bitmap_t *x1,
                                const roaring_bitmap_t *x2) {
    if (x1 == x2) return;
    int pos1 = 0, pos2 = 0, intersection_size = 0;
    const int length1 = ra_get_size(&x1->high_low_container);
    const int length2 = ra_get_size(&x2->high_low_container);

    // any skipped-over or newly emptied containers in x1
    // have to be freed.
    while (pos1 < length1 && pos2 < length2) {
        const uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
        const uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        if (s1 == s2) {
            uint8_t typecode1, typecode2, typecode_result;
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &typecode1);
            c1 = get_writable_copy_if_shared(c1, &typecode1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &typecode2);
            void *c =
                container_iand(c1, typecode1, c2, typecode2, &typecode_result);
            if (c != c1) {  // in this instance a new container was created, and
                            // we need to free the old one
                container_free(c1, typecode1);
            }
            if (container_nonzero_cardinality(c, typecode_result)) {
                ra_replace_key_and_container_at_index(&x1->high_low_container,
                                                      intersection_size, s1, c,
                                                      typecode_result);
                intersection_size++;
            } else {
                container_free(c, typecode_result);
            }
            ++pos1;
            ++pos2;
        } else if (s1 < s2) {
            pos1 = ra_advance_until_freeing(&x1->high_low_container, s2, pos1);
        } else {  // s1 > s2
            pos2 = ra_advance_until(&x2->high_low_container, s1, pos2);
        }
    }

    // if we ended early because x2 ran out, then all remaining in x1 should be
    // freed
    while (pos1 < length1) {
        container_free(x1->high_low_container.containers[pos1],
                       x1->high_low_container.typecodes[pos1]);
        ++pos1;
    }

    // all containers after this have either been copied or freed
    ra_downsize(&x1->high_low_container, intersection_size);
}

roaring_bitmap_t *roaring_bitmap_or(const roaring_bitmap_t *x1,
                                    const roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    if (0 == length1) {
        return roaring_bitmap_copy(x2);
    }
    if (0 == length2) {
        return roaring_bitmap_copy(x1);
    }
    roaring_bitmap_t *answer =
        roaring_bitmap_create_with_capacity(length1 + length2);
    answer->copy_on_write = x1->copy_on_write && x2->copy_on_write;
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c = container_or(c1, container_type_1, c2, container_type_2,
                                   &container_result_type);
            // since we assume that the initial containers are non-empty, the
            // result here
            // can only be non-empty
            ra_append(&answer->high_low_container, s1, c,
                      container_result_type);
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            // c1 = container_clone(c1, container_type_1);
            c1 =
                get_copy_of_container(c1, &container_type_1, x1->copy_on_write);
            if (x1->copy_on_write) {
                ra_set_container_at_index(&x1->high_low_container, pos1, c1,
                                          container_type_1);
            }
            ra_append(&answer->high_low_container, s1, c1, container_type_1);
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            // c2 = container_clone(c2, container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }
            ra_append(&answer->high_low_container, s2, c2, container_type_2);
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&answer->high_low_container,
                             &x2->high_low_container, pos2, length2,
                             x2->copy_on_write);
    } else if (pos2 == length2) {
        ra_append_copy_range(&answer->high_low_container,
                             &x1->high_low_container, pos1, length1,
                             x1->copy_on_write);
    }
    return answer;
}

// inplace or (modifies its first argument).
void roaring_bitmap_or_inplace(roaring_bitmap_t *x1,
                               const roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    int length1 = x1->high_low_container.size;
    const int length2 = x2->high_low_container.size;

    if (0 == length2) return;

    if (0 == length1) {
        roaring_bitmap_overwrite(x1, x2);
        return;
    }
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            if (!container_is_full(c1, container_type_1)) {
                c1 = get_writable_copy_if_shared(c1, &container_type_1);

                void *c2 = ra_get_container_at_index(&x2->high_low_container,
                                                     pos2, &container_type_2);
                void *c =
                    container_ior(c1, container_type_1, c2, container_type_2,
                                  &container_result_type);
                if (c !=
                    c1) {  // in this instance a new container was created, and
                           // we need to free the old one
                    container_free(c1, container_type_1);
                }

                ra_set_container_at_index(&x1->high_low_container, pos1, c,
                                          container_result_type);
            }
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }

            // void *c2_clone = container_clone(c2, container_type_2);
            ra_insert_new_key_value_at(&x1->high_low_container, pos1, s2, c2,
                                       container_type_2);
            pos1++;
            length1++;
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&x1->high_low_container, &x2->high_low_container,
                             pos2, length2, x2->copy_on_write);
    }
}

roaring_bitmap_t *roaring_bitmap_xor(const roaring_bitmap_t *x1,
                                     const roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    if (0 == length1) {
        return roaring_bitmap_copy(x2);
    }
    if (0 == length2) {
        return roaring_bitmap_copy(x1);
    }
    roaring_bitmap_t *answer =
        roaring_bitmap_create_with_capacity(length1 + length2);
    answer->copy_on_write = x1->copy_on_write && x2->copy_on_write;
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c = container_xor(c1, container_type_1, c2, container_type_2,
                                    &container_result_type);

            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_append(&answer->high_low_container, s1, c,
                          container_result_type);
            } else {
                container_free(c, container_result_type);
            }
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            c1 =
                get_copy_of_container(c1, &container_type_1, x1->copy_on_write);
            if (x1->copy_on_write) {
                ra_set_container_at_index(&x1->high_low_container, pos1, c1,
                                          container_type_1);
            }
            ra_append(&answer->high_low_container, s1, c1, container_type_1);
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }
            ra_append(&answer->high_low_container, s2, c2, container_type_2);
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&answer->high_low_container,
                             &x2->high_low_container, pos2, length2,
                             x2->copy_on_write);
    } else if (pos2 == length2) {
        ra_append_copy_range(&answer->high_low_container,
                             &x1->high_low_container, pos1, length1,
                             x1->copy_on_write);
    }
    return answer;
}

// inplace xor (modifies its first argument).

void roaring_bitmap_xor_inplace(roaring_bitmap_t *x1,
                                const roaring_bitmap_t *x2) {
    assert(x1 != x2);
    uint8_t container_result_type = 0;
    int length1 = x1->high_low_container.size;
    const int length2 = x2->high_low_container.size;

    if (0 == length2) return;

    if (0 == length1) {
        roaring_bitmap_overwrite(x1, x2);
        return;
    }

    // XOR can have new containers inserted from x2, but can also
    // lose containers when x1 and x2 are nonempty and identical.

    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            c1 = get_writable_copy_if_shared(c1, &container_type_1);

            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c = container_ixor(c1, container_type_1, c2, container_type_2,
                                     &container_result_type);

            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_set_container_at_index(&x1->high_low_container, pos1, c,
                                          container_result_type);
                ++pos1;
            } else {
                container_free(c, container_result_type);
                ra_remove_at_index(&x1->high_low_container, pos1);
                --length1;
            }

            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }

            ra_insert_new_key_value_at(&x1->high_low_container, pos1, s2, c2,
                                       container_type_2);
            pos1++;
            length1++;
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&x1->high_low_container, &x2->high_low_container,
                             pos2, length2, x2->copy_on_write);
    }
}

roaring_bitmap_t *roaring_bitmap_andnot(const roaring_bitmap_t *x1,
                                        const roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    if (0 == length1) {
        roaring_bitmap_t *empty_bitmap = roaring_bitmap_create();
        empty_bitmap->copy_on_write = x1->copy_on_write && x2->copy_on_write;
        return empty_bitmap;
    }
    if (0 == length2) {
        return roaring_bitmap_copy(x1);
    }
    roaring_bitmap_t *answer = roaring_bitmap_create_with_capacity(length1);
    answer->copy_on_write = x1->copy_on_write && x2->copy_on_write;

    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = 0;
    uint16_t s2 = 0;
    while (true) {
        s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
        s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c =
                container_andnot(c1, container_type_1, c2, container_type_2,
                                 &container_result_type);

            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_append(&answer->high_low_container, s1, c,
                          container_result_type);
            } else {
                container_free(c, container_result_type);
            }
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
        } else if (s1 < s2) {  // s1 < s2
            const int next_pos1 =
                ra_advance_until(&x1->high_low_container, s2, pos1);
            ra_append_copy_range(&answer->high_low_container,
                                 &x1->high_low_container, pos1, next_pos1,
                                 x1->copy_on_write);
            // TODO : perhaps some of the copy_on_write should be based on
            // answer rather than x1 (more stringent?).  Many similar cases
            pos1 = next_pos1;
            if (pos1 == length1) break;
        } else {  // s1 > s2
            pos2 = ra_advance_until(&x2->high_low_container, s1, pos2);
            if (pos2 == length2) break;
        }
    }
    if (pos2 == length2) {
        ra_append_copy_range(&answer->high_low_container,
                             &x1->high_low_container, pos1, length1,
                             x1->copy_on_write);
    }
    return answer;
}

// inplace andnot (modifies its first argument).

void roaring_bitmap_andnot_inplace(roaring_bitmap_t *x1,
                                   const roaring_bitmap_t *x2) {
    assert(x1 != x2);

    uint8_t container_result_type = 0;
    int length1 = x1->high_low_container.size;
    const int length2 = x2->high_low_container.size;
    int intersection_size = 0;

    if (0 == length2) return;

    if (0 == length1) {
        roaring_bitmap_clear(x1);
        return;
    }

    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            c1 = get_writable_copy_if_shared(c1, &container_type_1);

            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c =
                container_iandnot(c1, container_type_1, c2, container_type_2,
                                  &container_result_type);

            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_replace_key_and_container_at_index(&x1->high_low_container,
                                                      intersection_size++, s1,
                                                      c, container_result_type);
            } else {
                container_free(c, container_result_type);
            }

            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            if (pos1 != intersection_size) {
                void *c1 = ra_get_container_at_index(&x1->high_low_container,
                                                     pos1, &container_type_1);

                ra_replace_key_and_container_at_index(&x1->high_low_container,
                                                      intersection_size, s1, c1,
                                                      container_type_1);
            }
            intersection_size++;
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            pos2 = ra_advance_until(&x2->high_low_container, s1, pos2);
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }

    if (pos1 < length1) {
        // all containers between intersection_size and
        // pos1 are junk.  However, they have either been moved
        // (thus still referenced) or involved in an iandnot
        // that will clean up all containers that could not be reused.
        // Thus we should not free the junk containers between
        // intersection_size and pos1.
        if (pos1 > intersection_size) {
            // left slide of remaining items
            ra_copy_range(&x1->high_low_container, pos1, length1,
                          intersection_size);
        }
        // else current placement is fine
        intersection_size += (length1 - pos1);
    }
    ra_downsize(&x1->high_low_container, intersection_size);
}

uint64_t roaring_bitmap_get_cardinality(const roaring_bitmap_t *ra) {
    uint64_t card = 0;
    for (int i = 0; i < ra->high_low_container.size; ++i)
        card += container_get_cardinality(ra->high_low_container.containers[i],
                                          ra->high_low_container.typecodes[i]);
    return card;
}

uint64_t roaring_bitmap_range_cardinality(const roaring_bitmap_t *ra,
                                          uint64_t range_start,
                                          uint64_t range_end) {
    if (range_end > UINT32_MAX) {
        range_end = UINT32_MAX + UINT64_C(1);
    }
    if (range_start >= range_end) {
        return 0;
    }
    range_end--; // make range_end inclusive
    // now we have: 0 <= range_start <= range_end <= UINT32_MAX

    int minhb = range_start >> 16;
    int maxhb = range_end >> 16;

    uint64_t card = 0;

    int i = ra_get_index(&ra->high_low_container, minhb);
    if (i >= 0) {
        if (minhb == maxhb) {
            card += container_rank(ra->high_low_container.containers[i],
                                   ra->high_low_container.typecodes[i],
                                   range_end & 0xffff);
        } else {
            card += container_get_cardinality(ra->high_low_container.containers[i],
                                              ra->high_low_container.typecodes[i]);
        }
        if ((range_start & 0xffff) != 0) {
            card -= container_rank(ra->high_low_container.containers[i],
                                   ra->high_low_container.typecodes[i],
                                   (range_start & 0xffff) - 1);
        }
        i++;
    } else {
        i = -i - 1;
    }

    for (; i < ra->high_low_container.size; i++) {
        uint16_t key = ra->high_low_container.keys[i];
        if (key < maxhb) {
            card += container_get_cardinality(ra->high_low_container.containers[i],
                                              ra->high_low_container.typecodes[i]);
        } else if (key == maxhb) {
            card += container_rank(ra->high_low_container.containers[i],
                                   ra->high_low_container.typecodes[i],
                                   range_end & 0xffff);
            break;
        } else {
            break;
        }
    }

    return card;
}


bool roaring_bitmap_is_empty(const roaring_bitmap_t *ra) {
    return ra->high_low_container.size == 0;
}

void roaring_bitmap_to_uint32_array(const roaring_bitmap_t *ra, uint32_t *ans) {
    ra_to_uint32_array(&ra->high_low_container, ans);
}

bool roaring_bitmap_range_uint32_array(const roaring_bitmap_t *ra, size_t offset, size_t limit,  uint32_t *ans) {
    return ra_range_uint32_array(&ra->high_low_container, offset, limit, ans);
}

/** convert array and bitmap containers to run containers when it is more
 * efficient;
 * also convert from run containers when more space efficient.  Returns
 * true if the result has at least one run container.
*/
bool roaring_bitmap_run_optimize(roaring_bitmap_t *r) {
    bool answer = false;
    for (int i = 0; i < r->high_low_container.size; i++) {
        uint8_t typecode_original, typecode_after;
        ra_unshare_container_at_index(
            &r->high_low_container, i);  // TODO: this introduces extra cloning!
        void *c = ra_get_container_at_index(&r->high_low_container, i,
                                            &typecode_original);
        void *c1 = convert_run_optimize(c, typecode_original, &typecode_after);
        if (typecode_after == RUN_CONTAINER_TYPE_CODE) answer = true;
        ra_set_container_at_index(&r->high_low_container, i, c1,
                                  typecode_after);
    }
    return answer;
}

size_t roaring_bitmap_shrink_to_fit(roaring_bitmap_t *r) {
    size_t answer = 0;
    for (int i = 0; i < r->high_low_container.size; i++) {
        uint8_t typecode_original;
        void *c = ra_get_container_at_index(&r->high_low_container, i,
                                            &typecode_original);
        answer += container_shrink_to_fit(c, typecode_original);
    }
    answer += ra_shrink_to_fit(&r->high_low_container);
    return answer;
}

/**
 *  Remove run-length encoding even when it is more space efficient
 *  return whether a change was applied
 */
bool roaring_bitmap_remove_run_compression(roaring_bitmap_t *r) {
    bool answer = false;
    for (int i = 0; i < r->high_low_container.size; i++) {
        uint8_t typecode_original, typecode_after;
        void *c = ra_get_container_at_index(&r->high_low_container, i,
                                            &typecode_original);
        if (get_container_type(c, typecode_original) ==
            RUN_CONTAINER_TYPE_CODE) {
            answer = true;
            if (typecode_original == SHARED_CONTAINER_TYPE_CODE) {
                run_container_t *truec =
                    (run_container_t *)((shared_container_t *)c)->container;
                int32_t card = run_container_cardinality(truec);
                void *c1 = convert_to_bitset_or_array_container(
                    truec, card, &typecode_after);
                shared_container_free((shared_container_t *)c);
                ra_set_container_at_index(&r->high_low_container, i, c1,
                                          typecode_after);

            } else {
                int32_t card = run_container_cardinality((run_container_t *)c);
                void *c1 = convert_to_bitset_or_array_container(
                    (run_container_t *)c, card, &typecode_after);
                ra_set_container_at_index(&r->high_low_container, i, c1,
                                          typecode_after);
            }
        }
    }
    return answer;
}

size_t roaring_bitmap_serialize(const roaring_bitmap_t *ra, char *buf) {
    size_t portablesize = roaring_bitmap_portable_size_in_bytes(ra);
    uint64_t cardinality = roaring_bitmap_get_cardinality(ra);
    uint64_t sizeasarray = cardinality * sizeof(uint32_t) + sizeof(uint32_t);
    if (portablesize < sizeasarray) {
        buf[0] = SERIALIZATION_CONTAINER;
        return roaring_bitmap_portable_serialize(ra, buf + 1) + 1;
    } else {
        buf[0] = SERIALIZATION_ARRAY_UINT32;
        memcpy(buf + 1, &cardinality, sizeof(uint32_t));
        roaring_bitmap_to_uint32_array(
            ra, (uint32_t *)(buf + 1 + sizeof(uint32_t)));
        return 1 + (size_t)sizeasarray;
    }
}

size_t roaring_bitmap_size_in_bytes(const roaring_bitmap_t *ra) {
    size_t portablesize = roaring_bitmap_portable_size_in_bytes(ra);
    uint64_t sizeasarray = roaring_bitmap_get_cardinality(ra) * sizeof(uint32_t) +
                         sizeof(uint32_t);
    return portablesize < sizeasarray ? portablesize + 1 : (size_t)sizeasarray + 1;
}

size_t roaring_bitmap_portable_size_in_bytes(const roaring_bitmap_t *ra) {
    return ra_portable_size_in_bytes(&ra->high_low_container);
}


roaring_bitmap_t *roaring_bitmap_portable_deserialize_safe(const char *buf, size_t maxbytes) {
    roaring_bitmap_t *ans =
        (roaring_bitmap_t *)malloc(sizeof(roaring_bitmap_t));
    if (ans == NULL) {
        return NULL;
    }
    size_t bytesread;
    bool is_ok = ra_portable_deserialize(&ans->high_low_container, buf, maxbytes, &bytesread);
    if(is_ok) assert(bytesread <= maxbytes);
    ans->copy_on_write = false;
    if (!is_ok) {
        free(ans);
        return NULL;
    }
    return ans;
}

roaring_bitmap_t *roaring_bitmap_portable_deserialize(const char *buf) {
    return roaring_bitmap_portable_deserialize_safe(buf, SIZE_MAX);
}


size_t roaring_bitmap_portable_deserialize_size(const char *buf, size_t maxbytes) {
  return ra_portable_deserialize_size(buf, maxbytes);
}


size_t roaring_bitmap_portable_serialize(const roaring_bitmap_t *ra,
                                         char *buf) {
    return ra_portable_serialize(&ra->high_low_container, buf);
}

roaring_bitmap_t *roaring_bitmap_deserialize(const void *buf) {
    const char *bufaschar = (const char *)buf;
    if (*(const unsigned char *)buf == SERIALIZATION_ARRAY_UINT32) {
        /* This looks like a compressed set of uint32_t elements */
        uint32_t card;
        memcpy(&card, bufaschar + 1, sizeof(uint32_t));
        const uint32_t *elems =
            (const uint32_t *)(bufaschar + 1 + sizeof(uint32_t));

        return roaring_bitmap_of_ptr(card, elems);
    } else if (bufaschar[0] == SERIALIZATION_CONTAINER) {
        return roaring_bitmap_portable_deserialize(bufaschar + 1);
    } else
        return (NULL);
}

bool roaring_iterate(const roaring_bitmap_t *ra, roaring_iterator iterator,
                     void *ptr) {
    for (int i = 0; i < ra->high_low_container.size; ++i)
        if (!container_iterate(ra->high_low_container.containers[i],
                               ra->high_low_container.typecodes[i],
                               ((uint32_t)ra->high_low_container.keys[i]) << 16,
                               iterator, ptr)) {
            return false;
        }
    return true;
}

bool roaring_iterate64(const roaring_bitmap_t *ra, roaring_iterator64 iterator,
                       uint64_t high_bits, void *ptr) {
    for (int i = 0; i < ra->high_low_container.size; ++i)
        if (!container_iterate64(
                ra->high_low_container.containers[i],
                ra->high_low_container.typecodes[i],
                ((uint32_t)ra->high_low_container.keys[i]) << 16, iterator,
                high_bits, ptr)) {
            return false;
        }
    return true;
}

/****
* begin roaring_uint32_iterator_t
*****/

static bool loadfirstvalue(roaring_uint32_iterator_t *newit) {
    newit->in_container_index = 0;
    newit->run_index = 0;
    newit->current_value = 0;
    if (newit->container_index >=
        newit->parent->high_low_container.size) {  // otherwise nothing
        newit->current_value = UINT32_MAX;
        return (newit->has_value = false);
    }
    // assume not empty
    newit->has_value = true;
    // we precompute container, typecode and highbits so that successive
    // iterators do not have to grab them from odd memory locations
    // and have to worry about the (easily predicted) container_unwrap_shared
    // call.
    newit->container =
        newit->parent->high_low_container.containers[newit->container_index];
    newit->typecode =
        newit->parent->high_low_container.typecodes[newit->container_index];
    newit->highbits =
        ((uint32_t)
             newit->parent->high_low_container.keys[newit->container_index])
        << 16;
    newit->container =
        container_unwrap_shared(newit->container, &(newit->typecode));
    uint32_t wordindex;
    uint64_t word;  // used for bitsets
    switch (newit->typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            wordindex = 0;
            while ((word = ((const bitset_container_t *)(newit->container))
                               ->array[wordindex]) == 0)
                wordindex++;  // advance
            // here "word" is non-zero
            newit->in_container_index = wordindex * 64 + __builtin_ctzll(word);
            newit->current_value = newit->highbits | newit->in_container_index;
            break;
        case ARRAY_CONTAINER_TYPE_CODE:
            newit->current_value =
                newit->highbits |
                ((const array_container_t *)(newit->container))->array[0];
            break;
        case RUN_CONTAINER_TYPE_CODE:
            newit->current_value =
                newit->highbits |
                (((const run_container_t *)(newit->container))->runs[0].value);
            newit->in_run_index =
                newit->current_value +
                (((const run_container_t *)(newit->container))->runs[0].length);
            break;
        default:
            // if this ever happens, bug!
            assert(false);
    }  // switch (typecode)
    return true;
}

// prerequesite: the value should be in range of the container
static bool loadfirstvalue_largeorequal(roaring_uint32_iterator_t *newit, uint32_t val) {
    uint16_t lb = val & 0xFFFF;
    newit->in_container_index = 0;
    newit->run_index = 0;
    newit->current_value = 0;
    // assume it is found
    newit->has_value = true;
    newit->container =
        newit->parent->high_low_container.containers[newit->container_index];
    newit->typecode =
        newit->parent->high_low_container.typecodes[newit->container_index];
    newit->highbits =
        ((uint32_t)
             newit->parent->high_low_container.keys[newit->container_index])
        << 16;
    newit->container =
        container_unwrap_shared(newit->container, &(newit->typecode));
    switch (newit->typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            newit->in_container_index =  bitset_container_index_equalorlarger((const bitset_container_t *)(newit->container), lb);
            newit->current_value = newit->highbits | newit->in_container_index;
            break;
        case ARRAY_CONTAINER_TYPE_CODE:
            newit->in_container_index = array_container_index_equalorlarger((const array_container_t *)(newit->container), lb);
            newit->current_value =
                newit->highbits |
                ((const array_container_t *)(newit->container))->array[newit->in_container_index];
            break;
        case RUN_CONTAINER_TYPE_CODE:
            newit->run_index = run_container_index_equalorlarger((const run_container_t *)(newit->container), lb);
            if(((const run_container_t *)(newit->container))->runs[newit->run_index].value <= lb) {
              newit->current_value = val;
            } else {
              newit->current_value =
                newit->highbits |
                (((const run_container_t *)(newit->container))->runs[newit->run_index].value);
            }
            newit->in_run_index =
                (newit->highbits | (((const run_container_t *)(newit->container))->runs[newit->run_index].value)) +
                (((const run_container_t *)(newit->container))->runs[newit->run_index].length);

            break;
        default:
            // if this ever happens, bug!
            assert(false);
    }  // switch (typecode)
    return true;
}

void roaring_init_iterator(const roaring_bitmap_t *ra,
                           roaring_uint32_iterator_t *newit) {
    newit->parent = ra;
    newit->container_index = 0;
    newit->has_value = loadfirstvalue(newit);
}

roaring_uint32_iterator_t *roaring_create_iterator(const roaring_bitmap_t *ra) {
    roaring_uint32_iterator_t *newit =
        (roaring_uint32_iterator_t *)malloc(sizeof(roaring_uint32_iterator_t));
    if (newit == NULL) return NULL;
    roaring_init_iterator(ra, newit);
    return newit;
}

roaring_uint32_iterator_t *roaring_copy_uint32_iterator(
    const roaring_uint32_iterator_t *it) {
    roaring_uint32_iterator_t *newit =
        (roaring_uint32_iterator_t *)malloc(sizeof(roaring_uint32_iterator_t));
    memcpy(newit, it, sizeof(roaring_uint32_iterator_t));
    return newit;
}

bool roaring_move_uint32_iterator_equalorlarger(roaring_uint32_iterator_t *it, uint32_t val) {
    uint16_t hb = val >> 16;
    const int i = ra_get_index(& it->parent->high_low_container, hb);
    if (i >= 0) {
      uint32_t lowvalue = container_maximum(it->parent->high_low_container.containers[i], it->parent->high_low_container.typecodes[i]);
      uint16_t lb = val & 0xFFFF;
      if(lowvalue < lb ) {
        it->container_index = i+1; // will have to load first value of next container
      } else {// the value is necessarily within the range of the container
        it->container_index = i;
        it->has_value = loadfirstvalue_largeorequal(it, val);
        return it->has_value;
      }
    } else {
      // there is no matching, so we are going for the next container
      it->container_index = -i-1;
    }
    it->has_value = loadfirstvalue(it);
    return it->has_value;
}


bool roaring_advance_uint32_iterator(roaring_uint32_iterator_t *it) {
    if (it->container_index >= it->parent->high_low_container.size) {
        return (it->has_value = false);
    }
    uint32_t wordindex;  // used for bitsets
    uint64_t word;       // used for bitsets
    switch (it->typecode) {
        case BITSET_CONTAINER_TYPE_CODE:
            it->in_container_index++;
            wordindex = it->in_container_index / 64;
            if (wordindex >= BITSET_CONTAINER_SIZE_IN_WORDS) break;
            word = ((const bitset_container_t *)(it->container))
                       ->array[wordindex] &
                   (UINT64_MAX << (it->in_container_index % 64));
            // next part could be optimized/simplified
            while ((word == 0) &&
                   (wordindex + 1 < BITSET_CONTAINER_SIZE_IN_WORDS)) {
                wordindex++;
                word = ((const bitset_container_t *)(it->container))
                           ->array[wordindex];
            }
            if (word != 0) {
                it->in_container_index = wordindex * 64 + __builtin_ctzll(word);
                it->current_value = it->highbits | it->in_container_index;
                return (it->has_value = true);
            }
            break;
        case ARRAY_CONTAINER_TYPE_CODE:
            it->in_container_index++;
            if (it->in_container_index <
                ((const array_container_t *)(it->container))->cardinality) {
                it->current_value = it->highbits |
                                    ((const array_container_t *)(it->container))
                                        ->array[it->in_container_index];
                return true;
            }
            break;
        case RUN_CONTAINER_TYPE_CODE:
            if(it->current_value == UINT32_MAX) {
              return (it->has_value = false); // without this, we risk an overflow to zero
            }
            it->current_value++;
            if (it->current_value <= it->in_run_index) {
                return (it->has_value = true);
            }
            it->run_index++;
            if (it->run_index <
                ((const run_container_t *)(it->container))->n_runs) {
                it->current_value =
                    it->highbits | (((const run_container_t *)(it->container))
                                        ->runs[it->run_index]
                                        .value);
                it->in_run_index = it->current_value +
                                   ((const run_container_t *)(it->container))
                                       ->runs[it->run_index]
                                       .length;
                return (it->has_value = true);
            }
            break;
        default:
            // if this ever happens, bug!
            assert(false);
    }  // switch (typecode)
    // moving to next container
    it->container_index++;
    return (it->has_value = loadfirstvalue(it));
}

uint32_t roaring_read_uint32_iterator(roaring_uint32_iterator_t *it, uint32_t* buf, uint32_t count) {
  uint32_t ret = 0;
  uint32_t num_values;
  uint32_t wordindex;  // used for bitsets
  uint64_t word;       // used for bitsets
  const array_container_t* acont; //TODO remove
  const run_container_t* rcont; //TODO remove
  const bitset_container_t* bcont; //TODO remove

  while (it->has_value && ret < count) {
    switch (it->typecode) {
      case BITSET_CONTAINER_TYPE_CODE:
        bcont = (const bitset_container_t*)(it->container);
        wordindex = it->in_container_index / 64;
        word = bcont->array[wordindex] & (UINT64_MAX << (it->in_container_index % 64));
        do {
          while (word != 0 && ret < count) {
            buf[0] = it->highbits | (wordindex * 64 + __builtin_ctzll(word));
            word = word & (word - 1);
            buf++;
            ret++;
          }
          while (word == 0 && wordindex+1 < BITSET_CONTAINER_SIZE_IN_WORDS) {
            wordindex++;
            word = bcont->array[wordindex];
          }
        } while (word != 0 && ret < count);
        it->has_value = (word != 0);
        if (it->has_value) {
          it->in_container_index = wordindex * 64 + __builtin_ctzll(word);
          it->current_value = it->highbits | it->in_container_index;
        }
        break;
      case ARRAY_CONTAINER_TYPE_CODE:
        acont = (const array_container_t *)(it->container);
        num_values = minimum_uint32(acont->cardinality - it->in_container_index, count - ret);
        for (uint32_t i = 0; i < num_values; i++) {
          buf[i] = it->highbits | acont->array[it->in_container_index + i];
        }
        buf += num_values;
        ret += num_values;
        it->in_container_index += num_values;
        it->has_value = (it->in_container_index < acont->cardinality);
        if (it->has_value) {
          it->current_value = it->highbits | acont->array[it->in_container_index];
        }
        break;
      case RUN_CONTAINER_TYPE_CODE:
        rcont = (const run_container_t*)(it->container);
        //"in_run_index" name is misleading, read it as "max_value_in_current_run"
        do {
          num_values = minimum_uint32(it->in_run_index - it->current_value + 1, count - ret);
          for (uint32_t i = 0; i < num_values; i++) {
            buf[i] = it->current_value + i;
          }
          it->current_value += num_values; // this can overflow to zero: UINT32_MAX+1=0
          buf += num_values;
          ret += num_values;

          if (it->current_value > it->in_run_index || it->current_value == 0) {
            it->run_index++;
            if (it->run_index < rcont->n_runs) {
              it->current_value = it->highbits | rcont->runs[it->run_index].value;
              it->in_run_index = it->current_value + rcont->runs[it->run_index].length;
            } else {
              it->has_value = false;
            }
          }
        } while ((ret < count) && it->has_value);
        break;
      default:
        assert(false);
    }
    if (it->has_value) {
      assert(ret == count);
      return ret;
    }
    it->container_index++;
    it->has_value = loadfirstvalue(it);
  }
  return ret;
}



void roaring_free_uint32_iterator(roaring_uint32_iterator_t *it) { free(it); }

/****
* end of roaring_uint32_iterator_t
*****/

bool roaring_bitmap_equals(const roaring_bitmap_t *ra1,
                           const roaring_bitmap_t *ra2) {
    if (ra1->high_low_container.size != ra2->high_low_container.size) {
        return false;
    }
    for (int i = 0; i < ra1->high_low_container.size; ++i) {
        if (ra1->high_low_container.keys[i] !=
            ra2->high_low_container.keys[i]) {
            return false;
        }
    }
    for (int i = 0; i < ra1->high_low_container.size; ++i) {
        bool areequal = container_equals(ra1->high_low_container.containers[i],
                                         ra1->high_low_container.typecodes[i],
                                         ra2->high_low_container.containers[i],
                                         ra2->high_low_container.typecodes[i]);
        if (!areequal) {
            return false;
        }
    }
    return true;
}

bool roaring_bitmap_is_subset(const roaring_bitmap_t *ra1,
                              const roaring_bitmap_t *ra2) {
    const int length1 = ra1->high_low_container.size,
              length2 = ra2->high_low_container.size;

    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
        const uint16_t s1 = ra_get_key_at_index(&ra1->high_low_container, pos1);
        const uint16_t s2 = ra_get_key_at_index(&ra2->high_low_container, pos2);

        if (s1 == s2) {
            uint8_t container_type_1, container_type_2;
            void *c1 = ra_get_container_at_index(&ra1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&ra2->high_low_container, pos2,
                                                 &container_type_2);
            bool subset =
                container_is_subset(c1, container_type_1, c2, container_type_2);
            if (!subset) return false;
            ++pos1;
            ++pos2;
        } else if (s1 < s2) {  // s1 < s2
            return false;
        } else {  // s1 > s2
            pos2 = ra_advance_until(&ra2->high_low_container, s1, pos2);
        }
    }
    if (pos1 == length1)
        return true;
    else
        return false;
}

static void insert_flipped_container(roaring_array_t *ans_arr,
                                     const roaring_array_t *x1_arr, uint16_t hb,
                                     uint16_t lb_start, uint16_t lb_end) {
    const int i = ra_get_index(x1_arr, hb);
    const int j = ra_get_index(ans_arr, hb);
    uint8_t ctype_in, ctype_out;
    void *flipped_container = NULL;
    if (i >= 0) {
        void *container_to_flip =
            ra_get_container_at_index(x1_arr, i, &ctype_in);
        flipped_container =
            container_not_range(container_to_flip, ctype_in, (uint32_t)lb_start,
                                (uint32_t)(lb_end + 1), &ctype_out);

        if (container_get_cardinality(flipped_container, ctype_out))
            ra_insert_new_key_value_at(ans_arr, -j - 1, hb, flipped_container,
                                       ctype_out);
        else {
            container_free(flipped_container, ctype_out);
        }
    } else {
        flipped_container = container_range_of_ones(
            (uint32_t)lb_start, (uint32_t)(lb_end + 1), &ctype_out);
        ra_insert_new_key_value_at(ans_arr, -j - 1, hb, flipped_container,
                                   ctype_out);
    }
}

static void inplace_flip_container(roaring_array_t *x1_arr, uint16_t hb,
                                   uint16_t lb_start, uint16_t lb_end) {
    const int i = ra_get_index(x1_arr, hb);
    uint8_t ctype_in, ctype_out;
    void *flipped_container = NULL;
    if (i >= 0) {
        void *container_to_flip =
            ra_get_container_at_index(x1_arr, i, &ctype_in);
        flipped_container = container_inot_range(
            container_to_flip, ctype_in, (uint32_t)lb_start,
            (uint32_t)(lb_end + 1), &ctype_out);
        // if a new container was created, the old one was already freed
        if (container_get_cardinality(flipped_container, ctype_out)) {
            ra_set_container_at_index(x1_arr, i, flipped_container, ctype_out);
        } else {
            container_free(flipped_container, ctype_out);
            ra_remove_at_index(x1_arr, i);
        }

    } else {
        flipped_container = container_range_of_ones(
            (uint32_t)lb_start, (uint32_t)(lb_end + 1), &ctype_out);
        ra_insert_new_key_value_at(x1_arr, -i - 1, hb, flipped_container,
                                   ctype_out);
    }
}

static void insert_fully_flipped_container(roaring_array_t *ans_arr,
                                           const roaring_array_t *x1_arr,
                                           uint16_t hb) {
    const int i = ra_get_index(x1_arr, hb);
    const int j = ra_get_index(ans_arr, hb);
    uint8_t ctype_in, ctype_out;
    void *flipped_container = NULL;
    if (i >= 0) {
        void *container_to_flip =
            ra_get_container_at_index(x1_arr, i, &ctype_in);
        flipped_container =
            container_not(container_to_flip, ctype_in, &ctype_out);
        if (container_get_cardinality(flipped_container, ctype_out))
            ra_insert_new_key_value_at(ans_arr, -j - 1, hb, flipped_container,
                                       ctype_out);
        else {
            container_free(flipped_container, ctype_out);
        }
    } else {
        flipped_container = container_range_of_ones(0U, 0x10000U, &ctype_out);
        ra_insert_new_key_value_at(ans_arr, -j - 1, hb, flipped_container,
                                   ctype_out);
    }
}

static void inplace_fully_flip_container(roaring_array_t *x1_arr, uint16_t hb) {
    const int i = ra_get_index(x1_arr, hb);
    uint8_t ctype_in, ctype_out;
    void *flipped_container = NULL;
    if (i >= 0) {
        void *container_to_flip =
            ra_get_container_at_index(x1_arr, i, &ctype_in);
        flipped_container =
            container_inot(container_to_flip, ctype_in, &ctype_out);

        if (container_get_cardinality(flipped_container, ctype_out)) {
            ra_set_container_at_index(x1_arr, i, flipped_container, ctype_out);
        } else {
            container_free(flipped_container, ctype_out);
            ra_remove_at_index(x1_arr, i);
        }

    } else {
        flipped_container = container_range_of_ones(0U, 0x10000U, &ctype_out);
        ra_insert_new_key_value_at(x1_arr, -i - 1, hb, flipped_container,
                                   ctype_out);
    }
}

roaring_bitmap_t *roaring_bitmap_flip(const roaring_bitmap_t *x1,
                                      uint64_t range_start,
                                      uint64_t range_end) {
    if (range_start >= range_end) {
        return roaring_bitmap_copy(x1);
    }
    if(range_end >= UINT64_C(0x100000000)) {
        range_end = UINT64_C(0x100000000);
    }

    roaring_bitmap_t *ans = roaring_bitmap_create();
    ans->copy_on_write = x1->copy_on_write;

    uint16_t hb_start = (uint16_t)(range_start >> 16);
    const uint16_t lb_start = (uint16_t)range_start;  // & 0xFFFF;
    uint16_t hb_end = (uint16_t)((range_end - 1) >> 16);
    const uint16_t lb_end = (uint16_t)(range_end - 1);  // & 0xFFFF;

    ra_append_copies_until(&ans->high_low_container, &x1->high_low_container,
                           hb_start, x1->copy_on_write);
    if (hb_start == hb_end) {
        insert_flipped_container(&ans->high_low_container,
                                 &x1->high_low_container, hb_start, lb_start,
                                 lb_end);
    } else {
        // start and end containers are distinct
        if (lb_start > 0) {
            // handle first (partial) container
            insert_flipped_container(&ans->high_low_container,
                                     &x1->high_low_container, hb_start,
                                     lb_start, 0xFFFF);
            ++hb_start;  // for the full containers.  Can't wrap.
        }

        if (lb_end != 0xFFFF) --hb_end;  // later we'll handle the partial block

        for (uint32_t hb = hb_start; hb <= hb_end; ++hb) {
            insert_fully_flipped_container(&ans->high_low_container,
                                           &x1->high_low_container, hb);
        }

        // handle a partial final container
        if (lb_end != 0xFFFF) {
            insert_flipped_container(&ans->high_low_container,
                                     &x1->high_low_container, hb_end + 1, 0,
                                     lb_end);
            ++hb_end;
        }
    }
    ra_append_copies_after(&ans->high_low_container, &x1->high_low_container,
                           hb_end, x1->copy_on_write);
    return ans;
}

void roaring_bitmap_flip_inplace(roaring_bitmap_t *x1, uint64_t range_start,
                                 uint64_t range_end) {
    if (range_start >= range_end) {
        return;  // empty range
    }
    if(range_end >= UINT64_C(0x100000000)) {
        range_end = UINT64_C(0x100000000);
    }

    uint16_t hb_start = (uint16_t)(range_start >> 16);
    const uint16_t lb_start = (uint16_t)range_start;
    uint16_t hb_end = (uint16_t)((range_end - 1) >> 16);
    const uint16_t lb_end = (uint16_t)(range_end - 1);

    if (hb_start == hb_end) {
        inplace_flip_container(&x1->high_low_container, hb_start, lb_start,
                               lb_end);
    } else {
        // start and end containers are distinct
        if (lb_start > 0) {
            // handle first (partial) container
            inplace_flip_container(&x1->high_low_container, hb_start, lb_start,
                                   0xFFFF);
            ++hb_start;  // for the full containers.  Can't wrap.
        }

        if (lb_end != 0xFFFF) --hb_end;

        for (uint32_t hb = hb_start; hb <= hb_end; ++hb) {
            inplace_fully_flip_container(&x1->high_low_container, hb);
        }
        // handle a partial final container
        if (lb_end != 0xFFFF) {
            inplace_flip_container(&x1->high_low_container, hb_end + 1, 0,
                                   lb_end);
            ++hb_end;
        }
    }
}

roaring_bitmap_t *roaring_bitmap_lazy_or(const roaring_bitmap_t *x1,
                                         const roaring_bitmap_t *x2,
                                         const bool bitsetconversion) {
    uint8_t container_result_type = 0;
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    if (0 == length1) {
        return roaring_bitmap_copy(x2);
    }
    if (0 == length2) {
        return roaring_bitmap_copy(x1);
    }
    roaring_bitmap_t *answer =
        roaring_bitmap_create_with_capacity(length1 + length2);
    answer->copy_on_write = x1->copy_on_write && x2->copy_on_write;
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c;
            if (bitsetconversion && (get_container_type(c1, container_type_1) !=
                                     BITSET_CONTAINER_TYPE_CODE) &&
                (get_container_type(c2, container_type_2) !=
                 BITSET_CONTAINER_TYPE_CODE)) {
                void *newc1 =
                    container_mutable_unwrap_shared(c1, &container_type_1);
                newc1 = container_to_bitset(newc1, container_type_1);
                container_type_1 = BITSET_CONTAINER_TYPE_CODE;
                c = container_lazy_ior(newc1, container_type_1, c2,
                                       container_type_2,
                                       &container_result_type);
                if (c != newc1) {  // should not happen
                    container_free(newc1, container_type_1);
                }
            } else {
                c = container_lazy_or(c1, container_type_1, c2,
                                      container_type_2, &container_result_type);
            }
            // since we assume that the initial containers are non-empty,
            // the
            // result here
            // can only be non-empty
            ra_append(&answer->high_low_container, s1, c,
                      container_result_type);
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            c1 =
                get_copy_of_container(c1, &container_type_1, x1->copy_on_write);
            if (x1->copy_on_write) {
                ra_set_container_at_index(&x1->high_low_container, pos1, c1,
                                          container_type_1);
            }
            ra_append(&answer->high_low_container, s1, c1, container_type_1);
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }
            ra_append(&answer->high_low_container, s2, c2, container_type_2);
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&answer->high_low_container,
                             &x2->high_low_container, pos2, length2,
                             x2->copy_on_write);
    } else if (pos2 == length2) {
        ra_append_copy_range(&answer->high_low_container,
                             &x1->high_low_container, pos1, length1,
                             x1->copy_on_write);
    }
    return answer;
}

void roaring_bitmap_lazy_or_inplace(roaring_bitmap_t *x1,
                                    const roaring_bitmap_t *x2,
                                    const bool bitsetconversion) {
    uint8_t container_result_type = 0;
    int length1 = x1->high_low_container.size;
    const int length2 = x2->high_low_container.size;

    if (0 == length2) return;

    if (0 == length1) {
        roaring_bitmap_overwrite(x1, x2);
        return;
    }
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            if (!container_is_full(c1, container_type_1)) {
                if ((bitsetconversion == false) ||
                    (get_container_type(c1, container_type_1) ==
                     BITSET_CONTAINER_TYPE_CODE)) {
                    c1 = get_writable_copy_if_shared(c1, &container_type_1);
                } else {
                    // convert to bitset
                    void *oldc1 = c1;
                    uint8_t oldt1 = container_type_1;
                    c1 = container_mutable_unwrap_shared(c1, &container_type_1);
                    c1 = container_to_bitset(c1, container_type_1);
                    container_free(oldc1, oldt1);
                    container_type_1 = BITSET_CONTAINER_TYPE_CODE;
                }

                void *c2 = ra_get_container_at_index(&x2->high_low_container,
                                                     pos2, &container_type_2);
                void *c = container_lazy_ior(c1, container_type_1, c2,
                                             container_type_2,
                                             &container_result_type);
                if (c !=
                    c1) {  // in this instance a new container was created, and
                           // we need to free the old one
                    container_free(c1, container_type_1);
                }

                ra_set_container_at_index(&x1->high_low_container, pos1, c,
                                          container_result_type);
            }
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            // void *c2_clone = container_clone(c2, container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }
            ra_insert_new_key_value_at(&x1->high_low_container, pos1, s2, c2,
                                       container_type_2);
            pos1++;
            length1++;
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&x1->high_low_container, &x2->high_low_container,
                             pos2, length2, x2->copy_on_write);
    }
}

roaring_bitmap_t *roaring_bitmap_lazy_xor(const roaring_bitmap_t *x1,
                                          const roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    if (0 == length1) {
        return roaring_bitmap_copy(x2);
    }
    if (0 == length2) {
        return roaring_bitmap_copy(x1);
    }
    roaring_bitmap_t *answer =
        roaring_bitmap_create_with_capacity(length1 + length2);
    answer->copy_on_write = x1->copy_on_write && x2->copy_on_write;
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c =
                container_lazy_xor(c1, container_type_1, c2, container_type_2,
                                   &container_result_type);

            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_append(&answer->high_low_container, s1, c,
                          container_result_type);
            } else {
                container_free(c, container_result_type);
            }

            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            c1 =
                get_copy_of_container(c1, &container_type_1, x1->copy_on_write);
            if (x1->copy_on_write) {
                ra_set_container_at_index(&x1->high_low_container, pos1, c1,
                                          container_type_1);
            }
            ra_append(&answer->high_low_container, s1, c1, container_type_1);
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }
            ra_append(&answer->high_low_container, s2, c2, container_type_2);
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&answer->high_low_container,
                             &x2->high_low_container, pos2, length2,
                             x2->copy_on_write);
    } else if (pos2 == length2) {
        ra_append_copy_range(&answer->high_low_container,
                             &x1->high_low_container, pos1, length1,
                             x1->copy_on_write);
    }
    return answer;
}

void roaring_bitmap_lazy_xor_inplace(roaring_bitmap_t *x1,
                                     const roaring_bitmap_t *x2) {
    assert(x1 != x2);
    uint8_t container_result_type = 0;
    int length1 = x1->high_low_container.size;
    const int length2 = x2->high_low_container.size;

    if (0 == length2) return;

    if (0 == length1) {
        roaring_bitmap_overwrite(x1, x2);
        return;
    }
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            c1 = get_writable_copy_if_shared(c1, &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            void *c =
                container_lazy_ixor(c1, container_type_1, c2, container_type_2,
                                    &container_result_type);
            if (container_nonzero_cardinality(c, container_result_type)) {
                ra_set_container_at_index(&x1->high_low_container, pos1, c,
                                          container_result_type);
                ++pos1;
            } else {
                container_free(c, container_result_type);
                ra_remove_at_index(&x1->high_low_container, pos1);
                --length1;
            }
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            // void *c2_clone = container_clone(c2, container_type_2);
            c2 =
                get_copy_of_container(c2, &container_type_2, x2->copy_on_write);
            if (x2->copy_on_write) {
                ra_set_container_at_index(&x2->high_low_container, pos2, c2,
                                          container_type_2);
            }
            ra_insert_new_key_value_at(&x1->high_low_container, pos1, s2, c2,
                                       container_type_2);
            pos1++;
            length1++;
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_copy_range(&x1->high_low_container, &x2->high_low_container,
                             pos2, length2, x2->copy_on_write);
    }
}

void roaring_bitmap_repair_after_lazy(roaring_bitmap_t *ra) {
    for (int i = 0; i < ra->high_low_container.size; ++i) {
        const uint8_t original_typecode = ra->high_low_container.typecodes[i];
        void *container = ra->high_low_container.containers[i];
        uint8_t new_typecode = original_typecode;
        void *newcontainer =
            container_repair_after_lazy(container, &new_typecode);
        ra->high_low_container.containers[i] = newcontainer;
        ra->high_low_container.typecodes[i] = new_typecode;
    }
}



/**
* roaring_bitmap_rank returns the number of integers that are smaller or equal
* to x.
*/
uint64_t roaring_bitmap_rank(const roaring_bitmap_t *bm, uint32_t x) {
    uint64_t size = 0;
    uint32_t xhigh = x >> 16;
    for (int i = 0; i < bm->high_low_container.size; i++) {
        uint32_t key = bm->high_low_container.keys[i];
        if (xhigh > key) {
            size +=
                container_get_cardinality(bm->high_low_container.containers[i],
                                          bm->high_low_container.typecodes[i]);
        } else if (xhigh == key) {
            return size + container_rank(bm->high_low_container.containers[i],
                                         bm->high_low_container.typecodes[i],
                                         x & 0xFFFF);
        } else {
            return size;
        }
    }
    return size;
}

/**
* roaring_bitmap_smallest returns the smallest value in the set.
* Returns UINT32_MAX if the set is empty.
*/
uint32_t roaring_bitmap_minimum(const roaring_bitmap_t *bm) {
    if (bm->high_low_container.size > 0) {
        void *container = bm->high_low_container.containers[0];
        uint8_t typecode = bm->high_low_container.typecodes[0];
        uint32_t key = bm->high_low_container.keys[0];
        uint32_t lowvalue = container_minimum(container, typecode);
        return lowvalue | (key << 16);
    }
    return UINT32_MAX;
}

/**
* roaring_bitmap_smallest returns the greatest value in the set.
* Returns 0 if the set is empty.
*/
uint32_t roaring_bitmap_maximum(const roaring_bitmap_t *bm) {
    if (bm->high_low_container.size > 0) {
        void *container =
            bm->high_low_container.containers[bm->high_low_container.size - 1];
        uint8_t typecode =
            bm->high_low_container.typecodes[bm->high_low_container.size - 1];
        uint32_t key =
            bm->high_low_container.keys[bm->high_low_container.size - 1];
        uint32_t lowvalue = container_maximum(container, typecode);
        return lowvalue | (key << 16);
    }
    return 0;
}

bool roaring_bitmap_select(const roaring_bitmap_t *bm, uint32_t rank,
                           uint32_t *element) {
    void *container;
    uint8_t typecode;
    uint16_t key;
    uint32_t start_rank = 0;
    int i = 0;
    bool valid = false;
    while (!valid && i < bm->high_low_container.size) {
        container = bm->high_low_container.containers[i];
        typecode = bm->high_low_container.typecodes[i];
        valid =
            container_select(container, typecode, &start_rank, rank, element);
        i++;
    }

    if (valid) {
        key = bm->high_low_container.keys[i - 1];
        *element |= (key << 16);
        return true;
    } else
        return false;
}

bool roaring_bitmap_intersect(const roaring_bitmap_t *x1,
                                     const roaring_bitmap_t *x2) {
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    uint64_t answer = 0;
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
        const uint16_t s1 = ra_get_key_at_index(& x1->high_low_container, pos1);
        const uint16_t s2 = ra_get_key_at_index(& x2->high_low_container, pos2);

        if (s1 == s2) {
            uint8_t container_type_1, container_type_2;
            void *c1 = ra_get_container_at_index(& x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(& x2->high_low_container, pos2,
                                                 &container_type_2);
            if( container_intersect(c1, container_type_1, c2, container_type_2) ) return true;
            ++pos1;
            ++pos2;
        } else if (s1 < s2) {  // s1 < s2
            pos1 = ra_advance_until(& x1->high_low_container, s2, pos1);
        } else {  // s1 > s2
            pos2 = ra_advance_until(& x2->high_low_container, s1, pos2);
        }
    }
    return answer;
}


uint64_t roaring_bitmap_and_cardinality(const roaring_bitmap_t *x1,
                                        const roaring_bitmap_t *x2) {
    const int length1 = x1->high_low_container.size,
              length2 = x2->high_low_container.size;
    uint64_t answer = 0;
    int pos1 = 0, pos2 = 0;

    while (pos1 < length1 && pos2 < length2) {
        const uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
        const uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        if (s1 == s2) {
            uint8_t container_type_1, container_type_2;
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            answer += container_and_cardinality(c1, container_type_1, c2,
                                                container_type_2);
            ++pos1;
            ++pos2;
        } else if (s1 < s2) {  // s1 < s2
            pos1 = ra_advance_until(&x1->high_low_container, s2, pos1);
        } else {  // s1 > s2
            pos2 = ra_advance_until(&x2->high_low_container, s1, pos2);
        }
    }
    return answer;
}

double roaring_bitmap_jaccard_index(const roaring_bitmap_t *x1,
                                    const roaring_bitmap_t *x2) {
    const uint64_t c1 = roaring_bitmap_get_cardinality(x1);
    const uint64_t c2 = roaring_bitmap_get_cardinality(x2);
    const uint64_t inter = roaring_bitmap_and_cardinality(x1, x2);
    return (double)inter / (double)(c1 + c2 - inter);
}

uint64_t roaring_bitmap_or_cardinality(const roaring_bitmap_t *x1,
                                       const roaring_bitmap_t *x2) {
    const uint64_t c1 = roaring_bitmap_get_cardinality(x1);
    const uint64_t c2 = roaring_bitmap_get_cardinality(x2);
    const uint64_t inter = roaring_bitmap_and_cardinality(x1, x2);
    return c1 + c2 - inter;
}

uint64_t roaring_bitmap_andnot_cardinality(const roaring_bitmap_t *x1,
                                           const roaring_bitmap_t *x2) {
    const uint64_t c1 = roaring_bitmap_get_cardinality(x1);
    const uint64_t inter = roaring_bitmap_and_cardinality(x1, x2);
    return c1 - inter;
}

uint64_t roaring_bitmap_xor_cardinality(const roaring_bitmap_t *x1,
                                        const roaring_bitmap_t *x2) {
    const uint64_t c1 = roaring_bitmap_get_cardinality(x1);
    const uint64_t c2 = roaring_bitmap_get_cardinality(x2);
    const uint64_t inter = roaring_bitmap_and_cardinality(x1, x2);
    return c1 + c2 - 2 * inter;
}


/**
 * Check whether a range of values from range_start (included) to range_end (excluded) is present
 */
bool roaring_bitmap_contains_range(const roaring_bitmap_t *r, uint64_t range_start, uint64_t range_end) {
    if(range_end >= UINT64_C(0x100000000)) {
        range_end = UINT64_C(0x100000000);
    }
    if (range_start >= range_end) return true;  // empty range are always contained!
    if (range_end - range_start == 1) return roaring_bitmap_contains(r, (uint32_t)range_start);
    uint16_t hb_rs = (uint16_t)(range_start >> 16);
    uint16_t hb_re = (uint16_t)((range_end - 1) >> 16);
    const int32_t span = hb_re - hb_rs;
    const int32_t hlc_sz = ra_get_size(&r->high_low_container);
    if (hlc_sz < span + 1) {
      return false;
    }
    int32_t is = ra_get_index(&r->high_low_container, hb_rs);
    int32_t ie = ra_get_index(&r->high_low_container, hb_re);
    ie = (ie < 0 ? -ie - 1 : ie);
    if ((is < 0) || ((ie - is) != span)) {
       return false;
    }
    const uint32_t lb_rs = range_start & 0xFFFF;
    const uint32_t lb_re = ((range_end - 1) & 0xFFFF) + 1;
    uint8_t typecode;
    void *container = ra_get_container_at_index(&r->high_low_container, is, &typecode);
    if (hb_rs == hb_re) {
      return container_contains_range(container, lb_rs, lb_re, typecode);
    }
    if (!container_contains_range(container, lb_rs, 1 << 16, typecode)) {
      return false;
    }
    assert(ie < hlc_sz); // would indicate an algorithmic bug
    container = ra_get_container_at_index(&r->high_low_container, ie, &typecode);
    if (!container_contains_range(container, 0, lb_re, typecode)) {
        return false;
    }
    for (int32_t i = is + 1; i < ie; ++i) {
        container = ra_get_container_at_index(&r->high_low_container, i, &typecode);
        if (!container_is_full(container, typecode) ) {
          return false;
        }
    }
    return true;
}


bool roaring_bitmap_is_strict_subset(const roaring_bitmap_t *ra1,
                                            const roaring_bitmap_t *ra2) {
    return (roaring_bitmap_get_cardinality(ra2) >
                roaring_bitmap_get_cardinality(ra1) &&
            roaring_bitmap_is_subset(ra1, ra2));
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/roaring.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/roaring_array.c */
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>


// Convention: [0,ra->size) all elements are initialized
//  [ra->size, ra->allocation_size) is junk and contains nothing needing freeing

extern inline int32_t ra_get_size(const roaring_array_t *ra);
extern inline int32_t ra_get_index(const roaring_array_t *ra, uint16_t x);
extern inline void *ra_get_container_at_index(const roaring_array_t *ra,
                                              uint16_t i, uint8_t *typecode);
extern inline void ra_unshare_container_at_index(roaring_array_t *ra,
                                                 uint16_t i);
extern inline void ra_replace_key_and_container_at_index(roaring_array_t *ra,
                                                         int32_t i,
                                                         uint16_t key, void *c,
                                                         uint8_t typecode);
extern inline void ra_set_container_at_index(const roaring_array_t *ra,
                                             int32_t i, void *c,
                                             uint8_t typecode);

#define INITIAL_CAPACITY 4

static bool realloc_array(roaring_array_t *ra, int32_t new_capacity) {
    // because we combine the allocations, it is not possible to use realloc
    /*ra->keys =
    (uint16_t *)realloc(ra->keys, sizeof(uint16_t) * new_capacity);
ra->containers =
    (void **)realloc(ra->containers, sizeof(void *) * new_capacity);
ra->typecodes =
    (uint8_t *)realloc(ra->typecodes, sizeof(uint8_t) * new_capacity);
if (!ra->keys || !ra->containers || !ra->typecodes) {
    free(ra->keys);
    free(ra->containers);
    free(ra->typecodes);
    return false;
}*/

    if ( new_capacity == 0 ) {
      free(ra->containers);
      ra->containers = NULL;
      ra->keys = NULL;
      ra->typecodes = NULL;
      ra->allocation_size = 0;
      return true;
    }
    const size_t memoryneeded =
        new_capacity * (sizeof(uint16_t) + sizeof(void *) + sizeof(uint8_t));
    void *bigalloc = malloc(memoryneeded);
    if (!bigalloc) return false;
    void *oldbigalloc = ra->containers;
    void **newcontainers = (void **)bigalloc;
    uint16_t *newkeys = (uint16_t *)(newcontainers + new_capacity);
    uint8_t *newtypecodes = (uint8_t *)(newkeys + new_capacity);
    assert((char *)(newtypecodes + new_capacity) ==
           (char *)bigalloc + memoryneeded);
    if(ra->size > 0) {
      memcpy(newcontainers, ra->containers, sizeof(void *) * ra->size);
      memcpy(newkeys, ra->keys, sizeof(uint16_t) * ra->size);
      memcpy(newtypecodes, ra->typecodes, sizeof(uint8_t) * ra->size);
    }
    ra->containers = newcontainers;
    ra->keys = newkeys;
    ra->typecodes = newtypecodes;
    ra->allocation_size = new_capacity;
    free(oldbigalloc);
    return true;
}

bool ra_init_with_capacity(roaring_array_t *new_ra, uint32_t cap) {
    if (!new_ra) return false;
    new_ra->keys = NULL;
    new_ra->containers = NULL;
    new_ra->typecodes = NULL;

    new_ra->allocation_size = cap;
    new_ra->size = 0;
    if(cap > 0) {
      void *bigalloc =
        malloc(cap * (sizeof(uint16_t) + sizeof(void *) + sizeof(uint8_t)));
      if( bigalloc == NULL ) return false;
      new_ra->containers = (void **)bigalloc;
      new_ra->keys = (uint16_t *)(new_ra->containers + cap);
      new_ra->typecodes = (uint8_t *)(new_ra->keys + cap);
    }
    return true;
}

int ra_shrink_to_fit(roaring_array_t *ra) {
    int savings = (ra->allocation_size - ra->size) *
                  (sizeof(uint16_t) + sizeof(void *) + sizeof(uint8_t));
    if (!realloc_array(ra, ra->size)) {
      return 0;
    }
    ra->allocation_size = ra->size;
    return savings;
}

bool ra_init(roaring_array_t *t) {
    return ra_init_with_capacity(t, INITIAL_CAPACITY);
}

bool ra_copy(const roaring_array_t *source, roaring_array_t *dest,
             bool copy_on_write) {
    if (!ra_init_with_capacity(dest, source->size)) return false;
    dest->size = source->size;
    dest->allocation_size = source->size;
    if(dest->size > 0) {
      memcpy(dest->keys, source->keys, dest->size * sizeof(uint16_t));
    }
    // we go through the containers, turning them into shared containers...
    if (copy_on_write) {
        for (int32_t i = 0; i < dest->size; ++i) {
            source->containers[i] = get_copy_of_container(
                source->containers[i], &source->typecodes[i], copy_on_write);
        }
        // we do a shallow copy to the other bitmap
        if(dest->size > 0) {
          memcpy(dest->containers, source->containers,
               dest->size * sizeof(void *));
          memcpy(dest->typecodes, source->typecodes,
               dest->size * sizeof(uint8_t));
        }
    } else {
        if(dest->size > 0) {
          memcpy(dest->typecodes, source->typecodes,
               dest->size * sizeof(uint8_t));
        }
        for (int32_t i = 0; i < dest->size; i++) {
            dest->containers[i] =
                container_clone(source->containers[i], source->typecodes[i]);
            if (dest->containers[i] == NULL) {
                for (int32_t j = 0; j < i; j++) {
                    container_free(dest->containers[j], dest->typecodes[j]);
                }
                ra_clear_without_containers(dest);
                return false;
            }
        }
    }
    return true;
}

bool ra_overwrite(const roaring_array_t *source, roaring_array_t *dest,
                  bool copy_on_write) {
    ra_clear_containers(dest);  // we are going to overwrite them
    if (dest->allocation_size < source->size) {
        if (!realloc_array(dest, source->size)) {
            return false;
        }
    }
    dest->size = source->size;
    memcpy(dest->keys, source->keys, dest->size * sizeof(uint16_t));
    // we go through the containers, turning them into shared containers...
    if (copy_on_write) {
        for (int32_t i = 0; i < dest->size; ++i) {
            source->containers[i] = get_copy_of_container(
                source->containers[i], &source->typecodes[i], copy_on_write);
        }
        // we do a shallow copy to the other bitmap
        memcpy(dest->containers, source->containers,
               dest->size * sizeof(void *));
        memcpy(dest->typecodes, source->typecodes,
               dest->size * sizeof(uint8_t));
    } else {
        memcpy(dest->typecodes, source->typecodes,
               dest->size * sizeof(uint8_t));
        for (int32_t i = 0; i < dest->size; i++) {
            dest->containers[i] =
                container_clone(source->containers[i], source->typecodes[i]);
            if (dest->containers[i] == NULL) {
                for (int32_t j = 0; j < i; j++) {
                    container_free(dest->containers[j], dest->typecodes[j]);
                }
                ra_clear_without_containers(dest);
                return false;
            }
        }
    }
    return true;
}

void ra_clear_containers(roaring_array_t *ra) {
    for (int32_t i = 0; i < ra->size; ++i) {
        container_free(ra->containers[i], ra->typecodes[i]);
    }
}

void ra_reset(roaring_array_t *ra) {
  ra_clear_containers(ra);
  ra->size = 0;
  ra_shrink_to_fit(ra);
}

void ra_clear_without_containers(roaring_array_t *ra) {
    free(ra->containers);    // keys and typecodes are allocated with containers
    ra->size = 0;
    ra->allocation_size = 0;
    ra->containers = NULL;
    ra->keys = NULL;
    ra->typecodes = NULL;
}

void ra_clear(roaring_array_t *ra) {
    ra_clear_containers(ra);
    ra_clear_without_containers(ra);
}

bool extend_array(roaring_array_t *ra, int32_t k) {
    int32_t desired_size = ra->size + k;
    assert(desired_size <= MAX_CONTAINERS);
    if (desired_size > ra->allocation_size) {
        int32_t new_capacity =
            (ra->size < 1024) ? 2 * desired_size : 5 * desired_size / 4;
        if (new_capacity > MAX_CONTAINERS) {
            new_capacity = MAX_CONTAINERS;
        }

        return realloc_array(ra, new_capacity);
    }
    return true;
}

void ra_append(roaring_array_t *ra, uint16_t key, void *container,
               uint8_t typecode) {
    extend_array(ra, 1);
    const int32_t pos = ra->size;

    ra->keys[pos] = key;
    ra->containers[pos] = container;
    ra->typecodes[pos] = typecode;
    ra->size++;
}

void ra_append_copy(roaring_array_t *ra, const roaring_array_t *sa,
                    uint16_t index, bool copy_on_write) {
    extend_array(ra, 1);
    const int32_t pos = ra->size;

    // old contents is junk not needing freeing
    ra->keys[pos] = sa->keys[index];
    // the shared container will be in two bitmaps
    if (copy_on_write) {
        sa->containers[index] = get_copy_of_container(
            sa->containers[index], &sa->typecodes[index], copy_on_write);
        ra->containers[pos] = sa->containers[index];
        ra->typecodes[pos] = sa->typecodes[index];
    } else {
        ra->containers[pos] =
            container_clone(sa->containers[index], sa->typecodes[index]);
        ra->typecodes[pos] = sa->typecodes[index];
    }
    ra->size++;
}

void ra_append_copies_until(roaring_array_t *ra, const roaring_array_t *sa,
                            uint16_t stopping_key, bool copy_on_write) {
    for (int32_t i = 0; i < sa->size; ++i) {
        if (sa->keys[i] >= stopping_key) break;
        ra_append_copy(ra, sa, i, copy_on_write);
    }
}

void ra_append_copy_range(roaring_array_t *ra, const roaring_array_t *sa,
                          int32_t start_index, int32_t end_index,
                          bool copy_on_write) {
    extend_array(ra, end_index - start_index);
    for (int32_t i = start_index; i < end_index; ++i) {
        const int32_t pos = ra->size;
        ra->keys[pos] = sa->keys[i];
        if (copy_on_write) {
            sa->containers[i] = get_copy_of_container(
                sa->containers[i], &sa->typecodes[i], copy_on_write);
            ra->containers[pos] = sa->containers[i];
            ra->typecodes[pos] = sa->typecodes[i];
        } else {
            ra->containers[pos] =
                container_clone(sa->containers[i], sa->typecodes[i]);
            ra->typecodes[pos] = sa->typecodes[i];
        }
        ra->size++;
    }
}

void ra_append_copies_after(roaring_array_t *ra, const roaring_array_t *sa,
                            uint16_t before_start, bool copy_on_write) {
    int start_location = ra_get_index(sa, before_start);
    if (start_location >= 0)
        ++start_location;
    else
        start_location = -start_location - 1;
    ra_append_copy_range(ra, sa, start_location, sa->size, copy_on_write);
}

void ra_append_move_range(roaring_array_t *ra, roaring_array_t *sa,
                          int32_t start_index, int32_t end_index) {
    extend_array(ra, end_index - start_index);

    for (int32_t i = start_index; i < end_index; ++i) {
        const int32_t pos = ra->size;

        ra->keys[pos] = sa->keys[i];
        ra->containers[pos] = sa->containers[i];
        ra->typecodes[pos] = sa->typecodes[i];
        ra->size++;
    }
}

void ra_append_range(roaring_array_t *ra, roaring_array_t *sa,
                     int32_t start_index, int32_t end_index,
                     bool copy_on_write) {
    extend_array(ra, end_index - start_index);

    for (int32_t i = start_index; i < end_index; ++i) {
        const int32_t pos = ra->size;
        ra->keys[pos] = sa->keys[i];
        if (copy_on_write) {
            sa->containers[i] = get_copy_of_container(
                sa->containers[i], &sa->typecodes[i], copy_on_write);
            ra->containers[pos] = sa->containers[i];
            ra->typecodes[pos] = sa->typecodes[i];
        } else {
            ra->containers[pos] =
                container_clone(sa->containers[i], sa->typecodes[i]);
            ra->typecodes[pos] = sa->typecodes[i];
        }
        ra->size++;
    }
}

void *ra_get_container(roaring_array_t *ra, uint16_t x, uint8_t *typecode) {
    int i = binarySearch(ra->keys, (int32_t)ra->size, x);
    if (i < 0) return NULL;
    *typecode = ra->typecodes[i];
    return ra->containers[i];
}

extern void *ra_get_container_at_index(const roaring_array_t *ra, uint16_t i,
                                       uint8_t *typecode);

void *ra_get_writable_container(roaring_array_t *ra, uint16_t x,
                                uint8_t *typecode) {
    int i = binarySearch(ra->keys, (int32_t)ra->size, x);
    if (i < 0) return NULL;
    *typecode = ra->typecodes[i];
    return get_writable_copy_if_shared(ra->containers[i], typecode);
}

void *ra_get_writable_container_at_index(roaring_array_t *ra, uint16_t i,
                                         uint8_t *typecode) {
    assert(i < ra->size);
    *typecode = ra->typecodes[i];
    return get_writable_copy_if_shared(ra->containers[i], typecode);
}

uint16_t ra_get_key_at_index(const roaring_array_t *ra, uint16_t i) {
    return ra->keys[i];
}

extern int32_t ra_get_index(const roaring_array_t *ra, uint16_t x);

extern int32_t ra_advance_until(const roaring_array_t *ra, uint16_t x,
                                int32_t pos);

// everything skipped over is freed
int32_t ra_advance_until_freeing(roaring_array_t *ra, uint16_t x, int32_t pos) {
    while (pos < ra->size && ra->keys[pos] < x) {
        container_free(ra->containers[pos], ra->typecodes[pos]);
        ++pos;
    }
    return pos;
}

void ra_insert_new_key_value_at(roaring_array_t *ra, int32_t i, uint16_t key,
                                void *container, uint8_t typecode) {
    extend_array(ra, 1);
    // May be an optimization opportunity with DIY memmove
    memmove(&(ra->keys[i + 1]), &(ra->keys[i]),
            sizeof(uint16_t) * (ra->size - i));
    memmove(&(ra->containers[i + 1]), &(ra->containers[i]),
            sizeof(void *) * (ra->size - i));
    memmove(&(ra->typecodes[i + 1]), &(ra->typecodes[i]),
            sizeof(uint8_t) * (ra->size - i));
    ra->keys[i] = key;
    ra->containers[i] = container;
    ra->typecodes[i] = typecode;
    ra->size++;
}

// note: Java routine set things to 0, enabling GC.
// Java called it "resize" but it was always used to downsize.
// Allowing upsize would break the conventions about
// valid containers below ra->size.

void ra_downsize(roaring_array_t *ra, int32_t new_length) {
    assert(new_length <= ra->size);
    ra->size = new_length;
}

void ra_remove_at_index(roaring_array_t *ra, int32_t i) {
    memmove(&(ra->containers[i]), &(ra->containers[i + 1]),
            sizeof(void *) * (ra->size - i - 1));
    memmove(&(ra->keys[i]), &(ra->keys[i + 1]),
            sizeof(uint16_t) * (ra->size - i - 1));
    memmove(&(ra->typecodes[i]), &(ra->typecodes[i + 1]),
            sizeof(uint8_t) * (ra->size - i - 1));
    ra->size--;
}

void ra_remove_at_index_and_free(roaring_array_t *ra, int32_t i) {
    container_free(ra->containers[i], ra->typecodes[i]);
    ra_remove_at_index(ra, i);
}

// used in inplace andNot only, to slide left the containers from
// the mutated RoaringBitmap that are after the largest container of
// the argument RoaringBitmap.  In use it should be followed by a call to
// downsize.
//
void ra_copy_range(roaring_array_t *ra, uint32_t begin, uint32_t end,
                   uint32_t new_begin) {
    assert(begin <= end);
    assert(new_begin < begin);

    const int range = end - begin;

    // We ensure to previously have freed overwritten containers
    // that are not copied elsewhere

    memmove(&(ra->containers[new_begin]), &(ra->containers[begin]),
            sizeof(void *) * range);
    memmove(&(ra->keys[new_begin]), &(ra->keys[begin]),
            sizeof(uint16_t) * range);
    memmove(&(ra->typecodes[new_begin]), &(ra->typecodes[begin]),
            sizeof(uint8_t) * range);
}

void ra_shift_tail(roaring_array_t *ra, int32_t count, int32_t distance) {
    if (distance > 0) {
        extend_array(ra, distance);
    }
    int32_t srcpos = ra->size - count;
    int32_t dstpos = srcpos + distance;
    memmove(&(ra->keys[dstpos]), &(ra->keys[srcpos]),
            sizeof(uint16_t) * count);
    memmove(&(ra->containers[dstpos]), &(ra->containers[srcpos]),
            sizeof(void *) * count);
    memmove(&(ra->typecodes[dstpos]), &(ra->typecodes[srcpos]),
            sizeof(uint8_t) * count);
    ra->size += distance;
}


size_t ra_size_in_bytes(roaring_array_t *ra) {
    size_t cardinality = 0;
    size_t tot_len =
        1 /* initial byte type */ + 4 /* tot_len */ + sizeof(roaring_array_t) +
        ra->size * (sizeof(uint16_t) + sizeof(void *) + sizeof(uint8_t));
    for (int32_t i = 0; i < ra->size; i++) {
        tot_len +=
            (container_serialization_len(ra->containers[i], ra->typecodes[i]) +
             sizeof(uint16_t));
        cardinality +=
            container_get_cardinality(ra->containers[i], ra->typecodes[i]);
    }

    if ((cardinality * sizeof(uint32_t) + sizeof(uint32_t)) < tot_len) {
        return cardinality * sizeof(uint32_t) + 1 + sizeof(uint32_t);
    }
    return tot_len;
}

void ra_to_uint32_array(const roaring_array_t *ra, uint32_t *ans) {
    size_t ctr = 0;
    for (int32_t i = 0; i < ra->size; ++i) {
        int num_added = container_to_uint32_array(
            ans + ctr, ra->containers[i], ra->typecodes[i],
            ((uint32_t)ra->keys[i]) << 16);
        ctr += num_added;
    }
}

bool ra_range_uint32_array(const roaring_array_t *ra, size_t offset, size_t limit, uint32_t *ans) {
    size_t ctr = 0;
    size_t dtr = 0;

    size_t t_limit = 0;

    bool first = false;
    size_t first_skip = 0;

    uint32_t *t_ans = NULL;
    size_t cur_len = 0;

    for (int i = 0; i < ra->size; ++i) {
        
        const void *container = container_unwrap_shared(ra->containers[i], &ra->typecodes[i]);
        switch (ra->typecodes[i]) {
            case BITSET_CONTAINER_TYPE_CODE:
                t_limit = ((const bitset_container_t *)container)->cardinality;
                break;
            case ARRAY_CONTAINER_TYPE_CODE:
                t_limit = ((const array_container_t *)container)->cardinality;
                break;
            case RUN_CONTAINER_TYPE_CODE:
                t_limit = run_container_cardinality((const run_container_t *)container);
                break;
        }
        if (ctr + t_limit - 1 >= offset && ctr < offset + limit){
            if (!first){
                //first_skip = t_limit - (ctr + t_limit - offset);
                first_skip = offset - ctr;
                first = true;
                t_ans = (uint32_t *)malloc(sizeof(*t_ans) * (first_skip + limit));
                if(t_ans == NULL) {
                  return false;
                }
                memset(t_ans, 0, sizeof(*t_ans) * (first_skip + limit)) ;
                cur_len = first_skip + limit;
            }
            if (dtr + t_limit > cur_len){
                uint32_t * append_ans = (uint32_t *)malloc(sizeof(*append_ans) * (cur_len + t_limit));
                if(append_ans == NULL) {
                  if(t_ans != NULL) free(t_ans);
                  return false;
                }
                memset(append_ans, 0, sizeof(*append_ans) * (cur_len + t_limit));
                cur_len = cur_len + t_limit;
                memcpy(append_ans, t_ans, dtr * sizeof(uint32_t));
                free(t_ans);
                t_ans = append_ans;
            }
            switch (ra->typecodes[i]) {
                case BITSET_CONTAINER_TYPE_CODE:
                    container_to_uint32_array(
                        t_ans + dtr, (const bitset_container_t *)container,  ra->typecodes[i],
                        ((uint32_t)ra->keys[i]) << 16);
                    break;
                case ARRAY_CONTAINER_TYPE_CODE:
                    container_to_uint32_array(
                        t_ans + dtr, (const array_container_t *)container, ra->typecodes[i],
                        ((uint32_t)ra->keys[i]) << 16);
                    break;
                case RUN_CONTAINER_TYPE_CODE:
                    container_to_uint32_array(
                        t_ans + dtr, (const run_container_t *)container, ra->typecodes[i],
                        ((uint32_t)ra->keys[i]) << 16);
                    break;
            }
            dtr += t_limit;
        }
        ctr += t_limit;
        if (dtr-first_skip >= limit) break;
    }
    if(t_ans != NULL) {
      memcpy(ans, t_ans+first_skip, limit * sizeof(uint32_t));
      free(t_ans);
    }
    return true;
}

bool ra_has_run_container(const roaring_array_t *ra) {
    for (int32_t k = 0; k < ra->size; ++k) {
        if (get_container_type(ra->containers[k], ra->typecodes[k]) ==
            RUN_CONTAINER_TYPE_CODE)
            return true;
    }
    return false;
}

uint32_t ra_portable_header_size(const roaring_array_t *ra) {
    if (ra_has_run_container(ra)) {
        if (ra->size <
            NO_OFFSET_THRESHOLD) {  // for small bitmaps, we omit the offsets
            return 4 + (ra->size + 7) / 8 + 4 * ra->size;
        }
        return 4 + (ra->size + 7) / 8 +
               8 * ra->size;  // - 4 because we pack the size with the cookie
    } else {
        return 4 + 4 + 8 * ra->size;
    }
}

size_t ra_portable_size_in_bytes(const roaring_array_t *ra) {
    size_t count = ra_portable_header_size(ra);

    for (int32_t k = 0; k < ra->size; ++k) {
        count += container_size_in_bytes(ra->containers[k], ra->typecodes[k]);
    }
    return count;
}

size_t ra_portable_serialize(const roaring_array_t *ra, char *buf) {
    char *initbuf = buf;
    uint32_t startOffset = 0;
    bool hasrun = ra_has_run_container(ra);
    if (hasrun) {
        uint32_t cookie = SERIAL_COOKIE | ((ra->size - 1) << 16);
        memcpy(buf, &cookie, sizeof(cookie));
        buf += sizeof(cookie);
        uint32_t s = (ra->size + 7) / 8;
        uint8_t *bitmapOfRunContainers = (uint8_t *)calloc(s, 1);
        assert(bitmapOfRunContainers != NULL);  // todo: handle
        for (int32_t i = 0; i < ra->size; ++i) {
            if (get_container_type(ra->containers[i], ra->typecodes[i]) ==
                RUN_CONTAINER_TYPE_CODE) {
                bitmapOfRunContainers[i / 8] |= (1 << (i % 8));
            }
        }
        memcpy(buf, bitmapOfRunContainers, s);
        buf += s;
        free(bitmapOfRunContainers);
        if (ra->size < NO_OFFSET_THRESHOLD) {
            startOffset = 4 + 4 * ra->size + s;
        } else {
            startOffset = 4 + 8 * ra->size + s;
        }
    } else {  // backwards compatibility
        uint32_t cookie = SERIAL_COOKIE_NO_RUNCONTAINER;

        memcpy(buf, &cookie, sizeof(cookie));
        buf += sizeof(cookie);
        memcpy(buf, &ra->size, sizeof(ra->size));
        buf += sizeof(ra->size);

        startOffset = 4 + 4 + 4 * ra->size + 4 * ra->size;
    }
    for (int32_t k = 0; k < ra->size; ++k) {
        memcpy(buf, &ra->keys[k], sizeof(ra->keys[k]));
        buf += sizeof(ra->keys[k]);
        // get_cardinality returns a value in [1,1<<16], subtracting one
        // we get [0,1<<16 - 1] which fits in 16 bits
        uint16_t card = (uint16_t)(
            container_get_cardinality(ra->containers[k], ra->typecodes[k]) - 1);
        memcpy(buf, &card, sizeof(card));
        buf += sizeof(card);
    }
    if ((!hasrun) || (ra->size >= NO_OFFSET_THRESHOLD)) {
        // writing the containers offsets
        for (int32_t k = 0; k < ra->size; k++) {
            memcpy(buf, &startOffset, sizeof(startOffset));
            buf += sizeof(startOffset);
            startOffset =
                startOffset +
                container_size_in_bytes(ra->containers[k], ra->typecodes[k]);
        }
    }
    for (int32_t k = 0; k < ra->size; ++k) {
        buf += container_write(ra->containers[k], ra->typecodes[k], buf);
    }
    return buf - initbuf;
}

// Quickly checks whether there is a serialized bitmap at the pointer,
// not exceeding size "maxbytes" in bytes. This function does not allocate
// memory dynamically.
//
// This function returns 0 if and only if no valid bitmap is found.
// Otherwise, it returns how many bytes are occupied.
//
size_t ra_portable_deserialize_size(const char *buf, const size_t maxbytes) {
    size_t bytestotal = sizeof(int32_t);// for cookie
    if(bytestotal > maxbytes) return 0;
    uint32_t cookie;
    memcpy(&cookie, buf, sizeof(int32_t));
    buf += sizeof(uint32_t);
    if ((cookie & 0xFFFF) != SERIAL_COOKIE &&
        cookie != SERIAL_COOKIE_NO_RUNCONTAINER) {
        return 0;
    }
    int32_t size;

    if ((cookie & 0xFFFF) == SERIAL_COOKIE)
        size = (cookie >> 16) + 1;
    else {
        bytestotal += sizeof(int32_t);
        if(bytestotal > maxbytes) return 0;
        memcpy(&size, buf, sizeof(int32_t));
        buf += sizeof(uint32_t);
    }
    if (size > (1<<16)) {
       return 0; // logically impossible
    }
    char *bitmapOfRunContainers = NULL;
    bool hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE;
    if (hasrun) {
        int32_t s = (size + 7) / 8;
        bytestotal += s;
        if(bytestotal > maxbytes) return 0;
        bitmapOfRunContainers = (char *)buf;
        buf += s;
    }
    bytestotal += size * 2 * sizeof(uint16_t);
    if(bytestotal > maxbytes) return 0;
    uint16_t *keyscards = (uint16_t *)buf;
    buf += size * 2 * sizeof(uint16_t);
    if ((!hasrun) || (size >= NO_OFFSET_THRESHOLD)) {
        // skipping the offsets
        bytestotal += size * 4;
        if(bytestotal > maxbytes) return 0;
        buf += size * 4;
    }
    // Reading the containers
    for (int32_t k = 0; k < size; ++k) {
        uint16_t tmp;
        memcpy(&tmp, keyscards + 2*k+1, sizeof(tmp));
        uint32_t thiscard = tmp + 1;
        bool isbitmap = (thiscard > DEFAULT_MAX_SIZE);
        bool isrun = false;
        if(hasrun) {
          if((bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0) {
            isbitmap = false;
            isrun = true;
          }
        }
        if (isbitmap) {
            size_t containersize = BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
            bytestotal += containersize;
            if(bytestotal > maxbytes) return 0;
            buf += containersize;
        } else if (isrun) {
            bytestotal += sizeof(uint16_t);
            if(bytestotal > maxbytes) return 0;
            uint16_t n_runs;
            memcpy(&n_runs, buf, sizeof(uint16_t));
            buf += sizeof(uint16_t);
            size_t containersize = n_runs * sizeof(rle16_t);
            bytestotal += containersize;
            if(bytestotal > maxbytes) return 0;
            buf += containersize;
        } else {
            size_t containersize = thiscard * sizeof(uint16_t);
            bytestotal += containersize;
            if(bytestotal > maxbytes) return 0;
            buf += containersize;
        }
    }
    return bytestotal;
}


// this function populates answer from the content of buf (reading up to maxbytes bytes).
// The function returns false if a properly serialized bitmap cannot be found.
// if it returns true, readbytes is populated by how many bytes were read, we have that *readbytes <= maxbytes.
bool ra_portable_deserialize(roaring_array_t *answer, const char *buf, const size_t maxbytes, size_t * readbytes) {
    *readbytes = sizeof(int32_t);// for cookie
    if(*readbytes > maxbytes) {
      fprintf(stderr, "Ran out of bytes while reading first 4 bytes.\n");
      return false;
    }
    uint32_t cookie;
    memcpy(&cookie, buf, sizeof(int32_t));
    buf += sizeof(uint32_t);
    if ((cookie & 0xFFFF) != SERIAL_COOKIE &&
        cookie != SERIAL_COOKIE_NO_RUNCONTAINER) {
        fprintf(stderr, "I failed to find one of the right cookies. Found %" PRIu32 "\n",
                cookie);
        return false;
    }
    int32_t size;

    if ((cookie & 0xFFFF) == SERIAL_COOKIE)
        size = (cookie >> 16) + 1;
    else {
        *readbytes += sizeof(int32_t);
        if(*readbytes > maxbytes) {
          fprintf(stderr, "Ran out of bytes while reading second part of the cookie.\n");
          return false;
        }
        memcpy(&size, buf, sizeof(int32_t));
        buf += sizeof(uint32_t);
    }
    if (size > (1<<16)) {
       fprintf(stderr, "You cannot have so many containers, the data must be corrupted: %" PRId32 "\n",
                size);
       return false; // logically impossible
    }
    const char *bitmapOfRunContainers = NULL;
    bool hasrun = (cookie & 0xFFFF) == SERIAL_COOKIE;
    if (hasrun) {
        int32_t s = (size + 7) / 8;
        *readbytes += s;
        if(*readbytes > maxbytes) {// data is corrupted?
          fprintf(stderr, "Ran out of bytes while reading run bitmap.\n");
          return false;
        }
        bitmapOfRunContainers = buf;
        buf += s;
    }
    uint16_t *keyscards = (uint16_t *)buf;

    *readbytes += size * 2 * sizeof(uint16_t);
    if(*readbytes > maxbytes) {
      fprintf(stderr, "Ran out of bytes while reading key-cardinality array.\n");
      return false;
    }
    buf += size * 2 * sizeof(uint16_t);

    bool is_ok = ra_init_with_capacity(answer, size);
    if (!is_ok) {
        fprintf(stderr, "Failed to allocate memory for roaring array. Bailing out.\n");
        return false;
    }

    for (int32_t k = 0; k < size; ++k) {
        uint16_t tmp;
        memcpy(&tmp, keyscards + 2*k, sizeof(tmp));
        answer->keys[k] = tmp;
    }
    if ((!hasrun) || (size >= NO_OFFSET_THRESHOLD)) {
        *readbytes += size * 4;
        if(*readbytes > maxbytes) {// data is corrupted?
          fprintf(stderr, "Ran out of bytes while reading offsets.\n");
          ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
          return false;
        }

        // skipping the offsets
        buf += size * 4;
    }
    // Reading the containers
    for (int32_t k = 0; k < size; ++k) {
        uint16_t tmp;
        memcpy(&tmp, keyscards + 2*k+1, sizeof(tmp));
        uint32_t thiscard = tmp + 1;
        bool isbitmap = (thiscard > DEFAULT_MAX_SIZE);
        bool isrun = false;
        if(hasrun) {
          if((bitmapOfRunContainers[k / 8] & (1 << (k % 8))) != 0) {
            isbitmap = false;
            isrun = true;
          }
        }
        if (isbitmap) {
            // we check that the read is allowed
            size_t containersize = BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
            *readbytes += containersize;
            if(*readbytes > maxbytes) {
              fprintf(stderr, "Running out of bytes while reading a bitset container.\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            // it is now safe to read
            bitset_container_t *c = bitset_container_create();
            if(c == NULL) {// memory allocation failure
              fprintf(stderr, "Failed to allocate memory for a bitset container.\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            answer->size++;
            buf += bitset_container_read(thiscard, c, buf);
            answer->containers[k] = c;
            answer->typecodes[k] = BITSET_CONTAINER_TYPE_CODE;
        } else if (isrun) {
            // we check that the read is allowed
            *readbytes += sizeof(uint16_t);
            if(*readbytes > maxbytes) {
              fprintf(stderr, "Running out of bytes while reading a run container (header).\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            uint16_t n_runs;
            memcpy(&n_runs, buf, sizeof(uint16_t));
            size_t containersize = n_runs * sizeof(rle16_t);
            *readbytes += containersize;
            if(*readbytes > maxbytes) {// data is corrupted?
              fprintf(stderr, "Running out of bytes while reading a run container.\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            // it is now safe to read

            run_container_t *c = run_container_create();
            if(c == NULL) {// memory allocation failure
              fprintf(stderr, "Failed to allocate memory for a run container.\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            answer->size++;
            buf += run_container_read(thiscard, c, buf);
            answer->containers[k] = c;
            answer->typecodes[k] = RUN_CONTAINER_TYPE_CODE;
        } else {
            // we check that the read is allowed
            size_t containersize = thiscard * sizeof(uint16_t);
            *readbytes += containersize;
            if(*readbytes > maxbytes) {// data is corrupted?
              fprintf(stderr, "Running out of bytes while reading an array container.\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            // it is now safe to read
            array_container_t *c =
                array_container_create_given_capacity(thiscard);
            if(c == NULL) {// memory allocation failure
              fprintf(stderr, "Failed to allocate memory for an array container.\n");
              ra_clear(answer);// we need to clear the containers already allocated, and the roaring array
              return false;
            }
            answer->size++;
            buf += array_container_read(thiscard, c, buf);
            answer->containers[k] = c;
            answer->typecodes[k] = ARRAY_CONTAINER_TYPE_CODE;
        }
    }
    return true;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/roaring_array.c */
/* begin file /opt/bitmap/CRoaring-0.2.57/src/roaring_priority_queue.c */

struct roaring_pq_element_s {
    uint64_t size;
    bool is_temporary;
    roaring_bitmap_t *bitmap;
};

typedef struct roaring_pq_element_s roaring_pq_element_t;

struct roaring_pq_s {
    roaring_pq_element_t *elements;
    uint64_t size;
};

typedef struct roaring_pq_s roaring_pq_t;

static inline bool compare(roaring_pq_element_t *t1, roaring_pq_element_t *t2) {
    return t1->size < t2->size;
}

static void pq_add(roaring_pq_t *pq, roaring_pq_element_t *t) {
    uint64_t i = pq->size;
    pq->elements[pq->size++] = *t;
    while (i > 0) {
        uint64_t p = (i - 1) >> 1;
        roaring_pq_element_t ap = pq->elements[p];
        if (!compare(t, &ap)) break;
        pq->elements[i] = ap;
        i = p;
    }
    pq->elements[i] = *t;
}

static void pq_free(roaring_pq_t *pq) {
    free(pq->elements);
    pq->elements = NULL;  // paranoid
    free(pq);
}

static void percolate_down(roaring_pq_t *pq, uint32_t i) {
    uint32_t size = (uint32_t)pq->size;
    uint32_t hsize = size >> 1;
    roaring_pq_element_t ai = pq->elements[i];
    while (i < hsize) {
        uint32_t l = (i << 1) + 1;
        uint32_t r = l + 1;
        roaring_pq_element_t bestc = pq->elements[l];
        if (r < size) {
            if (compare(pq->elements + r, &bestc)) {
                l = r;
                bestc = pq->elements[r];
            }
        }
        if (!compare(&bestc, &ai)) {
            break;
        }
        pq->elements[i] = bestc;
        i = l;
    }
    pq->elements[i] = ai;
}

static roaring_pq_t *create_pq(const roaring_bitmap_t **arr, uint32_t length) {
    roaring_pq_t *answer = (roaring_pq_t *)malloc(sizeof(roaring_pq_t));
    answer->elements =
        (roaring_pq_element_t *)malloc(sizeof(roaring_pq_element_t) * length);
    answer->size = length;
    for (uint32_t i = 0; i < length; i++) {
        answer->elements[i].bitmap = (roaring_bitmap_t *)arr[i];
        answer->elements[i].is_temporary = false;
        answer->elements[i].size =
            roaring_bitmap_portable_size_in_bytes(arr[i]);
    }
    for (int32_t i = (length >> 1); i >= 0; i--) {
        percolate_down(answer, i);
    }
    return answer;
}

static roaring_pq_element_t pq_poll(roaring_pq_t *pq) {
    roaring_pq_element_t ans = *pq->elements;
    if (pq->size > 1) {
        pq->elements[0] = pq->elements[--pq->size];
        percolate_down(pq, 0);
    } else
        --pq->size;
    // memmove(pq->elements,pq->elements+1,(pq->size-1)*sizeof(roaring_pq_element_t));--pq->size;
    return ans;
}

// this function consumes and frees the inputs
static roaring_bitmap_t *lazy_or_from_lazy_inputs(roaring_bitmap_t *x1,
                                                  roaring_bitmap_t *x2) {
    uint8_t container_result_type = 0;
    const int length1 = ra_get_size(&x1->high_low_container),
              length2 = ra_get_size(&x2->high_low_container);
    if (0 == length1) {
        roaring_bitmap_free(x1);
        return x2;
    }
    if (0 == length2) {
        roaring_bitmap_free(x2);
        return x1;
    }
    uint32_t neededcap = length1 > length2 ? length2 : length1;
    roaring_bitmap_t *answer = roaring_bitmap_create_with_capacity(neededcap);
    int pos1 = 0, pos2 = 0;
    uint8_t container_type_1, container_type_2;
    uint16_t s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
    uint16_t s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
    while (true) {
        if (s1 == s2) {
            // todo: unsharing can be inefficient as it may create a clone where
            // none
            // is needed, but it has the benefit of being easy to reason about.
            ra_unshare_container_at_index(&x1->high_low_container, pos1);
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            assert(container_type_1 != SHARED_CONTAINER_TYPE_CODE);
            ra_unshare_container_at_index(&x2->high_low_container, pos2);
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            assert(container_type_2 != SHARED_CONTAINER_TYPE_CODE);
            void *c;

            if ((container_type_2 == BITSET_CONTAINER_TYPE_CODE) &&
                (container_type_1 != BITSET_CONTAINER_TYPE_CODE)) {
                c = container_lazy_ior(c2, container_type_2, c1,
                                       container_type_1,
                                       &container_result_type);
                container_free(c1, container_type_1);
                if (c != c2) {
                    container_free(c2, container_type_2);
                }
            } else {
                c = container_lazy_ior(c1, container_type_1, c2,
                                       container_type_2,
                                       &container_result_type);
                container_free(c2, container_type_2);
                if (c != c1) {
                    container_free(c1, container_type_1);
                }
            }
            // since we assume that the initial containers are non-empty, the
            // result here
            // can only be non-empty
            ra_append(&answer->high_low_container, s1, c,
                      container_result_type);
            ++pos1;
            ++pos2;
            if (pos1 == length1) break;
            if (pos2 == length2) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);

        } else if (s1 < s2) {  // s1 < s2
            void *c1 = ra_get_container_at_index(&x1->high_low_container, pos1,
                                                 &container_type_1);
            ra_append(&answer->high_low_container, s1, c1, container_type_1);
            pos1++;
            if (pos1 == length1) break;
            s1 = ra_get_key_at_index(&x1->high_low_container, pos1);

        } else {  // s1 > s2
            void *c2 = ra_get_container_at_index(&x2->high_low_container, pos2,
                                                 &container_type_2);
            ra_append(&answer->high_low_container, s2, c2, container_type_2);
            pos2++;
            if (pos2 == length2) break;
            s2 = ra_get_key_at_index(&x2->high_low_container, pos2);
        }
    }
    if (pos1 == length1) {
        ra_append_move_range(&answer->high_low_container,
                             &x2->high_low_container, pos2, length2);
    } else if (pos2 == length2) {
        ra_append_move_range(&answer->high_low_container,
                             &x1->high_low_container, pos1, length1);
    }
    ra_clear_without_containers(&x1->high_low_container);
    ra_clear_without_containers(&x2->high_low_container);
    free(x1);
    free(x2);
    return answer;
}

/**
 * Compute the union of 'number' bitmaps using a heap. This can
 * sometimes be faster than roaring_bitmap_or_many which uses
 * a naive algorithm. Caller is responsible for freeing the
 * result.
 */
roaring_bitmap_t *roaring_bitmap_or_many_heap(uint32_t number,
                                              const roaring_bitmap_t **x) {
    if (number == 0) {
        return roaring_bitmap_create();
    }
    if (number == 1) {
        return roaring_bitmap_copy(x[0]);
    }
    roaring_pq_t *pq = create_pq(x, number);
    while (pq->size > 1) {
        roaring_pq_element_t x1 = pq_poll(pq);
        roaring_pq_element_t x2 = pq_poll(pq);

        if (x1.is_temporary && x2.is_temporary) {
            roaring_bitmap_t *newb =
                lazy_or_from_lazy_inputs(x1.bitmap, x2.bitmap);
            // should normally return a fresh new bitmap *except* that
            // it can return x1.bitmap or x2.bitmap in degenerate cases
            bool temporary = !((newb == x1.bitmap) && (newb == x2.bitmap));
            uint64_t bsize = roaring_bitmap_portable_size_in_bytes(newb);
            roaring_pq_element_t newelement = {
                .size = bsize, .is_temporary = temporary, .bitmap = newb};
            pq_add(pq, &newelement);
        } else if (x2.is_temporary) {
            roaring_bitmap_lazy_or_inplace(x2.bitmap, x1.bitmap, false);
            x2.size = roaring_bitmap_portable_size_in_bytes(x2.bitmap);
            pq_add(pq, &x2);
        } else if (x1.is_temporary) {
            roaring_bitmap_lazy_or_inplace(x1.bitmap, x2.bitmap, false);
            x1.size = roaring_bitmap_portable_size_in_bytes(x1.bitmap);

            pq_add(pq, &x1);
        } else {
            roaring_bitmap_t *newb =
                roaring_bitmap_lazy_or(x1.bitmap, x2.bitmap, false);
            uint64_t bsize = roaring_bitmap_portable_size_in_bytes(newb);
            roaring_pq_element_t newelement = {
                .size = bsize, .is_temporary = true, .bitmap = newb};

            pq_add(pq, &newelement);
        }
    }
    roaring_pq_element_t X = pq_poll(pq);
    roaring_bitmap_t *answer = X.bitmap;
    roaring_bitmap_repair_after_lazy(answer);
    pq_free(pq);
    return answer;
}
/* end file /opt/bitmap/CRoaring-0.2.57/src/roaring_priority_queue.c */
