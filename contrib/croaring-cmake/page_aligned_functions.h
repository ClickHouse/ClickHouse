#pragma once

// Force-included into roaring.c. Page alignment keeps this function, and the array intersection loop ThinLTO inlines
// into it, inside one 4 KiB page: on aarch64 that loop runs about 30% slower whenever a page boundary falls inside it.
#include <roaring/roaring.h>

__attribute__((aligned(4096))) uint64_t roaring_bitmap_and_cardinality(const roaring_bitmap_t * r1, const roaring_bitmap_t * r2);
