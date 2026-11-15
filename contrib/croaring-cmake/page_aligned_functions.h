#pragma once

// Force-included into roaring.c and containers/mixed_union.c. Page alignment keeps each function, and the loop
// ThinLTO inlines into it, inside one 4 KiB page: whenever a page boundary falls inside such a loop it runs
// about 30% slower (the array intersection loop on aarch64, the array union loop on x86_64), and whether that
// happens depends only on the unrelated code the linker places before the function. Both functions are much
// smaller than 4 KiB, so a function that starts on a page boundary contains none.
#include <roaring/roaring.h>
#include <roaring/containers/mixed_union.h>

__attribute__((aligned(4096))) uint64_t roaring_bitmap_and_cardinality(const roaring_bitmap_t * r1, const roaring_bitmap_t * r2);

__attribute__((aligned(4096))) bool array_array_container_inplace_union(array_container_t * src_1, const array_container_t * src_2, container_t ** dst);
