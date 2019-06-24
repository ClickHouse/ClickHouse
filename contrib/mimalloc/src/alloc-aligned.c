/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/

#include "mimalloc.h"
#include "mimalloc-internal.h"

#include <string.h>  // memset

// ------------------------------------------------------
// Aligned Allocation
// ------------------------------------------------------

static void* mi_heap_malloc_zero_aligned_at(mi_heap_t* heap, size_t size, size_t alignment, size_t offset, bool zero) mi_attr_noexcept {
  // note: we don't require `size > offset`, we just guarantee that
  // the address at offset is aligned regardless of the allocated size.
  mi_assert(alignment > 0);
  if (alignment <= sizeof(uintptr_t)) return _mi_heap_malloc_zero(heap,size,zero);
  if (size >= (SIZE_MAX - alignment)) return NULL; // overflow

  // try if there is a current small block with just the right alignment
  if (size <= MI_SMALL_SIZE_MAX) {
    mi_page_t* page = _mi_heap_get_free_small_page(heap,size);
    if (page->free != NULL &&
        (((uintptr_t)page->free + offset) % alignment) == 0)
    {
      #if MI_STAT>1
        mi_heap_stat_increase( heap, malloc, size);
      #endif
      void* p = _mi_page_malloc(heap,page,size);
      mi_assert_internal(p != NULL);
      mi_assert_internal(((uintptr_t)p + offset) % alignment == 0);
      if (zero) memset(p,0,size);
      return p;
    }
  }

  // otherwise over-allocate
  void* p = _mi_heap_malloc_zero(heap, size + alignment - 1, zero);
  if (p == NULL) return NULL;

  // .. and align within the allocation
  _mi_ptr_page(p)->flags.has_aligned = true;
  uintptr_t adjust = alignment - (((uintptr_t)p + offset) % alignment);
  mi_assert_internal(adjust % sizeof(uintptr_t) == 0);
  void* aligned_p = (adjust == alignment ? p : (void*)((uintptr_t)p + adjust));
  mi_assert_internal(((uintptr_t)aligned_p + offset) % alignment == 0);
  mi_assert_internal( p == _mi_page_ptr_unalign(_mi_ptr_segment(aligned_p),_mi_ptr_page(aligned_p),aligned_p) );
  return aligned_p;
}

static void* mi_malloc_zero_aligned_at(size_t size, size_t alignment, size_t offset, bool zero) mi_attr_noexcept {
  return mi_heap_malloc_zero_aligned_at(mi_get_default_heap(),size,alignment,offset,zero);
}

void* mi_malloc_aligned_at(size_t size, size_t alignment, size_t offset) mi_attr_noexcept {
  return mi_malloc_zero_aligned_at(size, alignment, offset, false);
}

void* mi_malloc_aligned(size_t size, size_t alignment) mi_attr_noexcept {
  return mi_malloc_aligned_at(size, alignment, 0);
}

void* mi_zalloc_aligned_at(size_t size, size_t alignment, size_t offset) mi_attr_noexcept {
  return mi_malloc_zero_aligned_at(size,alignment,offset,true);
}

void* mi_zalloc_aligned(size_t size, size_t alignment) mi_attr_noexcept {
  return mi_zalloc_aligned_at(size,alignment,0);
}

void* mi_calloc_aligned_at(size_t count, size_t size, size_t alignment, size_t offset) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_zalloc_aligned_at(total,alignment,offset);
}

void* mi_calloc_aligned(size_t count, size_t size, size_t alignment) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_zalloc_aligned(total,alignment);
}


static void* mi_realloc_zero_aligned_at(void* p, size_t newsize, size_t alignment, size_t offset, bool zero) mi_attr_noexcept {
  mi_assert(alignment > 0);
  if (alignment <= sizeof(uintptr_t)) return _mi_realloc_zero(p,newsize,zero);
  if (p == NULL) return mi_malloc_zero_aligned_at(newsize,alignment,offset,zero);
  size_t size = mi_usable_size(p);
  if (newsize <= size && newsize >= (size - (size / 2))
      && (((uintptr_t)p + offset) % alignment) == 0) {
    return p;  // reallocation still fits, is aligned and not more than 50% waste
  }
  else {
    void* newp = mi_malloc_aligned_at(newsize,alignment,offset);
    if (newp != NULL) {
      if (zero && newsize > size) {
        // also set last word in the previous allocation to zero to ensure any padding is zero-initialized
        size_t start = (size >= sizeof(intptr_t) ? size - sizeof(intptr_t) : 0);
        memset((uint8_t*)newp + start, 0, newsize - start);
      }
      memcpy(newp, p, (newsize > size ? size : newsize));
      mi_free(p); // only free if successful
    }
    return newp;
  }
}

static void* _mi_realloc_aligned(void* p, size_t newsize, size_t alignment, bool zero) mi_attr_noexcept {
  mi_assert(alignment > 0);
  if (alignment <= sizeof(uintptr_t)) return _mi_realloc_zero(p,newsize,zero);
  size_t offset = ((uintptr_t)p % alignment); // use offset of previous allocation (p can be NULL)
  return mi_realloc_zero_aligned_at(p,newsize,alignment,offset,zero);
}

void* mi_realloc_aligned_at(void* p, size_t newsize, size_t alignment, size_t offset) mi_attr_noexcept {
  return mi_realloc_zero_aligned_at(p,newsize,alignment,offset,false);
}

void* mi_realloc_aligned(void* p, size_t newsize, size_t alignment) mi_attr_noexcept {
  return _mi_realloc_aligned(p,newsize,alignment,false);
}

void* mi_rezalloc_aligned_at(void* p, size_t newsize, size_t alignment, size_t offset) mi_attr_noexcept {
  return mi_realloc_zero_aligned_at(p,newsize,alignment,offset,true);
}

void* mi_rezalloc_aligned(void* p, size_t newsize, size_t alignment) mi_attr_noexcept {
  return _mi_realloc_aligned(p,newsize,alignment,true);
}

void* mi_recalloc_aligned_at(void* p, size_t count, size_t size, size_t alignment, size_t offset) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_rezalloc_aligned_at(p,total,alignment,offset);
}

void* mi_recalloc_aligned(void* p, size_t count, size_t size, size_t alignment) mi_attr_noexcept {
  size_t total;
  if (mi_mul_overflow(count,size,&total)) return NULL;
  return mi_rezalloc_aligned(p,total,alignment);
}

