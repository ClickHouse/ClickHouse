/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#include "mimalloc.h"
#include "mimalloc-internal.h"
#include "mimalloc-atomic.h"

#include <string.h>  // memset
#include <stdio.h>

#define MI_PAGE_HUGE_ALIGN  (256*1024)

/* -----------------------------------------------------------
  Segment allocation
  We allocate pages inside big OS allocated "segments"
  (4mb on 64-bit). This is to avoid splitting VMA's on Linux
  and reduce fragmentation on other OS's. Each thread
  owns its own segments.

  Currently we have:
  - small pages (64kb), 32 in one segment
  - large pages (4mb), 1 in one segment
  - huge blocks > MI_LARGE_SIZE_MAX (512kb) are directly allocated by the OS

  In any case the memory for a segment is virtual and only
  committed on demand (i.e. we are careful to not touch the memory
  until we actually allocate a block there)

  If a  thread ends, it "abandons" pages with used blocks
  and there is an abandoned segment list whose segments can
  be reclaimed by still running threads, much like work-stealing.
----------------------------------------------------------- */


#if (MI_DEBUG > 1)
static bool mi_segment_is_valid(mi_segment_t* segment) {
  mi_assert_internal(segment != NULL);
  mi_assert_internal(_mi_ptr_cookie(segment) == segment->cookie);
  mi_assert_internal(segment->used <= segment->capacity);
  mi_assert_internal(segment->abandoned <= segment->used);
  size_t nfree = 0;
  for (size_t i = 0; i < segment->capacity; i++) {
    if (!segment->pages[i].segment_in_use) nfree++;
  }
  mi_assert_internal(nfree + segment->used == segment->capacity);
  mi_assert_internal(segment->thread_id == _mi_thread_id()); // or 0
  return true;
}
#endif

/* -----------------------------------------------------------
  Queue of segments containing free pages
----------------------------------------------------------- */


#if (MI_DEBUG>1)
static bool mi_segment_queue_contains(const mi_segment_queue_t* queue, mi_segment_t* segment) {
  mi_assert_internal(segment != NULL);
  mi_segment_t* list = queue->first;
  while (list != NULL) {
    if (list == segment) break;
    mi_assert_internal(list->next==NULL || list->next->prev == list);
    mi_assert_internal(list->prev==NULL || list->prev->next == list);
    list = list->next;
  }
  return (list == segment);
}
#endif

// quick test to see if a segment is in the free pages queue
static bool mi_segment_is_in_free_queue(mi_segment_t* segment, mi_segments_tld_t* tld) {
  bool in_queue = (segment->next != NULL || segment->prev != NULL || tld->small_free.first == segment);
  if (in_queue) {
    mi_assert(segment->page_kind == MI_PAGE_SMALL); // for now we only support small pages
    mi_assert_expensive(mi_segment_queue_contains(&tld->small_free, segment));
  }
  return in_queue;
}

static bool mi_segment_queue_is_empty(const mi_segment_queue_t* queue) {
  return (queue->first == NULL);
}

static void mi_segment_queue_remove(mi_segment_queue_t* queue, mi_segment_t* segment) {
  mi_assert_expensive(mi_segment_queue_contains(queue, segment));
  if (segment->prev != NULL) segment->prev->next = segment->next;
  if (segment->next != NULL) segment->next->prev = segment->prev;
  if (segment == queue->first) queue->first = segment->next;
  if (segment == queue->last)  queue->last = segment->prev;
  segment->next = NULL;
  segment->prev = NULL;
}

static void mi_segment_enqueue(mi_segment_queue_t* queue, mi_segment_t* segment) {
  mi_assert_expensive(!mi_segment_queue_contains(queue, segment));
  segment->next = NULL;
  segment->prev = queue->last;
  if (queue->last != NULL) {
    mi_assert_internal(queue->last->next == NULL);
    queue->last->next = segment;
    queue->last = segment;
  }
  else {
    queue->last = queue->first = segment;
  }
}

static void mi_segment_queue_insert_before(mi_segment_queue_t* queue, mi_segment_t* elem, mi_segment_t* segment) {
  mi_assert_expensive(elem==NULL || mi_segment_queue_contains(queue, elem));
  mi_assert_expensive(segment != NULL && !mi_segment_queue_contains(queue, segment));

  segment->prev = (elem == NULL ? queue->last : elem->prev);
  if (segment->prev != NULL) segment->prev->next = segment;
                        else queue->first = segment;
  segment->next = elem;
  if (segment->next != NULL) segment->next->prev = segment;
                        else queue->last = segment;
}


// Start of the page available memory
uint8_t* _mi_segment_page_start(const mi_segment_t* segment, const mi_page_t* page, size_t* page_size)
{
  size_t   psize = (segment->page_kind == MI_PAGE_HUGE ? segment->segment_size : (size_t)1 << segment->page_shift);
  uint8_t* p     = (uint8_t*)segment + page->segment_idx*psize;

 if (page->segment_idx == 0) {
    // the first page starts after the segment info (and possible guard page)
    p     += segment->segment_info_size;
    psize -= segment->segment_info_size;
  }
  long secure = mi_option_get(mi_option_secure);
  if (secure > 1 || (secure == 1 && page->segment_idx == segment->capacity - 1)) {
    // secure == 1: the last page has an os guard page at the end
    // secure >  1: every page has an os guard page
    psize -= _mi_os_page_size();
  }

  if (page_size != NULL) *page_size = psize;
  mi_assert_internal(_mi_ptr_page(p) == page);
  mi_assert_internal(_mi_ptr_segment(p) == segment);
  return p;
}

static size_t mi_segment_size(size_t capacity, size_t required, size_t* pre_size, size_t* info_size) {
  /*
  if (mi_option_is_enabled(mi_option_secure)) {
    // always reserve maximally so the protection falls on
    // the same address area, as we need to reuse them from the caches interchangably.
    capacity = MI_SMALL_PAGES_PER_SEGMENT;
  }
  */
  size_t minsize   = sizeof(mi_segment_t) + ((capacity - 1) * sizeof(mi_page_t)) + 16 /* padding */;
  size_t guardsize = 0;
  size_t isize     = 0;

  if (!mi_option_is_enabled(mi_option_secure)) {
    // normally no guard pages
    isize = _mi_align_up(minsize, (16 > MI_MAX_ALIGN_SIZE ? 16 : MI_MAX_ALIGN_SIZE));
  }
  else {
    // in secure mode, we set up a protected page in between the segment info
    // and the page data (and one at the end of the segment)
    size_t page_size = _mi_os_page_size();
    isize = _mi_align_up(minsize, page_size);
    guardsize = page_size;
    required = _mi_align_up(required, page_size);
  }
;
  if (info_size != NULL) *info_size = isize;
  if (pre_size != NULL)  *pre_size  = isize + guardsize;
  return (required==0 ? MI_SEGMENT_SIZE : _mi_align_up( required + isize + 2*guardsize, MI_PAGE_HUGE_ALIGN) );
}


/* -----------------------------------------------------------
Segment caches
We keep a small segment cache per thread to avoid repeated allocation
and free in the OS if a program allocates memory and then frees
all again repeatedly. (We tried a one-element cache but that
proves to be too small for certain workloads).
----------------------------------------------------------- */

static void mi_segments_track_size(long segment_size, mi_segments_tld_t* tld) {
  if (segment_size>=0) mi_stat_increase(tld->stats->segments,1);
                  else mi_stat_decrease(tld->stats->segments,1);
  tld->current_size += segment_size;
  if (tld->current_size > tld->peak_size) tld->peak_size = tld->current_size;
}


static void mi_segment_os_free(mi_segment_t* segment, size_t segment_size, mi_segments_tld_t* tld) {
  mi_segments_track_size(-((long)segment_size),tld);
  _mi_os_free(segment, segment_size,tld->stats);
}

// The segment cache is limited to be at most 1/8 of the peak size
// in use (and no more than 32)
#define MI_SEGMENT_CACHE_MAX (32)
#define MI_SEGMENT_CACHE_FRACTION (8)


// Get a segment of at least `required` size.
// If `required == MI_SEGMENT_SIZE` the `segment_size` will match exactly
static mi_segment_t* _mi_segment_cache_findx(mi_segments_tld_t* tld, size_t required, bool reverse) {
  mi_assert_internal(required % _mi_os_page_size() == 0);
  mi_segment_t* segment = (reverse ? tld->cache.last : tld->cache.first);
  while (segment != NULL) {
    if (segment->segment_size >= required) {
      tld->cache_count--;
      tld->cache_size -= segment->segment_size;
      mi_segment_queue_remove(&tld->cache, segment);
      // exact size match?
      if (required==0 || segment->segment_size == required) {
        return segment;
      }
      // not more than 25% waste and on a huge page segment? (in that case the segment size does not need to match required)
      else if (required != MI_SEGMENT_SIZE && segment->segment_size - (segment->segment_size/4) <= required) {
        return segment;
      }
      // try to shrink the memory to match exactly
      else {
        if (mi_option_is_enabled(mi_option_secure)) {
          _mi_os_unprotect(segment, segment->segment_size);
        }
        if (_mi_os_shrink(segment, segment->segment_size, required)) {
          tld->current_size -= segment->segment_size;
          tld->current_size += required;
          segment->segment_size = required;
          return segment;
        }
        else {
          // if that all fails, we give up
          mi_segment_os_free(segment,segment->segment_size,tld);
          return NULL;
        }
      }
    }
    segment = (reverse ? segment->prev : segment->next);
  }
  return NULL;
}

static mi_segment_t* mi_segment_cache_find(mi_segments_tld_t* tld, size_t required) {
  return _mi_segment_cache_findx(tld,required,false);
}

static mi_segment_t* mi_segment_cache_evict(mi_segments_tld_t* tld) {
  // TODO: random eviction instead?
  return _mi_segment_cache_findx(tld, 0, true /* from the end */);
}

static bool mi_segment_cache_full(mi_segments_tld_t* tld) {
  if (tld->cache_count < MI_SEGMENT_CACHE_MAX &&
      tld->cache_size*MI_SEGMENT_CACHE_FRACTION < tld->peak_size) return false;
  // take the opportunity to reduce the segment cache if it is too large (now)
  while (tld->cache_size*MI_SEGMENT_CACHE_FRACTION >= tld->peak_size + 1) {
    mi_segment_t* segment = mi_segment_cache_evict(tld);
    mi_assert_internal(segment != NULL);
    if (segment != NULL) mi_segment_os_free(segment, segment->segment_size, tld);
  }
  return true;
}

static bool mi_segment_cache_insert(mi_segment_t* segment, mi_segments_tld_t* tld) {
  mi_assert_internal(segment->next==NULL && segment->prev==NULL);
  mi_assert_internal(!mi_segment_is_in_free_queue(segment,tld));
  mi_assert_expensive(!mi_segment_queue_contains(&tld->cache, segment));
  if (mi_segment_cache_full(tld)) return false;
  if (mi_option_is_enabled(mi_option_cache_reset) && !mi_option_is_enabled(mi_option_page_reset)) {
    _mi_os_reset((uint8_t*)segment + segment->segment_info_size, segment->segment_size - segment->segment_info_size);
  }
  // insert ordered
  mi_segment_t* seg = tld->cache.first;
  while (seg != NULL && seg->segment_size < segment->segment_size) {
    seg = seg->next;
  }
  mi_segment_queue_insert_before( &tld->cache, seg, segment );
  tld->cache_count++;
  tld->cache_size += segment->segment_size;
  return true;
}

// called by ending threads to free cached segments
void _mi_segment_thread_collect(mi_segments_tld_t* tld) {
  mi_segment_t* segment;
  while ((segment = mi_segment_cache_find(tld,0)) != NULL) {
    mi_segment_os_free(segment, MI_SEGMENT_SIZE, tld);
  }
  mi_assert_internal(tld->cache_count == 0 && tld->cache_size == 0);
  mi_assert_internal(mi_segment_queue_is_empty(&tld->cache));
}

/* -----------------------------------------------------------
   Segment allocation
----------------------------------------------------------- */


// Allocate a segment from the OS aligned to `MI_SEGMENT_SIZE` .
static mi_segment_t* mi_segment_alloc( size_t required, mi_page_kind_t page_kind, size_t page_shift, mi_segments_tld_t* tld, mi_os_tld_t* os_tld)
{
  // calculate needed sizes first

  size_t capacity;
  if (page_kind == MI_PAGE_HUGE) {
    mi_assert_internal(page_shift==MI_SEGMENT_SHIFT && required > 0);
    capacity = 1;
  }
  else {
    mi_assert_internal(required==0);
    size_t page_size = (size_t)1 << page_shift;
    capacity = MI_SEGMENT_SIZE / page_size;
    mi_assert_internal(MI_SEGMENT_SIZE % page_size == 0);
    mi_assert_internal(capacity >= 1 && capacity <= MI_SMALL_PAGES_PER_SEGMENT);
  }
  size_t info_size;
  size_t pre_size;
  size_t segment_size = mi_segment_size( capacity, required, &pre_size, &info_size);
  mi_assert_internal(segment_size >= required);
  size_t page_size = (page_kind == MI_PAGE_HUGE ? segment_size : (size_t)1 << page_shift);

  // Allocate the segment
  mi_segment_t* segment = NULL;

  // try to get it from our caches
  segment = mi_segment_cache_find(tld,segment_size);
  mi_assert_internal(segment == NULL ||
                     (segment_size==MI_SEGMENT_SIZE && segment_size == segment->segment_size) ||
                      (segment_size!=MI_SEGMENT_SIZE && segment_size <= segment->segment_size));
  if (segment != NULL && mi_option_is_enabled(mi_option_secure) && (segment->page_kind != page_kind || segment->segment_size != segment_size)) {
    _mi_os_unprotect(segment,segment->segment_size);
  }

  // and otherwise allocate it from the OS
  if (segment == NULL) {
    segment = (mi_segment_t*)_mi_os_alloc_aligned(segment_size, MI_SEGMENT_SIZE, os_tld);
    if (segment == NULL) return NULL;
    mi_segments_track_size((long)segment_size,tld);
  }

  mi_assert_internal((uintptr_t)segment % MI_SEGMENT_SIZE == 0);

  memset(segment, 0, info_size);
  if (mi_option_is_enabled(mi_option_secure)) {
    // in secure mode, we set up a protected page in between the segment info
    // and the page data
    mi_assert_internal( info_size == pre_size - _mi_os_page_size() && info_size % _mi_os_page_size() == 0);
    _mi_os_protect( (uint8_t*)segment + info_size, (pre_size - info_size) );
    size_t os_page_size = _mi_os_page_size();
    if (mi_option_get(mi_option_secure) <= 1) {
      // and protect the last page too
      _mi_os_protect( (uint8_t*)segment + segment_size - os_page_size, os_page_size );
    }
    else {
      // protect every page
      for (size_t i = 0; i < capacity; i++) {
        _mi_os_protect( (uint8_t*)segment + (i+1)*page_size - os_page_size, os_page_size );
      }
    }
  }

  segment->page_kind  = page_kind;
  segment->capacity   = capacity;
  segment->page_shift = page_shift;
  segment->segment_size = segment_size;
  segment->segment_info_size = pre_size;
  segment->thread_id  = _mi_thread_id();
  segment->cookie = _mi_ptr_cookie(segment);
  for (uint8_t i = 0; i < segment->capacity; i++) {
    segment->pages[i].segment_idx = i;
  }
  mi_stat_increase(tld->stats->committed, segment->segment_info_size);
  //fprintf(stderr,"mimalloc: alloc segment at %p\n", (void*)segment);
  return segment;
}

#if MI_STAT
// Available memory in a page
static size_t mi_page_size(const mi_page_t* page) {
  size_t psize;
  _mi_segment_page_start(_mi_page_segment(page), page, &psize);
  return psize;
}
#endif

static void mi_segment_free(mi_segment_t* segment, bool force, mi_segments_tld_t* tld) {
  //fprintf(stderr,"mimalloc: free segment at %p\n", (void*)segment);
  mi_assert(segment != NULL);
  if (mi_segment_is_in_free_queue(segment,tld)) {
    if (segment->page_kind != MI_PAGE_SMALL) {
      fprintf(stderr, "mimalloc: expecting small segment: %i, %p, %p, %p\n", segment->page_kind, segment->prev, segment->next, tld->small_free.first);
      fflush(stderr);
    }
    else {
      mi_assert_internal(segment->page_kind == MI_PAGE_SMALL); // for now we only support small pages
      mi_assert_expensive(mi_segment_queue_contains(&tld->small_free, segment));
      mi_segment_queue_remove(&tld->small_free, segment);
    }
  }
  mi_assert_expensive(!mi_segment_queue_contains(&tld->small_free, segment));
  mi_assert(segment->next == NULL);
  mi_assert(segment->prev == NULL);
  mi_stat_decrease( tld->stats->committed, segment->segment_info_size);
  segment->thread_id = 0;

  // update reset memory statistics
  for (uint8_t i = 0; i < segment->capacity; i++) {
    mi_page_t* page = &segment->pages[i];
    if (page->is_reset) {
      page->is_reset = false;
      mi_stat_decrease( tld->stats->reset,mi_page_size(page));
    }
  }

  if (!force && mi_segment_cache_insert(segment, tld)) {
    // it is put in our cache
  }
  else {
    // otherwise return it to the OS
    mi_segment_os_free(segment, segment->segment_size, tld);
  }
}




/* -----------------------------------------------------------
  Free page management inside a segment
----------------------------------------------------------- */


static bool mi_segment_has_free(const mi_segment_t* segment) {
  return (segment->used < segment->capacity);
}

static mi_page_t* mi_segment_find_free(mi_segment_t* segment) {
  mi_assert_internal(mi_segment_has_free(segment));
  mi_assert_expensive(mi_segment_is_valid(segment));
  for (size_t i = 0; i < segment->capacity; i++) {
    mi_page_t* page = &segment->pages[i];
    if (!page->segment_in_use) {
      return page;
    }
  }
  mi_assert(false);
  return NULL;
}


/* -----------------------------------------------------------
   Free
----------------------------------------------------------- */

static void mi_segment_abandon(mi_segment_t* segment, mi_segments_tld_t* tld);

static void mi_segment_page_clear(mi_segment_t* segment, mi_page_t* page, mi_stats_t* stats) {
  UNUSED(stats);
  mi_assert_internal(page->segment_in_use);
  mi_assert_internal(mi_page_all_free(page));
  size_t inuse = page->capacity * page->block_size;
  mi_stat_decrease( stats->committed, inuse);
  mi_stat_decrease( stats->pages, 1);

  // reset the page memory to reduce memory pressure?
  if (!page->is_reset && mi_option_is_enabled(mi_option_page_reset)) {
    size_t psize;
    uint8_t* start = _mi_segment_page_start(segment, page, &psize);
    mi_stat_increase( stats->reset, psize);  // for stats we assume resetting the full page
    page->is_reset = true;
    if (inuse > 0) {
      _mi_os_reset(start, inuse);
    }
  }

  // zero the page data
  uint8_t idx = page->segment_idx; // don't clear the index
  bool is_reset = page->is_reset;  // don't clear the reset flag
  memset(page, 0, sizeof(*page));
  page->segment_idx = idx;
  page->segment_in_use = false;
  page->is_reset = is_reset;
  segment->used--;
}

void _mi_segment_page_free(mi_page_t* page, bool force, mi_segments_tld_t* tld)
{
  mi_assert(page != NULL);
  mi_segment_t* segment = _mi_page_segment(page);
  mi_assert_expensive(mi_segment_is_valid(segment));

  // mark it as free now
  mi_segment_page_clear(segment, page, tld->stats);

  if (segment->used == 0) {
    // no more used pages; remove from the free list and free the segment
    mi_segment_free(segment, force, tld);
  }
  else {
    if (segment->used == segment->abandoned) {
      // only abandoned pages; remove from free list and abandon
      mi_segment_abandon(segment,tld);
    }
    else if (segment->used + 1 == segment->capacity) {
      mi_assert_internal(segment->page_kind == MI_PAGE_SMALL); // for now we only support small pages
      // move back to segments small pages free list
      mi_segment_enqueue(&tld->small_free, segment);
    }
  }
}


/* -----------------------------------------------------------
   Abandonment
----------------------------------------------------------- */

// When threads terminate, they can leave segments with
// live blocks (reached through other threads). Such segments
// are "abandoned" and will be reclaimed by other threads to
// reuse their pages and/or free them eventually
static volatile mi_segment_t* abandoned = NULL;
static volatile uintptr_t     abandoned_count = 0;

static void mi_segment_abandon(mi_segment_t* segment, mi_segments_tld_t* tld) {
  mi_assert_internal(segment->used == segment->abandoned);
  mi_assert_internal(segment->used > 0);
  mi_assert_internal(segment->abandoned_next == NULL);
  mi_assert_expensive(mi_segment_is_valid(segment));
  // remove the segment from the free page queue if needed
  if (mi_segment_is_in_free_queue(segment,tld)) {
    mi_assert(segment->page_kind == MI_PAGE_SMALL); // for now we only support small pages
    mi_assert_expensive(mi_segment_queue_contains(&tld->small_free, segment));
    mi_segment_queue_remove(&tld->small_free, segment);
  }
  mi_assert_internal(segment->next == NULL && segment->prev == NULL);
  // all pages in the segment are abandoned; add it to the abandoned list
  segment->thread_id = 0;
  do {
    segment->abandoned_next = (mi_segment_t*)abandoned;
  } while (!mi_atomic_compare_exchange_ptr((volatile void**)&abandoned, segment, segment->abandoned_next));
  mi_atomic_increment(&abandoned_count);
  mi_stat_increase( tld->stats->segments_abandoned,1);
}

void _mi_segment_page_abandon(mi_page_t* page, mi_segments_tld_t* tld) {
  mi_assert(page != NULL);
  mi_segment_t* segment = _mi_page_segment(page);
  mi_assert_expensive(mi_segment_is_valid(segment));
  segment->abandoned++;
  mi_stat_increase( tld->stats->pages_abandoned, 1);
  mi_assert_internal(segment->abandoned <= segment->used);
  if (segment->used == segment->abandoned) {
    // all pages are abandoned, abandon the entire segment
    mi_segment_abandon(segment,tld);
  }
}

bool _mi_segment_try_reclaim_abandoned( mi_heap_t* heap, bool try_all, mi_segments_tld_t* tld) {
  uintptr_t reclaimed = 0;
  uintptr_t atmost;
  if (try_all) {
    atmost = abandoned_count+16;   // close enough
  }
  else {
    atmost = abandoned_count/8;    // at most 1/8th of all outstanding (estimated)
    if (atmost < 8) atmost = 8;    // but at least 8
  }

  // for `atmost` `reclaimed` abandoned segments...
  while(atmost > reclaimed) {
    // try to claim the head of the abandoned segments
    mi_segment_t* segment;
    do {
      segment = (mi_segment_t*)abandoned;
    } while(segment != NULL && !mi_atomic_compare_exchange_ptr((volatile void**)&abandoned, segment->abandoned_next, segment));
    if (segment==NULL) break; // stop early if no more segments available

    // got it.
    mi_atomic_decrement(&abandoned_count);
    segment->thread_id = _mi_thread_id();
    segment->abandoned_next = NULL;
    mi_segments_track_size((long)segment->segment_size,tld);
    mi_assert_internal(segment->next == NULL && segment->prev == NULL);
    mi_assert_expensive(mi_segment_is_valid(segment));
    mi_stat_decrease(tld->stats->segments_abandoned,1);
    // add its free pages to the the current thread
    if (segment->page_kind == MI_PAGE_SMALL && mi_segment_has_free(segment)) {
      mi_segment_enqueue(&tld->small_free, segment);
    }
    // add its abandoned pages to the current thread
    mi_assert(segment->abandoned == segment->used);
    for (size_t i = 0; i < segment->capacity; i++) {
      mi_page_t* page = &segment->pages[i];
      if (page->segment_in_use) {
        segment->abandoned--;
        mi_assert(page->next == NULL);
        mi_stat_decrease( tld->stats->pages_abandoned, 1);
        if (mi_page_all_free(page)) {
          // if everything free by now, free the page
          mi_segment_page_clear(segment,page,tld->stats);
        }
        else {
          // otherwise reclaim it
          _mi_page_reclaim(heap,page);
        }
      }
    }
    mi_assert(segment->abandoned == 0);
    if (segment->used == 0) {  // due to page_clear
      mi_segment_free(segment,false,tld);
    }
    else {
      reclaimed++;
    }
  }
  return (reclaimed>0);
}


/* -----------------------------------------------------------
   Small page allocation
----------------------------------------------------------- */

// Allocate a small page inside a segment.
// Requires that the page has free pages
static mi_page_t* mi_segment_small_page_alloc_in(mi_segment_t* segment, mi_segments_tld_t* tld) {
  mi_assert_internal(mi_segment_has_free(segment));
  mi_page_t* page = mi_segment_find_free(segment);
  page->segment_in_use = true;
  segment->used++;
  mi_assert_internal(segment->used <= segment->capacity);
  if (segment->used == segment->capacity) {
    // if no more free pages, remove from the queue
    mi_assert_internal(!mi_segment_has_free(segment));
    mi_assert_expensive(mi_segment_queue_contains(&tld->small_free, segment));
    mi_segment_queue_remove(&tld->small_free, segment);
  }
  return page;
}

static mi_page_t* mi_segment_small_page_alloc(mi_segments_tld_t* tld, mi_os_tld_t* os_tld) {
  if (mi_segment_queue_is_empty(&tld->small_free)) {
    mi_segment_t* segment = mi_segment_alloc(0,MI_PAGE_SMALL,MI_SMALL_PAGE_SHIFT,tld,os_tld);
    if (segment == NULL) return NULL;
    mi_segment_enqueue(&tld->small_free, segment);
  }
  mi_assert_internal(tld->small_free.first != NULL);
  return mi_segment_small_page_alloc_in(tld->small_free.first,tld);
}


/* -----------------------------------------------------------
   large page allocation
----------------------------------------------------------- */

static mi_page_t* mi_segment_large_page_alloc(mi_segments_tld_t* tld, mi_os_tld_t* os_tld) {
  mi_segment_t* segment = mi_segment_alloc(0,MI_PAGE_LARGE,MI_LARGE_PAGE_SHIFT,tld,os_tld);
  if (segment == NULL) return NULL;
  segment->used = 1;
  mi_page_t* page = &segment->pages[0];
  page->segment_in_use = true;
  return page;
}

static mi_page_t* mi_segment_huge_page_alloc(size_t size, mi_segments_tld_t* tld, mi_os_tld_t* os_tld)
{
  mi_segment_t* segment = mi_segment_alloc(size, MI_PAGE_HUGE, MI_SEGMENT_SHIFT,tld,os_tld);
  if (segment == NULL) return NULL;
  mi_assert_internal(segment->segment_size - segment->segment_info_size >= size);
  segment->used = 1;
  mi_page_t* page = &segment->pages[0];
  page->segment_in_use = true;
  return page;
}

/* -----------------------------------------------------------
   Page allocation and free
----------------------------------------------------------- */

mi_page_t* _mi_segment_page_alloc(size_t block_size, mi_segments_tld_t* tld, mi_os_tld_t* os_tld) {
  mi_page_t* page;
  if (block_size < MI_SMALL_PAGE_SIZE / 8)
    // smaller blocks than 8kb (assuming MI_SMALL_PAGE_SIZE == 64kb)
    page = mi_segment_small_page_alloc(tld,os_tld);
  else if (block_size < (MI_LARGE_SIZE_MAX - sizeof(mi_segment_t)))
    page = mi_segment_large_page_alloc(tld, os_tld);
  else
    page = mi_segment_huge_page_alloc(block_size,tld,os_tld);
  mi_assert_expensive(mi_segment_is_valid(_mi_page_segment(page)));
  return page;
}
