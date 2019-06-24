/*----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/

/* -----------------------------------------------------------
  The core of the allocator. Every segment contains
  pages of a certain block size. The main function
  exported is `mi_malloc_generic`.
----------------------------------------------------------- */

#include "mimalloc.h"
#include "mimalloc-internal.h"
#include "mimalloc-atomic.h"

#include <string.h>  // memset, memcpy

/* -----------------------------------------------------------
  Definition of page queues for each block size
----------------------------------------------------------- */

#define MI_IN_PAGE_C
#include "page-queue.c"
#undef MI_IN_PAGE_C


/* -----------------------------------------------------------
  Page helpers
----------------------------------------------------------- */

// Index a block in a page
static inline mi_block_t* mi_page_block_at(const mi_page_t* page, void* page_start, size_t i) {
  mi_assert_internal(page != NULL);
  mi_assert_internal(i <= page->reserved);
  return (mi_block_t*)((uint8_t*)page_start + (i * page->block_size));
}

static void mi_page_init(mi_heap_t* heap, mi_page_t* page, size_t size, mi_stats_t* stats);


#if (MI_DEBUG>1)
static size_t mi_page_list_count(mi_page_t* page, mi_block_t* head) {
  size_t count = 0;
  while (head != NULL) {
    mi_assert_internal(page == _mi_ptr_page(head));
    count++;
    head = mi_block_next(page, head);
  }
  return count;
}

// Start of the page available memory
static inline uint8_t* mi_page_area(const mi_page_t* page) {
  return _mi_page_start(_mi_page_segment(page), page, NULL);
}


static bool mi_page_list_is_valid(mi_page_t* page, mi_block_t* p) {
  size_t psize;
  uint8_t* page_area = _mi_page_start(_mi_page_segment(page), page, &psize);
  mi_block_t* start = (mi_block_t*)page_area;
  mi_block_t* end   = (mi_block_t*)(page_area + psize);
  while(p != NULL) {
    if (p < start || p >= end) return false;
    p = mi_block_next(page, p);
  }
  return true;
}

static bool mi_page_is_valid_init(mi_page_t* page) {
  mi_assert_internal(page->block_size > 0);
  mi_assert_internal(page->used <= page->capacity);
  mi_assert_internal(page->capacity <= page->reserved);
  
  mi_segment_t* segment = _mi_page_segment(page);
  uint8_t* start = _mi_page_start(segment,page,NULL);
  mi_assert_internal(start == _mi_segment_page_start(segment,page,NULL));
  //mi_assert_internal(start + page->capacity*page->block_size == page->top);

  mi_assert_internal(mi_page_list_is_valid(page,page->free));
  mi_assert_internal(mi_page_list_is_valid(page,page->local_free));

  mi_block_t* tfree = (mi_block_t*)((uintptr_t)page->thread_free.head << MI_TF_PTR_SHIFT);
  mi_assert_internal(mi_page_list_is_valid(page, tfree));
  size_t tfree_count = mi_page_list_count(page, tfree);
  mi_assert_internal(tfree_count <= page->thread_freed + 1);

  size_t free_count = mi_page_list_count(page, page->free) + mi_page_list_count(page, page->local_free);
  mi_assert_internal(page->used + free_count == page->capacity);

  return true;
}

bool _mi_page_is_valid(mi_page_t* page) {
  mi_assert_internal(mi_page_is_valid_init(page));
  mi_assert_internal(page->cookie != 0);
  if (page->heap!=NULL) {
    mi_segment_t* segment = _mi_page_segment(page);
    mi_assert_internal(segment->thread_id == page->heap->thread_id);
    mi_page_queue_t* pq = mi_page_queue_of(page);
    mi_assert_internal(mi_page_queue_contains(pq, page));
    mi_assert_internal(pq->block_size==page->block_size || page->block_size > MI_LARGE_SIZE_MAX || page->flags.in_full);
    mi_assert_internal(mi_heap_contains_queue(page->heap,pq));
  }
  return true;
}
#endif


void _mi_page_use_delayed_free(mi_page_t* page, bool enable) {
  mi_thread_free_t tfree;
  mi_thread_free_t tfreex;

  do {
    tfreex = tfree = page->thread_free;
    tfreex.delayed = (enable ? MI_USE_DELAYED_FREE : MI_NO_DELAYED_FREE);
    if (mi_unlikely(tfree.delayed == MI_DELAYED_FREEING)) {
      mi_atomic_yield(); // delay until outstanding MI_DELAYED_FREEING are done.
      continue;          // and try again
    }
  }
  while(tfreex.delayed != tfree.delayed && // avoid atomic operation if already equal
        !mi_atomic_compare_exchange((volatile uintptr_t*)&page->thread_free, tfreex.value, tfree.value));
}


/* -----------------------------------------------------------
  Page collect the `local_free` and `thread_free` lists
----------------------------------------------------------- */

// Collect the local `thread_free` list using an atomic exchange.
// Note: The exchange must be done atomically as this is used right after
// moving to the full list in `mi_page_collect_ex` and we need to
// ensure that there was no race where the page became unfull just before the move.
static void mi_page_thread_free_collect(mi_page_t* page)
{
  mi_block_t* head;
  mi_thread_free_t tfree;
  mi_thread_free_t tfreex;
  do {
    tfreex = tfree = page->thread_free;
    head = (mi_block_t*)((uintptr_t)tfree.head << MI_TF_PTR_SHIFT);
    tfreex.head = 0;
  } while (!mi_atomic_compare_exchange((volatile uintptr_t*)&page->thread_free, tfreex.value, tfree.value));

  // return if the list is empty
  if (head == NULL) return;

  // find the tail
  uint16_t count = 1;
  mi_block_t* tail = head;
  mi_block_t* next;
  while ((next = mi_block_next(page,tail)) != NULL) {
    count++;
    tail = next;
  }

  // and prepend to the free list
  mi_block_set_next(page,tail, page->free);
  page->free = head;

  // update counts now
  mi_atomic_subtract(&page->thread_freed, count);
  page->used -= count;
}

void _mi_page_free_collect(mi_page_t* page) {
  mi_assert_internal(page!=NULL);
  //if (page->free != NULL) return; // avoid expensive append

  // free the local free list
  if (page->local_free != NULL) {
    if (mi_likely(page->free == NULL)) {  
      // usual caes
      page->free = page->local_free;
    }
    else {
      mi_block_t* tail = page->free;
      mi_block_t* next;
      while ((next = mi_block_next(page, tail)) != NULL) { 
        tail = next;  
      }
      mi_block_set_next(page, tail, page->local_free);
    }
    page->local_free = NULL;   
  }
  // and the thread free list
  if (page->thread_free.head != 0) {  // quick test to avoid an atomic operation
    mi_page_thread_free_collect(page);
  }
}



/* -----------------------------------------------------------
  Page fresh and retire
----------------------------------------------------------- */

// called from segments when reclaiming abandoned pages
void _mi_page_reclaim(mi_heap_t* heap, mi_page_t* page) {
  mi_assert_expensive(mi_page_is_valid_init(page));
  mi_assert_internal(page->heap == NULL);
  _mi_page_free_collect(page);
  mi_page_queue_t* pq = mi_page_queue(heap, page->block_size);
  mi_page_queue_push(heap, pq, page);
  mi_assert_expensive(_mi_page_is_valid(page));
}

// allocate a fresh page from a segment
static mi_page_t* mi_page_fresh_alloc(mi_heap_t* heap, mi_page_queue_t* pq, size_t block_size) {
  mi_assert_internal(mi_heap_contains_queue(heap, pq));
  mi_page_t* page = _mi_segment_page_alloc(block_size, &heap->tld->segments, &heap->tld->os);
  if (page == NULL) return NULL;
  mi_page_init(heap, page, block_size, &heap->tld->stats);
  mi_heap_stat_increase( heap, pages, 1);
  mi_page_queue_push(heap, pq, page);
  mi_assert_expensive(_mi_page_is_valid(page));
  return page;
}

// Get a fresh page to use
static mi_page_t* mi_page_fresh(mi_heap_t* heap, mi_page_queue_t* pq) {
  mi_assert_internal(mi_heap_contains_queue(heap, pq));

  // try to reclaim an abandoned page first
  mi_page_t* page = pq->first;
  if (!heap->no_reclaim &&
      _mi_segment_try_reclaim_abandoned(heap, false, &heap->tld->segments) &&
      page != pq->first)
  {
    // we reclaimed, and we got lucky with a reclaimed page in our queue
    page = pq->first;
    if (page->free != NULL) return page;
  }
  // otherwise allocate the page
  page = mi_page_fresh_alloc(heap, pq, pq->block_size);
  if (page==NULL) return NULL;
  mi_assert_internal(pq->block_size==page->block_size);
  mi_assert_internal(pq==mi_page_queue(heap,page->block_size));
  return page;
}

/* -----------------------------------------------------------
   Do any delayed frees
   (put there by other threads if they deallocated in a full page)
----------------------------------------------------------- */
void _mi_heap_delayed_free(mi_heap_t* heap) {
  // take over the list
  mi_block_t* block;
  do {
    block = (mi_block_t*)heap->thread_delayed_free;
  } while (block != NULL && !mi_atomic_compare_exchange_ptr((volatile void**)&heap->thread_delayed_free, NULL, block));

  // and free them all
  while(block != NULL) {
    mi_block_t* next = mi_block_nextx(heap->cookie,block);
    // use internal free instead of regular one to keep stats etc correct
    _mi_free_delayed_block(block);
    block = next;
  }
}

/* -----------------------------------------------------------
  Unfull, abandon, free and retire
----------------------------------------------------------- */

// Move a page from the full list back to a regular list
void _mi_page_unfull(mi_page_t* page) {
  mi_assert_internal(page != NULL);
  mi_assert_expensive(_mi_page_is_valid(page));
  mi_assert_internal(page->flags.in_full);

  _mi_page_use_delayed_free(page, false);
  if (!page->flags.in_full) return;

  mi_heap_t* heap = page->heap;
  mi_page_queue_t* pqfull = &heap->pages[MI_BIN_FULL];
  page->flags.in_full = false; // to get the right queue
  mi_page_queue_t* pq = mi_heap_page_queue_of(heap, page);
  page->flags.in_full = true;
  mi_page_queue_enqueue_from(pq, pqfull, page);
}

static void mi_page_to_full(mi_page_t* page, mi_page_queue_t* pq) {
  mi_assert_internal(pq == mi_page_queue_of(page));
  mi_assert_internal(!mi_page_immediate_available(page));
  mi_assert_internal(!page->flags.in_full);

  _mi_page_use_delayed_free(page, true);
  if (page->flags.in_full) return;

  mi_page_queue_enqueue_from(&page->heap->pages[MI_BIN_FULL], pq, page);
  mi_page_thread_free_collect(page);  // try to collect right away in case another thread freed just before MI_USE_DELAYED_FREE was set
}


// Abandon a page with used blocks at the end of a thread.
// Note: only call if it is ensured that no references exist from
// the `page->heap->thread_delayed_free` into this page.
// Currently only called through `mi_heap_collect_ex` which ensures this.
void _mi_page_abandon(mi_page_t* page, mi_page_queue_t* pq) {
  mi_assert_internal(page != NULL);
  mi_assert_expensive(_mi_page_is_valid(page));
  mi_assert_internal(pq == mi_page_queue_of(page));
  mi_assert_internal(page->heap != NULL);
  mi_assert_internal(page->thread_free.delayed == MI_NO_DELAYED_FREE);

#if MI_DEBUG>1
  // check there are no references left..
  for (mi_block_t* block = (mi_block_t*)page->heap->thread_delayed_free; block != NULL; block = mi_block_nextx(page->heap->cookie,block)) {
    mi_assert_internal(_mi_ptr_page(block) != page);
  }
#endif

  // and then remove from our page list
  mi_segments_tld_t* segments_tld = &page->heap->tld->segments;
  mi_page_queue_remove(pq, page);

  // and abandon it
  mi_assert_internal(page->heap == NULL);
  _mi_segment_page_abandon(page,segments_tld);
}


// Free a page with no more free blocks
void _mi_page_free(mi_page_t* page, mi_page_queue_t* pq, bool force) {
  mi_assert_internal(page != NULL);
  mi_assert_expensive(_mi_page_is_valid(page));
  mi_assert_internal(pq == mi_page_queue_of(page));
  mi_assert_internal(mi_page_all_free(page));
  mi_assert_internal(page->thread_free.delayed != MI_DELAYED_FREEING);

  page->flags.has_aligned = false;

  // account for huge pages here
  if (page->block_size > MI_LARGE_SIZE_MAX) {
    mi_heap_stat_decrease(page->heap, huge, page->block_size);
  }

  // remove from the page list
  // (no need to do _mi_heap_delayed_free first as all blocks are already free)
  mi_segments_tld_t* segments_tld = &page->heap->tld->segments;
  mi_page_queue_remove(pq, page);

  // and free it
  mi_assert_internal(page->heap == NULL);
  _mi_segment_page_free(page, force, segments_tld);
}

// Retire a page with no more used blocks
// Important to not retire too quickly though as new
// allocations might coming.
// Note: called from `mi_free` and benchmarks often
// trigger this due to freeing everything and then
// allocating again so careful when changing this.
void _mi_page_retire(mi_page_t* page) {
  mi_assert_internal(page != NULL);
  mi_assert_expensive(_mi_page_is_valid(page));
  mi_assert_internal(mi_page_all_free(page));

  page->flags.has_aligned = false;

  // don't retire too often..
  // (or we end up retiring and re-allocating most of the time)
  // NOTE: refine this more: we should not retire if this
  // is the only page left with free blocks. It is not clear
  // how to check this efficiently though... for now we just check
  // if its neighbours are almost fully used.
  if (mi_likely(page->block_size <= MI_LARGE_SIZE_MAX)) {
    if (mi_page_mostly_used(page->prev) && mi_page_mostly_used(page->next)) {
      return; // dont't retire after all
    }
  }

  _mi_page_free(page, mi_page_queue_of(page), false);
}


/* -----------------------------------------------------------
  Initialize the initial free list in a page.
  In secure mode we initialize a randomized list by 
  alternating between slices.
----------------------------------------------------------- */

#define MI_MAX_SLICE_SHIFT  (6)   // at most 64 slices
#define MI_MAX_SLICES       (1UL << MI_MAX_SLICE_SHIFT)
#define MI_MIN_SLICES       (2)

static void mi_page_free_list_extend( mi_heap_t* heap, mi_page_t* page, size_t extend, mi_stats_t* stats)
{
  UNUSED(stats);
  void* page_area = _mi_page_start(_mi_page_segment(page), page, NULL );
  size_t bsize = page->block_size;
  mi_block_t* start = mi_page_block_at(page, page_area, page->capacity);  
  if (extend < MI_MIN_SLICES || !mi_option_is_enabled(mi_option_secure)) {
    // initialize a sequential free list
    mi_block_t* end = mi_page_block_at(page, page_area, page->capacity + extend - 1);
    mi_block_t* block = start;
    for (size_t i = 0; i < extend; i++) {
      mi_block_t* next = (mi_block_t*)((uint8_t*)block + bsize);
      mi_block_set_next(page,block,next);
      block = next;
    }
    mi_block_set_next(page, end, NULL);
    page->free = start;
  }
  else {
    // initialize a randomized free list
    // set up `slice_count` slices to alternate between
    size_t shift  = MI_MAX_SLICE_SHIFT;
    while ((extend >> shift) == 0) {
      shift--; 
    }
    size_t slice_count = (size_t)1U << shift;
    size_t slice_extend = extend / slice_count;
    mi_assert_internal(slice_extend >= 1);
    mi_block_t* blocks[MI_MAX_SLICES];   // current start of the slice
    size_t      counts[MI_MAX_SLICES];   // available objects in the slice
    for (size_t i = 0; i < slice_count; i++) {
      blocks[i] = mi_page_block_at(page, page_area, page->capacity + i*slice_extend);      
      counts[i] = slice_extend;
    }
    counts[slice_count-1] += (extend % slice_count);  // final slice holds the modulus too (todo: distribute evenly?)

    // and initialize the free list by randomly threading through them    
    // set up first element
    size_t current = _mi_heap_random(heap) % slice_count;
    counts[current]--;
    page->free = blocks[current];
    // and iterate through the rest
    uintptr_t rnd = heap->random;
    for (size_t i = 1; i < extend; i++) {
      // call random_shuffle only every INTPTR_SIZE rounds
      size_t round = i%MI_INTPTR_SIZE;
      if (round == 0) rnd = _mi_random_shuffle(rnd);
      // select a random next slice index
      size_t next = ((rnd >> 8*round) & (slice_count-1));  
      while (counts[next]==0) {                            // ensure it still has space
        next++;
        if (next==slice_count) next = 0;
      }
      // and link the current block to it
      counts[next]--; 
      mi_block_t* block = blocks[current];
      blocks[current] = (mi_block_t*)((uint8_t*)block + bsize);  // bump to the following block
      mi_block_set_next(page, block, blocks[next]);   // and set next; note: we may have `current == next` 
      current = next;
    }
    mi_block_set_next( page, blocks[current], NULL);             // end of the list
    heap->random = _mi_random_shuffle(rnd);
  }
  // enable the new free list
  page->capacity += (uint16_t)extend;
  mi_stat_increase(stats->committed, extend * page->block_size);
}

/* -----------------------------------------------------------
  Page initialize and extend the capacity
----------------------------------------------------------- */

#define MI_MAX_EXTEND_SIZE    (4*1024)      // heuristic, one OS page seems to work well.
#if MI_SECURE
#define MI_MIN_EXTEND         (8*MI_SECURE) // extend at least by this many 
#else
#define MI_MIN_EXTEND         (1)
#endif

// Extend the capacity (up to reserved) by initializing a free list
// We do at most `MI_MAX_EXTEND` to avoid touching too much memory
// Note: we also experimented with "bump" allocation on the first
// allocations but this did not speed up any benchmark (due to an
// extra test in malloc? or cache effects?)
static void mi_page_extend_free(mi_heap_t* heap, mi_page_t* page, mi_stats_t* stats) {
  UNUSED(stats);
  mi_assert(page->free == NULL);
  mi_assert(page->local_free == NULL);
  mi_assert_expensive(mi_page_is_valid_init(page));
  if (page->free != NULL) return;
  if (page->capacity >= page->reserved) return;

  size_t page_size;
  _mi_page_start(_mi_page_segment(page), page, &page_size);
  if (page->is_reset) {
    page->is_reset = false;
    mi_stat_decrease( stats->reset, page_size);
  }

  mi_stat_increase( stats->pages_extended, 1);

  // calculate the extend count
  size_t extend = page->reserved - page->capacity;  
  size_t max_extend = MI_MAX_EXTEND_SIZE/page->block_size;
  if (max_extend < MI_MIN_EXTEND) max_extend = MI_MIN_EXTEND;
  
  if (extend > max_extend) {
    // ensure we don't touch memory beyond the page to reduce page commit.
    // the `lean` benchmark tests this. Going from 1 to 8 increases rss by 50%.
    extend = (max_extend==0 ? 1 : max_extend);
  }

  mi_assert_internal(extend > 0 && extend + page->capacity <= page->reserved);
  mi_assert_internal(extend < (1UL<<16));

  // and append the extend the free list
  mi_page_free_list_extend(heap, page, extend, stats );

  mi_assert_expensive(mi_page_is_valid_init(page));
}

// Initialize a fresh page
static void mi_page_init(mi_heap_t* heap, mi_page_t* page, size_t block_size, mi_stats_t* stats) {
  mi_assert(page != NULL);
  mi_segment_t* segment = _mi_page_segment(page);
  mi_assert(segment != NULL);
  // set fields
  size_t page_size;
  _mi_segment_page_start(segment, page, &page_size);
  page->block_size = block_size;
  mi_assert_internal(block_size>0);
  mi_assert_internal(page_size / block_size < (1L<<16));
  page->reserved = (uint16_t)(page_size / block_size);
  page->cookie = _mi_heap_random(heap) | 1;  

  mi_assert_internal(page->capacity == 0);
  mi_assert_internal(page->free == NULL);
  mi_assert_internal(page->used == 0);
  mi_assert_internal(page->thread_free.value == 0);
  mi_assert_internal(page->thread_freed == 0);
  mi_assert_internal(page->next == NULL);
  mi_assert_internal(page->prev == NULL);
  mi_assert_internal(page->flags.has_aligned == false);
  mi_assert_internal(page->cookie != 0);
  mi_assert_expensive(mi_page_is_valid_init(page));

  // initialize an initial free list
  mi_page_extend_free(heap,page,stats);
  mi_assert(mi_page_immediate_available(page));
}

/* -----------------------------------------------------------
  Find pages with free blocks
-------------------------------------------------------------*/

// Find a page with free blocks of `page->block_size`.
static mi_page_t* mi_page_queue_find_free_ex(mi_heap_t* heap, mi_page_queue_t* pq)
{
  // search through the pages in "next fit" order
  mi_page_t* rpage = NULL;
  size_t count = 0;
  size_t page_free_count = 0;
  mi_page_t* page = pq->first;
  while( page != NULL)
  {
    mi_page_t* next = page->next; // remember next
    count++;

    // 0. collect freed blocks by us and other threads
    _mi_page_free_collect(page);

    // 1. if the page contains free blocks, we are done
    if (mi_page_immediate_available(page)) {
      // If all blocks are free, we might retire this page instead.
      // do this at most 8 times to bound allocation time.
      // (note: this can happen if a page was earlier not retired due
      //  to having neighbours that were mostly full or due to concurrent frees)
      if (page_free_count < 8 && mi_page_all_free(page)) {
        page_free_count++;
        if (rpage != NULL) _mi_page_free(rpage,pq,false);
        rpage = page;
        page = next;
        continue;     // and keep looking
      }
      else {
        break;  // pick this one
      }
    }

    // 2. Try to extend
    if (page->capacity < page->reserved) {
      mi_page_extend_free(heap, page, &heap->tld->stats);
      mi_assert_internal(mi_page_immediate_available(page));
      break;
    }

    // 3. If the page is completely full, move it to the `mi_pages_full`
    // queue so we don't visit long-lived pages too often.
    mi_assert_internal(!page->flags.in_full && !mi_page_immediate_available(page));
    mi_page_to_full(page,pq);

    page = next;
  } // for each page

  mi_stat_counter_increase(heap->tld->stats.searches,count);

  if (page == NULL) {
    page = rpage;
    rpage = NULL;
  }
  if (rpage != NULL) {
    _mi_page_free(rpage,pq,false);
  }

  if (page == NULL) {
    page = mi_page_fresh(heap, pq);
  }
  else {
    mi_assert(pq->first == page);
  }
  mi_assert_internal(mi_page_immediate_available(page));
  return page;
}


// Find a page with free blocks of `size`.
static inline mi_page_t* mi_find_free_page(mi_heap_t* heap, size_t size) {
  _mi_heap_delayed_free(heap);
  mi_page_queue_t* pq = mi_page_queue(heap,size);
  mi_page_t* page = pq->first;
  if (page != NULL) {
    if (mi_option_get(mi_option_secure) >= 3 && page->capacity < page->reserved && ((_mi_heap_random(heap) & 1) == 1)) {
      // in secure mode, we extend half the time to increase randomness
      mi_page_extend_free(heap, page, &heap->tld->stats);
      mi_assert_internal(mi_page_immediate_available(page));
    }
    else {
      _mi_page_free_collect(page);
    }
    if (mi_page_immediate_available(page)) {
      return page; // fast path
    }
  }
  return mi_page_queue_find_free_ex(heap, pq);
}


/* -----------------------------------------------------------
  Users can register a deferred free function called
  when the `free` list is empty. Since the `local_free`
  is separate this is deterministically called after
  a certain number of allocations.
----------------------------------------------------------- */

static mi_deferred_free_fun* deferred_free = NULL;

void _mi_deferred_free(mi_heap_t* heap, bool force) {
  heap->tld->heartbeat++;
  if (deferred_free != NULL) {
    deferred_free(force, heap->tld->heartbeat);
  }
}

void mi_register_deferred_free(mi_deferred_free_fun* fn) mi_attr_noexcept {
  deferred_free = fn;
}


/* -----------------------------------------------------------
  General allocation
----------------------------------------------------------- */

// A huge page is allocated directly without being in a queue
static mi_page_t* mi_huge_page_alloc(mi_heap_t* heap, size_t size) {
  size_t block_size = _mi_wsize_from_size(size) * sizeof(uintptr_t);
  mi_assert_internal(_mi_bin(block_size) == MI_BIN_HUGE);
  mi_page_queue_t* pq = mi_page_queue(heap,block_size);
  mi_assert_internal(mi_page_queue_is_huge(pq));
  mi_page_t* page = mi_page_fresh_alloc(heap,pq,block_size);
  if (page != NULL) {
    mi_assert_internal(mi_page_immediate_available(page));
    mi_assert_internal(page->block_size == block_size);
    mi_heap_stat_increase( heap, huge, block_size);
  }
  return page;
}


// Generic allocation routine if the fast path (`alloc.c:mi_page_malloc`) does not succeed.
void* _mi_malloc_generic(mi_heap_t* heap, size_t size) mi_attr_noexcept
{
  mi_assert_internal(heap != NULL);

  // initialize if necessary
  if (mi_unlikely(!mi_heap_is_initialized(heap))) {
    mi_thread_init(); // calls `_mi_heap_init` in turn
    heap = mi_get_default_heap();
  }
  mi_assert_internal(mi_heap_is_initialized(heap));

  // call potential deferred free routines
  _mi_deferred_free(heap, false);

  // huge allocation?
  mi_page_t* page;
  if (mi_unlikely(size > MI_LARGE_SIZE_MAX)) {
    page = mi_huge_page_alloc(heap,size);
  }
  else {
    // otherwise find a page with free blocks in our size segregated queues
    page = mi_find_free_page(heap,size);
  }
  if (page == NULL) return NULL; // out of memory

  mi_assert_internal(mi_page_immediate_available(page));
  mi_assert_internal(page->block_size >= size);

  // and try again, this time succeeding! (i.e. this should never recurse)
  return _mi_page_malloc(heap, page, size);
}
