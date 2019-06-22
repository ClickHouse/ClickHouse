#include <stdio.h>
#include <assert.h>
#include <mimalloc.h>

void test_heap(void* p_out) {
  mi_heap_t* heap = mi_heap_new();
  void* p1 = mi_heap_malloc(heap,32);
  void* p2 = mi_heap_malloc(heap,48);
  mi_free(p_out);
  mi_heap_destroy(heap);
  //mi_heap_delete(heap); mi_free(p1); mi_free(p2);
}

int main() {
  void* p1 = mi_malloc(16);
  void* p2 = mi_malloc(1000000);
  mi_free(p1);
  mi_free(p2);
  p1 = mi_malloc(16);
  p2 = mi_malloc(16);
  mi_free(p1);
  mi_free(p2);

  test_heap(mi_malloc(32));

  p1 = mi_malloc_aligned(64, 16);
  p2 = mi_malloc_aligned(160,24);
  mi_free(p2);
  mi_free(p1);
  
  mi_collect(true);
  mi_stats_print(NULL);
  return 0;
}

