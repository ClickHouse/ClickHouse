#include <stdio.h>
#include <assert.h>
#include <rcmalloc.h>

int main() {
  void* p1 = rc_malloc(16);
  void* p2 = rc_malloc(16);
  rc_free(p1);
  rc_free(p2);
  p1 = rc_malloc(16);
  p2 = rc_malloc(16);
  rc_free(p1);
  rc_free(p2);
  rc_collect(true);
  rc_stats_print();
  return 0;
}
