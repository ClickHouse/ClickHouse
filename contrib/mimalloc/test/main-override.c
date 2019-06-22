#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#include <mimalloc.h>


int main() {
  mi_stats_reset();
  void* p1 = malloc(78);
  void* p2 = malloc(24);
  free(p1);
  p1 = malloc(8);
  char* s = strdup("hello\n");
  free(p2);
  p2 = malloc(16);
  p1 = realloc(p1, 32);
  free(p1);
  free(p2);
  free(s);
  mi_collect(true);
  mi_stats_print(NULL);
  return 0;
}
