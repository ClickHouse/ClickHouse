#define DWYU_WANT_API 1
#include <exported/a/shared.inc>
#include <exported/b/shared.inc>
#include "outside.inc"
int dwyu_prefix_owner() { return ARES_TEST_VALUE + CPP_TEST_VALUE; }
