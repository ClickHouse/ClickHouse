#define DWYU_WANT_API 1
#include "repeated.inc"
#undef DWYU_WANT_API
#define DWYU_WANT_API 0
#include "repeated.inc"
int dwyu_repeated() { return ARES_TEST_VALUE + CPP_TEST_VALUE; }
