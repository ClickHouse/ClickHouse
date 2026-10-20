#ifdef __cplusplus
#include <cpp_api.h>
int guard(void) { return CPP_TEST_VALUE; }
#else
#include <ares.h>
int guard(void) { return ARES_TEST_VALUE; }
#endif
