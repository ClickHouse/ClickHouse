#if !defined(DWYU_WANT_API)
#error "repeated.inc requires DWYU_WANT_API"
#endif
#if DWYU_WANT_API
#include <ares.h>
#else
#include <cpp_api.h>
#endif
