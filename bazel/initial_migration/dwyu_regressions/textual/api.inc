#if !defined(DWYU_WANT_API)
#error "api.inc requires its includer to define DWYU_WANT_API"
#endif
#if DWYU_WANT_API
#include <ares.h>
#endif
