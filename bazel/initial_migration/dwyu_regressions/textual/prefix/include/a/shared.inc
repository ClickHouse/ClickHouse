#if !defined(DWYU_WANT_API)
#error "prefix a/shared.inc requires DWYU_WANT_API"
#endif
#if DWYU_WANT_API
#include <ares.h>
#endif
