#include <api/detail.h>
_Static_assert(DWYU_RESOLUTION_DETAIL_ANGLE == 101, "angle lookup must reach angled_detail");
#ifdef DWYU_RESOLUTION_DETAIL_QUOTE
#error "angle lookup incorrectly searched -iquote"
#endif
#include "api/detail.h"
_Static_assert(DWYU_RESOLUTION_DETAIL_QUOTE == 103, "quote lookup must reach quoted_detail");
#include <api/detail.h>
#include "api/detail.h"
