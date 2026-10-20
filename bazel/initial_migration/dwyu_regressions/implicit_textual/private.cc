#define DWYU_ALIAS_FUNCTION(type) int dwyu_alias_private(type value) { return value.value; }
#include "private.inc"
#undef DWYU_ALIAS_FUNCTION
