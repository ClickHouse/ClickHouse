#pragma once

#if !defined(ARCADIA_BUILD)
static constexpr bool IS_ARCADIA_BUILD = false;
#else
static constexpr bool IS_ARCADIA_BUILD = true;
#endif
