#pragma once

#include <base/defines.h>

#if defined(SILK_THREAD_LOCAL_STORAGE_SANITIZER) && !defined(DEBUG_OR_SANITIZER_BUILD)
#error "silk thread local storage sanitizer requires a debug or sanitizer build: otherwise chassert never fires, leaving all of the overhead and none of the checking"
#endif

extern "C" void silk_thread_local_storage_sanitizer_access_hook(void * address, const char * name) noexcept;
extern "C" void silk_thread_local_storage_sanitizer_fiber_init_hook() noexcept;
