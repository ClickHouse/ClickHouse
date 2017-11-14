#pragma once

// https://github.com/yandex/ClickHouse/issues/1488

#if defined(__apple_build_version__) && __apple_build_version__ <= 9000038
#define THREAD_LOCAL __thread
#else
#define THREAD_LOCAL thread_local
#endif
