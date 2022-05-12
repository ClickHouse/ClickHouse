set (SRCS
    ../gperftools/src/gperftools/heap-checker.h
    ../gperftools/src/gperftools/heap-profiler.h
    ../gperftools/src/gperftools/malloc_extension.h
    ../gperftools/src/gperftools/malloc_extension_c.h
    ../gperftools/src/gperftools/malloc_hook.h
    ../gperftools/src/gperftools/malloc_hook_c.h
    ../gperftools/src/gperftools/nallocx.h
    ../gperftools/src/gperftools/profiler.h
    ../gperftools/src/gperftools/stacktrace.h
)

add_library(_gperftools ${SRCS})
target_include_directories(_gperftools SYSTEM PUBLIC ../gperftools/src)
add_library(ch_contrib::gperftools ALIAS _gperftools)
