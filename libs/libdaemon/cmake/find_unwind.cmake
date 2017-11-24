include (CMakePushCheckState)
cmake_push_check_state ()

option (ENABLE_UNWIND "Enable libunwind (better stacktraces)" ON)

if (ENABLE_UNWIND)

if (CMAKE_SYSTEM MATCHES "Linux" AND NOT ARCH_ARM AND NOT ARCH_32)
    option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" ${NOT_UNBUNDLED})
else ()
    option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" OFF)
endif ()

if (NOT USE_INTERNAL_UNWIND_LIBRARY)
    find_library (UNWIND_LIBRARY unwind)
    find_path (UNWIND_INCLUDE_DIR NAMES unwind.h PATHS ${UNWIND_INCLUDE_PATHS})

    include (CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_INCLUDES ${UNWIND_INCLUDE_DIR})
    set(CMAKE_REQUIRED_LIBRARIES ${UNWIND_LIBRARY})
    check_cxx_source_compiles("
    #include <ucontext.h>
    #define UNW_LOCAL_ONLY
    #include <libunwind.h>
    int main () {
       ucontext_t context;
       unw_cursor_t cursor;
       unw_init_local2(&cursor, &context, UNW_INIT_SIGNAL_FRAME);
       return 0;
    }
    " HAVE_UNWIND_INIT_LOCAL_SIGNAL)
    if (NOT HAVE_UNWIND_INIT_LOCAL_SIGNAL)
       set(UNWIND_LIBRARY "")
       set(UNWIND_INCLUDE_DIR "")
    endif ()

endif ()

if (UNWIND_LIBRARY AND UNWIND_INCLUDE_DIR)
    set (USE_UNWIND 1)
elseif (CMAKE_SYSTEM MATCHES "Linux" AND NOT ARCH_ARM AND NOT ARCH_32)
    set (USE_INTERNAL_UNWIND_LIBRARY 1)
    set (UNWIND_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libunwind/include")
    set (UNWIND_LIBRARY unwind)
    set (USE_UNWIND 1)
endif ()

endif ()

message (STATUS "Using unwind=${USE_UNWIND}: ${UNWIND_INCLUDE_DIR} : ${UNWIND_LIBRARY}")

cmake_pop_check_state ()
