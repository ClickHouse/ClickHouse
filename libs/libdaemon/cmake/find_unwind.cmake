if (CMAKE_SYSTEM MATCHES "Linux")
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
    #include <libunwind.h>
    int main () {
       ucontext_t context;
       unw_cursor_t cursor;
       unw_init_local_signal(&cursor, &context);
       return 0;
    }
    " HAVE_UNWIND_INIT_LOCAL_SIGNAL)
    if (NOT HAVE_UNWIND_INIT_LOCAL_SIGNAL)
       set(UNWIND_LIBRARY "")
       set(UNWIND_INCLUDE_DIR "")
    endif ()

endif ()

if (UNWIND_LIBRARY AND UNWIND_INCLUDE_DIR)
    #include_directories (${UNWIND_INCLUDE_DIR})
    set (USE_UNWIND 1)
elseif (CMAKE_SYSTEM MATCHES "Linux")
    set (USE_INTERNAL_UNWIND_LIBRARY 1)
    set (UNWIND_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libunwind/include")
    #include_directories (BEFORE ${UNWIND_INCLUDE_DIR})
    set (UNWIND_LIBRARY unwind)
    set (USE_UNWIND 1)
endif ()

message (STATUS "Using unwind=${USE_UNWIND}: ${UNWIND_INCLUDE_DIR} : ${UNWIND_LIBRARY}")
