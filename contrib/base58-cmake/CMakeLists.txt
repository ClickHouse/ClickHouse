set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/base58")

set (SRCS
        ${LIBRARY_DIR}/base_x.hh
        ${LIBRARY_DIR}/uinteger_t.hh
        )

add_library(_base58 ${SRCS})

target_include_directories(_base58 SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

if (XCODE OR XCODE_VERSION)
    # https://gitlab.kitware.com/cmake/cmake/issues/17457
    #     Some native build systems may not like targets that have only object files, so consider adding at least one real source file
    # This applies to Xcode.
    if (NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/dummy.c")
        file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/dummy.c" "")
    endif ()
    target_sources(_base58 PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/dummy.c")
endif ()

add_library(ch_contrib::base58 ALIAS _base58)