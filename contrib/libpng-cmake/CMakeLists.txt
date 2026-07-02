option(ENABLE_LIBPNG "Enable libpng" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBPNG)
    message(STATUS "Not using libpng")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpng")
set(SCRIPTS_DIR "${LIBRARY_DIR}/scripts")

set(PNG_LIBCONF_PREBUILT "${SCRIPTS_DIR}/pnglibconf.h.prebuilt")
set(PNG_LIBCONF_OUT "${CMAKE_CURRENT_BINARY_DIR}/pnglibconf.h")

# TODO: Generate pnglibconf.h instead of copying it
# See: https://github.com/pnggroup/libpng/issues/647
add_custom_command(
    OUTPUT ${PNG_LIBCONF_OUT}
    COMMAND ${CMAKE_COMMAND} -E copy ${PNG_LIBCONF_PREBUILT} ${PNG_LIBCONF_OUT}
    COMMENT "Copying prebuilt pnglibconf.h"
    DEPENDS ${PNG_LIBCONF_PREBUILT}
)

add_custom_target(copy_pnglibconf_h ALL DEPENDS ${PNG_LIBCONF_OUT})

# Use the explicit upstream library source list. A `*.c` glob would also pull in
# `example.c` and `pngtest.c`, which are tool/example sources (each defines `main`)
# that upstream keeps in separate targets and must not be linked into the library.
set(SRCS
    "${LIBRARY_DIR}/png.c"
    "${LIBRARY_DIR}/pngerror.c"
    "${LIBRARY_DIR}/pngget.c"
    "${LIBRARY_DIR}/pngmem.c"
    "${LIBRARY_DIR}/pngpread.c"
    "${LIBRARY_DIR}/pngread.c"
    "${LIBRARY_DIR}/pngrio.c"
    "${LIBRARY_DIR}/pngrtran.c"
    "${LIBRARY_DIR}/pngrutil.c"
    "${LIBRARY_DIR}/pngset.c"
    "${LIBRARY_DIR}/pngtrans.c"
    "${LIBRARY_DIR}/pngwio.c"
    "${LIBRARY_DIR}/pngwrite.c"
    "${LIBRARY_DIR}/pngwtran.c"
    "${LIBRARY_DIR}/pngwutil.c"
)

add_library(_libpng ${SRCS})

add_dependencies(_libpng copy_pnglibconf_h)

target_link_libraries(_libpng PRIVATE ch_contrib::zlib)

target_include_directories(_libpng SYSTEM BEFORE PUBLIC
    "${LIBRARY_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}"
)

add_library(ch_contrib::libpng ALIAS _libpng)
