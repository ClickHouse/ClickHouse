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

file(GLOB SRCS "${LIBRARY_DIR}/*.c")

add_library(_libpng ${SRCS})

add_dependencies(_libpng copy_pnglibconf_h)

target_link_libraries(_libpng PRIVATE ch_contrib::zlib)


target_include_directories(_libpng SYSTEM BEFORE PUBLIC
    "${LIBRARY_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}" 
)

add_library(ch_contrib::libpng ALIAS _libpng)