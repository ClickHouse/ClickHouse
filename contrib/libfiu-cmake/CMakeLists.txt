if (NOT ENABLE_LIBFIU)
  message (STATUS "Not using libfiu")
  return ()
endif ()

set(LIBFIU_DIR "${ClickHouse_SOURCE_DIR}/contrib/libfiu/")

set(LIBFIU_SOURCES
    ${LIBFIU_DIR}/libfiu/fiu.c
    ${LIBFIU_DIR}/libfiu/fiu-rc.c
    ${LIBFIU_DIR}/libfiu/backtrace.c
    ${LIBFIU_DIR}/libfiu/wtable.c
)

set(LIBFIU_HEADERS "${LIBFIU_DIR}/libfiu")

add_library(_libfiu ${LIBFIU_SOURCES})
target_compile_definitions(_libfiu PUBLIC DUMMY_BACKTRACE)
target_compile_definitions(_libfiu PUBLIC FIU_ENABLE)
target_include_directories(_libfiu PUBLIC ${LIBFIU_HEADERS})
add_library(ch_contrib::libfiu ALIAS _libfiu)
