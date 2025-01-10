if (NOT ENABLE_FIU)
  message (STATUS "Not using fiu")
  return ()
endif ()

set(FIU_DIR "${ClickHouse_SOURCE_DIR}/contrib/libfiu/")

set(FIU_SOURCES
  ${FIU_DIR}/libfiu/fiu.c
  ${FIU_DIR}/libfiu/fiu-rc.c
  ${FIU_DIR}/libfiu/backtrace.c
  ${FIU_DIR}/libfiu/wtable.c
)

set(FIU_HEADERS "${FIU_DIR}/libfiu")

add_library(_fiu ${FIU_SOURCES})
target_compile_definitions(_fiu PUBLIC DUMMY_BACKTRACE)
target_include_directories(_fiu PUBLIC ${FIU_HEADERS})
add_library(ch_contrib::fiu ALIAS _fiu)
