if (NOT ENABLE_GWP_ASAN)
  message (STATUS "Not using gwp-asan")
  return ()
endif ()

set(COMPILER_RT_GWP_ASAN_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/lib/gwp_asan")

set(GWP_ASAN_SOURCES
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/common.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/crash_handler.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/common_posix.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/guarded_pool_allocator_posix.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/mutex_posix.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/utilities_posix.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/guarded_pool_allocator.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/stack_trace_compressor.cpp
  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/optional/options_parser.cpp
)

set(GWP_ASAN_HEADERS "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/lib")

add_library(_gwp_asan ${GWP_ASAN_SOURCES})
target_include_directories (_gwp_asan SYSTEM PUBLIC ${GWP_ASAN_HEADERS})
add_library(ch_contrib::gwp_asan ALIAS _gwp_asan)
