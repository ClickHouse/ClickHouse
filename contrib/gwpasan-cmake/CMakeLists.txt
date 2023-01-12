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
  #PARENT_SCOPE
)

#set(GWP_ASAN_LIBS RTGwpAsan RTGwpAsanOptionsParser RTGwpAsanBacktraceLibc       RTGwpAsanSegvHandler)

set(GWP_ASAN_HEADERS "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/lib")

#set(GWP_ASAN_HEADERS_FILES
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/common.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/crash_handler.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/definitions.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/guarded_pool_allocator.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/mutex.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/options.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/options.inc
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/guarded_pool_allocator_fuchsia.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/guarded_pool_allocator_posix.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/guarded_pool_allocator_tls.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/mutex_fuchsia.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/platform_specific/mutex_posix.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/stack_trace_compressor.h
#  ${COMPILER_RT_GWP_ASAN_SRC_DIR}/utilities.h
#  PARENT_SCOPE
#)

add_library(_gwp_asan ${GWP_ASAN_SOURCES})
#target_link_libraries (_gwp_asan INTERFACE ${GWP_ASAN_SOURCES})
target_include_directories (_gwp_asan SYSTEM PUBLIC ${GWP_ASAN_HEADERS})
add_library(ch_contrib::gwp_asan ALIAS _gwp_asan)
