set(ABSL_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp")
set(ABSL_COMMON_INCLUDE_DIRS "${ABSL_ROOT_DIR}")

# To avoid errors "'X' does not refer to a value" while using `offsetof` function.
set(CMAKE_CXX_STANDARD 17)

# This is a minimized version of the function definition in CMake/AbseilHelpers.cmake

#
# Copyright 2017 The Abseil Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
function(absl_cc_library)
  cmake_parse_arguments(ABSL_CC_LIB
    "DISABLE_INSTALL;PUBLIC;TESTONLY"
    "NAME"
    "HDRS;SRCS;COPTS;DEFINES;LINKOPTS;DEPS"
    ${ARGN}
  )

  set(_NAME "absl_${ABSL_CC_LIB_NAME}")

  # Check if this is a header-only library
  set(ABSL_CC_SRCS "${ABSL_CC_LIB_SRCS}")
  foreach(src_file IN LISTS ABSL_CC_SRCS)
    if(${src_file} MATCHES ".*\\.(h|inc)")
      list(REMOVE_ITEM ABSL_CC_SRCS "${src_file}")
    endif()
  endforeach()

  if(ABSL_CC_SRCS STREQUAL "")
    set(ABSL_CC_LIB_IS_INTERFACE 1)
  else()
    set(ABSL_CC_LIB_IS_INTERFACE 0)
  endif()

  if(NOT ABSL_CC_LIB_IS_INTERFACE)
      add_library(${_NAME} "")
      target_sources(${_NAME} PRIVATE ${ABSL_CC_LIB_SRCS} ${ABSL_CC_LIB_HDRS})
      target_link_libraries(${_NAME}
      PUBLIC ${ABSL_CC_LIB_DEPS}
      PRIVATE
        ${ABSL_CC_LIB_LINKOPTS}
        ${ABSL_DEFAULT_LINKOPTS}
      )

    target_include_directories(${_NAME}
      SYSTEM PUBLIC "${ABSL_COMMON_INCLUDE_DIRS}")
    target_compile_options(${_NAME}
      PRIVATE ${ABSL_CC_LIB_COPTS})
    target_compile_definitions(${_NAME} PUBLIC ${ABSL_CC_LIB_DEFINES})

  else()
    # Generating header-only library
    add_library(${_NAME} INTERFACE)
    target_include_directories(${_NAME}
      SYSTEM INTERFACE "${ABSL_COMMON_INCLUDE_DIRS}")

    target_link_libraries(${_NAME}
      INTERFACE
        ${ABSL_CC_LIB_DEPS}
        ${ABSL_CC_LIB_LINKOPTS}
        ${ABSL_DEFAULT_LINKOPTS}
    )
    target_compile_definitions(${_NAME} INTERFACE ${ABSL_CC_LIB_DEFINES})

  endif()

  add_library(absl::${ABSL_CC_LIB_NAME} ALIAS ${_NAME})
endfunction()

# The following definitions are an amalgamation of the CMakeLists.txt files in absl/*/
# To refresh them when upgrading to a new version:
# - copy them over from upstream
# - remove calls of 'absl_cc_test'
# - remove calls of `absl_cc_library` that contain `TESTONLY`
# - append '${DIR}' to the file definitions

set(DIR ${ABSL_ROOT_DIR}/absl/algorithm)

absl_cc_library(
  NAME
    algorithm
  HDRS
    "${DIR}/algorithm.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

absl_cc_library(
  NAME
    algorithm_container
  HDRS
    "${DIR}/container.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::algorithm
    absl::core_headers
    absl::meta
    absl::nullability
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/base)

absl_cc_library(
  NAME
    atomic_hook
  HDRS
    "${DIR}/internal/atomic_hook.h"
  DEPS
    absl::config
    absl::core_headers
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    errno_saver
  HDRS
    "${DIR}/internal/errno_saver.h"
  DEPS
    absl::config
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

absl_cc_library(
  NAME
    log_severity
  HDRS
    "${DIR}/log_severity.h"
  SRCS
    "${DIR}/log_severity.cc"
  DEPS
    absl::config
    absl::core_headers
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

absl_cc_library(
  NAME
    no_destructor
  HDRS
    "${DIR}/no_destructor.h"
  DEPS
    absl::config
    absl::nullability
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

absl_cc_library(
  NAME
    nullability
  HDRS
    "${DIR}/nullability.h"
  SRCS
    "${DIR}/internal/nullability_impl.h"
  DEPS
    absl::core_headers
    absl::type_traits
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    raw_logging_internal
  HDRS
    "${DIR}/internal/raw_logging.h"
  SRCS
    "${DIR}/internal/raw_logging.cc"
  DEPS
    absl::atomic_hook
    absl::config
    absl::core_headers
    absl::errno_saver
    absl::log_severity
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    spinlock_wait
  HDRS
    "${DIR}/internal/spinlock_wait.h"
  SRCS
    "${DIR}/internal/spinlock_akaros.inc"
    "${DIR}/internal/spinlock_linux.inc"
    "${DIR}/internal/spinlock_posix.inc"
    "${DIR}/internal/spinlock_wait.cc"
    "${DIR}/internal/spinlock_win32.inc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::core_headers
    absl::errno_saver
)

absl_cc_library(
  NAME
    config
  HDRS
    "${DIR}/config.h"
    "${DIR}/options.h"
    "${DIR}/policy_checks.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  PUBLIC
)

absl_cc_library(
  NAME
    dynamic_annotations
  HDRS
    "${DIR}/dynamic_annotations.h"
  SRCS
    "${DIR}/internal/dynamic_annotations.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

absl_cc_library(
  NAME
    core_headers
  HDRS
    "${DIR}/attributes.h"
    "${DIR}/const_init.h"
    "${DIR}/macros.h"
    "${DIR}/optimization.h"
    "${DIR}/port.h"
    "${DIR}/thread_annotations.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    malloc_internal
  HDRS
    "${DIR}/internal/direct_mmap.h"
    "${DIR}/internal/low_level_alloc.h"
  SRCS
    "${DIR}/internal/low_level_alloc.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::base_internal
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::raw_logging_internal
    Threads::Threads
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    base_internal
  HDRS
    "${DIR}/internal/hide_ptr.h"
    "${DIR}/internal/identity.h"
    "${DIR}/internal/inline_variable.h"
    "${DIR}/internal/invoke.h"
    "${DIR}/internal/scheduling_mode.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::type_traits
)

absl_cc_library(
  NAME
    base
  HDRS
    "${DIR}/call_once.h"
    "${DIR}/casts.h"
    "${DIR}/internal/cycleclock.h"
    "${DIR}/internal/cycleclock_config.h"
    "${DIR}/internal/low_level_scheduling.h"
    "${DIR}/internal/per_thread_tls.h"
    "${DIR}/internal/spinlock.h"
    "${DIR}/internal/sysinfo.h"
    "${DIR}/internal/thread_identity.h"
    "${DIR}/internal/tsan_mutex_interface.h"
    "${DIR}/internal/unscaledcycleclock.h"
    "${DIR}/internal/unscaledcycleclock_config.h"
  SRCS
    "${DIR}/internal/cycleclock.cc"
    "${DIR}/internal/spinlock.cc"
    "${DIR}/internal/sysinfo.cc"
    "${DIR}/internal/thread_identity.cc"
    "${DIR}/internal/unscaledcycleclock.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
    $<$<BOOL:${LIBRT}>:-lrt>
    $<$<BOOL:${MINGW}>:-ladvapi32>
  DEPS
    absl::atomic_hook
    absl::base_internal
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::log_severity
    absl::nullability
    absl::raw_logging_internal
    absl::spinlock_wait
    absl::type_traits
    Threads::Threads
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    throw_delegate
  HDRS
    "${DIR}/internal/throw_delegate.h"
  SRCS
    "${DIR}/internal/throw_delegate.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::raw_logging_internal
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    pretty_function
  HDRS
    "${DIR}/internal/pretty_function.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    endian
  HDRS
    "${DIR}/internal/endian.h"
    "${DIR}/internal/unaligned_access.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::nullability
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    scoped_set_env
  SRCS
    "${DIR}/internal/scoped_set_env.cc"
  HDRS
    "${DIR}/internal/scoped_set_env.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::raw_logging_internal
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    strerror
  SRCS
    "${DIR}/internal/strerror.cc"
  HDRS
    "${DIR}/internal/strerror.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::errno_saver
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    fast_type_id
  HDRS
    "${DIR}/internal/fast_type_id.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
)

absl_cc_library(
  NAME
    prefetch
  HDRS
    "${DIR}/prefetch.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
)

set(DIR ${ABSL_ROOT_DIR}/absl/cleanup)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cleanup_internal
  HDRS
    "${DIR}/internal/cleanup.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::core_headers
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    cleanup
  HDRS
    "${DIR}/cleanup.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::cleanup_internal
    absl::config
    absl::core_headers
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/container)

absl_cc_library(
  NAME
    btree
  HDRS
    "${DIR}/btree_map.h"
    "${DIR}/btree_set.h"
    "${DIR}/internal/btree.h"
    "${DIR}/internal/btree_container.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::common_policy_traits
    absl::compare
    absl::compressed_tuple
    absl::config
    absl::container_common
    absl::container_memory
    absl::cord
    absl::core_headers
    absl::layout
    absl::memory
    absl::raw_logging_internal
    absl::strings
    absl::throw_delegate
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    compressed_tuple
  HDRS
    "${DIR}/internal/compressed_tuple.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    fixed_array
  HDRS
   "${DIR}/fixed_array.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::compressed_tuple
    absl::algorithm
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::throw_delegate
    absl::memory
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    inlined_vector_internal
  HDRS
   "${DIR}/internal/inlined_vector.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::compressed_tuple
    absl::config
    absl::core_headers
    absl::memory
    absl::span
    absl::type_traits
  PUBLIC
)

absl_cc_library(
  NAME
    inlined_vector
  HDRS
   "${DIR}/inlined_vector.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::algorithm
    absl::core_headers
    absl::inlined_vector_internal
    absl::throw_delegate
    absl::memory
    absl::type_traits
  PUBLIC
)

absl_cc_library(
  NAME
    flat_hash_map
  HDRS
    "${DIR}/flat_hash_map.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::container_memory
    absl::core_headers
    absl::hash_container_defaults
    absl::raw_hash_map
    absl::algorithm_container
    absl::memory
  PUBLIC
)

absl_cc_library(
  NAME
    flat_hash_set
  HDRS
    "${DIR}/flat_hash_set.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::container_memory
    absl::hash_container_defaults
    absl::raw_hash_set
    absl::algorithm_container
    absl::core_headers
    absl::memory
  PUBLIC
)

absl_cc_library(
  NAME
    node_hash_map
  HDRS
    "${DIR}/node_hash_map.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::container_memory
    absl::core_headers
    absl::hash_container_defaults
    absl::node_slot_policy
    absl::raw_hash_map
    absl::algorithm_container
    absl::memory
  PUBLIC
)

absl_cc_library(
  NAME
    node_hash_set
  HDRS
    "${DIR}/node_hash_set.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::container_memory
    absl::core_headers
    absl::hash_container_defaults
    absl::node_slot_policy
    absl::raw_hash_set
    absl::algorithm_container
    absl::memory
  PUBLIC
)

absl_cc_library(
  NAME
    hash_container_defaults
  HDRS
    "${DIR}/hash_container_defaults.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::hash_function_defaults
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    container_memory
  HDRS
    "${DIR}/internal/container_memory.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::memory
    absl::type_traits
    absl::utility
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    hash_function_defaults
  HDRS
    "${DIR}/internal/hash_function_defaults.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::container_common
    absl::cord
    absl::hash
    absl::strings
    absl::type_traits
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    hash_policy_traits
  HDRS
    "${DIR}/internal/hash_policy_traits.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::common_policy_traits
    absl::meta
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    common_policy_traits
  HDRS
    "${DIR}/internal/common_policy_traits.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::meta
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    hashtablez_sampler
  HDRS
    "${DIR}/internal/hashtablez_sampler.h"
  SRCS
    "${DIR}/internal/hashtablez_sampler.cc"
    "${DIR}/internal/hashtablez_sampler_force_weak_definition.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::exponential_biased
    absl::no_destructor
    absl::raw_logging_internal
    absl::sample_recorder
    absl::synchronization
    absl::time
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    hashtable_debug
  HDRS
    "${DIR}/internal/hashtable_debug.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::hashtable_debug_hooks
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    hashtable_debug_hooks
  HDRS
    "${DIR}/internal/hashtable_debug_hooks.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    node_slot_policy
  HDRS
    "${DIR}/internal/node_slot_policy.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    raw_hash_map
  HDRS
    "${DIR}/internal/raw_hash_map.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::container_memory
    absl::core_headers
    absl::raw_hash_set
    absl::throw_delegate
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    container_common
  HDRS
    "${DIR}/internal/common.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    raw_hash_set
  HDRS
    "${DIR}/internal/raw_hash_set.h"
  SRCS
    "${DIR}/internal/raw_hash_set.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bits
    absl::compressed_tuple
    absl::config
    absl::container_common
    absl::container_memory
    absl::core_headers
    absl::dynamic_annotations
    absl::endian
    absl::hash
    absl::hash_policy_traits
    absl::hashtable_debug_hooks
    absl::hashtablez_sampler
    absl::memory
    absl::meta
    absl::optional
    absl::prefetch
    absl::raw_logging_internal
    absl::utility
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    layout
  HDRS
    "${DIR}/internal/layout.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::debugging_internal
    absl::meta
    absl::strings
    absl::span
    absl::utility
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/crc)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    crc_cpu_detect
  HDRS
    "${DIR}/internal/cpu_detect.h"
  SRCS
    "${DIR}/internal/cpu_detect.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    crc_internal
  HDRS
    "${DIR}/internal/crc.h"
    "${DIR}/internal/crc32_x86_arm_combined_simd.h"
  SRCS
    "${DIR}/internal/crc.cc"
    "${DIR}/internal/crc_internal.h"
    "${DIR}/internal/crc_x86_arm_combined.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::crc_cpu_detect
    absl::config
    absl::core_headers
    absl::endian
    absl::prefetch
    absl::raw_logging_internal
    absl::memory
    absl::bits
)

absl_cc_library(
  NAME
    crc32c
  HDRS
    "${DIR}/crc32c.h"
    "${DIR}/internal/crc32c.h"
    "${DIR}/internal/crc_memcpy.h"
  SRCS
    "${DIR}/crc32c.cc"
    "${DIR}/internal/crc32c_inline.h"
    "${DIR}/internal/crc_memcpy_fallback.cc"
    "${DIR}/internal/crc_memcpy_x86_arm_combined.cc"
    "${DIR}/internal/crc_non_temporal_memcpy.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::crc_cpu_detect
    absl::crc_internal
    absl::non_temporal_memcpy
    absl::config
    absl::core_headers
    absl::endian
    absl::prefetch
    absl::str_format
    absl::strings
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    non_temporal_arm_intrinsics
  HDRS
    "${DIR}/internal/non_temporal_arm_intrinsics.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    non_temporal_memcpy
  HDRS
    "${DIR}/internal/non_temporal_memcpy.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::non_temporal_arm_intrinsics
    absl::config
    absl::core_headers
)

absl_cc_library(
  NAME
    crc_cord_state
  HDRS
    "${DIR}/internal/crc_cord_state.h"
  SRCS
    "${DIR}/internal/crc_cord_state.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::crc32c
    absl::config
    absl::strings
    absl::no_destructor
)

set(DIR ${ABSL_ROOT_DIR}/absl/debugging)

absl_cc_library(
  NAME
    stacktrace
  HDRS
    "${DIR}/stacktrace.h"
    "${DIR}/internal/stacktrace_aarch64-inl.inc"
    "${DIR}/internal/stacktrace_arm-inl.inc"
    "${DIR}/internal/stacktrace_config.h"
    "${DIR}/internal/stacktrace_emscripten-inl.inc"
    "${DIR}/internal/stacktrace_generic-inl.inc"
    "${DIR}/internal/stacktrace_powerpc-inl.inc"
    "${DIR}/internal/stacktrace_riscv-inl.inc"
    "${DIR}/internal/stacktrace_unimplemented-inl.inc"
    "${DIR}/internal/stacktrace_win32-inl.inc"
    "${DIR}/internal/stacktrace_x86-inl.inc"
  SRCS
    "${DIR}/stacktrace.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    $<$<BOOL:${EXECINFO_LIBRARY}>:${EXECINFO_LIBRARY}>
  DEPS
    absl::debugging_internal
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::raw_logging_internal
  PUBLIC
)

absl_cc_library(
  NAME
    symbolize
  HDRS
    "${DIR}/symbolize.h"
    "${DIR}/internal/symbolize.h"
  SRCS
    "${DIR}/symbolize.cc"
    "${DIR}/symbolize_darwin.inc"
    "${DIR}/symbolize_elf.inc"
    "${DIR}/symbolize_emscripten.inc"
    "${DIR}/symbolize_unimplemented.inc"
    "${DIR}/symbolize_win32.inc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
    $<$<BOOL:${MINGW}>:-ldbghelp>
  DEPS
    absl::debugging_internal
    absl::demangle_internal
    absl::base
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::malloc_internal
    absl::raw_logging_internal
    absl::strings
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    examine_stack
  HDRS
    "${DIR}/internal/examine_stack.h"
  SRCS
    "${DIR}/internal/examine_stack.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::stacktrace
    absl::symbolize
    absl::config
    absl::core_headers
    absl::raw_logging_internal
)

absl_cc_library(
  NAME
    failure_signal_handler
  HDRS
    "${DIR}/failure_signal_handler.h"
  SRCS
    "${DIR}/failure_signal_handler.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::examine_stack
    absl::stacktrace
    absl::base
    absl::config
    absl::core_headers
    absl::raw_logging_internal
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    debugging_internal
  HDRS
    "${DIR}/internal/address_is_readable.h"
    "${DIR}/internal/elf_mem_image.h"
    "${DIR}/internal/vdso_support.h"
  SRCS
    "${DIR}/internal/address_is_readable.cc"
    "${DIR}/internal/elf_mem_image.cc"
    "${DIR}/internal/vdso_support.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::core_headers
    absl::config
    absl::dynamic_annotations
    absl::errno_saver
    absl::raw_logging_internal
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    demangle_internal
  HDRS
    "${DIR}/internal/demangle.h"
    "${DIR}/internal/demangle_rust.h"
  SRCS
    "${DIR}/internal/demangle.cc"
    "${DIR}/internal/demangle_rust.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::core_headers
  PUBLIC
)

absl_cc_library(
  NAME
    leak_check
  HDRS
    "${DIR}/leak_check.h"
  SRCS
    "${DIR}/leak_check.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
  PUBLIC
)

# component target
absl_cc_library(
  NAME
    debugging
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::stacktrace
    absl::leak_check
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/flags)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    flags_path_util
  HDRS
    "${DIR}/internal/path_util.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::strings
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    flags_program_name
  SRCS
    "${DIR}/internal/program_name.cc"
  HDRS
    "${DIR}/internal/program_name.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::flags_path_util
    absl::strings
    absl::synchronization
  PUBLIC
)

absl_cc_library(
  NAME
    flags_config
  SRCS
    "${DIR}/usage_config.cc"
  HDRS
    "${DIR}/config.h"
    "${DIR}/usage_config.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::flags_path_util
    absl::flags_program_name
    absl::core_headers
    absl::strings
    absl::synchronization
)

absl_cc_library(
  NAME
    flags_marshalling
  SRCS
    "${DIR}/marshalling.cc"
  HDRS
    "${DIR}/marshalling.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_severity
    absl::int128
    absl::optional
    absl::strings
    absl::str_format
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    flags_commandlineflag_internal
  SRCS
    "${DIR}/internal/commandlineflag.cc"
  HDRS
    "${DIR}/internal/commandlineflag.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::dynamic_annotations
    absl::fast_type_id
)

absl_cc_library(
  NAME
    flags_commandlineflag
  SRCS
    "${DIR}/commandlineflag.cc"
  HDRS
    "${DIR}/commandlineflag.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::fast_type_id
    absl::flags_commandlineflag_internal
    absl::optional
    absl::strings
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    flags_private_handle_accessor
  SRCS
    "${DIR}/internal/private_handle_accessor.cc"
  HDRS
    "${DIR}/internal/private_handle_accessor.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::flags_commandlineflag
    absl::flags_commandlineflag_internal
    absl::strings
)

absl_cc_library(
  NAME
    flags_reflection
  SRCS
    "${DIR}/reflection.cc"
  HDRS
    "${DIR}/reflection.h"
    "${DIR}/internal/registry.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::flags_commandlineflag
    absl::flags_private_handle_accessor
    absl::flags_config
    absl::strings
    absl::synchronization
    absl::flat_hash_map
    absl::no_destructor
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    flags_internal
  SRCS
    "${DIR}/internal/flag.cc"
  HDRS
    "${DIR}/internal/flag.h"
    "${DIR}/internal/sequence_lock.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base
    absl::config
    absl::flags_commandlineflag
    absl::flags_commandlineflag_internal
    absl::flags_config
    absl::flags_marshalling
    absl::synchronization
    absl::meta
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    flags
  HDRS
    "${DIR}/declare.h"
    "${DIR}/flag.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::flags_commandlineflag
    absl::flags_config
    absl::flags_internal
    absl::flags_reflection
    absl::core_headers
    absl::strings
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    flags_usage_internal
  SRCS
    "${DIR}/internal/usage.cc"
  HDRS
    "${DIR}/internal/usage.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::flags_config
    absl::flags
    absl::flags_commandlineflag
    absl::flags_internal
    absl::flags_path_util
    absl::flags_private_handle_accessor
    absl::flags_program_name
    absl::flags_reflection
    absl::strings
    absl::synchronization
)

absl_cc_library(
  NAME
    flags_usage
  SRCS
    "${DIR}/usage.cc"
  HDRS
    "${DIR}/usage.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::flags_usage_internal
    absl::raw_logging_internal
    absl::strings
    absl::synchronization
)

absl_cc_library(
  NAME
    flags_parse
  SRCS
    "${DIR}/parse.cc"
  HDRS
    "${DIR}/internal/parse.h"
    "${DIR}/parse.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::algorithm_container
    absl::config
    absl::core_headers
    absl::flags_config
    absl::flags
    absl::flags_commandlineflag
    absl::flags_commandlineflag_internal
    absl::flags_internal
    absl::flags_private_handle_accessor
    absl::flags_program_name
    absl::flags_reflection
    absl::flags_usage
    absl::strings
    absl::synchronization
)

############################################################################
# Unit tests in alphabetical order.

set(DIR ${ABSL_ROOT_DIR}/absl/functional)

absl_cc_library(
  NAME
    any_invocable
  SRCS
    "${DIR}/internal/any_invocable.h"
  HDRS
    "${DIR}/any_invocable.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::config
    absl::core_headers
    absl::type_traits
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    bind_front
  SRCS
    "${DIR}/internal/front_binder.h"
  HDRS
    "${DIR}/bind_front.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::compressed_tuple
  PUBLIC
)

absl_cc_library(
  NAME
    function_ref
  SRCS
    "${DIR}/internal/function_ref.h"
  HDRS
    "${DIR}/function_ref.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::core_headers
    absl::any_invocable
    absl::meta
  PUBLIC
)

absl_cc_library(
  NAME
    overload
  HDRS
    "${DIR}/overload.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::meta
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/hash)

absl_cc_library(
  NAME
    hash
  HDRS
    "${DIR}/hash.h"
  SRCS
    "${DIR}/internal/hash.cc"
    "${DIR}/internal/hash.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bits
    absl::city
    absl::config
    absl::core_headers
    absl::endian
    absl::fixed_array
    absl::function_ref
    absl::meta
    absl::int128
    absl::strings
    absl::optional
    absl::variant
    absl::utility
    absl::low_level_hash
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    city
  HDRS
    "${DIR}/internal/city.h"
  SRCS
    "${DIR}/internal/city.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::endian
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    low_level_hash
  HDRS
    "${DIR}/internal/low_level_hash.h"
  SRCS
    "${DIR}/internal/low_level_hash.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::endian
    absl::int128
    absl::prefetch
)

set(DIR ${ABSL_ROOT_DIR}/absl/log)

# Internal targets
absl_cc_library(
  NAME
    log_internal_check_impl
  SRCS
  HDRS
    "${DIR}/internal/check_impl.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::core_headers
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_message
    absl::log_internal_strip
)

absl_cc_library(
  NAME
    log_internal_check_op
  SRCS
    "${DIR}/internal/check_op.cc"
  HDRS
    "${DIR}/internal/check_op.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_internal_nullguard
    absl::log_internal_nullstream
    absl::log_internal_strip
    absl::strings
)

absl_cc_library(
  NAME
    log_internal_conditions
  SRCS
    "${DIR}/internal/conditions.cc"
  HDRS
    "${DIR}/internal/conditions.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::log_internal_voidify
)

absl_cc_library(
  NAME
    log_internal_config
  SRCS
  HDRS
    "${DIR}/internal/config.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
)

absl_cc_library(
  NAME
    log_internal_flags
  SRCS
  HDRS
    "${DIR}/internal/flags.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::flags
)

absl_cc_library(
  NAME
    log_internal_format
  SRCS
    "${DIR}/internal/log_format.cc"
  HDRS
    "${DIR}/internal/log_format.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_internal_append_truncated
    absl::log_internal_config
    absl::log_internal_globals
    absl::log_severity
    absl::strings
    absl::str_format
    absl::time
    absl::span
)

absl_cc_library(
  NAME
    log_internal_globals
  SRCS
    "${DIR}/internal/globals.cc"
  HDRS
    "${DIR}/internal/globals.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_severity
    absl::raw_logging_internal
    absl::strings
    absl::time
)

absl_cc_library(
  NAME
    log_internal_log_impl
  SRCS
  HDRS
    "${DIR}/internal/log_impl.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::log_internal_conditions
    absl::log_internal_message
    absl::log_internal_strip
    absl::absl_vlog_is_on
)

absl_cc_library(
  NAME
    log_internal_proto
  SRCS
    "${DIR}/internal/proto.cc"
  HDRS
    "${DIR}/internal/proto.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::strings
    absl::span
)

absl_cc_library(
  NAME
    log_internal_message
  SRCS
    "${DIR}/internal/log_message.cc"
  HDRS
    "${DIR}/internal/log_message.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::errno_saver
    absl::inlined_vector
    absl::examine_stack
    absl::log_internal_append_truncated
    absl::log_internal_format
    absl::log_internal_globals
    absl::log_internal_proto
    absl::log_internal_log_sink_set
    absl::log_internal_nullguard
    absl::log_globals
    absl::log_entry
    absl::log_severity
    absl::log_sink
    absl::log_sink_registry
    absl::memory
    absl::raw_logging_internal
    absl::strings
    absl::strerror
    absl::time
    absl::span
)

absl_cc_library(
  NAME
    log_internal_log_sink_set
  SRCS
    "${DIR}/internal/log_sink_set.cc"
  HDRS
    "${DIR}/internal/log_sink_set.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
    $<$<BOOL:${ANDROID}>:-llog>
  DEPS
    absl::base
    absl::cleanup
    absl::config
    absl::core_headers
    absl::log_internal_config
    absl::log_internal_globals
    absl::log_globals
    absl::log_entry
    absl::log_severity
    absl::log_sink
    absl::no_destructor
    absl::raw_logging_internal
    absl::synchronization
    absl::span
    absl::strings
)

absl_cc_library(
  NAME
    log_internal_nullguard
  SRCS
    "${DIR}/internal/nullguard.cc"
  HDRS
    "${DIR}/internal/nullguard.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
)

absl_cc_library(
  NAME
    log_internal_nullstream
  SRCS
  HDRS
    "${DIR}/internal/nullstream.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_severity
    absl::strings
)

absl_cc_library(
  NAME
    log_internal_strip
  SRCS
  HDRS
    "${DIR}/internal/strip.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::core_headers
    absl::log_internal_message
    absl::log_internal_nullstream
    absl::log_severity
)

absl_cc_library(
  NAME
    log_internal_voidify
  SRCS
  HDRS
    "${DIR}/internal/voidify.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
)

absl_cc_library(
  NAME
    log_internal_append_truncated
  SRCS
  HDRS
    "${DIR}/internal/append_truncated.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::strings
    absl::span
)

# Public targets
absl_cc_library(
  NAME
    absl_check
  SRCS
  HDRS
    "${DIR}/absl_check.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::log_internal_check_impl
  PUBLIC
)

absl_cc_library(
  NAME
    absl_log
  SRCS
  HDRS
    "${DIR}/absl_log.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::log_internal_log_impl
  PUBLIC
)

absl_cc_library(
  NAME
    check
  SRCS
  HDRS
    "${DIR}/check.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::log_internal_check_impl
    absl::core_headers
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_message
    absl::log_internal_strip
  PUBLIC
)

absl_cc_library(
  NAME
    die_if_null
  SRCS
    "${DIR}/die_if_null.cc"
  HDRS
    "${DIR}/die_if_null.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log
    absl::strings
  PUBLIC
)

# Warning: Many linkers will strip the contents of this library because its
# symbols are only used in a global constructor. A workaround is for clients
# to link this using $<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags> instead of
# the plain absl::log_flags.
# TODO(b/320467376): Implement the equivalent of Bazel's alwayslink=True.
absl_cc_library(
  NAME
    log_flags
  SRCS
    "${DIR}/flags.cc"
  HDRS
    "${DIR}/flags.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_globals
    absl::log_severity
    absl::log_internal_config
    absl::log_internal_flags
    absl::flags
    absl::flags_marshalling
    absl::strings
    absl::vlog_config_internal
  PUBLIC
)

absl_cc_library(
  NAME
    log_globals
  SRCS
    "${DIR}/globals.cc"
  HDRS
    "${DIR}/globals.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::atomic_hook
    absl::config
    absl::core_headers
    absl::hash
    absl::log_severity
    absl::raw_logging_internal
    absl::strings
    absl::vlog_config_internal
)

absl_cc_library(
  NAME
    log_initialize
  SRCS
    "${DIR}/initialize.cc"
  HDRS
    "${DIR}/initialize.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::log_globals
    absl::log_internal_globals
    absl::time
  PUBLIC
)

absl_cc_library(
  NAME
    log
  SRCS
  HDRS
    "${DIR}/log.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::log_internal_log_impl
    absl::vlog_is_on
  PUBLIC
)

absl_cc_library(
  NAME
    log_entry
  SRCS
    "${DIR}/log_entry.cc"
  HDRS
    "${DIR}/log_entry.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::log_internal_config
    absl::log_severity
    absl::span
    absl::strings
    absl::time
  PUBLIC
)

absl_cc_library(
  NAME
    log_sink
  SRCS
    "${DIR}/log_sink.cc"
  HDRS
    "${DIR}/log_sink.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::log_entry
  PUBLIC
)

absl_cc_library(
  NAME
    log_sink_registry
  SRCS
  HDRS
    "${DIR}/log_sink_registry.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::log_sink
    absl::log_internal_log_sink_set
  PUBLIC
)

absl_cc_library(
  NAME
    log_streamer
  SRCS
  HDRS
    "${DIR}/log_streamer.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::absl_log
    absl::log_severity
    absl::optional
    absl::strings
    absl::strings_internal
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    log_internal_structured
  HDRS
    "${DIR}/internal/structured.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::log_internal_message
    absl::strings
)

absl_cc_library(
  NAME
    log_structured
  HDRS
    "${DIR}/structured.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::log_internal_structured
    absl::strings
  PUBLIC
)

absl_cc_library(
  NAME
    vlog_config_internal
  SRCS
    "${DIR}/internal/vlog_config.cc"
  HDRS
    "${DIR}/internal/vlog_config.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::log_internal_fnmatch
    absl::memory
    absl::no_destructor
    absl::strings
    absl::synchronization
    absl::optional
)

absl_cc_library(
  NAME
    absl_vlog_is_on
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  HDRS
    "${DIR}/absl_vlog_is_on.h"
  DEPS
    absl::vlog_config_internal
    absl::config
    absl::core_headers
    absl::strings
)

absl_cc_library(
  NAME
    vlog_is_on
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  HDRS
    "${DIR}/vlog_is_on.h"
  DEPS
    absl::absl_vlog_is_on
)

absl_cc_library(
  NAME
    log_internal_fnmatch
  SRCS
    "${DIR}/internal/fnmatch.cc"
  HDRS
    "${DIR}/internal/fnmatch.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::strings
)

# Test targets

set(DIR ${ABSL_ROOT_DIR}/absl/memory)

absl_cc_library(
  NAME
    memory
  HDRS
    "${DIR}/memory.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::core_headers
    absl::meta
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/meta)

absl_cc_library(
  NAME
    type_traits
  HDRS
    "${DIR}/type_traits.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
  PUBLIC
)

# component target
absl_cc_library(
  NAME
    meta
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::type_traits
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/numeric)

absl_cc_library(
  NAME
    bits
  HDRS
    "${DIR}/bits.h"
    "${DIR}/internal/bits.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::core_headers
  PUBLIC
)

absl_cc_library(
  NAME
    int128
  HDRS
    "${DIR}/int128.h"
  SRCS
    "${DIR}/int128.cc"
    "${DIR}/int128_have_intrinsic.inc"
    "${DIR}/int128_no_intrinsic.inc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::compare
    absl::config
    absl::core_headers
    absl::bits
  PUBLIC
)

# component target
absl_cc_library(
  NAME
    numeric
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::int128
  PUBLIC
)

absl_cc_library(
  NAME
    numeric_representation
  HDRS
    "${DIR}/internal/representation.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/profiling)

absl_cc_library(
  NAME
    sample_recorder
  HDRS
    "${DIR}/internal/sample_recorder.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::synchronization
)

absl_cc_library(
  NAME
    exponential_biased
  SRCS
    "${DIR}/internal/exponential_biased.cc"
  HDRS
    "${DIR}/internal/exponential_biased.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
)

absl_cc_library(
  NAME
    periodic_sampler
  SRCS
    "${DIR}/internal/periodic_sampler.cc"
  HDRS
    "${DIR}/internal/periodic_sampler.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::core_headers
    absl::exponential_biased
)

set(DIR ${ABSL_ROOT_DIR}/absl/random)

absl_cc_library(
  NAME
    random_random
  HDRS
    "${DIR}/random.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::random_distributions
    absl::random_internal_nonsecure_base
    absl::random_internal_pcg_engine
    absl::random_internal_pool_urbg
    absl::random_internal_randen_engine
    absl::random_seed_sequences
)

absl_cc_library(
  NAME
    random_bit_gen_ref
  HDRS
    "${DIR}/bit_gen_ref.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::core_headers
    absl::random_internal_distribution_caller
    absl::random_internal_fast_uniform_bits
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_mock_helpers
  HDRS
    "${DIR}/internal/mock_helpers.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::fast_type_id
    absl::optional
)

absl_cc_library(
  NAME
    random_distributions
  SRCS
    "${DIR}/discrete_distribution.cc"
    "${DIR}/gaussian_distribution.cc"
  HDRS
    "${DIR}/bernoulli_distribution.h"
    "${DIR}/beta_distribution.h"
    "${DIR}/discrete_distribution.h"
    "${DIR}/distributions.h"
    "${DIR}/exponential_distribution.h"
    "${DIR}/gaussian_distribution.h"
    "${DIR}/log_uniform_int_distribution.h"
    "${DIR}/poisson_distribution.h"
    "${DIR}/uniform_int_distribution.h"
    "${DIR}/uniform_real_distribution.h"
    "${DIR}/zipf_distribution.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base_internal
    absl::config
    absl::core_headers
    absl::random_internal_generate_real
    absl::random_internal_distribution_caller
    absl::random_internal_fast_uniform_bits
    absl::random_internal_fastmath
    absl::random_internal_iostream_state_saver
    absl::random_internal_traits
    absl::random_internal_uniform_helper
    absl::random_internal_wide_multiply
    absl::strings
    absl::type_traits
)

absl_cc_library(
  NAME
    random_seed_gen_exception
  SRCS
    "${DIR}/seed_gen_exception.cc"
  HDRS
    "${DIR}/seed_gen_exception.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
)

absl_cc_library(
  NAME
    random_seed_sequences
  SRCS
    "${DIR}/seed_sequences.cc"
  HDRS
    "${DIR}/seed_sequences.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::inlined_vector
    absl::nullability
    absl::random_internal_pool_urbg
    absl::random_internal_salted_seed_seq
    absl::random_internal_seed_material
    absl::random_seed_gen_exception
    absl::span
    absl::string_view
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_traits
  HDRS
    "${DIR}/internal/traits.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_distribution_caller
  HDRS
    "${DIR}/internal/distribution_caller.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::utility
    absl::fast_type_id
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_fast_uniform_bits
  HDRS
    "${DIR}/internal/fast_uniform_bits.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_seed_material
  SRCS
    "${DIR}/internal/seed_material.cc"
  HDRS
    "${DIR}/internal/seed_material.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
    $<$<BOOL:${MINGW}>:-lbcrypt>
  DEPS
    absl::core_headers
    absl::optional
    absl::random_internal_fast_uniform_bits
    absl::raw_logging_internal
    absl::span
    absl::strings
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_pool_urbg
  SRCS
    "${DIR}/internal/pool_urbg.cc"
  HDRS
    "${DIR}/internal/pool_urbg.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::endian
    absl::random_internal_randen
    absl::random_internal_seed_material
    absl::random_internal_traits
    absl::random_seed_gen_exception
    absl::raw_logging_internal
    absl::span
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_salted_seed_seq
  HDRS
    "${DIR}/internal/salted_seed_seq.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::inlined_vector
    absl::optional
    absl::span
    absl::random_internal_seed_material
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_iostream_state_saver
  HDRS
    "${DIR}/internal/iostream_state_saver.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::int128
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_generate_real
  HDRS
    "${DIR}/internal/generate_real.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::bits
    absl::random_internal_fastmath
    absl::random_internal_traits
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_wide_multiply
  HDRS
    "${DIR}/internal/wide_multiply.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::bits
    absl::config
    absl::int128
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_fastmath
  HDRS
    "${DIR}/internal/fastmath.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::bits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_nonsecure_base
  HDRS
    "${DIR}/internal/nonsecure_base.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::core_headers
    absl::inlined_vector
    absl::random_internal_pool_urbg
    absl::random_internal_salted_seed_seq
    absl::random_internal_seed_material
    absl::span
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_pcg_engine
  HDRS
    "${DIR}/internal/pcg_engine.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::int128
    absl::random_internal_fastmath
    absl::random_internal_iostream_state_saver
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_randen_engine
  HDRS
    "${DIR}/internal/randen_engine.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::endian
    absl::random_internal_iostream_state_saver
    absl::random_internal_randen
    absl::raw_logging_internal
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_platform
  HDRS
    "${DIR}/internal/randen_traits.h"
    "${DIR}/internal/platform.h"
  SRCS
    "${DIR}/internal/randen_round_keys.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_randen
  SRCS
    "${DIR}/internal/randen.cc"
  HDRS
    "${DIR}/internal/randen.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::random_internal_platform
    absl::random_internal_randen_hwaes
    absl::random_internal_randen_slow
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_randen_slow
  SRCS
    "${DIR}/internal/randen_slow.cc"
  HDRS
    "${DIR}/internal/randen_slow.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::random_internal_platform
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_randen_hwaes
  SRCS
    "${DIR}/internal/randen_detect.cc"
  HDRS
    "${DIR}/internal/randen_detect.h"
    "${DIR}/internal/randen_hwaes.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
    ${ABSL_RANDOM_RANDEN_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::random_internal_platform
    absl::random_internal_randen_hwaes_impl
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_randen_hwaes_impl
  SRCS
    "${DIR}/internal/randen_hwaes.cc"
    "${DIR}/internal/randen_hwaes.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
    ${ABSL_RANDOM_RANDEN_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::random_internal_platform
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_distribution_test_util
  SRCS
    "${DIR}/internal/chi_square.cc"
    "${DIR}/internal/distribution_test_util.cc"
  HDRS
    "${DIR}/internal/chi_square.h"
    "${DIR}/internal/distribution_test_util.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::raw_logging_internal
    absl::strings
    absl::str_format
    absl::span
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    random_internal_uniform_helper
  HDRS
    "${DIR}/internal/uniform_helper.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  LINKOPTS
    ${ABSL_DEFAULT_LINKOPTS}
  DEPS
    absl::config
    absl::random_internal_traits
    absl::type_traits
)

set(DIR ${ABSL_ROOT_DIR}/absl/status)

absl_cc_library(
  NAME
    status
  HDRS
    "${DIR}/status.h"
  SRCS
    "${DIR}/internal/status_internal.h"
    "${DIR}/internal/status_internal.cc"
    "${DIR}/status.cc"
    "${DIR}/status_payload_printer.h"
    "${DIR}/status_payload_printer.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEFINES
    "$<$<PLATFORM_ID:AIX>:_LINUX_SOURCE_COMPAT>"
  DEPS
    absl::atomic_hook
    absl::config
    absl::cord
    absl::core_headers
    absl::function_ref
    absl::inlined_vector
    absl::memory
    absl::no_destructor
    absl::nullability
    absl::optional
    absl::raw_logging_internal
    absl::span
    absl::stacktrace
    absl::strerror
    absl::str_format
    absl::strings
    absl::symbolize
  PUBLIC
)

absl_cc_library(
  NAME
    statusor
  HDRS
    "${DIR}/statusor.h"
  SRCS
    "${DIR}/statusor.cc"
    "${DIR}/internal/statusor_internal.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::has_ostream_operator
    absl::nullability
    absl::raw_logging_internal
    absl::status
    absl::str_format
    absl::strings
    absl::type_traits
    absl::utility
    absl::variant
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/strings)

absl_cc_library(
  NAME
    string_view
  HDRS
    "${DIR}/string_view.h"
  SRCS
    "${DIR}/string_view.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::nullability
    absl::throw_delegate
  PUBLIC
)

absl_cc_library(
  NAME
    strings
  HDRS
    "${DIR}/ascii.h"
    "${DIR}/charconv.h"
    "${DIR}/escaping.h"
    "${DIR}/has_absl_stringify.h"
    "${DIR}/internal/damerau_levenshtein_distance.h"
    "${DIR}/internal/string_constant.h"
    "${DIR}/internal/has_absl_stringify.h"
    "${DIR}/match.h"
    "${DIR}/numbers.h"
    "${DIR}/str_cat.h"
    "${DIR}/str_join.h"
    "${DIR}/str_replace.h"
    "${DIR}/str_split.h"
    "${DIR}/strip.h"
    "${DIR}/substitute.h"
  SRCS
    "${DIR}/ascii.cc"
    "${DIR}/charconv.cc"
    "${DIR}/escaping.cc"
    "${DIR}/internal/charconv_bigint.cc"
    "${DIR}/internal/charconv_bigint.h"
    "${DIR}/internal/charconv_parse.cc"
    "${DIR}/internal/charconv_parse.h"
    "${DIR}/internal/damerau_levenshtein_distance.cc"
    "${DIR}/internal/memutil.cc"
    "${DIR}/internal/memutil.h"
    "${DIR}/internal/stringify_sink.h"
    "${DIR}/internal/stringify_sink.cc"
    "${DIR}/internal/stl_type_traits.h"
    "${DIR}/internal/str_join_internal.h"
    "${DIR}/internal/str_split_internal.h"
    "${DIR}/match.cc"
    "${DIR}/numbers.cc"
    "${DIR}/str_cat.cc"
    "${DIR}/str_replace.cc"
    "${DIR}/str_split.cc"
    "${DIR}/substitute.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::string_view
    absl::strings_internal
    absl::base
    absl::bits
    absl::charset
    absl::config
    absl::core_headers
    absl::endian
    absl::int128
    absl::memory
    absl::nullability
    absl::raw_logging_internal
    absl::throw_delegate
    absl::type_traits
  PUBLIC
)

absl_cc_library(
  NAME
    charset
  HDRS
    charset.h
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::core_headers
    absl::string_view
  PUBLIC
)

absl_cc_library(
  NAME
    has_ostream_operator
  HDRS
    "${DIR}/has_ostream_operator.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    strings_internal
  HDRS
    "${DIR}/internal/escaping.cc"
    "${DIR}/internal/escaping.h"
    "${DIR}/internal/ostringstream.h"
    "${DIR}/internal/resize_uninitialized.h"
    "${DIR}/internal/utf8.h"
  SRCS
    "${DIR}/internal/ostringstream.cc"
    "${DIR}/internal/utf8.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::endian
    absl::raw_logging_internal
    absl::type_traits
)

absl_cc_library(
  NAME
    str_format
  HDRS
    "${DIR}/str_format.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::nullability
    absl::span
    absl::str_format_internal
    absl::string_view
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    str_format_internal
  HDRS
    "${DIR}/internal/str_format/arg.h"
    "${DIR}/internal/str_format/bind.h"
    "${DIR}/internal/str_format/checker.h"
    "${DIR}/internal/str_format/constexpr_parser.h"
    "${DIR}/internal/str_format/extension.h"
    "${DIR}/internal/str_format/float_conversion.h"
    "${DIR}/internal/str_format/output.h"
    "${DIR}/internal/str_format/parser.h"
  SRCS
    "${DIR}/internal/str_format/arg.cc"
    "${DIR}/internal/str_format/bind.cc"
    "${DIR}/internal/str_format/extension.cc"
    "${DIR}/internal/str_format/float_conversion.cc"
    "${DIR}/internal/str_format/output.cc"
    "${DIR}/internal/str_format/parser.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bits
    absl::strings
    absl::config
    absl::core_headers
    absl::fixed_array
    absl::inlined_vector
    absl::numeric_representation
    absl::type_traits
    absl::utility
    absl::int128
    absl::span
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cord_internal
  HDRS
    "${DIR}/internal/cord_data_edge.h"
    "${DIR}/internal/cord_internal.h"
    "${DIR}/internal/cord_rep_btree.h"
    "${DIR}/internal/cord_rep_btree_navigator.h"
    "${DIR}/internal/cord_rep_btree_reader.h"
    "${DIR}/internal/cord_rep_crc.h"
    "${DIR}/internal/cord_rep_consume.h"
    "${DIR}/internal/cord_rep_flat.h"
  SRCS
    "${DIR}/internal/cord_internal.cc"
    "${DIR}/internal/cord_rep_btree.cc"
    "${DIR}/internal/cord_rep_btree_navigator.cc"
    "${DIR}/internal/cord_rep_btree_reader.cc"
    "${DIR}/internal/cord_rep_crc.cc"
    "${DIR}/internal/cord_rep_consume.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::compressed_tuple
    absl::config
    absl::container_memory
    absl::core_headers
    absl::crc_cord_state
    absl::endian
    absl::inlined_vector
    absl::layout
    absl::raw_logging_internal
    absl::strings
    absl::throw_delegate
    absl::type_traits
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_update_tracker
  HDRS
    "${DIR}/internal/cordz_update_tracker.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_functions
  HDRS
    "${DIR}/internal/cordz_functions.h"
  SRCS
    "${DIR}/internal/cordz_functions.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::exponential_biased
    absl::raw_logging_internal
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_statistics
  HDRS
    "${DIR}/internal/cordz_statistics.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::cordz_update_tracker
    absl::synchronization
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_handle
  HDRS
    "${DIR}/internal/cordz_handle.h"
  SRCS
    "${DIR}/internal/cordz_handle.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::no_destructor
    absl::raw_logging_internal
    absl::synchronization
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_info
  HDRS
    "${DIR}/internal/cordz_info.h"
  SRCS
    "${DIR}/internal/cordz_info.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::cord_internal
    absl::cordz_functions
    absl::cordz_handle
    absl::cordz_statistics
    absl::cordz_update_tracker
    absl::core_headers
    absl::inlined_vector
    absl::span
    absl::raw_logging_internal
    absl::stacktrace
    absl::synchronization
    absl::time
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_sample_token
  HDRS
    "${DIR}/internal/cordz_sample_token.h"
  SRCS
    "${DIR}/internal/cordz_sample_token.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::cordz_handle
    absl::cordz_info
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    cordz_update_scope
  HDRS
    "${DIR}/internal/cordz_update_scope.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::cord_internal
    absl::cordz_info
    absl::cordz_update_tracker
    absl::core_headers
)

absl_cc_library(
  NAME
    cord
  HDRS
    "${DIR}/cord.h"
    "${DIR}/cord_buffer.h"
  SRCS
    "${DIR}/cord.cc"
    "${DIR}/cord_analysis.cc"
    "${DIR}/cord_analysis.h"
    "${DIR}/cord_buffer.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::cord_internal
    absl::cordz_functions
    absl::cordz_info
    absl::cordz_update_scope
    absl::cordz_update_tracker
    absl::core_headers
    absl::crc32c
    absl::crc_cord_state
    absl::endian
    absl::function_ref
    absl::inlined_vector
    absl::nullability
    absl::optional
    absl::raw_logging_internal
    absl::span
    absl::strings
    absl::type_traits
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/synchronization)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    graphcycles_internal
  HDRS
    "${DIR}/internal/graphcycles.h"
  SRCS
    "${DIR}/internal/graphcycles.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::base_internal
    absl::config
    absl::core_headers
    absl::malloc_internal
    absl::raw_logging_internal
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    kernel_timeout_internal
  HDRS
    "${DIR}/internal/kernel_timeout.h"
  SRCS
    "${DIR}/internal/kernel_timeout.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::config
    absl::core_headers
    absl::raw_logging_internal
    absl::time
)

absl_cc_library(
  NAME
    synchronization
  HDRS
    "${DIR}/barrier.h"
    "${DIR}/blocking_counter.h"
    "${DIR}/internal/create_thread_identity.h"
    "${DIR}/internal/futex.h"
    "${DIR}/internal/futex_waiter.h"
    "${DIR}/internal/per_thread_sem.h"
    "${DIR}/internal/pthread_waiter.h"
    "${DIR}/internal/sem_waiter.h"
    "${DIR}/internal/stdcpp_waiter.h"
    "${DIR}/internal/waiter.h"
    "${DIR}/internal/waiter_base.h"
    "${DIR}/internal/win32_waiter.h"
    "${DIR}/mutex.h"
    "${DIR}/notification.h"
  SRCS
    "${DIR}/barrier.cc"
    "${DIR}/blocking_counter.cc"
    "${DIR}/internal/create_thread_identity.cc"
    "${DIR}/internal/futex_waiter.cc"
    "${DIR}/internal/per_thread_sem.cc"
    "${DIR}/internal/pthread_waiter.cc"
    "${DIR}/internal/sem_waiter.cc"
    "${DIR}/internal/stdcpp_waiter.cc"
    "${DIR}/internal/waiter_base.cc"
    "${DIR}/internal/win32_waiter.cc"
    "${DIR}/notification.cc"
    "${DIR}/mutex.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::graphcycles_internal
    absl::kernel_timeout_internal
    absl::atomic_hook
    absl::base
    absl::base_internal
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::malloc_internal
    absl::raw_logging_internal
    absl::stacktrace
    absl::symbolize
    absl::time
    Threads::Threads
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/time)

absl_cc_library(
  NAME
    time
  HDRS
    "${DIR}/civil_time.h"
    "${DIR}/clock.h"
    "${DIR}/time.h"
  SRCS
    "${DIR}/civil_time.cc"
    "${DIR}/clock.cc"
    "${DIR}/duration.cc"
    "${DIR}/format.cc"
    "${DIR}/internal/get_current_time_chrono.inc"
    "${DIR}/internal/get_current_time_posix.inc"
    "${DIR}/time.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base
    absl::civil_time
    absl::core_headers
    absl::int128
    absl::raw_logging_internal
    absl::strings
    absl::time_zone
  PUBLIC
)

absl_cc_library(
  NAME
    civil_time
  HDRS
    "${DIR}/internal/cctz/include/cctz/civil_time.h"
    "${DIR}/internal/cctz/include/cctz/civil_time_detail.h"
  SRCS
  "${DIR}/internal/cctz/src/civil_time_detail.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
)

absl_cc_library(
  NAME
    time_zone
  HDRS
    "${DIR}/internal/cctz/include/cctz/time_zone.h"
    "${DIR}/internal/cctz/include/cctz/zone_info_source.h"
  SRCS
    "${DIR}/internal/cctz/src/time_zone_fixed.cc"
    "${DIR}/internal/cctz/src/time_zone_fixed.h"
    "${DIR}/internal/cctz/src/time_zone_format.cc"
    "${DIR}/internal/cctz/src/time_zone_if.cc"
    "${DIR}/internal/cctz/src/time_zone_if.h"
    "${DIR}/internal/cctz/src/time_zone_impl.cc"
    "${DIR}/internal/cctz/src/time_zone_impl.h"
    "${DIR}/internal/cctz/src/time_zone_info.cc"
    "${DIR}/internal/cctz/src/time_zone_info.h"
    "${DIR}/internal/cctz/src/time_zone_libc.cc"
    "${DIR}/internal/cctz/src/time_zone_libc.h"
    "${DIR}/internal/cctz/src/time_zone_lookup.cc"
    "${DIR}/internal/cctz/src/time_zone_posix.cc"
    "${DIR}/internal/cctz/src/time_zone_posix.h"
    "${DIR}/internal/cctz/src/tzfile.h"
    "${DIR}/internal/cctz/src/zone_info_source.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    Threads::Threads
    # TODO(#1495): Use $<LINK_LIBRARY:FRAMEWORK,CoreFoundation> once our
    # minimum CMake version >= 3.24
    $<$<PLATFORM_ID:Darwin>:-Wl,-framework,CoreFoundation>
)

set(DIR ${ABSL_ROOT_DIR}/absl/types)

absl_cc_library(
  NAME
    any
  HDRS
    "${DIR}/any.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bad_any_cast
    absl::config
    absl::core_headers
    absl::fast_type_id
    absl::type_traits
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    bad_any_cast
  HDRS
   "${DIR}/bad_any_cast.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bad_any_cast_impl
    absl::config
  PUBLIC
)

# Internal-only target, do not depend on directly.
absl_cc_library(
  NAME
    bad_any_cast_impl
  SRCS
    "${DIR}/bad_any_cast.h"
    "${DIR}/bad_any_cast.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::raw_logging_internal
)

absl_cc_library(
  NAME
    span
  HDRS
    "${DIR}/span.h"
  SRCS
    "${DIR}/internal/span.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::algorithm
    absl::core_headers
    absl::nullability
    absl::throw_delegate
    absl::type_traits
  PUBLIC
)

absl_cc_library(
  NAME
    optional
  HDRS
    "${DIR}/optional.h"
  SRCS
    "${DIR}/internal/optional.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bad_optional_access
    absl::base_internal
    absl::config
    absl::core_headers
    absl::memory
    absl::nullability
    absl::type_traits
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    bad_optional_access
  HDRS
    "${DIR}/bad_optional_access.h"
  SRCS
    "${DIR}/bad_optional_access.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::raw_logging_internal
  PUBLIC
)

absl_cc_library(
  NAME
    bad_variant_access
  HDRS
    "${DIR}/bad_variant_access.h"
  SRCS
    "${DIR}/bad_variant_access.cc"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::raw_logging_internal
  PUBLIC
)

absl_cc_library(
  NAME
    variant
  HDRS
    "${DIR}/variant.h"
  SRCS
    "${DIR}/internal/variant.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::bad_variant_access
    absl::base_internal
    absl::config
    absl::core_headers
    absl::type_traits
    absl::utility
  PUBLIC
)

absl_cc_library(
  NAME
    compare
  HDRS
    "${DIR}/compare.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
    absl::core_headers
    absl::type_traits
  PUBLIC
)

set(DIR ${ABSL_ROOT_DIR}/absl/utility)

absl_cc_library(
  NAME
    utility
  HDRS
    "${DIR}/utility.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::base_internal
    absl::config
    absl::type_traits
  PUBLIC
)

absl_cc_library(
  NAME
    if_constexpr
  HDRS
    "${DIR}/internal/if_constexpr.h"
  COPTS
    ${ABSL_DEFAULT_COPTS}
  DEPS
    absl::config
  PUBLIC
)


add_library(_abseil_swiss_tables INTERFACE)
target_include_directories (_abseil_swiss_tables SYSTEM BEFORE INTERFACE ${ABSL_ROOT_DIR})
add_library(ch_contrib::abseil_swiss_tables ALIAS _abseil_swiss_tables)
