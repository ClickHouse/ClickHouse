# Block all CMake build-time probes from being used in ClickHouse-authored code.
#
# Rationale: ClickHouse fixes its compiler (Clang, with a known minimum version - see
# `cmake/tools.cmake`) and its toolchain (vendored sysroots under `cmake/linux/` etc.),
# so the set of supported compiler flags, headers, and intrinsics is fixed and known
# statically. Running probes like `check_cxx_source_compiles` or `try_compile` at configure
# time is wasteful, slow, non-deterministic, and tends to produce different binaries on
# different machines. Where compiler-version-specific behaviour is genuinely required, it
# must be gated on `CMAKE_CXX_COMPILER_VERSION` instead.
#
# This file installs fatal-error overrides for every CMake check macro / function we are
# aware of, plus the `try_compile` / `try_run` primitives. Each override mentions which
# command was attempted and points at the right alternative.
#
# Scope: project-wide. Included before `add_subdirectory(contrib)` so the block also
# applies to third-party libraries we vendor. Contrib `*-cmake` configurations have been
# rewritten to set values directly based on the known toolchain (see cmake/tools.cmake
# for the compiler, and cmake/arch.cmake / the toolchain files under cmake/linux/ etc.
# for target architecture and sysroot).

set (_CH_BLOCKED_CHECK_HINT
    "Build-time CMake checks are disabled in ClickHouse. The compiler and toolchain are fixed, \
so the set of supported features is known statically; gate any version-specific behaviour on \
CMAKE_CXX_COMPILER_VERSION instead. See cmake/block_build_time_checks.cmake for details.")

# Helper: define a function `name` that aborts with a uniform message when called.
macro (_ch_block_check name)
    function (${name})
        message (FATAL_ERROR "Forbidden CMake command '${name}' was called. ${_CH_BLOCKED_CHECK_HINT}")
    endfunction ()
endmacro ()

_ch_block_check (check_c_compiler_flag)
_ch_block_check (check_cxx_compiler_flag)
_ch_block_check (check_compiler_flag)
_ch_block_check (check_linker_flag)

_ch_block_check (check_function_exists)
_ch_block_check (check_variable_exists)
_ch_block_check (check_library_exists)

_ch_block_check (check_include_file)
_ch_block_check (check_include_file_cxx)
_ch_block_check (check_include_files)

_ch_block_check (check_symbol_exists)
_ch_block_check (check_cxx_symbol_exists)

_ch_block_check (check_c_source_compiles)
_ch_block_check (check_cxx_source_compiles)
_ch_block_check (check_source_compiles)
_ch_block_check (check_c_source_runs)
_ch_block_check (check_cxx_source_runs)
_ch_block_check (check_source_runs)

_ch_block_check (check_pie_supported)
_ch_block_check (check_prototype_definition)
_ch_block_check (check_struct_has_member)
_ch_block_check (check_type_size)
_ch_block_check (check_language)

_ch_block_check (test_big_endian)

# `try_compile` and `try_run` are CMake built-ins, but they can still be shadowed by a
# user-defined function of the same name. The original built-in remains accessible as
# `_try_compile` / `_try_run` should we ever need it.
_ch_block_check (try_compile)
_ch_block_check (try_run)
