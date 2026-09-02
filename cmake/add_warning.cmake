# Append -W${flag} to the compile flags.
#
# Historically these macros invoked `check_cxx_compiler_flag` to probe the compiler at configure
# time. We have removed all such build-time checks: the project requires Clang of a known minimum
# version (see `cmake/tools.cmake`), so the set of supported warning flags is fixed and known
# statically. The right place to gate version-specific warnings is `cmake/warnings.cmake`,
# behind an explicit `CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL N` guard.

macro (add_warning flag)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W${flag}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W${flag}")
endmacro ()

macro (no_warning flag)
    add_warning(no-${flag})
endmacro ()

macro (target_add_warning target flag)
    target_compile_options (${target} PRIVATE "-W${flag}")
endmacro ()

macro (target_no_warning target flag)
    target_add_warning(${target} no-${flag})
endmacro ()
