set (CMAKE_SYSTEM_NAME "FreeBSD")
set (CMAKE_SYSTEM_PROCESSOR "aarch64")
set (CMAKE_C_COMPILER_TARGET "aarch64-unknown-freebsd12")
set (CMAKE_CXX_COMPILER_TARGET "aarch64-unknown-freebsd12")
set (CMAKE_ASM_COMPILER_TARGET "aarch64-unknown-freebsd12")
set (CMAKE_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/freebsd-aarch64")

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)  # disable linkage check - it doesn't work in CMake

# Will be changed later, but somehow needed to be set here.
set (CMAKE_AR "ar")
set (CMAKE_RANLIB "ranlib")

set (HAS_PRE_1970_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_PRE_1970_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)

set (HAS_POST_2038_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_POST_2038_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)
