set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "ppc64le")
set (CMAKE_C_COMPILER_TARGET "ppc64le-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "ppc64le-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "ppc64le-linux-gnu")

# Will be changed later, but somehow needed to be set here.
set (CMAKE_AR "ar")
set (CMAKE_RANLIB "ranlib")

set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-powerpc64le")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/powerpc64le-linux-gnu/libc")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")

set (HAS_PRE_1970_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_PRE_1970_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)

set (HAS_POST_2038_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_POST_2038_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)
