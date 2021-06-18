set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "ppc64le")
set (CMAKE_C_COMPILER_TARGET "ppc64le-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "ppc64le-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "ppc64le-linux-gnu")
set (CMAKE_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../toolchain/linux-ppc64le/ppc64le")

set (CMAKE_AR "${CMAKE_CURRENT_LIST_DIR}/../toolchain/linux-ppc64le/bin/powerpc64le-linux-gnu-ar" CACHE FILEPATH "" FORCE)
set (CMAKE_RANLIB "${CMAKE_CURRENT_LIST_DIR}/../toolchain/linux-ppc64le/bin/powerpc64le-linux-gnu-ranlib" CACHE FILEPATH "" FORCE)

set (CMAKE_C_FLAGS_INIT "${CMAKE_C_FLAGS} --gcc-toolchain=${CMAKE_CURRENT_LIST_DIR}/../toolchain/linux-ppc64le")
set (CMAKE_CXX_FLAGS_INIT "${CMAKE_CXX_FLAGS} --gcc-toolchain=${CMAKE_CURRENT_LIST_DIR}/../toolchain/linux-ppc64le")
set (CMAKE_ASM_FLAGS_INIT "${CMAKE_ASM_FLAGS} --gcc-toolchain=${CMAKE_CURRENT_LIST_DIR}/../toolchain/linux-ppc64le")

set (LINKER_NAME "lld" CACHE STRING "" FORCE)

set (CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=lld")
set (CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=lld")

set (HAS_PRE_1970_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_PRE_1970_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)

set (HAS_POST_2038_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_POST_2038_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)
