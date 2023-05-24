set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "riscv64")
set (CMAKE_C_COMPILER_TARGET "riscv64-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "riscv64-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "riscv64-linux-gnu")

# Will be changed later, but somehow needed to be set here.
set (CMAKE_AR "ar")
set (CMAKE_RANLIB "ranlib")

set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-riscv64")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")

set (CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=bfd")
set (CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=bfd")

# Currently, lld does not work with the error:
# ld.lld: error: section size decrease is too large
# But GNU BinUtils work.
set (LINKER_NAME "riscv64-linux-gnu-ld.bfd" CACHE STRING "Linker name" FORCE)

set (HAS_PRE_1970_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_PRE_1970_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)

set (HAS_POST_2038_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
set (HAS_POST_2038_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)
