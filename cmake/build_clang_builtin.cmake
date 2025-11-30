# Function to build Clang builtins for a specified target triple from source
# Note that the arguments are hardcoded and based on the current sources
# To add support for more target triples, please extend the first if block
function (build_clang_builtin target_triple OUT_VARIABLE)
    message (NOTICE "Builtins library for target ${CMAKE_CXX_COMPILER_TARGET} not found in the system")

    set (BUILTINS_DEFAULT_TARGET_TRIPLE ${target_triple})
    # (Some) Linux target triples
    if (target_triple STREQUAL "x86_64-linux-musl")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-x86_64-musl.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-x86_64.a")
    elseif (target_triple STREQUAL "riscv64-linux-gnu")
            set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-riscv64.cmake")
            set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-riscv64.a")
    elseif (target_triple STREQUAL "s390x-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-s390x.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-s390x.a")
    # FREEBSD target triples
    elseif (target_triple STREQUAL "aarch64-unknown-freebsd13")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/freebsd/toolchain-aarch64.cmake")
        set (BUILTINS_TARGET "lib/freebsd/libclang_rt.builtins-aarch64.a")
    elseif (target_triple STREQUAL "powerpc64le-unknown-freebsd13")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/freebsd/toolchain-ppc64le.cmake")
        set (BUILTINS_TARGET "lib/freebsd/libclang_rt.builtins-powerpc64le.a")
    elseif (target_triple STREQUAL "x86_64-pc-freebsd13")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/freebsd/toolchain-x86_64.cmake")
        set (BUILTINS_TARGET "lib/freebsd/libclang_rt.builtins-x86_64.a")
    else ()
        message (FATAL_ERROR "Unknown target triple: ${target_triple}. Please set up the toolchain and target in ./cmake/build_clang_builtin.cmake")
    endif ()

    set (BUILTINS_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt")
    set (BUILTINS_BINARY_DIR "${ClickHouse_BINARY_DIR}/clang-builtins")

    if (NOT EXISTS "${BUILTINS_BINARY_DIR}/${BUILTINS_TARGET}")
        execute_process(
                COMMAND mkdir -p ${BUILTINS_BINARY_DIR}
                COMMAND_ECHO STDOUT
                COMMAND_ERROR_IS_FATAL ANY
        )

        message (NOTICE "Building builtins for target ${CMAKE_CXX_COMPILER_TARGET} from source")

        # We use our llvm sources to build the builtins for the architecture that we need
        execute_process(
                COMMAND ${CMAKE_COMMAND}
                "-G${CMAKE_GENERATOR}"
                "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
                "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                "-DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}"
                "-DCMAKE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER}"
                "-DCMAKE_BUILD_TYPE=Release"
                "-DCOMPILER_RT_DEFAULT_TARGET_TRIPLE=${BUILTINS_DEFAULT_TARGET_TRIPLE}"
                "-DCMAKE_TOOLCHAIN_FILE=${BUILTINS_TOOLCHAIN_FILE}"
                "-S ${BUILTINS_SOURCE_DIR}"
                "-B ${BUILTINS_BINARY_DIR}"
                WORKING_DIRECTORY ${BUILTINS_BINARY_DIR}
                COMMAND_ECHO STDOUT
                COMMAND_ERROR_IS_FATAL ANY
        )

        execute_process(
                COMMAND ${CMAKE_COMMAND} --build ${BUILTINS_BINARY_DIR} --target ${BUILTINS_TARGET}
                COMMAND_ECHO STDOUT
                COMMAND_ERROR_IS_FATAL ANY
        )
    endif ()

    set (${OUT_VARIABLE} "${BUILTINS_BINARY_DIR}/${BUILTINS_TARGET}" PARENT_SCOPE)


endfunction()