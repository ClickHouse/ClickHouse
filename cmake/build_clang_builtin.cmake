# Function to build Clang builtins for a specified target triple from source
# Note that the arguments are hardcoded and based on the current sources
# To add support for more target triples, please extend the first if block
function (build_clang_builtin target_triple OUT_VARIABLE)

    set (BUILTINS_DEFAULT_TARGET_TRIPLE ${target_triple})
    # Linux target triples
    if (target_triple STREQUAL "x86_64-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-x86_64.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-x86_64.a")
    elseif (target_triple STREQUAL "x86_64-linux-musl")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-x86_64-musl.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-x86_64.a")
    elseif (target_triple STREQUAL "aarch64-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-aarch64.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-aarch64.a")
    elseif (target_triple STREQUAL "loongarch64-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-loongarch64.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-loongarch64.a")
    elseif (target_triple STREQUAL "powerpc64le-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-ppc64le.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-powerpc64le.a")
    elseif (target_triple STREQUAL "riscv64-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-riscv64.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-riscv64.a")
    elseif (target_triple STREQUAL "s390x-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-s390x.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-s390x.a")
    elseif (target_triple STREQUAL "e2k-linux-gnu")
        set (BUILTINS_TOOLCHAIN_FILE "${ClickHouse_SOURCE_DIR}/cmake/linux/toolchain-e2k.cmake")
        set (BUILTINS_TARGET "lib/linux/libclang_rt.builtins-e2k.a")
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
    set (NEED_BUILTIN_BUILD FALSE)

    set (OUT_LIBS "${BUILTINS_BINARY_DIR}/${BUILTINS_TARGET}")
    if (NOT EXISTS "${BUILTINS_BINARY_DIR}/${BUILTINS_TARGET}")
        set (NEED_BUILTIN_BUILD TRUE)
    endif ()

    set (${OUT_VARIABLE} "${BUILTINS_BINARY_DIR}/${BUILTINS_TARGET}" PARENT_SCOPE)
    set (EXTRA_TARGETS)
    set (EXTRA_BUILTINS_LIBRARIES)
    if (SANITIZE)
        if (SANITIZE STREQUAL "address")
            set (EXTRA_BUILTINS_LIBRARIES "asan_static" "asan" "asan_cxx")
        elseif (SANITIZE STREQUAL "memory")
            set (EXTRA_BUILTINS_LIBRARIES "msan" "msan_cxx")
        elseif (SANITIZE STREQUAL "thread")
            set (EXTRA_BUILTINS_LIBRARIES "tsan" "tsan_cxx")
        elseif (SANITIZE STREQUAL "undefined")
            set (EXTRA_BUILTINS_LIBRARIES "ubsan_standalone" "ubsan_standalone_cxx")
        endif ()
        # We are providing our own sanitizer runtimes, so disable linking the system ones
        set (OUT_LIBS "${OUT_LIBS} -fno-sanitize-link-runtime")
    endif ()

    if (ENABLE_XRAY)
        set (EXTRA_BUILTINS_LIBRARIES "xray")
        # We are providing our runtime, so disable linking the system ones
        set (OUT_LIBS "${OUT_LIBS} -fno-xray-link-deps")
    endif()

    foreach (LIB IN ITEMS ${EXTRA_BUILTINS_LIBRARIES})
        string(REPLACE "builtins" "${LIB}" NEW_TARGET "${BUILTINS_TARGET}")
        list (APPEND EXTRA_TARGETS "--target" "${NEW_TARGET}")
        set (OUT_LIBS "${OUT_LIBS} ${BUILTINS_BINARY_DIR}/${NEW_TARGET}")

        if (NOT EXISTS "${BUILTINS_BINARY_DIR}/${NEW_TARGET}")
            set (NEED_BUILTIN_BUILD TRUE)
        endif ()
    endforeach ()

    if (NEED_BUILTIN_BUILD)
        execute_process(
                COMMAND mkdir -p ${BUILTINS_BINARY_DIR}
                COMMAND_ECHO STDOUT
                COMMAND_ERROR_IS_FATAL ANY
        )

        message (NOTICE "Building builtins for target ${CMAKE_CXX_COMPILER_TARGET} from source")

        disable_dummy_launchers_if_needed()

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
                # Trick the build into using libc++ from our clang instead of the sysroot one (which isn't there)
                # Needed for xray (sanitizers use C headers only)
                "-DSANITIZER_COMMON_CFLAGS=-isystem;${BUILTINS_SOURCE_DIR}/../libcxx/include"
                "-S ${BUILTINS_SOURCE_DIR}"
                "-B ${BUILTINS_BINARY_DIR}"
                WORKING_DIRECTORY ${BUILTINS_BINARY_DIR}
                COMMAND_ECHO STDOUT
                COMMAND_ERROR_IS_FATAL ANY
        )

        enable_dummy_launchers_if_needed()

        execute_process(
                COMMAND ${CMAKE_COMMAND} --build ${BUILTINS_BINARY_DIR} --target ${BUILTINS_TARGET} ${EXTRA_TARGETS}
                COMMAND_ECHO STDOUT
                COMMAND_ERROR_IS_FATAL ANY
        )
    endif ()

    set (${OUT_VARIABLE} ${OUT_LIBS} PARENT_SCOPE)
endfunction()
