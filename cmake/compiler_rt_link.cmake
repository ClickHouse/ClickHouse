# Shared compiler-rt linking logic.
#
# All compiler-rt runtimes (builtins, sanitizers, XRay) are built as regular
# cmake targets in contrib/compiler-rt-cmake/. Here we wire the resulting
# .a files into CMAKE_EXE_LINKER_FLAGS with --whole-archive so compiler-
# generated calls (builtins, sanitizer interceptors, XRay trampolines) always
# resolve. The build-order dependency is established in
# contrib/compiler-rt-cmake/CMakeLists.txt by registering each clang_rt_*
# target into global-libs.
#
# We pass the .a paths via CMAKE_EXE_LINKER_FLAGS (rather than via cmake target
# names) because $<LINK_LIBRARY:WHOLE_ARCHIVE,...> doesn't survive the
# $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES> indirection used in
# global-group (see CMakeLists.txt around line 437).

set (COMPILER_RT_DIR "${CMAKE_BINARY_DIR}/contrib/compiler-rt-cmake")
set (BUILTINS_LIBRARY "${COMPILER_RT_DIR}/libclang_rt_builtins.a")

set (SANITIZER_RUNTIMES "")
if (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "address,undefined")
    # When ASan and UBSan are combined, the ASan runtime covers UBSan too.
    # ubsan_standalone must NOT be added — it shares sanitizer_common symbols
    # with asan and causes duplicate symbol errors.
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_asan_static.a"
        "${COMPILER_RT_DIR}/libclang_rt_asan.a"
        "${COMPILER_RT_DIR}/libclang_rt_asan_cxx.a"
    )
elseif (SANITIZE STREQUAL "memory")
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_msan.a"
        "${COMPILER_RT_DIR}/libclang_rt_msan_cxx.a"
    )
elseif (SANITIZE STREQUAL "thread")
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_tsan.a"
        "${COMPILER_RT_DIR}/libclang_rt_tsan_cxx.a"
    )
elseif (SANITIZE STREQUAL "undefined")
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_ubsan_standalone.a"
        "${COMPILER_RT_DIR}/libclang_rt_ubsan_standalone_cxx.a"
    )
endif()
if (SANITIZE)
    # Tell clang not to inject its own (host-system) sanitizer runtime — we
    # provide ours.
    list (APPEND SANITIZER_RUNTIMES "-fno-sanitize-link-runtime")
endif()
if (ENABLE_XRAY)
    list (APPEND SANITIZER_RUNTIMES
        "-fno-xray-link-deps"
        "${COMPILER_RT_DIR}/libclang_rt_xray.a"
    )
endif()
string (REPLACE ";" " " SANITIZER_RUNTIMES "${SANITIZER_RUNTIMES}")

message(STATUS "Builtins library: ${BUILTINS_LIBRARY}")
if (SANITIZER_RUNTIMES)
    message(STATUS "Sanitizer/XRay runtimes: ${SANITIZER_RUNTIMES}")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${BUILTINS_LIBRARY} ${SANITIZER_RUNTIMES} -Wl,--no-whole-archive")
