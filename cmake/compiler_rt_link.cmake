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
if (WITH_COVERAGE)
    # `-noprofilelib` tells clang not to inject its own (host-system) profile
    # runtime. It also drops the `-u __llvm_profile_runtime` anchor the driver
    # would add, so the anchor is restored here explicitly. Our own runtime is
    # NOT named here: contrib/compiler-rt-cmake registers `clang_rt_profile`
    # into global-libs, which places the archive in every link AFTER the object
    # files, and the anchor makes the linker pull it from there.
    #
    # Both properties are load-bearing, and each was broken once:
    #
    #   * The runtime must come after the objects. It defines
    #     `__llvm_profile_counter_bias` as a WEAK alias of its own default
    #     variable to detect whether the compiler emitted the real bias variable
    #     (`-mllvm -runtime-counter-relocation`, continuous mode `%c`); the
    #     compiler's definition is weak too, and the linker keeps the first weak
    #     definition. Linked before the objects (the old --whole-archive in
    #     CMAKE_EXE_LINKER_FLAGS), the alias always won and every process failed
    #     continuous-mode startup with "LLVM Profile Error: Neither
    #     __llvm_profile_counter_bias nor __llvm_profile_bitmap_bias is defined".
    #
    #   * The anchor must exist. Without it, nothing references the runtime, the
    #     lazy archive contributes no members, and every process silently writes
    #     no profile at all — there is not even an error, because the code that
    #     would print one is exactly what is missing.
    #
    #   * The runtime must not be linked twice. A --whole-archive copy placed
    #     after the lazy global-libs copy force-loads every member on top of the
    #     lazily selected ones and fails the link with duplicate `lprof*`
    #     symbols.
    list (APPEND SANITIZER_RUNTIMES
        "-noprofilelib"
        "-Wl,-u,__llvm_profile_runtime"
    )
endif()
string (REPLACE ";" " " SANITIZER_RUNTIMES "${SANITIZER_RUNTIMES}")

message(STATUS "Builtins library: ${BUILTINS_LIBRARY}")
if (SANITIZER_RUNTIMES)
    message(STATUS "Sanitizer/XRay runtimes: ${SANITIZER_RUNTIMES}")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${BUILTINS_LIBRARY} ${SANITIZER_RUNTIMES} -Wl,--no-whole-archive")
