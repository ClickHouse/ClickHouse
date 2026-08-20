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
    # Tell clang not to inject its own (host-system) profile runtime — we
    # provide ours (appended after the objects below).
    list (APPEND SANITIZER_RUNTIMES
        "-noprofilelib"
    )
endif()
string (REPLACE ";" " " SANITIZER_RUNTIMES "${SANITIZER_RUNTIMES}")

message(STATUS "Builtins library: ${BUILTINS_LIBRARY}")
if (SANITIZER_RUNTIMES)
    message(STATUS "Sanitizer/XRay runtimes: ${SANITIZER_RUNTIMES}")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${BUILTINS_LIBRARY} ${SANITIZER_RUNTIMES} -Wl,--no-whole-archive")

if (WITH_COVERAGE)
    # The profile runtime must come AFTER the object files in the link line,
    # unlike the runtimes above. It defines `__llvm_profile_counter_bias` as a
    # WEAK alias of its own `__llvm_profile_counter_bias_default` in order to
    # detect whether the compiler emitted the real bias variable (which happens
    # under `-mllvm -runtime-counter-relocation`, used for continuous mode `%c`):
    # the compiler's definition is weak too, so the linker keeps whichever
    # definition it encounters first. Linked through CMAKE_EXE_LINKER_FLAGS
    # (which the link rule places before the objects), the runtime's alias always
    # won, the runtime concluded the compiler did not define the bias, and every
    # instrumented process failed continuous-mode startup with "LLVM Profile
    # Error: Neither __llvm_profile_counter_bias nor __llvm_profile_bitmap_bias
    # is defined" and wrote no profile. CMAKE_<LANG>_STANDARD_LIBRARIES is
    # appended after the objects and the target link libraries, where the normal
    # clang driver would put the runtime, so the compiler-emitted bias wins.
    set (CMAKE_C_STANDARD_LIBRARIES "${CMAKE_C_STANDARD_LIBRARIES} -Wl,--whole-archive ${COMPILER_RT_DIR}/libclang_rt_profile.a -Wl,--no-whole-archive")
    set (CMAKE_CXX_STANDARD_LIBRARIES "${CMAKE_CXX_STANDARD_LIBRARIES} -Wl,--whole-archive ${COMPILER_RT_DIR}/libclang_rt_profile.a -Wl,--no-whole-archive")
endif()
