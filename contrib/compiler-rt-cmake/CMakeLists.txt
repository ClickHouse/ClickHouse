# Build compiler-rt (builtins, sanitizer runtimes, XRay) for ClickHouse.
#
# Replaces the external CMake invocation that previously lived in
# cmake/build_clang_builtin.cmake with direct add_library() targets,
# following the same pattern as contrib/llvm-project-cmake/ for LLVM.
#
# Targets created:
#   * clang_rt_builtins             — always
#   * clang_rt_asan / asan_static / asan_cxx — if SANITIZE matches address
#   * clang_rt_msan / msan_cxx               — if SANITIZE = memory
#   * clang_rt_tsan / tsan_cxx               — if SANITIZE = thread
#   * clang_rt_ubsan_standalone / ubsan_standalone_cxx — if SANITIZE = undefined
#   * clang_rt_xray                          — if ENABLE_XRAY

set(COMPILER_RT_DIR     "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt")
set(BUILTINS_SOURCE_DIR "${COMPILER_RT_DIR}/lib/builtins")
set(SANCOMMON_DIR       "${COMPILER_RT_DIR}/lib/sanitizer_common")
set(INTERCEPTION_DIR    "${COMPILER_RT_DIR}/lib/interception")
set(LSAN_DIR            "${COMPILER_RT_DIR}/lib/lsan")
set(UBSAN_DIR           "${COMPILER_RT_DIR}/lib/ubsan")
set(ASAN_DIR            "${COMPILER_RT_DIR}/lib/asan")
set(MSAN_DIR            "${COMPILER_RT_DIR}/lib/msan")
set(TSAN_DIR            "${COMPILER_RT_DIR}/lib/tsan/rtl")
set(XRAY_DIR            "${COMPILER_RT_DIR}/lib/xray")

if (NOT IS_DIRECTORY "${BUILTINS_SOURCE_DIR}")
    message(WARNING "compiler-rt source not found at ${COMPILER_RT_DIR}, skipping")
    return()
endif()

# Only Linux and FreeBSD wire our in-tree compiler-rt into the link.
# SunOS uses -lgcc_s (see cmake/sunos/default_libs.cmake), Darwin relies on
# Apple's bundled compiler-rt. Building our targets unconditionally would
# create unused archives on those platforms — and linking them via global-libs
# would risk duplicate-runtime conflicts.
if (NOT (OS_LINUX OR OS_FREEBSD))
    return()
endif()

#
# Reset inherited compiler flags so the compiler-rt sources get a clean,
# Release-style baseline. The runtime cannot inherit:
#   * -fsanitize=...        — runtime would be instrumented by the sanitizer it implements
#   * -fxray-instrument...  — runtime would be instrumented by xray
#   * -flto / -fwhole-program-vtables — runtime is built standalone, can't be LTO'd
#   * -fprofile-instr-generate / -fcoverage-mapping — coverage instrumentation
#   * project-specific warning, hardening, debug-info flags — irrelevant here
#
# The previous external-cmake build (cmake/build_clang_builtin.cmake) avoided
# this entirely by spawning a separate cmake invocation with only the toolchain
# file and CMAKE_BUILD_TYPE=Release. Now that we build in-process, we have to
# strip those flags explicitly. We keep only the toolchain bits added by
# cmake/linux/toolchain-*.cmake (--gcc-toolchain, --no-default-config); the
# --target= flag is applied automatically by cmake from CMAKE_<LANG>_COMPILER_TARGET.
set(_compiler_rt_keep_re "(--gcc-toolchain=[^ ]+|--no-default-config)")
foreach (lang C CXX ASM)
    string(REGEX MATCHALL "${_compiler_rt_keep_re}" _kept "${CMAKE_${lang}_FLAGS}")
    string(JOIN " " CMAKE_${lang}_FLAGS ${_kept})
    foreach (cfg DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
        set(CMAKE_${lang}_FLAGS_${cfg} "")
    endforeach()
endforeach()

# Use Release-style optimisation everywhere. The original external-cmake build
# passed -DCMAKE_BUILD_TYPE=Release, so this matches that behaviour.
add_compile_options(-O3 -DNDEBUG)

include(sources/builtins.cmake)

#
# Helper: prepend a directory to each item in a list of relative paths.
#
function(_prepend_dir out_var dir)
    set(result "")
    foreach (item ${ARGN})
        if (IS_ABSOLUTE "${item}")
            list(APPEND result "${item}")
        else()
            list(APPEND result "${dir}/${item}")
        endif()
    endforeach()
    set(${out_var} "${result}" PARENT_SCOPE)
endfunction()

#
# ============================================================
# 1. Builtins
# ============================================================
#
if (ARCH_AMD64)
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ${BUILTINS_GENERIC_TF_SOURCES}
        ${BUILTINS_X86_80_BIT_SOURCES}
        cpu_model/x86.c
        i386/fp_mode.c
        x86_64/floatdidf.c
        x86_64/floatdisf.c
        x86_64/floatundidf.S
        x86_64/floatundisf.S
        x86_64/floatdixf.c
        x86_64/floatundixf.S
    )
elseif (ARCH_AARCH64)
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ${BUILTINS_GENERIC_TF_SOURCES}
        cpu_model/aarch64.c
        aarch64/fp_mode.c
        aarch64/emupac.cpp
    )

    # Generate outline atomics helpers from lse.S
    set(_oa_dir "${CMAKE_CURRENT_BINARY_DIR}/outline_atomic_helpers.dir")
    file(MAKE_DIRECTORY "${_oa_dir}")
    foreach (pat cas swp ldadd ldclr ldeor ldset)
        foreach (size 1 2 4 8 16)
            foreach (model 1 2 3 4 5)
                if (pat STREQUAL "cas" OR NOT size STREQUAL "16")
                    set(_helper "${_oa_dir}/outline_atomic_${pat}${size}_${model}.S")
                    add_custom_command(
                        OUTPUT  "${_helper}"
                        COMMAND ${CMAKE_COMMAND} -E create_symlink
                                "${BUILTINS_SOURCE_DIR}/aarch64/lse.S" "${_helper}"
                        DEPENDS "${BUILTINS_SOURCE_DIR}/aarch64/lse.S"
                    )
                    set_source_files_properties("${_helper}" PROPERTIES
                        COMPILE_DEFINITIONS "L_${pat};SIZE=${size};MODEL=${model}"
                        INCLUDE_DIRECTORIES "${BUILTINS_SOURCE_DIR}"
                    )
                    list(APPEND BUILTINS_ARCH_SOURCES "${_helper}")
                endif()
            endforeach()
        endforeach()
    endforeach()
elseif (ARCH_PPC64LE)
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ppc/divtc3.c   ppc/fixtfdi.c    ppc/fixtfti.c    ppc/fixunstfdi.c
        ppc/fixunstfti.c ppc/floatditf.c ppc/floattitf.c ppc/floatunditf.c
        ppc/gcc_qadd.c ppc/gcc_qdiv.c   ppc/gcc_qmul.c   ppc/gcc_qsub.c
        ppc/multc3.c
    )
elseif (ARCH_RISCV64)
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ${BUILTINS_GENERIC_TF_SOURCES}
        cpu_model/riscv.c
        riscv/fp_mode.c
        riscv/save.S
        riscv/restore.S
        riscv/muldi3.S
    )
elseif (ARCH_S390X)
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ${BUILTINS_GENERIC_TF_SOURCES}
    )
elseif (ARCH_LOONGARCH64)
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ${BUILTINS_GENERIC_TF_SOURCES}
        loongarch/fp_mode.c
    )
elseif (ARCH_E2K)
    # MCST Elbrus 2000 — no architecture-specific builtins in upstream
    # compiler-rt, so we use the generic set (this matches the previous
    # external-cmake build, which used GENERIC_SOURCES + GENERIC_TF_SOURCES
    # via the e2k-linux-gnu target triple).
    set(BUILTINS_ARCH_SOURCES
        ${BUILTINS_GENERIC_SOURCES}
        ${BUILTINS_GENERIC_TF_SOURCES}
    )
else()
    message(FATAL_ERROR "Unsupported architecture for compiler-rt builtins")
endif()

# Filter: arch-specific sources (in subdirectories) replace any same-name generic .c
foreach (src ${BUILTINS_ARCH_SOURCES})
    get_filename_component(_dir "${src}" DIRECTORY)
    if (NOT "${_dir}" STREQUAL "")
        get_filename_component(_name "${src}" NAME)
        string(REGEX REPLACE "\\.S$"   ".c" _cname "${_name}")
        string(REGEX REPLACE "\\.cpp$" ".c" _cname "${_cname}")
        if (EXISTS "${BUILTINS_SOURCE_DIR}/${_cname}")
            list(REMOVE_ITEM BUILTINS_ARCH_SOURCES "${_cname}")
        endif()
    endif()
endforeach()

# BFloat16 builtin support depends on the compiler having a `__bf16` type for
# the target ABI. On x86_64 and aarch64 this is true since clang 15 (we use
# clang 21+). On ppc64le and s390x clang refuses `__bf16` ("not supported on
# this target") so we omit BF16 sources — no compiler-generated code on those
# arches will reference them.
if (ARCH_AMD64 OR ARCH_AARCH64)
    list(APPEND BUILTINS_ARCH_SOURCES ${BUILTINS_BF16_SOURCES})
endif()

_prepend_dir(BUILTINS_SOURCES "${BUILTINS_SOURCE_DIR}" ${BUILTINS_ARCH_SOURCES})

add_library(clang_rt_builtins STATIC ${BUILTINS_SOURCES})
target_include_directories(clang_rt_builtins PRIVATE
    "${BUILTINS_SOURCE_DIR}"
    "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/third-party/siphash/include"
)
target_compile_options(clang_rt_builtins PRIVATE
    # These flags mirror BUILTIN_CFLAGS in
    # contrib/llvm-project/compiler-rt/lib/builtins/CMakeLists.txt
    # (see the "if (APPLE) ... else()" branch starting around line 894):
    #   * -fPIC                 ← COMPILER_RT_BUILTINS_ENABLE_PIC default ON
    #   * -fno-builtin          ← COMPILER_RT_HAS_FNO_BUILTIN_FLAG
    #   * -fvisibility=hidden   ← COMPILER_RT_BUILTINS_HIDE_SYMBOLS default ON
    #   * -fomit-frame-pointer  ← release builds (NOT COMPILER_RT_DEBUG)
    #   * -nostdinc++           ← COMPILER_RT_HAS_NOSTDINCXX_FLAG
    -fPIC
    -fno-builtin
    -fvisibility=hidden
    -fomit-frame-pointer
    -nostdinc++
    # Suppress all warnings — these are external sources we don't audit.
    -w
)
target_compile_definitions(clang_rt_builtins PRIVATE VISIBILITY_HIDDEN)
set_target_properties(clang_rt_builtins PROPERTIES
    C_STANDARD 11 CXX_STANDARD 17
    C_STANDARD_REQUIRED ON CXX_STANDARD_REQUIRED ON
    C_EXTENSIONS OFF CXX_EXTENSIONS OFF
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    OUTPUT_NAME "clang_rt_builtins"
)
add_library(ch_contrib::builtins ALIAS clang_rt_builtins)

# Add builtins to global-libs for build-order dependency. The actual --whole-archive
# linking is configured in cmake/linux/default_libs.cmake via CMAKE_EXE_LINKER_FLAGS,
# because $<LINK_LIBRARY:WHOLE_ARCHIVE,...> does not propagate through
# global-group's $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES> indirection.
target_link_libraries(global-libs INTERFACE clang_rt_builtins)

#
# ============================================================
# 2. Sanitizer / XRay common build setup
# ============================================================
#
# We only build sanitizer/XRay components when actually needed. Each runtime
# is a STATIC library bundling its own sources plus the relevant compiler-rt
# common-helper sources, matching the upstream layout (libclang_rt.asan.a,
# libclang_rt.msan.a, ...).

if (NOT (SANITIZE OR ENABLE_XRAY))
    return()
endif()

include(sources/sanitizer_common.cmake)
include(sources/interception.cmake)
include(sources/lsan.cmake)
include(sources/ubsan.cmake)
include(sources/asan.cmake)
include(sources/msan.cmake)
include(sources/tsan.cmake)
include(sources/xray.cmake)

# Common include directories shared by every sanitizer.
set(SANITIZER_INCLUDE_DIRS
    "${COMPILER_RT_DIR}/lib"
    "${COMPILER_RT_DIR}/include"
    "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/third-party/siphash/include"
)

# Per-CFG flags shared by every sanitizer/XRay component.
#
# These mirror SANITIZER_COMMON_CFLAGS as constructed in
# contrib/llvm-project/compiler-rt/CMakeLists.txt (the block starting
# "Provide some common commandline flags for Sanitizer runtimes" near line 380),
# combined with SANITIZER_CFLAGS additions from
# contrib/llvm-project/compiler-rt/lib/sanitizer_common/CMakeLists.txt.
# Per-component additions (-fPIE, -ffreestanding, -msse4.2, RTTI for ubsan_cxx,
# ...) are applied separately on each target below — exactly as the upstream
# XYZ_CFLAGS variables in each lib/<component>/CMakeLists.txt do.
set(SANITIZER_COMMON_FLAGS
    # COMPILER_RT_HAS_FPIC_FLAG / COMPILER_RT_HAS_FNO_BUILTIN_FLAG / ...
    -fPIC
    -fno-builtin
    -fno-exceptions
    # RTTI is disabled by default in compiler-rt
    # (lib/sanitizer_common/CMakeLists.txt sets RTTI OFF; ubsan_cxx overrides).
    -fno-rtti
    -fomit-frame-pointer
    -funwind-tables
    -fno-stack-protector
    -fvisibility=hidden
    # Suppress all warnings — these are external sources we don't audit.
    -w
)

set(SANITIZER_COMMON_DEFS HAVE_RPC_XDR_H=0)

#
# add_compiler_rt_runtime(<NAME> SOURCE_DIR <dir> SOURCES <files...>
#                          [CFLAGS <extra-cflags...>] [DEFS <extra-defs...>]
#                          [INCLUDE_DIRS <extra-include-dirs...>])
#
# Builds a STATIC library named clang_rt_<NAME> with our standard flags +
# extras. Sources are paths relative to SOURCE_DIR.
#
function(add_compiler_rt_runtime name)
    cmake_parse_arguments(ARG "" "SOURCE_DIR" "SOURCES;CFLAGS;DEFS;INCLUDE_DIRS" ${ARGN})

    if (NOT ARG_SOURCE_DIR OR NOT ARG_SOURCES)
        message(FATAL_ERROR "add_compiler_rt_runtime(${name}): SOURCE_DIR and SOURCES are required")
    endif()

    _prepend_dir(_full_sources "${ARG_SOURCE_DIR}" ${ARG_SOURCES})

    set(_target clang_rt_${name})
    add_library(${_target} STATIC ${_full_sources})

    target_include_directories(${_target} PRIVATE
        ${SANITIZER_INCLUDE_DIRS} ${ARG_INCLUDE_DIRS}
    )
    target_compile_options(${_target} PRIVATE
        ${SANITIZER_COMMON_FLAGS} ${ARG_CFLAGS}
    )
    target_compile_definitions(${_target} PRIVATE
        ${SANITIZER_COMMON_DEFS} ${ARG_DEFS}
    )
    set_target_properties(${_target} PROPERTIES
        C_STANDARD 11 CXX_STANDARD 17
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
        OUTPUT_NAME "${_target}"
    )
    # Add to global-libs so ninja knows the .a must exist before any executable
    # is linked. The .a path is also referenced from cmake/linux/default_libs.cmake's
    # CMAKE_EXE_LINKER_FLAGS so it gets the --whole-archive treatment; the link
    # libraries here are redundant for symbol resolution but cheap (the linker
    # deduplicates archive members) and necessary for build-order correctness.
    target_link_libraries(global-libs INTERFACE ${_target})
endfunction()

# Helper used by inline target definitions below (we don't go through
# add_compiler_rt_runtime when sources come from multiple lib/<X>/ directories).
function(_register_compiler_rt_target target)
    target_link_libraries(global-libs INTERFACE ${target})
endfunction()

#
# ============================================================
# 3. ASan
# ============================================================
#
if (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "address,undefined")
    # Combined sources for the main asan archive: preinit + main + sanitizer_common
    # + interception + lsan_common + ubsan helpers, all flat in one archive so
    # everything resolves with --whole-archive and no inter-archive ordering.
    #
    # Bundle list mirrors the OBJECT_LIBS list passed to add_compiler_rt_runtime
    # for clang_rt.asan in contrib/llvm-project/compiler-rt/lib/asan/CMakeLists.txt
    # (the "STATIC, NOT WIN32" branch):
    #   RTAsan_preinit, RTAsan, RTInterception, RTSanitizerCommon[+Libc/Coverage/
    #   Symbolizer], RTLSanCommon, RTUbsan.
    # asan_cxx is a separate archive for symbol-resolution reasons (a process
    # that overrides operator new/delete itself can skip linking asan_cxx).
    # asan_static is a separate archive holding only the static helpers
    # (asan_rtl_static.cpp + asan_rtl_x86_64.S) that the compiler emits direct
    # references to from instrumented code.
    set(_asan_combined
        # asan main runtime
        ${ASAN_PREINIT_SOURCES}
        ${ASAN_SOURCES}
    )
    if (NOT WIN32 AND NOT APPLE)
        list(APPEND _asan_combined ${ASAN_ASM_SOURCES})
    endif()
    set(_asan_combined_full "")
    foreach (s ${_asan_combined})
        list(APPEND _asan_combined_full "${ASAN_DIR}/${s}")
    endforeach()
    foreach (s ${SANITIZER_COMMON_SOURCES} ${SANITIZER_COMMON_LIBC_SOURCES}
                ${SANITIZER_COMMON_COVERAGE_SOURCES} ${SANITIZER_COMMON_SYMBOLIZER_SOURCES})
        list(APPEND _asan_combined_full "${SANCOMMON_DIR}/${s}")
    endforeach()
    foreach (s ${INTERCEPTION_SOURCES})
        list(APPEND _asan_combined_full "${INTERCEPTION_DIR}/${s}")
    endforeach()
    foreach (s ${LSAN_COMMON_SOURCES})
        list(APPEND _asan_combined_full "${LSAN_DIR}/${s}")
    endforeach()
    foreach (s ${UBSAN_SOURCES})
        list(APPEND _asan_combined_full "${UBSAN_DIR}/${s}")
    endforeach()

    add_library(clang_rt_asan STATIC ${_asan_combined_full})
    target_include_directories(clang_rt_asan PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_asan PRIVATE ${SANITIZER_COMMON_FLAGS})
    target_compile_definitions(clang_rt_asan PRIVATE ${SANITIZER_COMMON_DEFS})
    set_target_properties(clang_rt_asan PROPERTIES
        C_STANDARD 11 CXX_STANDARD 17
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_asan)

    # asan_static — separate archive.
    set(_asan_static_full "${ASAN_DIR}/${ASAN_STATIC_SOURCES}")
    if (ARCH_AMD64)
        list(APPEND _asan_static_full "${ASAN_DIR}/${ASAN_STATIC_X86_64_ASM_SOURCES}")
    endif()
    add_library(clang_rt_asan_static STATIC ${_asan_static_full})
    target_include_directories(clang_rt_asan_static PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_asan_static PRIVATE ${SANITIZER_COMMON_FLAGS})
    set_target_properties(clang_rt_asan_static PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_asan_static)

    # asan_cxx — only asan_new_delete.cpp, RTTI disabled (matches upstream).
    add_library(clang_rt_asan_cxx STATIC "${ASAN_DIR}/${ASAN_CXX_SOURCES}")
    target_include_directories(clang_rt_asan_cxx PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_asan_cxx PRIVATE ${SANITIZER_COMMON_FLAGS})
    set_target_properties(clang_rt_asan_cxx PROPERTIES
        CXX_STANDARD 17
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_asan_cxx)
endif()

#
# ============================================================
# 4. MSan
# ============================================================
#
if (SANITIZE STREQUAL "memory")
    # MSan main archive: msan + sanitizer_common + interception + ubsan helpers.
    #
    # Bundle list matches the SOURCES + $<TARGET_OBJECTS:...> entries for
    # clang_rt.msan (STATIC) in
    # contrib/llvm-project/compiler-rt/lib/msan/CMakeLists.txt:
    #   MSAN_RTL_SOURCES + RTInterception + RTSanitizerCommon[+Libc/Coverage/
    #   Symbolizer] + RTUbsan.
    # msan_cxx is a separate archive (matches clang_rt.msan_cxx upstream).
    set(_msan_combined_full "")
    foreach (s ${MSAN_SOURCES})
        list(APPEND _msan_combined_full "${MSAN_DIR}/${s}")
    endforeach()
    foreach (s ${SANITIZER_COMMON_SOURCES} ${SANITIZER_COMMON_LIBC_SOURCES}
                ${SANITIZER_COMMON_COVERAGE_SOURCES} ${SANITIZER_COMMON_SYMBOLIZER_SOURCES})
        list(APPEND _msan_combined_full "${SANCOMMON_DIR}/${s}")
    endforeach()
    foreach (s ${INTERCEPTION_SOURCES})
        list(APPEND _msan_combined_full "${INTERCEPTION_DIR}/${s}")
    endforeach()
    foreach (s ${UBSAN_SOURCES})
        list(APPEND _msan_combined_full "${UBSAN_DIR}/${s}")
    endforeach()

    add_library(clang_rt_msan STATIC ${_msan_combined_full})
    target_include_directories(clang_rt_msan PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_msan PRIVATE
        ${SANITIZER_COMMON_FLAGS}
        # MSan-specific flags from MSAN_RTL_CFLAGS in
        # contrib/llvm-project/compiler-rt/lib/msan/CMakeLists.txt:
        #   * -fPIE         (NOT FreeBSD branch; required by MSan's shadow-memory
        #                    layout, which assumes a fixed, predictable PIE map)
        #   * -ffreestanding (avoids the compiler emitting calls to libc
        #                    functions that MSan needs to intercept)
        -fPIE
        -ffreestanding
    )
    target_compile_definitions(clang_rt_msan PRIVATE ${SANITIZER_COMMON_DEFS})
    set_target_properties(clang_rt_msan PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_msan)

    add_library(clang_rt_msan_cxx STATIC "${MSAN_DIR}/${MSAN_CXX_SOURCES}")
    target_include_directories(clang_rt_msan_cxx PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_msan_cxx PRIVATE
        ${SANITIZER_COMMON_FLAGS} -fPIE -ffreestanding
    )
    set_target_properties(clang_rt_msan_cxx PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_msan_cxx)
endif()

#
# ============================================================
# 5. TSan
# ============================================================
#
if (SANITIZE STREQUAL "thread")
    # TSan main archive: tsan + per-arch low-level asm + sanitizer_common +
    # interception + ubsan helpers.
    #
    # Bundle list matches the SOURCES + $<TARGET_OBJECTS:...> entries for
    # clang_rt.tsan (STATIC) in
    # contrib/llvm-project/compiler-rt/lib/tsan/rtl/CMakeLists.txt:
    #   TSAN_SOURCES + TSAN_ASM_SOURCES + TSAN_PREINIT_SOURCES +
    #   RTInterception + RTSanitizerCommon[+Libc/Coverage/Symbolizer] + RTUbsan.
    # tsan_cxx is a separate archive (matches clang_rt.tsan_cxx upstream).
    set(_tsan_arch_asm "")
    if (ARCH_AMD64)
        set(_tsan_arch_asm ${TSAN_X86_64_ASM_SOURCES})
    elseif (ARCH_AARCH64)
        set(_tsan_arch_asm ${TSAN_AARCH64_ASM_SOURCES})
    elseif (ARCH_PPC64LE)
        set(_tsan_arch_asm ${TSAN_PPC64LE_ASM_SOURCES})
    elseif (ARCH_LOONGARCH64)
        set(_tsan_arch_asm ${TSAN_LOONGARCH64_ASM_SOURCES})
    elseif (ARCH_RISCV64)
        set(_tsan_arch_asm ${TSAN_RISCV64_ASM_SOURCES})
    elseif (ARCH_S390X)
        set(_tsan_arch_asm ${TSAN_S390X_ASM_SOURCES})
    endif()

    set(_tsan_combined_full "")
    foreach (s ${TSAN_SOURCES} ${TSAN_PREINIT_SOURCES} ${_tsan_arch_asm})
        list(APPEND _tsan_combined_full "${TSAN_DIR}/${s}")
    endforeach()
    foreach (s ${SANITIZER_COMMON_SOURCES} ${SANITIZER_COMMON_LIBC_SOURCES}
                ${SANITIZER_COMMON_COVERAGE_SOURCES} ${SANITIZER_COMMON_SYMBOLIZER_SOURCES})
        list(APPEND _tsan_combined_full "${SANCOMMON_DIR}/${s}")
    endforeach()
    foreach (s ${INTERCEPTION_SOURCES})
        list(APPEND _tsan_combined_full "${INTERCEPTION_DIR}/${s}")
    endforeach()
    foreach (s ${UBSAN_SOURCES})
        list(APPEND _tsan_combined_full "${UBSAN_DIR}/${s}")
    endforeach()

    # TSan-specific flag from TSAN_RTL_CFLAGS in
    # contrib/llvm-project/compiler-rt/lib/tsan/rtl/CMakeLists.txt:
    #   * -msse4.2 — gated by COMPILER_RT_HAS_MSSE4_2_FLAG; tsan_md5 / hash
    #                routines use SSE 4.2 intrinsics directly. Only meaningful
    #                on x86_64 — the flag is rejected by the compiler on other
    #                architectures, so gate it explicitly.
    set(_tsan_extra_flags "")
    if (ARCH_AMD64)
        list(APPEND _tsan_extra_flags -msse4.2)
    endif()

    add_library(clang_rt_tsan STATIC ${_tsan_combined_full})
    target_include_directories(clang_rt_tsan PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_tsan PRIVATE ${SANITIZER_COMMON_FLAGS} ${_tsan_extra_flags})
    target_compile_definitions(clang_rt_tsan PRIVATE ${SANITIZER_COMMON_DEFS})
    set_target_properties(clang_rt_tsan PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_tsan)

    add_library(clang_rt_tsan_cxx STATIC "${TSAN_DIR}/${TSAN_CXX_SOURCES}")
    target_include_directories(clang_rt_tsan_cxx PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_tsan_cxx PRIVATE ${SANITIZER_COMMON_FLAGS} ${_tsan_extra_flags})
    set_target_properties(clang_rt_tsan_cxx PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_tsan_cxx)
endif()

#
# ============================================================
# 6. UBSan (standalone)
# ============================================================
#
if (SANITIZE STREQUAL "undefined")
    # UBSan standalone archive: ubsan + ubsan_standalone + preinit +
    # sanitizer_common + interception. Bundle list matches the OBJECT_LIBS for
    # clang_rt.ubsan_standalone (STATIC) in
    # contrib/llvm-project/compiler-rt/lib/ubsan/CMakeLists.txt:
    #   RTSanitizerCommon[+Libc/Coverage/Symbolizer] + RTUbsan +
    #   RTUbsan_standalone + RTInterception + ubsan_init_standalone_preinit.cpp.
    # ubsan_standalone_cxx is a separate archive (matches upstream).
    set(_ubsan_combined_full "")
    foreach (s ${UBSAN_SOURCES} ${UBSAN_STANDALONE_SOURCES} ubsan_init_standalone_preinit.cpp)
        list(APPEND _ubsan_combined_full "${UBSAN_DIR}/${s}")
    endforeach()
    foreach (s ${SANITIZER_COMMON_SOURCES} ${SANITIZER_COMMON_LIBC_SOURCES}
                ${SANITIZER_COMMON_COVERAGE_SOURCES} ${SANITIZER_COMMON_SYMBOLIZER_SOURCES})
        list(APPEND _ubsan_combined_full "${SANCOMMON_DIR}/${s}")
    endforeach()
    foreach (s ${INTERCEPTION_SOURCES})
        list(APPEND _ubsan_combined_full "${INTERCEPTION_DIR}/${s}")
    endforeach()

    add_library(clang_rt_ubsan_standalone STATIC ${_ubsan_combined_full})
    target_include_directories(clang_rt_ubsan_standalone PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_ubsan_standalone PRIVATE ${SANITIZER_COMMON_FLAGS})
    target_compile_definitions(clang_rt_ubsan_standalone PRIVATE ${SANITIZER_COMMON_DEFS})
    set_target_properties(clang_rt_ubsan_standalone PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_ubsan_standalone)

    set(_ubsan_cxx_full "")
    foreach (s ${UBSAN_CXX_SOURCES})
        list(APPEND _ubsan_cxx_full "${UBSAN_DIR}/${s}")
    endforeach()
    add_library(clang_rt_ubsan_standalone_cxx STATIC ${_ubsan_cxx_full})
    target_include_directories(clang_rt_ubsan_standalone_cxx PRIVATE ${SANITIZER_INCLUDE_DIRS})
    # ubsan_cxx is the only sanitizer component compiled with RTTI: it inspects
    # std::type_info at runtime to format type-mismatch reports. This matches
    # UBSAN_CXXFLAGS in contrib/llvm-project/compiler-rt/lib/ubsan/CMakeLists.txt
    # ("set_property(SOURCE ... PROPERTY COMPILE_FLAGS -frtti)" / "RTTI ON").
    set(_ubsan_cxx_flags ${SANITIZER_COMMON_FLAGS})
    list(REMOVE_ITEM _ubsan_cxx_flags -fno-rtti)
    list(APPEND _ubsan_cxx_flags -frtti)
    target_compile_options(clang_rt_ubsan_standalone_cxx PRIVATE ${_ubsan_cxx_flags})
    set_target_properties(clang_rt_ubsan_standalone_cxx PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_ubsan_standalone_cxx)
endif()

#
# ============================================================
# 7. XRay
# ============================================================
#
if (ENABLE_XRAY)
    # XRay archive: common runtime + all three modes (FDR, basic, profiling) +
    # per-arch trampolines + sanitizer_common (XRay reuses sanitizer_common's
    # allocator and printing helpers).
    #
    # Bundle list mirrors the OBJECT_LIBS for clang_rt.xray (STATIC) in
    # contrib/llvm-project/compiler-rt/lib/xray/CMakeLists.txt:
    #   RTXray (= XRAY_SOURCES + ${arch}_SOURCES) + RTXrayFDR + RTXrayBASIC +
    #   RTXrayPROFILING + RTSanitizerCommon[+Libc].
    # Upstream additionally builds clang_rt.xray-fdr/-basic/-profiling as
    # separate archives so an application can link only the modes it uses;
    # ClickHouse always wants all of them, so we collapse into one archive.
    set(_xray_arch_cpp "")
    set(_xray_arch_asm "")
    if     (ARCH_AMD64)
        set(_xray_arch_cpp ${XRAY_X86_64_SOURCES})
        set(_xray_arch_asm ${XRAY_X86_64_ASM_SOURCES})
    elseif (ARCH_AARCH64)
        set(_xray_arch_cpp ${XRAY_AARCH64_SOURCES})
        set(_xray_arch_asm ${XRAY_AARCH64_ASM_SOURCES})
    elseif (ARCH_PPC64LE)
        set(_xray_arch_cpp ${XRAY_PPC64LE_SOURCES})
        set(_xray_arch_asm ${XRAY_PPC64LE_ASM_SOURCES})
    elseif (ARCH_RISCV64)
        set(_xray_arch_cpp ${XRAY_RISCV64_SOURCES})
        set(_xray_arch_asm ${XRAY_RISCV64_ASM_SOURCES})
    elseif (ARCH_S390X)
        set(_xray_arch_cpp ${XRAY_S390X_SOURCES})
        set(_xray_arch_asm ${XRAY_S390X_ASM_SOURCES})
    elseif (ARCH_LOONGARCH64)
        set(_xray_arch_cpp ${XRAY_LOONGARCH64_SOURCES})
        set(_xray_arch_asm ${XRAY_LOONGARCH64_ASM_SOURCES})
    endif()

    set(_xray_combined_full "")
    foreach (s ${XRAY_COMMON_SOURCES} ${XRAY_FDR_SOURCES} ${XRAY_BASIC_SOURCES}
                ${XRAY_PROFILING_SOURCES} ${_xray_arch_cpp} ${_xray_arch_asm})
        list(APPEND _xray_combined_full "${XRAY_DIR}/${s}")
    endforeach()
    foreach (s ${SANITIZER_COMMON_SOURCES} ${SANITIZER_COMMON_LIBC_SOURCES})
        list(APPEND _xray_combined_full "${SANCOMMON_DIR}/${s}")
    endforeach()

    add_library(clang_rt_xray STATIC ${_xray_combined_full})
    target_include_directories(clang_rt_xray PRIVATE ${SANITIZER_INCLUDE_DIRS})
    target_compile_options(clang_rt_xray PRIVATE ${SANITIZER_COMMON_FLAGS})
    # XRay-specific defines from XRAY_COMMON_DEFINITIONS in
    # contrib/llvm-project/compiler-rt/lib/xray/CMakeLists.txt:
    #   * SANITIZER_COMMON_NO_REDEFINE_BUILTINS — XRay reuses sanitizer_common
    #     but doesn't intercept libc; this disables the redefinitions.
    #   * XRAY_HAS_EXCEPTIONS=1 — enables EH-aware logging code paths.
    #   * XRAY_SUPPORTED=1 — gates the per-arch entry points.
    target_compile_definitions(clang_rt_xray PRIVATE
        ${SANITIZER_COMMON_DEFS}
        SANITIZER_COMMON_NO_REDEFINE_BUILTINS
        XRAY_HAS_EXCEPTIONS=1
        XRAY_SUPPORTED=1
    )
    set_target_properties(clang_rt_xray PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    _register_compiler_rt_target(clang_rt_xray)
endif()
