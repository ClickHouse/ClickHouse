# https://software.intel.com/sites/landingpage/IntrinsicsGuide/

# On x86-64, the build target is specified as a microarchitecture level (1, 2, 3, 4) via `X86_ARCH_LEVEL`.
# All of this is unrelated to the instruction set of the host machine
# (you can compile for a newer instruction set on old machines and vice versa).

set(RUSTFLAGS_CPU)
if (ARCH_AARCH64)
    # ARM publishes almost every year a new revision of it's ISA [1]. Each version comes with new mandatory and optional features from
    # which CPU vendors can pick and choose. This creates a lot of variability ... We provide two build "profiles", one for maximum
    # compatibility intended to run on all 64-bit ARM hardware released after 2013 (e.g. Raspberry Pi 4), and one for modern ARM server
    # CPUs, (e.g. Graviton).
    #
    # [1] https://en.wikipedia.org/wiki/AArch64
    option (NO_ARMV81_OR_HIGHER "Disable ARMv8.1 or higher on Aarch64 for maximum compatibility with older/embedded hardware." 0)

    if (NO_ARMV81_OR_HIGHER)
        # crc32 is optional in v8.0 and mandatory in v8.1. Enable it as __crc32()* is used in lot's of places and even very old ARM CPUs
        # support it.
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=armv8+crc")
        list(APPEND RUSTFLAGS_CPU "-C" "target_feature=+crc,-neon")
    else ()
        # ARMv8.2 is quite ancient but the lowest common denominator supported by both Graviton 2 and 3 processors [1, 10]. In particular, it
        # includes LSE (made mandatory with ARMv8.1) which provides nice speedups without having to fall back to compat flag
        # "-moutline-atomics" for v8.0 [2, 3, 4] that requires a recent glibc with runtime dispatch helper, limiting our ability to run on
        # old OSs.
        #
        # simd:    NEON, introduced as optional in v8.0, A few extensions were added with v8.1 but it's still not mandatory. Enables the
        #          compiler to auto-vectorize.
        # sve:     Scalable Vector Extensions, introduced as optional in v8.2. Available in Graviton 3 but not in Graviton 2, and most likely
        #          also not in CI machines. Compiler support for autovectorization is rudimentary at the time of writing, see [5]. Can be
        #          enabled one-fine-day (TM) but not now.
        # ssbs:    "Speculative Store Bypass Safe". Optional in v8.0, mandatory in v8.5. Meltdown/spectre countermeasure.
        # crypto:  SHA1, SHA256, AES. Optional in v8.0. In v8.4, further algorithms were added but it's still optional, see [6].
        # dotprod: Scalar vector product (SDOT and UDOT instructions). Probably the most obscure extra flag with doubtful performance benefits
        #          but it has been activated since always, so why not enable it. It's not 100% clear in which revision this flag was
        #          introduced as optional, either in v8.2 [7] or in v8.4 [8].
        # rcpc:    Load-Acquire RCpc Register. Better support of release/acquire of atomics. Good for allocators and high contention code.
        #          Optional in v8.2, mandatory in v8.3 [9]. Supported in Graviton >=2, Azure and GCP instances.
        # bf16:    Bfloat16, a half-precision floating point format developed by Google Brain. Optional in v8.2, mandatory in v8.6.
        #
        # [1]  https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md
        # [2]  https://community.arm.com/arm-community-blogs/b/tools-software-ides-blog/posts/making-the-most-of-the-arm-architecture-in-gcc-10
        # [3]  https://mysqlonarm.github.io/ARM-LSE-and-MySQL/
        # [4]  https://dev.to/aws-builders/large-system-extensions-for-aws-graviton-processors-3eci
        # [5]  https://developer.arm.com/tools-and-software/open-source-software/developer-tools/llvm-toolchain/sve-support
        # [6]  https://developer.arm.com/documentation/100067/0612/armclang-Command-line-Options/-mcpu?lang=en
        # [7]  https://gcc.gnu.org/onlinedocs/gcc/ARM-Options.html
        # [8]  https://developer.arm.com/documentation/102651/a/What-are-dot-product-intructions-
        # [9]  https://developer.arm.com/documentation/dui0801/g/A64-Data-Transfer-Instructions/LDAPR?lang=en
        # [10] https://github.com/aws/aws-graviton-getting-started/blob/main/README.md
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16")
        # Not adding `+v8.2a,+crypto` to rust because it complains about them being unstable
        list(APPEND RUSTFLAGS_CPU "-C" "target_feature=+dotprod,+ssbs,+rcpc,+bf16")
    endif ()

    # Best-effort check: The build generates and executes intermediate binaries, e.g. protoc and llvm-tablegen. If we build on ARM for ARM
    # and the build machine is too old, i.e. doesn't satisfy above modern profile, then these intermediate binaries will not run (dump
    # SIGILL). Even if they could run, the build machine wouldn't be able to run the ClickHouse binary. In that case, suggest to run the
    # build with the compat profile.
    if (OS_LINUX AND CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*|ARM64.*)" AND NOT NO_ARMV81_OR_HIGHER)
        # CPU features in /proc/cpuinfo and compiler flags don't align :( ... pick an obvious flag contained in the modern but not in the
        # legacy profile (full Graviton 3 /proc/cpuinfo is "fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm
        # jscvt fcma lrcpc dcpop sha3 sm3 sm4 asimddp sha512 sve asimdfhm dit uscat ilrcpc flagm ssbs paca pacg dcpodp svei8mm svebf16 i8mm
        # bf16 dgh rng")
        execute_process(
            COMMAND grep -P "^(?=.*atomic)" /proc/cpuinfo
            OUTPUT_VARIABLE FLAGS)
        if (NOT FLAGS)
            MESSAGE(FATAL_ERROR "The build machine does not satisfy the minimum CPU requirements, try to run cmake with -DNO_ARMV81_OR_HIGHER=1")
        endif()
    endif()

elseif (ARCH_PPC64LE)
    # By Default, build for power8 and up, allow building for power9 and up
    # Note that gcc and clang have support for x86 SSE2 intrinsics when building for PowerPC
    option (POWER9 "Build for Power 9 CPU and above" 0)
    if(POWER9)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -maltivec -mcpu=power9 -D__SSE2__=1 -DNO_WARN_X86_INTRINSICS")
        list(APPEND RUSTFLAGS_CPU "-C" "target_feature=+power9-altivec")
    else ()
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -maltivec -mcpu=power8 -D__SSE2__=1 -DNO_WARN_X86_INTRINSICS")
        list(APPEND RUSTFLAGS_CPU "-C" "target_feature=+power8-altivec")
    endif ()

elseif (ARCH_AMD64)
    # x86-64 microarchitecture levels (https://en.wikipedia.org/wiki/X86-64#Microarchitecture_levels):
    #   1 — SSE2 baseline, maximum compatibility with older/embedded hardware
    #   2 — SSE4.2, SSSE3, POPCNT (default, matches ClickHouse's historical baseline)
    #   3 — AVX2, BMI1/2, FMA, F16C etc.
    #   4 — AVX-512F/BW/CD/DQ/VL
    set (X86_ARCH_LEVEL "2" CACHE STRING "x86-64 microarchitecture level (1, 2, 3, 4)")
    set_property (CACHE X86_ARCH_LEVEL PROPERTY STRINGS "1" "2" "3" "4")

    if (NOT X86_ARCH_LEVEL MATCHES "^[1-4]$")
        message (FATAL_ERROR "X86_ARCH_LEVEL must be one of: 1, 2, 3, 4 (got '${X86_ARCH_LEVEL}')")
    endif ()

    # Same best-effort check for x86 as above for ARM.
    if (OS_LINUX AND CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "amd64|x86_64" AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 2)
        # Test for flags in the default v2 profile.
        execute_process(
            COMMAND grep -P "^(?=.*ssse3)(?=.*sse4_1)(?=.*sse4_2)" /proc/cpuinfo
            OUTPUT_VARIABLE FLAGS)
        if (NOT FLAGS)
            MESSAGE(FATAL_ERROR "The build machine does not satisfy the minimum CPU requirements, try to run cmake with -DX86_ARCH_LEVEL=1")
        endif()
    endif()

    # ClickHouse can be cross-compiled (e.g. on an ARM host for x86) but it is also possible to build ClickHouse on x86 w/o AVX for x86 w/
    # AVX. We only assume that the compiler can emit certain SIMD instructions, we don't care if the host system is able to run the binary.

    if (X86_ARCH_LEVEL VERSION_GREATER_EQUAL 2)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=x86-64-v${X86_ARCH_LEVEL}")
        list (APPEND RUSTFLAGS_CPU "-C" "target-cpu=x86-64-v${X86_ARCH_LEVEL}")

        # PCLMULQDQ is not formally part of any psABI microarchitecture level but ClickHouse's baseline has always included it and
        # third-party dependencies (zlib-ng, RocksDB) rely on it. All CPUs that support x86-64-v2 also support PCLMULQDQ.
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -mpclmul")
        list (APPEND RUSTFLAGS_CPU "-C" "target-feature=+pclmulqdq")
    endif ()

else ()
    # RISC-V + exotic platforms
endif ()
