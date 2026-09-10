# compiler-rt builtins source list (paths relative to lib/builtins/).
#
# Source: contrib/llvm-project/compiler-rt/lib/builtins/CMakeLists.txt
#   - GENERIC_SOURCES (lines 72-193 in upstream)
#   - GENERIC_TF_SOURCES (lines 204-231)
#   - x86_80_BIT_SOURCES (lines 304-321)
#   - BF16_SOURCES (lines 196-202)
#
# Files we always include (the upstream cmake gates on platform booleans we
# treat as true here):
#   * emutls.c, enable_execute_stack.c, eprintf.c — guarded by
#     "NOT FUCHSIA AND NOT COMPILER_RT_BAREMETAL_BUILD AND NOT COMPILER_RT_GPU_BUILD"
#   * gcc_personality_v0.c — guarded by HAVE_UNWIND_H (true on Linux/FreeBSD)
#   * clear_cache.c — guarded by "NOT FUCHSIA"
# We don't include:
#   * atomic.c — gated by COMPILER_RT_HAS_ATOMIC_KEYWORD AND
#     NOT COMPILER_RT_EXCLUDE_ATOMIC_BUILTIN; the latter defaults to ON.
#   * Apple-only atomic_flag_*.c

# Generic sources (architecture-independent)
set(BUILTINS_GENERIC_SOURCES
    absvdi2.c
    absvsi2.c
    absvti2.c
    adddf3.c
    addsf3.c
    addvdi3.c
    addvsi3.c
    addvti3.c
    apple_versioning.c
    ashldi3.c
    ashlti3.c
    ashrdi3.c
    ashrti3.c
    bswapdi2.c
    bswapsi2.c
    clzdi2.c
    clzsi2.c
    clzti2.c
    cmpdi2.c
    cmpti2.c
    comparedf2.c
    comparesf2.c
    ctzdi2.c
    ctzsi2.c
    ctzti2.c
    divdc3.c
    divdf3.c
    divdi3.c
    divmoddi4.c
    divmodsi4.c
    divmodti4.c
    divsc3.c
    divsf3.c
    divsi3.c
    divti3.c
    extendsfdf2.c
    extendhfsf2.c
    extendhfdf2.c
    ffsdi2.c
    ffssi2.c
    ffsti2.c
    fixdfdi.c
    fixdfsi.c
    fixdfti.c
    fixsfdi.c
    fixsfsi.c
    fixsfti.c
    fixunsdfdi.c
    fixunsdfsi.c
    fixunsdfti.c
    fixunssfdi.c
    fixunssfsi.c
    fixunssfti.c
    floatdidf.c
    floatdisf.c
    floatsidf.c
    floatsisf.c
    floattidf.c
    floattisf.c
    floatundidf.c
    floatundisf.c
    floatunsidf.c
    floatunsisf.c
    floatuntidf.c
    floatuntisf.c
    fp_mode.c
    int_util.c
    lshrdi3.c
    lshrti3.c
    moddi3.c
    modsi3.c
    modti3.c
    muldc3.c
    muldf3.c
    muldi3.c
    mulodi4.c
    mulosi4.c
    muloti4.c
    mulsc3.c
    mulsf3.c
    multi3.c
    mulvdi3.c
    mulvsi3.c
    mulvti3.c
    negdf2.c
    negdi2.c
    negsf2.c
    negti2.c
    negvdi2.c
    negvsi2.c
    negvti2.c
    os_version_check.c
    paritydi2.c
    paritysi2.c
    parityti2.c
    popcountdi2.c
    popcountsi2.c
    popcountti2.c
    powidf2.c
    powisf2.c
    subdf3.c
    subsf3.c
    subvdi3.c
    subvsi3.c
    subvti3.c
    trampoline_setup.c
    truncdfhf2.c
    truncdfsf2.c
    truncsfhf2.c
    ucmpdi2.c
    ucmpti2.c
    udivdi3.c
    udivmoddi4.c
    udivmodsi4.c
    udivmodti4.c
    udivsi3.c
    udivti3.c
    umoddi3.c
    umodsi3.c
    umodti3.c
    # Non-fuchsia, non-baremetal additions (always true for ClickHouse)
    emutls.c
    enable_execute_stack.c
    eprintf.c
    # Has unwind.h (always true for Linux/FreeBSD)
    gcc_personality_v0.c
    # Non-fuchsia
    clear_cache.c
)

# 128-bit floating point (long double on non-x86 architectures, __float128 on x86_64)
set(BUILTINS_GENERIC_TF_SOURCES
    addtf3.c
    comparetf2.c
    divtc3.c
    divtf3.c
    extenddftf2.c
    extendhftf2.c
    extendsftf2.c
    fixtfdi.c
    fixtfsi.c
    fixtfti.c
    fixunstfdi.c
    fixunstfsi.c
    fixunstfti.c
    floatditf.c
    floatsitf.c
    floattitf.c
    floatunditf.c
    floatunsitf.c
    floatuntitf.c
    multc3.c
    multf3.c
    powitf2.c
    subtf3.c
    trunctfdf2.c
    trunctfhf2.c
    trunctfsf2.c
)

# 80-bit extended precision (x86 long double)
set(BUILTINS_X86_80_BIT_SOURCES
    divxc3.c
    extendhfxf2.c
    extendxftf2.c
    fixxfdi.c
    fixxfti.c
    fixunsxfdi.c
    fixunsxfsi.c
    fixunsxfti.c
    floatdixf.c
    floattixf.c
    floatundixf.c
    floatuntixf.c
    mulxc3.c
    powixf2.c
    trunctfxf2.c
    truncxfhf2.c
)

# BFloat16 sources (conditionally compiled if __bf16 is supported)
set(BUILTINS_BF16_SOURCES
    extendbfsf2.c
    truncdfbf2.c
    truncxfbf2.c
    truncsfbf2.c
    trunctfbf2.c
)
