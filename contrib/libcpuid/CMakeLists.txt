add_library(cpuid
include/cpuid/asm-bits.c
include/cpuid/cpuid_main.c
include/cpuid/libcpuid_util.c
include/cpuid/rdtsc.c
include/cpuid/recog_amd.c
include/cpuid/recog_intel.c

include/cpuid/asm-bits.h
include/cpuid/config.h
include/cpuid/libcpuid_constants.h
include/cpuid/libcpuid.h
include/cpuid/libcpuid_types.h
include/cpuid/libcpuid_util.h
include/cpuid/rdtsc.h
include/cpuid/recog_amd.h
include/cpuid/recog_intel.h
)

target_include_directories (cpuid PUBLIC include)
