add_library(cpuid
include/libcpuid/asm-bits.c
include/libcpuid/cpuid_main.c
include/libcpuid/libcpuid_util.c
include/libcpuid/rdtsc.c
include/libcpuid/recog_amd.c
include/libcpuid/recog_intel.c

include/libcpuid/asm-bits.h
include/libcpuid/config.h
include/libcpuid/libcpuid_constants.h
include/libcpuid/libcpuid.h
include/libcpuid/libcpuid_types.h
include/libcpuid/libcpuid_util.h
include/libcpuid/rdtsc.h
include/libcpuid/recog_amd.h
include/libcpuid/recog_intel.h
)

target_include_directories (cpuid SYSTEM PUBLIC include)
