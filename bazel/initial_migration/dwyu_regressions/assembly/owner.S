#ifndef __ASSEMBLER__
#error The assembly source must use assembler-with-cpp
#endif
#if defined(DWYU_CONLY_MUST_NOT_REACH_ASM) || defined(DWYU_CXX_MUST_NOT_REACH_ASM)
#error conlyopts and cxxopts must not reach an assembly source
#endif
#if DWYU_COMMON_ASM_VALUE != 73
#error Global/rule copts must reach assembly
#endif
#include <asm_api.h>
.section .rodata
.balign 4
.Lassembly_owner:
.long DWYU_ASM_API_VALUE + DWYU_COMMON_ASM_VALUE
