#ifndef __ASSEMBLER__
#error assembler-with-cpp required
#endif
#include <asm_api.h>
.section .rodata
.balign 4
.Lassembly_dual:
.long DWYU_ASM_API_VALUE
