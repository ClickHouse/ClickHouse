#if !defined(__ASSEMBLER__) || !defined(DWYU_ASSEMBLY_CALLER)
#error This fragment requires its assembly caller
#endif
#include <asm_api.h>
.section .rodata
.balign 4
.Lassembly_fragment:
.long DWYU_ASM_API_VALUE
