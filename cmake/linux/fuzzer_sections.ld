/* SanitizerCoverage's PC table (`__sancov_pcs`) is reached only through absolute pointers and GOT-indirect section
   bounds, so it may lie outside `R_X86_64_PC32` reach of `.text`; after `.bss` it no longer adds to the distance
   between code at the start of `.text` and the static data in `.bss`. */
SECTIONS
{
    __sancov_pcs : { *(__sancov_pcs) }
}
INSERT AFTER .bss;
