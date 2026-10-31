/* Keep the code of a static PIE out of the top 2 GiB of the user address space.

   A static PIE has no PT_INTERP, so the kernel does not load it at ELF_ET_DYN_BASE like a dynamic PIE
   (0x555555554000 + ASLR offset on x86_64): fs/binfmt_elf.c gives it load_bias = 0 and lets mmap pick
   the address, which is the top-down mmap area right under the stack gap. With ASLR disabled (the
   performance comparison runs servers under `setarch -R`, and so does gdb) the whole image therefore
   sits in the top 2 GiB (0x7fff80000000 - 0x7fffffffffff on x86_64).

   On Intel Sapphire Rapids code in that range executes markedly slower: the decoded instruction cache
   is mostly ineffective there (IDQ.MITE_UOPS 35% instead of 2% of all uops, DSB2MITE_SWITCHES.PENALTY_CYCLES
   8-10x higher), which cost 15-20% on tight loops in the ClickHouse performance tests. The same binary
   loaded 2 GiB lower, or anywhere else, is as fast as the glibc build.

   The kernel maps the span from the lowest to the highest PT_LOAD at once, so a one-byte read-only
   section 4 GiB above everything else makes it reserve 4 GiB more and lands the code 4 GiB lower (4 GiB
   rather than 2 GiB so that the section stays above the image even for the largest sanitizer builds and
   for non-PIE executables based at 0x200000; any distance beyond 2 GiB performs the same). The only cost
   is one extra page mapped 4 GiB above the image. With ASLR enabled the mmap base is already random
   (and almost never in that range), so nothing changes there.

   The section's single byte comes from cmake/linux/static_pie_pad.c; a section without an input section
   would be folded into the writable segment and turn the gap into 4 GiB of `.bss`. */
SECTIONS
{
    .clickhouse.pad 0x100000000 : { *(.clickhouse.pad) }
}
INSERT AFTER .bss;
