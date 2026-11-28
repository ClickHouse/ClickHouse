/* Starts the code of the `clickhouse` binary at an address that is a multiple of 2 MiB relative to the image.

   The speed of the code depends on where it is located relative to the 2 MiB boundaries of the address space
   (the image is normally loaded at a 2 MiB-aligned address). `.text` follows the read-only data, so without this
   any change of the size of the read-only data that is not a multiple of 2 MiB moves all the code relative to
   these boundaries, and a change that does not touch the hot code makes some queries 15% slower or faster.

   `.text_alignment` is the empty section from `text_alignment.S`. It is placed right before `.text`, the first
   section of the executable segment, so `.text` starts at the same address. The address is set by an expression
   instead of by the alignment of a section, because the largest alignment of the sections of a segment becomes
   the alignment of the segment, and the linker pads the file to keep the file offset of the segment congruent to
   its address modulo that alignment. This way the padding is only in the address space, and the file does not grow.

   A `SECTIONS` command with `INSERT` does not replace the default layout of `lld`, and it does nothing in a link
   without `text_alignment.S`. */
SECTIONS
{
    .text_alignment ALIGN(0x200000) : { KEEP(*(.text_alignment)) }
}
INSERT BEFORE .text;
