/* An empty executable section, which the linker script `text_alignment.ld` puts in front of `.text` to start
   the code at a multiple of 2 MiB (see the explanation there). The script cannot do it alone: an output section
   without input sections is not executable, so it would belong to the read-only segment and pad the file. */
.section .text_alignment,"ax",%progbits
