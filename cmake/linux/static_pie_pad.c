/// One byte that cmake/linux/static_pie_pad.ld places 4 GiB above the rest of the image, so that
/// the kernel reserves that much address space when it maps a static PIE and the code ends up
/// well below the top of the user address space. See the linker script for the reason.
/// Nothing references it: `used` only keeps it from being optimized out of the object (compiled without LTO),
/// and `KEEP` in the linker script keeps `--gc-sections` from discarding the section.
__attribute__((section(".clickhouse.pad"), used)) const char clickhouse_static_pie_pad = 0;
