/// One byte that cmake/linux/static_pie_pad.ld places 4 GiB above the rest of the image, so that
/// the kernel reserves that much address space when it maps a static PIE and the code ends up
/// well below the top of the user address space. See the linker script for the reason.
/// `used` keeps the section even though nothing references it; the object is compiled without LTO.
__attribute__((section(".clickhouse.pad"), used)) const char clickhouse_static_pie_pad = 0;
