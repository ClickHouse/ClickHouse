# To Generate crc32_constants.h 

- Run make file in `../crc32-vpmsum` directory using following options and CRC polynomial. These options should use the same polynomial and order used by intel intrinisic functions
```bash
make crc32_constants.h CRC="0x11EDC6F41" OPTIONS="-x -r -c"
```
- move the generated `crc32_constants.h` into this directory
- To understand more about this go here: https://masterchef2209.wordpress.com/2020/06/17/guide-to-intel-sse4-2-crc-intrinisics-implementation-for-simde/
- Here is the link to information about intel intrinsic functions: https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_mm_crc32_u64&ig_expand=1492,1493,1559
