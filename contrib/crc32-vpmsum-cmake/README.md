# To Generate crc32_constants.h 

- Run make file in `../crc32-vpmsum` diretory using folling options and CRC polynomial. These options should use the same polynomial and order used by intel intrinisic functions
```bash
make crc32_constants.h CRC="0x11EDC6F41" OPTIONS="-x -r -c"
```
- move the generated `crc32_constants.h` into this directory
- To understand more about this go here: https://masterchef2209.wordpress.com/2020/06/17/guide-to-intel-sse4-2-crc-intrinisics-implementation-for-simde/
