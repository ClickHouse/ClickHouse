This doc gives specfic build tips for QPL_Deflate codec based on Clickhouse generic [build instructions](https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/development/build.md)

Several tips to build Clickhouse with QPL_deflate enabled:
- The QPL build only support x86_64 platform with avx2/avx512 support.
- The QPL build requires nasm 2.15.0 or higher (e.g., can be obtained from https://www.nasm.us)
- The QPL requires C++ compiler with C++17 standard support.
- Pass the following flag to CMake. Which one to choose depends on your target hardware supported with AVX2 or AVX512.
``` bash
cmake -DENABLE_AVX2=1 -DENABLE_QPL=1 ..
```
or
``` bash
cmake -DENABLE_AVX512=1 -DENABLE_QPL=1 ..
```