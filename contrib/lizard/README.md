Lizard - efficient compression with very fast decompression
--------------------------------------------------------

Lizard (formerly LZ5) is a lossless compression algorithm which contains 4 compression methods:
- fastLZ4 : compression levels -10...-19 are designed to give better decompression speed than [LZ4] i.e. over 2000 MB/s
- LIZv1 : compression levels -20...-29 are designed to give better ratio than [LZ4] keeping 75% decompression speed
- fastLZ4 + Huffman : compression levels -30...-39 add Huffman coding to fastLZ4
- LIZv1 + Huffman : compression levels -40...-49 give the best ratio (comparable to [zlib] and low levels of [zstd]/[brotli]) at decompression speed of 1000 MB/s 

Lizard library is based on frequently used [LZ4] library by Yann Collet but the Lizard compression format is not compatible with LZ4.
Lizard library is provided as open-source software using BSD 2-Clause license.
The high compression/decompression speed is achieved without any SSE and AVX extensions.


|Branch      |Status   |
|------------|---------|
|lz5_v1.5    | [![Build Status][travis15Badge]][travisLink]    [![Build status][Appveyor15Badge]][AppveyorLink]     |
|lizard      | [![Build Status][travis20Badge]][travisLink]    [![Build status][Appveyor20Badge]][AppveyorLink]     |

[travis15Badge]: https://travis-ci.org/inikep/lizard.svg?branch=lz5_v1.5 "Continuous Integration test suite"
[travis20Badge]: https://travis-ci.org/inikep/lizard.svg?branch=lizard "Continuous Integration test suite"
[travisLink]: https://travis-ci.org/inikep/lizard
[Appveyor15Badge]: https://ci.appveyor.com/api/projects/status/cqw7emcuqge369p0/branch/lz5_v1.5?svg=true "Visual test suite"
[Appveyor20Badge]: https://ci.appveyor.com/api/projects/status/cqw7emcuqge369p0/branch/lizard?svg=true "Visual test suite"
[AppveyorLink]: https://ci.appveyor.com/project/inikep/lizard
[LZ4]: https://github.com/lz4/lz4
[zlib]: https://github.com/madler/zlib
[zstd]: https://github.com/facebook/zstd
[brotli]: https://github.com/google/brotli


Benchmarks
-------------------------

The following results are obtained with [lzbench](https://github.com/inikep/lzbench) and `-t16,16`
using 1 core of Intel Core i5-4300U, Windows 10 64-bit (MinGW-w64 compilation under gcc 6.2.0)
with [silesia.tar] which contains tarred files from [Silesia compression corpus](http://sun.aei.polsl.pl/~sdeor/index.php?page=silesia).

| Compressor name         | Compression| Decompress.| Compr. size | Ratio |
| ---------------         | -----------| -----------| ----------- | ----- |
| memcpy                  |  7332 MB/s |  8719 MB/s |   211947520 |100.00 |
| lz4 1.7.3               |   440 MB/s |  2318 MB/s |   100880800 | 47.60 |
| lz4hc 1.7.3 -1          |    98 MB/s |  2121 MB/s |    87591763 | 41.33 |
| lz4hc 1.7.3 -4          |    55 MB/s |  2259 MB/s |    79807909 | 37.65 |
| lz4hc 1.7.3 -9          |    22 MB/s |  2315 MB/s |    77892285 | 36.75 |
| lz4hc 1.7.3 -12         |    17 MB/s |  2323 MB/s |    77849762 | 36.73 |
| lz4hc 1.7.3 -16         |    10 MB/s |  2323 MB/s |    77841782 | 36.73 |
| lizard 1.0 -10          |   346 MB/s |  2610 MB/s |   103402971 | 48.79 |
| lizard 1.0 -12          |   103 MB/s |  2458 MB/s |    86232422 | 40.69 |
| lizard 1.0 -15          |    50 MB/s |  2552 MB/s |    81187330 | 38.31 |
| lizard 1.0 -19          |  3.04 MB/s |  2497 MB/s |    77416400 | 36.53 |
| lizard 1.0 -21          |   157 MB/s |  1795 MB/s |    89239174 | 42.10 |
| lizard 1.0 -23          |    30 MB/s |  1778 MB/s |    81097176 | 38.26 |
| lizard 1.0 -26          |  6.63 MB/s |  1734 MB/s |    74503695 | 35.15 |
| lizard 1.0 -29          |  1.37 MB/s |  1634 MB/s |    68694227 | 32.41 |
| lizard 1.0 -30          |   246 MB/s |   909 MB/s |    85727429 | 40.45 |
| lizard 1.0 -32          |    94 MB/s |  1244 MB/s |    76929454 | 36.30 |
| lizard 1.0 -35          |    47 MB/s |  1435 MB/s |    73850400 | 34.84 |
| lizard 1.0 -39          |  2.94 MB/s |  1502 MB/s |    69807522 | 32.94 |
| lizard 1.0 -41          |   126 MB/s |   961 MB/s |    76100661 | 35.91 |
| lizard 1.0 -43          |    28 MB/s |  1101 MB/s |    70955653 | 33.48 |
| lizard 1.0 -46          |  6.25 MB/s |  1073 MB/s |    65413061 | 30.86 |
| lizard 1.0 -49          |  1.27 MB/s |  1064 MB/s |    60679215 | 28.63 |
| zlib 1.2.8 -1           |    66 MB/s |   244 MB/s |    77259029 | 36.45 |
| zlib 1.2.8 -6           |    20 MB/s |   263 MB/s |    68228431 | 32.19 |
| zlib 1.2.8 -9           |  8.37 MB/s |   266 MB/s |    67644548 | 31.92 |
| zstd 1.1.1 -1           |   235 MB/s |   645 MB/s |    73659468 | 34.75 |
| zstd 1.1.1 -2           |   181 MB/s |   600 MB/s |    70168955 | 33.11 |
| zstd 1.1.1 -5           |    88 MB/s |   565 MB/s |    65002208 | 30.67 |
| zstd 1.1.1 -8           |    31 MB/s |   619 MB/s |    61026497 | 28.79 |
| zstd 1.1.1 -11          |    16 MB/s |   613 MB/s |    59523167 | 28.08 |
| zstd 1.1.1 -15          |  4.97 MB/s |   639 MB/s |    58007773 | 27.37 |
| zstd 1.1.1 -18          |  2.87 MB/s |   583 MB/s |    55294241 | 26.09 |
| zstd 1.1.1 -22          |  1.44 MB/s |   505 MB/s |    52731930 | 24.88 |
| brotli 0.5.2 -0         |   217 MB/s |   244 MB/s |    78226979 | 36.91 |
| brotli 0.5.2 -2         |    96 MB/s |   283 MB/s |    68066621 | 32.11 |
| brotli 0.5.2 -5         |    24 MB/s |   312 MB/s |    60801716 | 28.69 |
| brotli 0.5.2 -8         |  5.56 MB/s |   324 MB/s |    57382470 | 27.07 |
| brotli 0.5.2 -11        |  0.39 MB/s |   266 MB/s |    51138054 | 24.13 |

[silesia.tar]: https://drive.google.com/file/d/0BwX7dtyRLxThenZpYU9zLTZhR1k/view?usp=sharing


Documentation
-------------------------

The raw Lizard block compression format is detailed within [lizard_Block_format].

To compress an arbitrarily long file or data stream, multiple blocks are required.
Organizing these blocks and providing a common header format to handle their content
is the purpose of the Frame format, defined into [lizard_Frame_format].
Interoperable versions of Lizard must respect this frame format.

[lizard_Block_format]: doc/lizard_Block_format.md
[lizard_Frame_format]: doc/lizard_Frame_format.md
