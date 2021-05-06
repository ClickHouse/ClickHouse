DENSITY
========
Superfast compression library

DENSITY is a free C99, open-source, BSD licensed compression library.

It is focused on high-speed compression, at the best ratio possible. **All three** of DENSITY's algorithms are currently at the **pareto frontier** of compression speed vs ratio (cf. [here](https://github.com/inikep/lzbench/blob/master/lzbench18_sorted.md) for an independent benchmark).

DENSITY features a simple API to enable quick integration in any project.

Branch|Linux & MacOS|Windows
--- | --- | ---
master|[![Build Status](https://travis-ci.org/k0dai/density.svg?branch=master)](https://travis-ci.org/k0dai/density)|[![Build status](https://ci.appveyor.com/api/projects/status/rf7x3x829il72cii/branch/master?svg=true)](https://ci.appveyor.com/project/gpnuma/density/branch/master)
dev|[![Build Status](https://travis-ci.org/k0dai/density.svg?branch=dev)](https://travis-ci.org/k0dai/density)|[![Build status](https://ci.appveyor.com/api/projects/status/rf7x3x829il72cii/branch/dev?svg=true)](https://ci.appveyor.com/project/gpnuma/density/branch/dev)

Why is it so fast ?
-------------------

One of the biggest assets of DENSITY is that its work unit is **not a byte** like other libraries, but **a group of 4 bytes**.

When other libraries consume one byte of data and then apply an algorithmic processing to it, DENSITY consumes 4 bytes and then applies its algorithmic processing.

That's why DENSITY's algorithms were designed from scratch. They have to alleviate for 4-byte work units and still provide interesting compression ratios.

**Speed pedigree traits**

*   4-byte work units
*   heavy use of registers as opposed to memory for processing
*   avoidance of or use of minimal branching when possible
*   use of low memory data structures to favor processor cache Lx accesses
*   library wide inlining
*   specific unrollings
*   prefetching and branching hints
*   restricted pointers to maximize compiler optimizations

A "blowup protection" is provided, dramatically increasing the processing speed of incompressible input data. Also, the output, compressed data size will **never exceed** the original uncompressed data size by more than 1% in case of incompressible, reasonably-sized inputs.

Benchmarks
----------

**Quick benchmark**

DENSITY features an **integrated in-memory benchmark**. After building the project (see [build](#build)), a *benchmark* executable will be present in the build directory. If run without arguments, usage help will be displayed.

File used : enwik8 (100 MB)

Platform : MacBook Pro, MacOS 10.13.3, 2.3 GHz Intel Core i7, 8Gb 1600 MHz DDR, SSD, compiling with Clang/LLVM 9.0.0

Timing : using the *time* function, and taking the best *user* output after multiple runs. In the case of density, the in-memory integrated benchmark's best value (which uses the same usermode CPU timing) is used.

<sub>Library</sub>|<sub>Algorithm</sub>|<sub>Compress</sub>|<sub>Decompress</sub>|<sub>Size</sub>|<sub>Ratio</sub>|<sub>Round trip</sub>
---|---|---|---|---|---|---
<sub>**density** 0.14.2</sub>|<sub>Chameleon</sub>|<sub>0.092s (1085 MB/s)</sub>|<sub>0.059s (1684 MB/s)</sub>|<sub>61 524 084</sub>|<sub>61,52%</sub>|<sub>0.151s</sub>
<sub>lz4 r129</sub>|<sub>-1</sub>|<sub>0.468s (214 MB/s)</sub>|<sub>0.115s (870 MB/s)</sub>|<sub>57 285 990</sub>|<sub>57,29%</sub>|<sub>0.583s</sub>
<sub>lzo 2.08</sub>|<sub>-1</sub>|<sub>0.367s (272 MB/s)</sub>|<sub>0.309s (324 MB/s)</sub>|<sub>56 709 096</sub>|<sub>56,71%</sub>|<sub>0.676s</sub>
<sub>**density** 0.14.2</sub>|<sub>Cheetah</sub>|<sub>0.170s (587 MB/s)</sub>|<sub>0.126s (796 MB/s)</sub>|<sub>53 156 668</sub>|<sub>53,16%</sub>|<sub>0.296s</sub>
<sub>**density** 0.14.2</sub>|<sub>Lion</sub>|<sub>0.303s (330 MB/s)</sub>|<sub>0.288s (347 MB/s)</sub>|<sub>47 817 692</sub>|<sub>47,82%</sub>|<sub>0.591s</sub>
<sub>lz4 r129</sub>|<sub>-3</sub>|<sub>1.685s (59 MB/s)</sub>|<sub>0.118s (847 MB/s)</sub>|<sub>44 539 940</sub>|<sub>44,54%</sub>|<sub>1.803s</sub>
<sub>lzo 2.08</sub>|<sub>-7</sub>|<sub>9.562s (10 MB/s)</sub>|<sub>0.319s (313 MB/s)</sub>|<sub>41 720 721</sub>|<sub>41,72%</sub>|<sub>9.881s</sub>

**Other benchmarks**

Here are a few other benchmarks featuring DENSITY (non exhaustive list) :

*   [**squash**](https://github.com/quixdb/squash) is an abstraction layer for compression algorithms, and has an extremely exhaustive set of benchmark results, including density's, [available here](https://quixdb.github.io/squash-benchmark/?dataset=dickens&machine=s-desktop).

*   [**lzbench**](https://github.com/inikep/lzbench) is an in-memory benchmark of open-source LZ77/LZSS/LZMA compressors.

*   [**fsbench**](https://github.com/gpnuma/fsbench-density) is a command line utility that enables real-time testing of compression algorithms, but also hashes and much more. A fork with density releases is [available here](https://github.com/gpnuma/fsbench-density) for easy access.
The original author's repository [can be found here](https://chiselapp.com/user/Justin_be_my_guide/repository/fsbench/).

Build
-----
DENSITY can be built on a number of platforms, via the provided makefiles.

It was developed and optimized against Clang/LLVM which makes it the preferred compiler, but GCC and MSVC are also supported. Please use the latest compiler versions for best performance.

**MacOS**

On MacOS, Clang/LLVM is the default compiler, which makes things simpler.

1) Get the source code :

```
    git clone https://github.com/k0dai/density.git
    cd density
```

2) Build and test :

```
    make
    build/benchmark -f
```

Alternatively, thanks to the [Homebrew project](https://brew.sh), DENSITY can also be installed with a single command on MacOS:

```
    brew install density
```

**Linux**

On Linux, Clang/LLVM is not always available by default, but can be easily added thanks to the provided package managers.
The following example assumes a Debian or Ubuntu distribution with *apt-get*.

1) From the command line, install Clang/LLVM (*optional*, GCC is also supported if Clang/LLVM can't be used) and other prerequisites.

```
    sudo apt-get install clang git
```

2) Get the source code :

```
    git clone https://github.com/k0dai/density.git
    cd density
```

3) Build and test :

```
    make
```
or
```
    make CC=gcc-... AR=gcc-ar-...
```
or
```
    make CC=clang-... AR=llvm-ar-...
```
to choose alternative compilers. For a quick test of resulting binaries, run
```
    build/benchmark -f
```

**Windows**

Please install [git for Windows](https://git-scm.com/download/win) to begin with.

On Windows, density can be built in different ways.
The **first method** is to use mingw's gcc compiler; for that it is necessary to download and install [mingw-w64](https://sourceforge.net/projects/mingw-w64/).

1) Once mingw-w64 is installed, get the source :

```
    git clone https://github.com/k0dai/density.git
    cd density
```

2) Build and test :

```
    mingw32-make.exe
    build/benchmark.exe -f
```

As an alternative, [MSYS2](http://www.msys2.org/) also offers a linux-like environment for Windows.

The **second method** is to download and install Microsoft's [Visual Studio IDE community edition](https://www.visualstudio.com/thank-you-downloading-visual-studio/?sku=Community). It comes with Microsoft's own compilers and is free.

1) Once Visual Studio is installed, open a [developer command prompt](https://docs.microsoft.com/en-us/dotnet/framework/tools/developer-command-prompt-for-vs) and type :

```
    git clone https://github.com/k0dai/density.git
    cd density\msvc
```

2) Build and test :

```
    msbuild Density.sln
    bin\Release\benchmark.exe -f
```

An extra **recommended step** would be to install *Clang/LLVM* for Windows. It is downloadable from [this link](http://releases.llvm.org/5.0.1/LLVM-5.0.1-win64.exe). Once installed, open the Visual Studio IDE by double-clicking on *Density.sln*, then right-click on project names and change the platform toolsets to *LLVM*. Rebuild the solution to generate binaries with Clang/LLVM.

Output format
-------------
DENSITY outputs compressed data in a simple format, which enables file storage and optional parallelization for both compression and decompression.

A very short header holding vital informations (like DENSITY version and algorithm used) precedes the binary compressed data.

APIs
----
DENSITY features a straightforward *API*, simple yet powerful enough to keep users' creativity unleashed.

For advanced developers, it allows use of custom dictionaries and exportation of generated dictionaries after a compression session. Although using the default, blank dictionary is perfectly fine in most cases, setting up your own, tailored dictionaries could somewhat improve compression ratio especially for low sized input datum.

Please see the [*quick start*](#quick-start-a-simple-example-using-the-api) at the bottom of this page.

About the algorithms
--------------------

**Chameleon** ( *DENSITY_ALGORITHM_CHAMELEON* )

Chameleon is a dictionary lookup based compression algorithm. It is designed for absolute speed and usually reaches a 60% compression ratio on compressible data.
Decompression is just as fast. This algorithm is a great choice when main concern is speed.

**Cheetah** ( *DENSITY_ALGORITHM_CHEETAH* )

Cheetah was developed with inputs from [Piotr Tarsa](https://github.com/tarsa).
It is derived from chameleon and uses swapped double dictionary lookups and predictions. It can be extremely good with highly compressible data (ratio reaching 10% or less).
On typical compressible data compression ratio is about 50% or less. It is still extremely fast for both compression and decompression and is a great, efficient all-rounder algorithm.

**Lion** ( *DENSITY_ALGORITHM_LION* )

Lion is a multiform compression algorithm derived from cheetah. It goes further in the areas of dynamic adaptation and fine-grained analysis.
It uses multiple swapped dictionary lookups and predictions, and forms rank entropy coding.
Lion provides the best compression ratio of all three algorithms under any circumstance, and is still very fast.

Quick start (a simple example using the API)
--------------------------------------------
Using DENSITY in your application couldn't be any simpler.

First you need to include this file in your project :

*   density_api.h

When this is done you can start using the **DENSITY API** :

```C
    #include <string.h>
    #include "density_api.h"

    char* text = "This is a simple example on how to use the simple Density API.  This is a simple example on how to use the simple Density API.";
    uint64_t text_length = (uint64_t)strlen(text);

    // Determine safe buffer sizes
    uint_fast64_t compress_safe_size = density_compress_safe_size(text_length);
    uint_fast64_t decompress_safe_size = density_decompress_safe_size(text_length);

    // Allocate required memory
    uint8_t *outCompressed   = malloc(compress_safe_size * sizeof(char));
    uint8_t *outDecompressed = malloc(decompress_safe_size * sizeof(char));
    density_processing_result result;

    // Compress
    result = density_compress(text, text_length, outCompressed, compress_safe_size, DENSITY_COMPRESSION_MODE_CHAMELEON_ALGORITHM);
    if(!result.state)
        printf("Compressed %llu bytes to %llu bytes\n", result.bytesRead, result.bytesWritten);

    // Decompress
    result = density_decompress(outCompressed, result.bytesWritten, outDecompressed, decompress_safe_size);
    if(!result.state)
        printf("Decompressed %llu bytes to %llu bytes\n", result.bytesRead, result.bytesWritten);

    // Free memory_allocated
    free(outCompressed);
    free(outDecompressed);
```

And that's it ! We've done a compression/decompression round trip with a few lines !

Related projects
----------------

*   **SHARC** (archiver using density algorithms) [https://github.com/gpnuma/sharc](https://github.com/gpnuma/sharc)
*   **fsbench-density** (in-memory transformations benchmark) [https://github.com/gpnuma/fsbench-density](https://github.com/gpnuma/fsbench-density)
*   **densityxx** (c++ port of density) [https://github.com/charlesw1234/densityxx](https://github.com/charlesw1234/densityxx)
