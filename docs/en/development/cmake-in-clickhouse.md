---
sidebar_position: 69
sidebar_label: CMake in ClickHouse
description: How to make ClickHouse compile and link faster
---

# CMake in ClickHouse

How to make ClickHouse compile and link faster. Minimal ClickHouse build example:

```bash
cmake .. \
    -DCMAKE_C_COMPILER=$(which clang-13) \
    -DCMAKE_CXX_COMPILER=$(which clang++-13) \
    -DCMAKE_BUILD_TYPE=Debug \
    -DENABLE_UTILS=OFF \
    -DENABLE_TESTS=OFF
```

## CMake files types

1. ClickHouse source CMake files (located in the root directory and in /src).
2. Arch-dependent CMake files (located in /cmake/*os_name*).
3. Libraries finders (search for contrib libraries, located in /contrib/*/CMakeLists.txt).
4. Contrib build CMake files (used instead of libraries' own CMake files, located in /cmake/modules)

## List of CMake flags
- The flag name is a link to its position in the code.
- If an option's default value is itself an option, it's also a link to its position in this list.

## ClickHouse modes

<table>
<thead>
<tr>
<th>Name</th>
<th>Default value</th>
<th>Description</th>
<th>Comment</th>
</tr>
</thead>
<tbody>
<tr>
<td><a name="enable-clickhouse-all"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L10" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Enable all ClickHouse modes by default</td>
<td>The <code class="syntax">clickhouse</code> binary is a multi purpose tool that contains multiple execution modes (client, server, etc.), each of them may be built and linked as a separate library. If you do not know what modes you need, turn this option OFF and enable SERVER and CLIENT only.</td>
</tr>
<tr>
<td><a name="enable-clickhouse-benchmark"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L20" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_BENCHMARK</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Queries benchmarking mode</td>
<td><a href="https://clickhouse.com/docs/en/operations/utilities/clickhouse-benchmark/" target="_blank">https://clickhouse.com/docs/en/operations/utilities/clickhouse-benchmark/</a></td>
</tr>
<tr>
<td><a name="enable-clickhouse-client"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L13" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_CLIENT</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Client mode (interactive tui/shell that connects to the server)</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-compressor"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L25" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_COMPRESSOR</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Data compressor and decompressor</td>
<td><a href="https://clickhouse.com/docs/en/operations/utilities/clickhouse-compressor/" target="_blank">https://clickhouse.com/docs/en/operations/utilities/clickhouse-compressor/</a></td>
</tr>
<tr>
<td><a name="enable-clickhouse-copier"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L28" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_COPIER</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Inter-cluster data copying mode</td>
<td><a href="https://clickhouse.com/docs/en/operations/utilities/clickhouse-copier/" target="_blank">https://clickhouse.com/docs/en/operations/utilities/clickhouse-copier/</a></td>
</tr>
<tr>
<td><a name="enable-clickhouse-extract-from-config"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L22" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Configs processor (extract values etc.)</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-format"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L30" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_FORMAT</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Queries pretty-printer and formatter with syntax highlighting</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-git-import"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L50" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_GIT_IMPORT</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>A tool to analyze Git repositories</td>
<td><a href="https://presentations.clickhouse.com/matemarketing_2020/" target="_blank">https://presentations.clickhouse.com/matemarketing_2020/</a></td>
</tr>
<tr>
<td><a name="enable-clickhouse-install"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L67" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_INSTALL</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Install ClickHouse without .deb/.rpm/.tgz packages (having the binary only)</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-keeper"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L54" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_KEEPER</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>ClickHouse alternative to ZooKeeper</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-keeper-converter"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L56" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_KEEPER_CONVERTER</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Util allows to convert ZooKeeper logs and snapshots into clickhouse-keeper snapshot</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-library-bridge"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L46" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_LIBRARY_BRIDGE</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>HTTP-server working like a proxy to Library dictionary source</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-local"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L17" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_LOCAL</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Local files fast processing mode</td>
<td><a href="https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/" target="_blank">https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/</a></td>
</tr>
<tr>
<td><a name="enable-clickhouse-obfuscator"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L34" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_OBFUSCATOR</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Table data obfuscator (convert real data to benchmark-ready one)</td>
<td><a href="https://clickhouse.com/docs/en/operations/utilities/clickhouse-obfuscator/" target="_blank">https://clickhouse.com/docs/en/operations/utilities/clickhouse-obfuscator/</a></td>
</tr>
<tr>
<td><a name="enable-clickhouse-odbc-bridge"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L40" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_ODBC_BRIDGE</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>HTTP-server working like a proxy to ODBC driver</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-server"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L12" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_SERVER</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>Server mode (main mode)</td>
<td></td>
</tr>
<tr>
<td><a name="enable-clickhouse-static-files-disk-uploader"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/programs/CMakeLists.txt#L52" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER</code></a></td>
<td><code class="syntax">ENABLE_CLICKHOUSE_ALL</code></td>
<td>A tool to export table data files to be later put to a static files web server</td>
<td></td>
</tr>
</tbody>
</table>


## External libraries
Note that ClickHouse uses forks of these libraries, see https://github.com/ClickHouse-Extras.
<table>
<thead>
<tr>
<th>Name</th>
<th>Default value</th>
<th>Description</th>
<th>Comment</th>
</tr>
</thead>
<tbody>
<tr>
<td><a name="enable-avx"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L19" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_AVX</code></a></td>
<td><code class="syntax">0</code></td>
<td>Use AVX instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-avx"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L20" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_AVX2</code></a></td>
<td><code class="syntax">0</code></td>
<td>Use AVX2 instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-avx-for-spec-op"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L23" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_AVX2_FOR_SPEC_OP</code></a></td>
<td><code class="syntax">0</code></td>
<td>Use avx2 instructions for specific operations on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-avx"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L21" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_AVX512</code></a></td>
<td><code class="syntax">0</code></td>
<td>Use AVX512 instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-avx-for-spec-op"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L24" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_AVX512_FOR_SPEC_OP</code></a></td>
<td><code class="syntax">0</code></td>
<td>Use avx512 instructions for specific operations on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-bmi"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L22" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_BMI</code></a></td>
<td><code class="syntax">0</code></td>
<td>Use BMI instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-ccache"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/ccache.cmake#L22" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CCACHE</code></a></td>
<td><code class="syntax">ENABLE_CCACHE_BY_DEFAULT</code></td>
<td>Speedup re-compilations using ccache (external tool)</td>
<td><a href="https://ccache.dev/" target="_blank">https://ccache.dev/</a></td>
</tr>
<tr>
<td><a name="enable-clang-tidy"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/clang_tidy.cmake#L2" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CLANG_TIDY</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Use clang-tidy static analyzer</td>
<td><a href="https://clang.llvm.org/extra/clang-tidy/" target="_blank">https://clang.llvm.org/extra/clang-tidy/</a></td>
</tr>
<tr>
<td><a name="enable-pclmulqdq"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L17" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_PCLMULQDQ</code></a></td>
<td><code class="syntax">1</code></td>
<td>Use pclmulqdq instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-popcnt"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L18" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_POPCNT</code></a></td>
<td><code class="syntax">1</code></td>
<td>Use popcnt instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-sse"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L15" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_SSE41</code></a></td>
<td><code class="syntax">1</code></td>
<td>Use SSE4.1 instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-sse"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L16" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_SSE42</code></a></td>
<td><code class="syntax">1</code></td>
<td>Use SSE4.2 instructions on x86_64</td>
<td></td>
</tr>
<tr>
<td><a name="enable-ssse"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L14" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_SSSE3</code></a></td>
<td><code class="syntax">1</code></td>
<td>Use SSSE3 instructions on x86_64</td>
<td></td>
</tr>
</tbody>
</table>


## Other flags

<table>
<thead>
<tr>
<th>Name</th>
<th>Default value</th>
<th>Description</th>
<th>Comment</th>
</tr>
</thead>
<tbody>
<tr>
<td><a name="add-gdb-index-for-gold"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L226" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ADD_GDB_INDEX_FOR_GOLD</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Add .gdb-index to resulting binaries for gold linker.</td>
<td>Ignored if <code class="syntax">lld</code> is used</td>
</tr>
<tr>
<td><a name="arch-native"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/cpu_features.cmake#L26" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ARCH_NATIVE</code></a></td>
<td><code class="syntax">0</code></td>
<td>Add -march=native compiler flag. This makes your binaries non-portable but more performant code may be generated. This option overrides ENABLE_* options for specific instruction set. Highly not recommended to use.</td>
<td></td>
</tr>
<tr>
<td><a name="build-standalone-keeper"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L253" rel="external nofollow noreferrer" target="_blank"><code class="syntax">BUILD_STANDALONE_KEEPER</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Build keeper as small standalone binary</td>
<td></td>
</tr>
<tr>
<td><a name="clickhouse-split-binary"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L113" rel="external nofollow noreferrer" target="_blank"><code class="syntax">CLICKHOUSE_SPLIT_BINARY</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Make several binaries (clickhouse-server, clickhouse-client etc.) instead of one bundled</td>
<td></td>
</tr>
<tr>
<td><a name="compiler-pipe"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L281" rel="external nofollow noreferrer" target="_blank"><code class="syntax">COMPILER_PIPE</code></a></td>
<td><code class="syntax">ON</code></td>
<td>-pipe compiler option</td>
<td>Less <code class="syntax">/tmp</code> usage, more RAM usage.</td>
</tr>
<tr>
<td><a name="enable-build-path-mapping"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L299" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_BUILD_PATH_MAPPING</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Enable remap file source paths in debug info, predefined preprocessor macros and __builtin_FILE(). It's to generate reproducible builds. See <a href="https://reproducible-builds.org/docs/build-path" target="_blank">https://reproducible-builds.org/docs/build-path</a></td>
<td>Reproducible builds If turned <code class="syntax">ON</code>, remap file source paths in debug info, predefined preprocessor macros and __builtin_FILE().</td>
</tr>
<tr>
<td><a name="enable-check-heavy-builds"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L81" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_CHECK_HEAVY_BUILDS</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Don't allow C++ translation units to compile too long or to take too much memory while compiling.</td>
<td>Take care to add prlimit in command line before ccache, or else ccache thinks that prlimit is compiler, and clang++ is its input file, and refuses to work  with multiple inputs, e.g in ccache log: [2021-03-31T18:06:32.655327 36900] Command line: /usr/bin/ccache prlimit --as=10000000000 --data=5000000000 --cpu=600 /usr/bin/clang++-11 - ...... std=gnu++2a -MD -MT src/CMakeFiles/dbms.dir/Storages/MergeTree/IMergeTreeDataPart.cpp.o -MF src/CMakeFiles/dbms.dir/Storages/MergeTree/IMergeTreeDataPart.cpp.o.d -o src/CMakeFiles/dbms.dir/Storages/MergeTree/IMergeTreeDataPart.cpp.o -c ../src/Storages/MergeTree/IMergeTreeDataPart.cpp  [2021-03-31T18:06:32.656704 36900] Multiple input files: /usr/bin/clang++-11 and ../src/Storages/MergeTree/IMergeTreeDataPart.cpp  Another way would be to use --ccache-skip option before clang++-11 to make ccache ignore it.</td>
</tr>
<tr>
<td><a name="enable-colored-build"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L160" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_COLORED_BUILD</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Enable colored diagnostics in build log.</td>
<td></td>
</tr>
<tr>
<td><a name="enable-examples"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L201" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_EXAMPLES</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Build all example programs in 'examples' subdirectories</td>
<td></td>
</tr>
<tr>
<td><a name="enable-fuzzing"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L129" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_FUZZING</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Fuzzy testing using libfuzzer</td>
<td></td>
</tr>
<tr>
<td><a name="enable-libraries"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L413" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_LIBRARIES</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Enable all external libraries by default</td>
<td>Turns on all external libs like s3, kafka, ODBC, ...</td>
</tr>
<tr>
<td><a name="enable-multitarget-code"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/src/Functions/CMakeLists.txt#L102" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_MULTITARGET_CODE</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Enable platform-dependent code</td>
<td>ClickHouse developers may use platform-dependent code under some macro (e.g. <code class="syntax">ifdef ENABLE_MULTITARGET</code>). If turned ON, this option defines such macro. See <code class="syntax">src/Functions/TargetSpecific.h</code></td>
</tr>
<tr>
<td><a name="enable-tests"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L200" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_TESTS</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Provide unit_test_dbms target with Google.Test unit tests</td>
<td>If turned <code class="syntax">ON</code>, assumes the user has either the system GTest library or the bundled one.</td>
</tr>
<tr>
<td><a name="enable-thinlto"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L386" rel="external nofollow noreferrer" target="_blank"><code class="syntax">ENABLE_THINLTO</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Clang-specific link time optimization</td>
<td><a href="https://clang.llvm.org/docs/ThinLTO.html" target="_blank">https://clang.llvm.org/docs/ThinLTO.html</a> Applies to clang only. Disabled when building with tests or sanitizers.</td>
</tr>
<tr>
<td><a name="fail-on-unsupported-options-combination"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L32" rel="external nofollow noreferrer" target="_blank"><code class="syntax">FAIL_ON_UNSUPPORTED_OPTIONS_COMBINATION</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Stop/Fail CMake configuration if some ENABLE_XXX option is defined (either ON or OFF)   but is not possible to satisfy</td>
<td>If turned off: e.g. when ENABLE_FOO is ON, but FOO tool was not found, the CMake will continue.</td>
</tr>
<tr>
<td><a name="glibc-compatibility"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L205" rel="external nofollow noreferrer" target="_blank"><code class="syntax">GLIBC_COMPATIBILITY</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Enable compatibility with older glibc libraries.</td>
<td>Only for Linux, x86_64 or aarch64.</td>
</tr>
<tr>
<td><a name="install-stripped-binaries"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L270" rel="external nofollow noreferrer" target="_blank"><code class="syntax">INSTALL_STRIPPED_BINARIES</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Build stripped binaries with debug info in separate directory</td>
<td></td>
</tr>
<tr>
<td><a name="linker-name"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/tools.cmake#L58" rel="external nofollow noreferrer" target="_blank"><code class="syntax">LINKER_NAME</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Linker name or full path</td>
<td>Example values: <code class="syntax">lld-10</code>, <code class="syntax">gold</code>.</td>
</tr>
<tr>
<td><a name="parallel-compile-jobs"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/limit_jobs.cmake#L10" rel="external nofollow noreferrer" target="_blank"><code class="syntax">PARALLEL_COMPILE_JOBS</code></a></td>
<td><code class="syntax">""</code></td>
<td>Maximum number of concurrent compilation jobs</td>
<td>1 if not set</td>
</tr>
<tr>
<td><a name="parallel-link-jobs"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/limit_jobs.cmake#L13" rel="external nofollow noreferrer" target="_blank"><code class="syntax">PARALLEL_LINK_JOBS</code></a></td>
<td><code class="syntax">""</code></td>
<td>Maximum number of concurrent link jobs</td>
<td>1 if not set</td>
</tr>
<tr>
<td><a name="sanitize"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/sanitize.cmake#L7" rel="external nofollow noreferrer" target="_blank"><code class="syntax">SANITIZE</code></a></td>
<td><code class="syntax">""</code></td>
<td>Enable one of the code sanitizers</td>
<td>Possible values: - <code class="syntax">address</code> (ASan) - <code class="syntax">memory</code> (MSan) - <code class="syntax">thread</code> (TSan) - <code class="syntax">undefined</code> (UBSan) - "" (no sanitizing)</td>
</tr>
<tr>
<td><a name="split-shared-libraries"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L111" rel="external nofollow noreferrer" target="_blank"><code class="syntax">SPLIT_SHARED_LIBRARIES</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Keep all internal libraries as separate .so files</td>
<td>DEVELOPER ONLY. Faster linking if turned on.</td>
</tr>
<tr>
<td><a name="strip-debug-symbols-functions"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/src/Functions/CMakeLists.txt#L48" rel="external nofollow noreferrer" target="_blank"><code class="syntax">STRIP_DEBUG_SYMBOLS_FUNCTIONS</code></a></td>
<td><code class="syntax">STRIP_DSF_DEFAULT</code></td>
<td>Do not generate debugger info for ClickHouse functions</td>
<td>Provides faster linking and lower binary size. Tradeoff is the inability to debug some source files with e.g. gdb (empty stack frames and no local variables)."</td>
</tr>
<tr>
<td><a name="use-debug-helpers"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L252" rel="external nofollow noreferrer" target="_blank"><code class="syntax">USE_DEBUG_HELPERS</code></a></td>
<td><code class="syntax">USE_DEBUG_HELPERS</code></td>
<td>Enable debug helpers</td>
<td></td>
</tr>
<tr>
<td><a name="use-static-libraries"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L106" rel="external nofollow noreferrer" target="_blank"><code class="syntax">USE_STATIC_LIBRARIES</code></a></td>
<td><code class="syntax">ON</code></td>
<td>Disable to use shared libraries</td>
<td></td>
</tr>
<tr>
<td><a name="use-unwind"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/cmake/unwind.cmake#L1" rel="external nofollow noreferrer" target="_blank"><code class="syntax">USE_UNWIND</code></a></td>
<td><code class="syntax">ENABLE_LIBRARIES</code></td>
<td>Enable libunwind (better stacktraces)</td>
<td></td>
</tr>
<tr>
<td><a name="werror"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L417" rel="external nofollow noreferrer" target="_blank"><code class="syntax">WERROR</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Enable -Werror compiler option</td>
<td>Using system libs can cause a lot of warnings in includes (on macro expansion).</td>
</tr>
<tr>
<td><a name="with-coverage"></a><a href="https://github.com/clickhouse/clickhouse/blob/master/CMakeLists.txt#L344" rel="external nofollow noreferrer" target="_blank"><code class="syntax">WITH_COVERAGE</code></a></td>
<td><code class="syntax">OFF</code></td>
<td>Profile the resulting binary/binaries</td>
<td>Compiler-specific coverage flags e.g. -fcoverage-mapping for gcc</td>
</tr>
</tbody>
</table>

## Developer's guide for adding new CMake options

#### Don't be obvious. Be informative.

Bad:

```
option (ENABLE_TESTS "Enables testing" OFF)
```

This description is quite useless as it neither gives the viewer any additional information nor explains the option purpose.

Better:

```
option(ENABLE_TESTS "Provide unit_test_dbms target with Google.test unit tests" OFF)
```

If the option's purpose can't be guessed by its name, or the purpose guess may be misleading, or option has some
pre-conditions, leave a comment above the option() line and explain what it does.
The best way would be linking the docs page (if it exists).
The comment is parsed into a separate column (see below).

Even better:

```
# implies ${TESTS_ARE_ENABLED}
# see tests/CMakeLists.txt for implementation detail.
option(ENABLE_TESTS "Provide unit_test_dbms target with Google.test unit tests" OFF)
```

#### If the option's state could produce unwanted (or unusual) result, explicitly warn the user.

Suppose you have an option that may strip debug symbols from the ClickHouse part.
This can speed up the linking process, but produces a binary that cannot be debugged.
In that case, prefer explicitly raising a warning telling the developer that he may be doing something wrong.
Also, such options should be disabled if applies.

Bad:

```
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS
    "Do not generate debugger info for ClickHouse functions.
    ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif()
```

Better:

```
# Provides faster linking and lower binary size.
# Tradeoff is the inability to debug some source files with e.g. gdb
# (empty stack frames and no local variables)."
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS
    "Do not generate debugger info for ClickHouse functions."
    ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    message(WARNING "Not generating debugger info for ClickHouse functions")
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif()
```

#### In the option's description, explain WHAT the option does rather than WHY it does something.
The WHY explanation should be placed in the comment. You may find that the option's name is self-descriptive.

Bad:

```
option(ENABLE_THINLTO "Enable Thin LTO. Only applicable for clang. It's also suppressed when building with tests or sanitizers." ON)
```

Better:

```
# Only applicable for clang.
# Turned off when building with tests or sanitizers.
option(ENABLE_THINLTO "Clang-specific link time optimisation" ON).
```

#### Don't assume other developers know as much as you do.
In ClickHouse, there are many tools used that an ordinary developer may not know. If you are in doubt, give a link to
the tool's docs. It won't take much of your time.

Bad:

```
option(ENABLE_THINLTO "Enable Thin LTO. Only applicable for clang. It's also suppressed when building with tests or sanitizers." ON)
```

Better (combined with the above hint):

```
# https://clang.llvm.org/docs/ThinLTO.html
# Only applicable for clang.
# Turned off when building with tests or sanitizers.
option(ENABLE_THINLTO "Clang-specific link time optimisation" ON).
```

Other example, bad:

```
option (USE_INCLUDE_WHAT_YOU_USE "Use 'include-what-you-use' tool" OFF)
```

Better:

```
# https://github.com/include-what-you-use/include-what-you-use
option (USE_INCLUDE_WHAT_YOU_USE "Reduce unneeded #include s (external tool)" OFF)
```

#### Prefer consistent default values.
CMake allows you to pass a plethora of values representing boolean true/false, e.g. 1, ON, YES, ....

Prefer the ON/OFF values, if possible.

