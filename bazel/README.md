# Native Bazel build

The port has Linux x86-64 and ARM64 configurations. Bazel fetches third-party
sources through BCR modules and pinned repository rules. See
[`README.bazel.md`](../README.bazel.md) for the current binary comparison and
the validation status below for earlier execution coverage.

Bazel 9.3.0 is pinned in `.bazelversion`; its cross-compilation fix allows
building test binaries without registering an execution platform for them.
Running those tests still requires a compatible execution platform.
Bazel builds require no initialized Git submodules. Bazelisk selects the
pinned Bazel version automatically.

The C/C++ toolchain is BCR's `llvm` module (hermetic-llvm). `llvm.MODULE.bazel`
selects ClickHouse's LLVM fork and the `stage1_from_source` option builds the
compiler from it. The same external repository supplies LLVM, libc++,
libc++abi, libunwind, compiler-rt, and llvm-libc. Rust uses `rules_rs` and
ClickHouse's nightly/2026-03-22 toolchain.

```sh
bazel build --config=x86_64 //:clickhouse
bazel build --config=cmake_match_aarch64 //:clickhouse
bazel test --config=x86_64 //:unit_tests_dbms
```

The second command runs compilers, code generators, and Cargo on BuildBuddy
ARM64 Linux executors. `cmake_match_aarch64` includes the ARM64 execution
platform required for tests. It also registers the Linux x86-64 BuildBuddy
executor, after the ARM64 executors, for the platform-constrained ClickStack
Next build. See `README.bazel.md` for the commands and recorded build results.
The unit test target runs the complete GoogleTest suite with the same
`OPENSSL_CONF=/dev/null` setting as ClickHouse's CI.

## Third-party dependencies

Prefer BCR modules when their sources and configuration preserve ClickHouse's
required behavior. `initial_migration/bcr_dependencies.json` records adopted modules and maps
CMake targets and include directories to them. `initial_migration/any2bazel.json` records the
corresponding reference-target exclusions; these are module substitutions,
not removed functionality.

`rules/contrib_repositories.json` records the 159 upstream submodule revisions,
official GitHub archive URLs, SHA-256 checksums, and archive prefixes.
`rules/repositories.bzl` fetches them with `http_archive`, except LLVM, which uses
hermetic-llvm's `llvm.from_archive`. Generated BUILD overlays
under `bazel/dependencies` contain the dependency's `cc_library`, `cc_binary`,
and assembly rules with ClickHouse's native build configuration. Sources
compiled within the dependency stay local; file exports are retained for
cross-repository inputs such as licenses and code generators. The repository
rule removes upstream Bazel package boundaries from those overlaid trees;
LLVM retains its upstream Bazel packages.
Fetches remain lazy. `REPO.bazel` excludes checkout submodules and local build
outputs from Bazel discovery. The rest of `contrib/` stays visible because
ClickHouse owns sources, configuration headers, and generator inputs there,
including `contrib/*-cmake`.

`lz4@1.10.0.bcr.2` provides the same four C translation units as ClickHouse's
vendored build. All release `lib/` files match the pinned submodule byte for
byte. `lz4-fast-decoder.patch` preserves `LZ4_FAST_DEC_LOOP=1` on Linux ARM64,
where upstream otherwise disables it for Clang. BCR prefixes the internal
legacy xxHash symbols with `LZ4_`; ClickHouse's separate xxHash dependency uses
its inline implementation.

Some dependencies require further work before substituting a stock module:

| Dependency | Reason to retain pinned sources |
| --- | --- |
| zlib-ng | ClickHouse's 2.1.7 fork changes function-table initialization and fixes an uninitialized inflate state. BCR has no matching 2.1.7 module. |
| libdeflate | The 1.26 fork adds streaming decompression and CPU-feature race fixes. |
| Zstandard | The pinned source reports 1.5.8; the audited BCR module is 1.5.7. |
| xxHash | The header differs from BCR 0.8.3, including namespace and C-linkage controls. |
| fmt | The pinned 12.1.0 sources include a trailing-percent `vprintf` bounds fix absent from the matching BCR release. |
| Arrow and Parquet | ClickHouse pins its Arrow 25.0.0 fork at `363af92e`. BCR's `apache_arrow@25.0.1` uses Apache release sources, different Thrift and version configuration, and excludes the SIMD sources selected by ClickHouse. |
| libc++ and related runtimes | The `llvm` module uses ClickHouse's source fork to preserve its ABI and exception-stack-trace changes across the binary. |

These dependencies compile from the fetched, pinned source revisions.
The port uses upstream archives without repackaging them or hosting custom
archives. Google API schemas use the archive already checked into ClickHouse.
GitHub archives honor upstream `export-ignore` and `export-subst` attributes:
an audit found 616 omitted files and one LLVM archive-metadata substitution,
with no material compile, generator, license, or Rust input changes.

The [BCR Arrow overlay](https://github.com/bazelbuild/bazel-central-registry/blob/main/modules/apache_arrow/25.0.1/overlay/BUILD.bazel)
is a useful reference for public header ownership and named component
dependencies. Its broad source globs, generated feature headers, and
`strip_include_prefix` cannot be copied into this build: ClickHouse depends on
its ordered translation units, generated FlatBuffers include precedence, and
the file names embedded in the normalized executable. Keep the pinned sources,
explicit source order, and ClickHouse configuration when adopting its header
organization for other dependencies.

Cargo metadata resolves the original relative path dependencies in a small
repository assembled from the fetched sources and first-party manifests.
`rules_rs` owns Rust compilation. The offline cbindgen action uses fetched
Delta and Cargo vendor sources. OpenSSL's source and configured headers are
assembled into a declared output directory for its Cargo build script.
Wasmtime uses its original fetched headers beside the configured `conf.h`;
unchanged copies are not checked into the port.

ClickStack is built from HyperDX sources by
`//bazel/rules/clickstack:source_export`, using `rules_js`, HyperDX's Yarn
lockfile, checked Google CSS source responses, and checksum-pinned font files.
`//bazel:generate_native_ClickStack` compresses that output and generates the
header used by ClickHouse. The ARM64 comparison uses native CMake's ARM64
resource timestamps; both targets build the same Next assets. HyperDX source,
Node, and font revisions are in `clickstack.MODULE.bazel`. The ClickStack
dependency overlay exports only the license; it no longer exports prebuilt
Next assets.

Configured dependency headers are declared outputs in their owning external
repositories. `initial_migration/configuration.py` recovers the original CMake templates and
substitutions and verifies that they reproduce the reference outputs before
emitting `expand_template`, `copy_file`, or `write_file` rules. The port does
not check these configured headers into `bazel/config`. The empty ICU source
used by CMake's library declaration is omitted; ICU's actual sources remain.

ARM64 Vectorscan also uses the pinned sysroot's `asm/hwcap.h`, which supplies
the capability bits needed for its runtime CPU checks. Its remaining libc
and kernel headers come from hermetic-llvm.

## Initial migration and validation

The one-time BUILD generators, CMake reference builders, parity checks, and
CMake comparison snapshots are in [`initial_migration/`](initial_migration/).
The active build rules and Python tools are in [`rules/`](rules/). The active
rules read `rules/codegen_rules.bzl`, the link-order and Rust inventories,
`rules/contrib_repositories.json`, and `rules/license_manifest.json`.
These inputs are used by ordinary Bazel builds, so they live with the active
rules. `initial_migration/cmake_metadata.bzl` and
`initial_migration/license_substitutions_*.json` record values for the
`--//bazel:cmake_metadata` comparison configuration.
Arrow and both MongoDB drivers also read the recorded C++ compiler version
from `initial_migration/cmake_metadata.bzl` when generating their headers.

ClickHouse BUILD files reside with their sources under `src/`, `programs/`,
`base/`, and `contrib/`. For example, Poco NetSSL belongs to
`//base/poco/NetSSL_OpenSSL:_poco_net_ssl`; DBMS and common I/O span several
source directories and belong to `//src:dbms` and
`//src:clickhouse_common_io`. Third-party BUILD overlays remain in
`bazel/dependencies/`. The root retains `//:clickhouse` so its output remains
`bazel-bin/clickhouse`, and `//:unit_tests_dbms` remains a test entry point.
BUILD files can be maintained directly. BUILD-file generation from both CMake
reference graphs overwrites the BUILD files; run it only for an intentional
migration refresh.

`initial_migration/configure_reference.py` configures both complete CMake graphs and invokes
EngFlow/any2bazel's CMake extractor. `initial_migration/generate_build.py` translates the File
API graphs and declared Ninja generator commands into native Bazel targets.
`initial_migration/build_file.py` renders rules from both captured graphs.
`initial_migration/first_party_packages.py` assigns the native rules to their
source directories, updates labels across the main and external repositories,
and checks source order on both architectures. `//contrib` keeps
auxiliary dependency targets in the complete `//...` build, even when the
server does not depend on them.
`initial_migration/build_file.py` reads unconditional `common --copt`, `--conlyopt`, and
`--cxxopt` entries from `.bazelrc`. It omits those flags from generated
native libraries, including `-O3`, `-DNDEBUG`, and `-gz=zlib`.
`OS_LINUX`, position independence, and diagnostics shared by C and C++ are
also `common --copt` options. The generator recognizes both `-DOS_LINUX`
and CMake's two-argument `-D OS_LINUX` spelling.
`rules/ch_cc_library.bzl` selects the base `-march` flags from the target platform's
`@platforms//cpu` constraint. Its CPU flags precede the library's `copts`,
so a later ISA override still wins. `clickhouse_code = True` adds the
ClickHouse XRay settings, `-Wall`, and `-Wextra` only for CMake groups which
originally requested all five flags. Other warning exceptions remain in
`copts`. NASM, Cargo, custom runtime actions, and executable code generators
keep their own compiler flags.

`ch_cc_library` also supplies the pinned ClickHouse sysroot and GCC
toolchain, source-file prefix maps, and the three standard native features.
The defaults describe C++ on both supported CPUs. Only C, assembly,
single-CPU groups, and the few libc groups that omit libc++ headers need
explicit overrides. Prefix maps still apply in the CMake comparison
configurations: `__FILE__` and embedded file names survive stripping.

When a CMake library has one native compile group, the generated
`ch_cc_library` carries its public name and ordered dependencies directly.
Multiple compile groups, runtimes, NASM and executables retain separate rules.
`initial_migration/cmake_link_plan.py` resolves each original archive to its actual declared
owner and verifies object order on both platforms.

CMake's system include directories in native C/C++/ASM rules are emitted as
`local_includes`. The rules request `system_include_paths` so rules_cc
passes these directories as system headers to Clang. These directories are
local to the compiling target. Regular CMake `-I` directories remain in
`copts` so Clang does not treat them as system headers.
`repository_local_include` rebases foreign repository directories against
the target's package; rules_cc supplies both source and generated directories.
The custom NASM and CMake runtime rules construct their own commands and do
not have rules_cc's `local_includes` attribute.

Generated libraries list headers in `hdrs` when more than one CMake target
uses the directory; private headers are in `srcs`. Local headers use
`glob()`. `@clickhouse_boost//:boost_headers` owns Boost's public `boost/**`
headers. Each Boost component keeps its own private `libs/<component>`
inputs. Foreign consumers depend on `boost_headers` and do not copy Boost's
documentation, tests, and implementation inventories. Public fmt,
FlatBuffers, libuv, Arrow and librdkafka headers likewise come from their
existing libraries. For example, cppkafka depends on librdkafka instead of
listing librdkafka's headers again.

`initial_migration/external_headers.py` declares public `cc_library` targets for header-only
dependencies such as RapidJSON, nlohmann/json and xsimd. It collects the
source header directories actually used in the configured native graphs,
emits header globs in each owning repository, and adds dependencies only for
headers those globs supply. It leaves generated headers, licenses, Rust
sources, and generator inputs under their existing rules. Foreign file labels
remain exported where an actual cross-repository consumer still needs them.

AWS CRT, ORC, DMLC, and ICU translation units compile in their owning fetched
repositories. ClickHouse configuration inputs remain in `contrib/*-cmake`;
`initial_migration/cmake_link_plan.py` verifies that the declared CMake archives have the same
ordered objects after moving compilation.

Textually included implementation files and embedded files remain compiler
inputs. They must not be compiled as additional translation units: Poco
textually includes some `.c` files that are also separate translation units,
and ClickHouse embeds non-header resources. The native reference's source and
link order remain authoritative.

`initial_migration/compact_build.py` removes empty list additions and collapses identical
x86-64/ARM64 `select()` lists. It retains differing or single-platform
selections and verifies the expanded generated BUILD statements.

The native reference configuration can also build ClickHouse with CMake.
For cross-compilation, the reference exposes inline TableGen targets through
`initial_migration/reference_codegen.cmake` for graph extraction only; Bazel's platform
transition builds those generators for the host. Use an ordinary CMake
configuration for an actual CMake cross-build. LLVM's configured default
triple follows the target (`aarch64-unknown-linux-gnu` for ARM64).
Reference configuration and regeneration still use upstream's initialized
submodules (`git submodule update --init --recursive`). This is separate from
the Bazel build's repository-rule acquisition.
When updating a submodule, update its revision, archive URL, checksum, prefix,
and removed BUILD-file list in `rules/contrib_repositories.json` as well. Reconfigure
both reference architectures and regenerate the overlays before validating
the new dependency. The build generator consumes these pins; it does not
discover new revisions or download archives.
Run it after updating either reference configuration:

```sh
python3 bazel/initial_migration/generate_build.py
```

The generator formats all BUILD files with `buildifier` on `PATH`. To format
or check BUILD files without regenerating them:

```sh
python3 bazel/initial_migration/format_build_files.py
python3 bazel/initial_migration/format_build_files.py --check
```

`.buildifier.json` preserves CMake source and dependency order. It also keeps
overlay labels unchanged: overlays are Bazel packages in external repositories,
although the files live under `bazel/dependencies` in this checkout.

`initial_migration/coverage.json` retains target/action counts, untranslated outputs, and an
explicit inventory of CMake-only commands. All current custom outputs are
either translated or classified: cache/install/test entry points, replaced
Cargo and ExternalProject steps, or manual contributor-list generation.
CMake installation commands are inventoried; Bazel packaging is not implemented.
`initial_migration/codegen.py` records actions for
native Bazel-built TableGen, Protobuf, and FlatBuffers tools. NASM comes from
its BCR module and executes as a declared Bazel tool. Building `//:clickhouse`
does not run C/C++ compilation or linking through CMake or Ninja.
`//bazel:cmake_reference_aarch64` separately builds the original native CMake
reference on an ARM64 executor for the stripped binary comparison.
`bazel/initial_migration/run_native_reference.sh` downloads the CMake status and hashes.
Pass `--migration-evidence` for the generated files used by `refresh_cmake_metadata.py`
or `--compare` to compare that checkout's CMake build against refreshed Bazel metadata.
To compare against the saved original instead, provide `CMAKE_REFERENCE_ELF` to
`CMAKE_REFERENCE_ARCH=aarch64 bash bazel/initial_migration/run_stripped_hash.sh --compare-originals`.

Python generators use the pinned Python 3.11 toolchain. Their shell launchers
start that interpreter directly, without requiring a host Python installation.
Bytecode caching is disabled for these tools so startup does not write into
their input trees.

Clang resolves quoted `#embed` paths from the source file's real path.
The affected sources and assets are therefore copied together, byte for
byte, into Bazel's generated output tree. Their relative layout is preserved
so that dependency files refer to declared build outputs.

The parity configuration maps reference build paths to the corresponding
Bazel configuration and generated-file paths, and records executable label
renames. Generated source mappings name each file separately: any2bazel's
source mapping replaces the entire path, unlike its include mapping.
The reference `--gcc-toolchain` and `--ld-path` options are omitted
because the requested hermetic-llvm toolchain owns those locations. This
does not establish ABI compatibility between the reference and Bazel sysroots;
that still requires checking the linked binaries.

`system.build_options` uses Bazel's selected compiler paths and toolchain
flags, combined with ClickHouse's configured common flags. It also records
command-line compiler/linker overrides; the compiler version comes from
Clang's predefined macros. Feature and release-version values retain the
reference configuration.
Native ClickHouse targets disable hermetic-llvm's extra `_FORTIFY_SOURCE`
definition to match the CMake reference; the toolchain's default remains
enabled for other consumers.

`initial_migration/audit_native_actions.py` supplements any2bazel's unmodified extractor and
diff. It selects the output configuration of the main `clickhouse` binary,
so host-tool actions cannot satisfy target-platform checks. It compares each
source within its original target, preserving the ISA and define variants
that a project-wide source union would merge. It also
checks NASM actions, every executable's native object closure, mandatory
object retention, Rust archive inputs, system libraries, and link flags.
Rust archives must appear on the linker command line, even when they are
already declared as action inputs. Skim's separately compiled native bridge,
ABI guard and CXX runtime objects must also reach the linker.
Direct object inputs come from Ninja's link edges and compilation database.
CMake can forward `TARGET_OBJECTS` through an interface library without
recording a dependency edge in the File API model.
The stock any2bazel diff does not yet converge: its source-level union merges
repeated compile contexts, including host and target actions in cross builds.
Its extractor omits these direct link objects. Its role inference also
excludes assembly-only and `.c++` compile groups, and
its link comparison does not equate mandatory objects with whole archives.
The supplemental audit covers these cases without changing the upstream
scripts or suppressing correctness flags. A passing result is separate
from stock-diff convergence and from successful compilation or execution.
For example, after extracting the C/C++ model and capturing the native,
archive-MRI and NASM aqueries:

```sh
python3 bazel/initial_migration/audit_native_actions.py \
  --any2bazel /path/to/any2bazel \
  --cmake-model build_bazel_reference_x86_64/model.cmake.json \
  --bazel-model /path/to/model.bazel.x86_64.json \
  --native-aquery /path/to/native.aquery.json \
  --mri-aquery /path/to/mri.aquery.json \
  --assembly-aquery /path/to/graph-nasm-x86_64.aquery.json \
  --output /path/to/native-actions-x86_64.json
```

LLVM's module groups some libraries differently from CMake. The audit maps
their exact source identities and checks that the resulting objects reach
each binary. The module also uses its own runtime and LLVM build settings;
the audit currently reports differences from CMake, so it does not establish
complete action parity for the current port. The four LZ4 translation units
are covered by the separate BCR equivalence check.

HDFS's Yasm source is converted to NASM syntax by `rules/prepare_hdfs_crc.py`.
The conversion was validated against the original Yasm object: executable
and data sections, relocation, and exported symbol matched, and the
Bazel-produced object passed 29,952 comparisons with software CRC32C.
ISA-L also uses the BCR NASM tool.

All seven imported Rust libraries have native `rules_rs` targets under
`//bazel/rust`. `initial_migration/generate_rust_inputs.py --cargo /path/to/pinned/cargo` resolves
the Delta and Chdig dependency workspaces offline from ClickHouse's vendored
crates and rejects package versions absent from their upstream lockfiles.
The `*_processed` targets run ClickHouse's original symbol-localization and
stripping scripts. Build these targets explicitly when validating archives:
building a `cc_import` wrapper alone does not execute its archive producer.
Delta's C headers are generated separately by its pinned cbindgen; Chdig's
Perfetto generator uses the exact protoc binary from its Cargo.lock as an
explicit execution tool.

Skim's C++ bridge is generated by the lock-pinned `cxxbridge` CLI. Its native
bridge, CXX runtime and original `check_abi.cpp` compile with ClickHouse's
`RUST_CXXFLAGS` and patched libc++ headers. They remain direct dependencies
of `skim_cc` after Rust archive processing, which preserves only the archive.
This is required for ClickHouse's extended `std::exception` layout.

The existing PCO, PRQL, and Polyglot inline tests reuse the Rust library
targets and their dependencies:

```sh
bazel test --config=x86_64 //rust/workspace:pco_test \
  //rust/workspace:prql_test //rust/workspace:polyglot_test
```

Use `bazel build --config=aarch64` with the same labels to cross-compile the
test binaries. Their standalone Rust harnesses require glibc 2.28 or newer;
the final ClickHouse binary has a separate glibc compatibility layer.
The PRQL and Polyglot test helpers use `c_char` for C strings so they compile
on both architectures. Earlier execution results are recorded below.

The Rust action graphs on both architectures use the same 555 registry
package versions. Their downloaded sources match ClickHouse's vendor tree,
apart from the declared protoc runtime-path patch and generated Bazel build
definitions. The three Git packages also match, with only Cargo manifest
normalization differences. The 33 local dependency repositories reference
the fetched, pinned dependency sources.

## Earlier validation

These results predate the current source-built LLVM and configured-header
changes. They do not establish that the current revision builds or that its
CMake and Bazel executables have equal hashes.

As of 2026-10-17, the repository-rule implementation at
`c765dd3572eb26c9cdb3c76a414cf6c616235ece` passes complete sandboxed `//...`
builds for all 2,072 root-repository targets on both architectures, using
retained action caches. All 159 submodule directories remain empty, and the
validation checkout's inputs are unchanged after each build and Bazel test
run. Both final binaries and the test binaries are built from fetched sources.
That implementation's supplemental audits matched 18,352 x86-64 and 18,258
ARM64 native compile actions and all ten executable link closures on each
architecture. Those results predate the switch to module-owned LLVM targets.

Both ClickHouse binaries pass the canonical GLIBC and PIE checks, architecture
and non-executable-stack checks, and isolated local, server/client, and Keeper
checks. Five existing functional cases covering PRQL, Keeper client, fuzzy
search, Polyglot, and PCO pass on each architecture without skips or retries.
All 12 existing Rust cases pass through Bazel on each architecture with test
result caching disabled and no ignored, filtered, or retried cases.
ARM64 runtime and Rust execution use QEMU user-mode with glibc 2.36.

The complete x86-64 C++ suite runs without a filter or cached test result:
29,937 cases start, 25,145 pass, 4,792 are skipped, and three additional cases
are disabled, with no failures or errors. `FunctionsStress.stress` completes
its 600-second run. The skips cover incompatible codec parameters,
Coordination parameter exclusions, missing AI credentials, and missing
capabilities. The disabled cases are upstream microbenchmarks.

The full ARM64 C++ suite remains unvalidated. A full-system QEMU run stopped
at `Common.ReverseDNS` in a guest without a DNS service and did not produce
a complete-suite result.

These results cover selected functional tests, not the entire functional or
integration test repository. Cross-compilation and emulation do not validate
building or running on native ARM64 hardware. Old-distribution runtime
compatibility and official packaging also remain unvalidated. Stock any2bazel
diff convergence is a separate limitation described above.
