# Building ClickHouse with Bazel

This port uses Bazel 9.3.0, hermetic-llvm, and `rules_rs`. Bazel fetches pinned
third-party sources, so a Bazel build does not require initialized Git
submodules. The compiler and LLVM libraries come from ClickHouse's LLVM fork
at `86b7fe802669a4513da4931d909c36f4728729b9`; the compiler is built from source.

```sh
bazel build --config=x86_64 //:clickhouse
bazel build --config=x86_64 //...
bazel test --config=x86_64 //:unit_tests_dbms
```

The `aarch64` configuration supports cross-compilation. ARM64 execution is
outside the current validation run.

## BuildBuddy remote execution

Set up a BuildBuddy API key with remote execution permission in your user
`~/.bazelrc` or the ignored `.bazelrc.local`:

```bazelrc
common:remote --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
common:remote --bes_header=x-buildbuddy-api-key=YOUR_API_KEY
```

Then build the Linux x86-64 executable:

```sh
bazel build --config=remote --config=x86_64 //:clickhouse
```

The full `//:clickhouse` build analyzes over 400,000 targets, including the
source-built LLVM compiler. If the local Bazel server cannot hold that graph
and its fetched repositories, run Bazel itself on a BuildBuddy remote runner:

```sh
bb remote --runner_exec_properties=EstimatedMemory=32GB \
  --runner_exec_properties=EstimatedCPU=8 \
  --runner_exec_properties=EstimatedFreeDiskBytes=50GB \
  --host_jvm_args=-Xmx24g build --config=remote --config=x86_64 \
  --remote_download_outputs=minimal //:clickhouse
```

`remote` selects `//bazel:buildbuddy_linux_x86_64`, which inherits
`@llvm//:rbe_linux_x86_64` and uses BuildBuddy's Ubuntu 20.04 execution
image; `x86_64` selects the ClickHouse target platform. LLVM,
Rust, and code generators execute on Linux even when Bazel runs on macOS.
Bazel's `common:remote` configuration sets `--jobs=500` for remote builds
and enables the same remote configuration for `aquery`.
Bazel still evaluates repository rules and fetches source dependencies on
the client, so the client needs free disk space for the fetched repositories.
Only top-level outputs are downloaded. On macOS, the result is a Linux ELF
executable and cannot be run directly on the client.

Keep API keys out of version control. Existing BuildBuddy credentials in
`~/.bazelrc` also work; no additional local file is required in that case.

Dependency ownership, generation, and action-audit details are in
[`bazel/README.md`](bazel/README.md).

## Binary comparison

On a Linux x86-64 host with BuildBuddy remote execution configured, build the
CMake reference with the compiler selected by this repository:

```sh
bash bazel/run_cmake_reference.sh
python3 bazel/refresh_cmake_metadata.py --arch=x86_64 \
  --build-dir="$PWD/build_bazel_reference_x86_64"
python3 bazel/refresh_cmake_metadata.py --check --arch=x86_64 \
  --build-dir="$PWD/build_bazel_reference_x86_64"
```

`bazel/run_cmake_reference.sh` builds `//bazel:cmake_toolchain_files`,
installs that compiler and its builtin headers, configures Release CMake with
the pinned submodules and Rust compiler, and builds the native CMake
`clickhouse` target. It records the compiler and executable SHA-256 values,
CMake File API replies, CMake trace, Ninja commands, and generated metadata in
`build_bazel_reference_x86_64`. It also uploads those files when run on a
BuildBuddy hosted runner. `CMAKE_REFERENCE_JOBS` controls Ninja on that Linux
host; it defaults to 12 and is independent of Bazel's `--jobs=500`.

To replay the CMake reference's embedded compiler paths, versions, flags,
and CMake version, and omit the extra `BUILD_SYSTEM` row, use:

```sh
bazel build --config=remote --config=x86_64 --//bazel:cmake_metadata //:clickhouse
```

For the stripped x86-64 comparison, run on a BuildBuddy Linux hosted runner:

```sh
bash bazel/run_stripped_hash.sh
```

This uses `common:remote` (`--jobs=500`), verifies that the Clang and Clang++
SHA-256 values match the recorded CMake toolchain, and builds `//:clickhouse`.
After Bazel finishes, it verifies the original CMake artifact in a temporary
directory outside the checkout. It first verifies the original
`llvm-strip --strip-all` hash, then compares copies made with:

```sh
llvm-strip --strip-all --remove-section=.note.gnu.build-id -o OUTPUT INPUT
```

Both copies must have the same SHA-256 and pass `cmp`. The comparison exits
at that point. LLVM's linker hashes the original ELF, including symbol and debug
tables, to create the build ID; retaining that ID also retains differences from
sections which stripping removes. The original ELF and its build ID are kept.
`stripped-summary.txt` records both original and plain-stripped hashes, the
comparison hashes, the strip flags, and `cmp` status.

The comparison passed in
[BuildBuddy invocation 10ea5019-d0e8-4f22-bb88-ef81fce6ac47](https://app.buildbuddy.io/invocation/10ea5019-d0e8-4f22-bb88-ef81fce6ac47).
Its [Bazel build](https://app.buildbuddy.io/invocation/7eb00bd2-e40d-4f8a-be7a-4554e4b53182)
completed 4,296 remote executions. The original CMake ELF and both selected
compilers were rehashed and verified against the recorded reference before
stripping. Results (SHA-256):

| Executable | CMake | Bazel |
| --- | --- | --- |
| Original | `cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc` | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` |
| `llvm-strip --strip-all` | `6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe` | `2ca4dbdd6bfd4511189c0547ebc62cb505cb1b760aa5346c553175ded61e5c93` |
| `llvm-strip --strip-all --remove-section=.note.gnu.build-id` | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` |

`cmp` of the last row returned 0, and `PARITY_STATUS=0`. This is stripped
binary equality; the original binaries and ordinary stripped build IDs differ.

On the same Linux host, run the complete comparison:

```sh
bash bazel/run_bazel_parity.sh
```

`bazel/run_bazel_parity.sh` verifies the saved reference hashes and metadata,
rebuilds `//bazel:cmake_toolchain_files` and confirms that Bazel selected the
recorded CMake compiler. It also compares all embedded LLVM generated headers
against the CMake headers. It then builds the Bazel executable and seven Rust
link archives, records both complete ELF hashes, the `cmp` result and all
seven native/Bazel archive hashes,
and compares the CMake File API and Ninja commands to Bazel `aquery` with
EngFlow/any2bazel. It retains the reports in the CMake build directory and
uploads the reports on a BuildBuddy hosted runner. Its status distinguishes a
binary or command difference from a failed build or failed audit.

`--//bazel:cmake_metadata` uses the values recorded by
`bazel/generate_build.py` or refreshed by `bazel/refresh_cmake_metadata.py`
from the configured CMake reference. It also maps generated source filenames
to the corresponding CMake build directory, so compiler-generated filenames
such as `__FILE__` match that reference. The reference must match the CMake
build being compared. Without the flag, `system.build_options` reports the
actual Bazel toolchain and flags, and generated filenames use Bazel directories.
The metadata generation has been executed in both modes; with the flag, its
output matches the configured CMake metadata source byte for byte.

The Linux x86-64 CMake reference linked successfully in
[BuildBuddy invocation 4ea26abf-d8ee-4634-9d07-458a3645f3bc](https://app.buildbuddy.io/invocation/4ea26abf-d8ee-4634-9d07-458a3645f3bc).
The SHA-256 of `build_bazel_reference_x86_64/programs/clickhouse` is:

```text
cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc
```

The executable has 929,619,736 bytes. BuildBuddy's uploaded
`clickhouse.cmake` artifact has the same digest as the
`cmake.sha256` produced by the build. This reference used source commit
`f61e9b32a64778e7949ae174be73b7950a52527f` with its pinned submodules,
and the LLVM 22.1.1 stage-1 compiler built by `//bazel:cmake_toolchain_files`
from ClickHouse LLVM commit `86b7fe802669a4513da4931d909c36f4728729b9`.
The compiler SHA-256 is
`52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce`.
The CMake build was Release with libraries, Rust, tests and ClickHouse ALL
enabled, compiler cache disabled, `SOURCE_DATE_EPOCH=1`, the repository's
`cmake/linux/toolchain-x86_64.cmake` sysroot, CMake 3.28.3 and Rust
`nightly-2026-03-22`. The exact invocation is in the uploaded
`configure-command.json`; `bash bazel/run_cmake_reference.sh` reproduces
the setup and build on a Linux x86-64 host with BuildBuddy configured.

This is the reference for commit `f61e9b32`, including after PR #1 was
rebased to `4213ffa205c2ab7e99e638e76626b166e12a992a`. Before the
CMake-ordered Bazel link and the separate ClickHouse LLVM library targets
were added, the rebased Bazel build with `--//bazel:cmake_metadata` produced:

```text
14e03f0d21619b51a1b9ecc8b4c128ffb29007966f4a000d4d93b8abea8bd418
```

That build completed in
[BuildBuddy invocation 038e6ee0-4b67-406f-b464-ea6f317f20cc](https://app.buildbuddy.io/invocation/038e6ee0-4b67-406f-b464-ea6f317f20cc).
The comparison in
[BuildBuddy invocation 92e53e41-9f66-42ca-87f1-defc308f033a](https://app.buildbuddy.io/invocation/92e53e41-9f66-42ca-87f1-defc308f033a)
rehashed the preserved CMake ELF as `cfbf99ef…087cbc` and `cmp` returned 1.
The rebased Clang, Clang++, LLVM ar and linker driver still all hash to
`52a7d0fd…6262ce`, verified in
[BuildBuddy invocation c5256bb3-ae51-4b71-9491-a2feec3e0fda](https://app.buildbuddy.io/invocation/c5256bb3-ae51-4b71-9491-a2feec3e0fda).
The original link did not reproduce the CMake binary.

The first Bazel build using the CMake archive order, actual Bazel-compiled
objects and separate ClickHouse LLVM library targets then linked successfully:

```text
b5f8f0e61dc44ecdc257202f85d8d731924b7e63f5e52d1de2b2cf9ad1700b38
```

[BuildBuddy invocation 2e7d3927-c264-406d-8b0e-27077a7f1700](https://app.buildbuddy.io/invocation/2e7d3927-c264-406d-8b0e-27077a7f1700)
completed 58,060 actions with 4,354 remote executions. The complete ELF SHA-256
is preserved in the `bazel.sha256` artifact of
[BuildBuddy invocation 71bc2804-e383-47ce-a991-e74e472c5bba](https://app.buildbuddy.io/invocation/71bc2804-e383-47ce-a991-e74e472c5bba).
This build predates the Release frame-pointer and copied-source filename fixes.
Its hash differs from the recorded CMake hash, so matching the LLVM sources and
archive order alone did not reproduce the executable.

After separating the ClickHouse LLVM libraries from the compiler build, the
comparison in
[BuildBuddy invocation 5f7fd5ec-0af7-4465-be86-335619767563](https://app.buildbuddy.io/invocation/5f7fd5ec-0af7-4465-be86-335619767563)
confirmed that both compilers still match the recorded compiler SHA-256 and all
59 generated LLVM headers match CMake byte for byte. Its
[Bazel build](https://app.buildbuddy.io/invocation/cfb35394-b1ac-4f90-bc73-4a2d546303fb)
linked successfully after applying the CMake Release flags to ClickHouse
compile groups and produced:

```text
380d0fe4326fd38ff7f0302c6ed08abf6aa11417f6a1baccebe2e01375a6faa0
```

`cmp` against the rehashed CMake ELF returned 1. This build preceded the final
correction of CMake source-prefix maps, including the empty replacement CMake
actually used. LLVM header identity and matched Release flags therefore still
do not establish executable equality.

The build with the recorded empty CMake source-prefix replacement and
metadata-mode generated filenames also linked successfully:

```text
526bcee78b28c0acecfbfbcb4967200997c7934f497fc9ba4a541141bb1a887a
```

[BuildBuddy invocation 7f3c8e95-a8e8-4182-9788-b390673b9bf3](https://app.buildbuddy.io/invocation/7f3c8e95-a8e8-4182-9788-b390673b9bf3)
completed with 16,479 remote executions. Its full `bazel.sha256` is recorded in
[BuildBuddy invocation 55cb8cd4-84e6-48f0-ac49-fe289e891947](https://app.buildbuddy.io/invocation/55cb8cd4-84e6-48f0-ac49-fe289e891947).
This is a distinct complete ELF hash from the CMake reference, so source-prefix
and Release-flag parity still do not establish executable equality.

The next step replaces six imported bootstrap archives with the exact CMake
`cxx`, `cxxabi`, `unwind`, `clang_rt_builtins`, `clang_rt_xray`, and
`libllvmlibc` compile groups. The focused
[Bazel runtime build](https://app.buildbuddy.io/invocation/48f1710d-578c-4a16-b7d9-074f65a06c9a)
succeeded with 364 remote compiles. Its
[command audit](https://app.buildbuddy.io/invocation/8d49c7d6-3122-45c6-a2c0-5f21f6c65f03)
verified all six CMake source counts, both selected compiler SHA-256 hashes,
and absence of bootstrap libc++ include directories in those compile actions.
The 125 AArch64 outline-atomic sources are generated from the `lse.S`
symlinks verified against the saved Ninja commands. This validates runtime
inputs; executable equality still requires the full link and `cmp`.

The full Bazel build with all six native runtime libraries completed in
[BuildBuddy invocation 865f8d87-e8df-47c2-904d-b2d7cc513cf7](https://app.buildbuddy.io/invocation/865f8d87-e8df-47c2-904d-b2d7cc513cf7).
It executed 450 remote actions, linked all 411 validated archive/object
references and produced:

```text
3cbde2adf20b849b9e29d8ea40c47d4c5ab997fe1ca3f2af55f17476ad74f556
```

[BuildBuddy invocation 6ec60425-63bd-47e3-a6f5-891cab529e39](https://app.buildbuddy.io/invocation/6ec60425-63bd-47e3-a6f5-891cab529e39)
rechecked the original CMake SHA-256 as `cfbf99ef…087cbc`, both compiler
SHA-256 values as `52a7d0fd…6262ce`, and all 59 generated LLVM headers.
`cmp` returned 1. ELF capture found 46 native sections and 47 Bazel sections,
with 45 differing table entries or contents; the CMake byte-for-byte target
has not yet been reproduced.

[BuildBuddy invocation 5d1cb4eb-a1f6-4ee7-ace6-f2e3a19d39b4](https://app.buildbuddy.io/invocation/5d1cb4eb-a1f6-4ee7-ace6-f2e3a19d39b4)
subsequently verified 122 actual compile actions for PostgreSQL, ClickHouse
glibc compatibility, and allocation interceptors with the pinned ClickHouse
sysroot. The compiler hash still matches the CMake reference.
Both CMake's and Bazel's `src/port/noblock.c.o` hash to
`a36c33b4e4abcd560d5a6f4e1d908a17e061c9c4baa80545a3476e416b30b321`.
This verifies that object, not the complete executable.

The full build using that pinned sysroot and native runtime sources completed
in [BuildBuddy invocation c6b645aa-9fb0-44d5-8394-d97a1accb134](https://app.buildbuddy.io/invocation/c6b645aa-9fb0-44d5-8394-d97a1accb134).
It ran 16,934 remote actions, with 40,950 action-cache hits, and produced
this complete Bazel executable SHA-256:

```text
8f4893576486f640d0d419d5654d6d87224445c6c8c6c5dede1291de98fd16d4
```

[BuildBuddy invocation 6be6d683-3a3b-4b47-9ac0-8301aa22a5fe](https://app.buildbuddy.io/invocation/6be6d683-3a3b-4b47-9ac0-8301aa22a5fe)
verified the original CMake executable and both compilers before this build.
`cmp` returned 1. Relative to CMake, Bazel's `.text` was 1,621,680 bytes
larger and `.rodata` was 694,702 bytes larger. This build used the earlier
Rust rules; it did not contain the Cargo corrections described below.

[BuildBuddy invocation 3f72be06-7136-445b-8ce4-d74d73deac15](https://app.buildbuddy.io/invocation/3f72be06-7136-445b-8ce4-d74d73deac15)
compiled PCO, PRQL, and Polyglot with the pinned Cargo and Rust compiler
from Bazel's declared source and vendor inputs. The
[comparison](https://app.buildbuddy.io/invocation/3f203571-a2c7-4490-80ff-87972e24bffd)
rechecked the original CMake executable and both Rust tool hashes. All three
raw Cargo archives and all three processed link archives match byte for byte,
including archive member order and names:

| Archive | Raw Cargo SHA-256 (both builds) | Processed SHA-256 (both builds) |
| --- | --- | --- |
| PCO | `4ff85afd197267e797443c9418b647fdf980ac6620a17d215349ae75fdf9988b` | `43a5186e80e9fa8e8132c7f1bb933fe717e8d1ef71e7b3f429b48d9f561b02cf` |
| PRQL | `c8be19bad2d042c0bfa2ca594f6f1b9b543aeb3e1cf8ac547d891ee5ccd9f725` | `6b585395d4779325864febc38f564b5ce4d98c55a655c4e92d1b12ede29362f7` |
| Polyglot | `6dc134152f87338fe80ed924497fe64c2439e751d3b531ad895b6623817e9cd3` | `309f9f7e0e9232bb7c3c705b70dc1fd6186805038a5e95515544d51f1bec8811` |

Cargo derives member filenames from the exact `RUSTFLAGS` argument vector,
including split `-C` arguments and the literal sysroot directory. PRQL's
build script also embeds the pinned rust_vendor revision. The Bazel Cargo
rule reproduces both from the repository's source and toolchain data.
Archive equality does not by itself establish complete executable equality.

The full Bazel build using all seven Cargo rules and the recorded CMake
assembly include directories succeeded in
[BuildBuddy invocation 2fa143e3-2820-4ce7-aefd-ac779d056316](https://app.buildbuddy.io/invocation/2fa143e3-2820-4ce7-aefd-ac779d056316).
The complete unstripped Bazel executable SHA-256 is:

```text
871e8a1a4a665e91261ef47a6192d85f972962feabf24431c0ca4d57caafce63
```

[BuildBuddy invocation 9b87cd8e-2a38-46ea-a03b-6f508b636a33](https://app.buildbuddy.io/invocation/9b87cd8e-2a38-46ea-a03b-6f508b636a33)
verified the original CMake hash, both compiler hashes, and all 59 LLVM
generated headers before building. `cmp` returned 1. Both executables have
46 ELF sections; relative to CMake, Bazel's `.text` is 20,544 bytes smaller,
`.rodata` 4,096 bytes smaller and `.eh_frame` 568 bytes smaller. The
`.gcc_except_table`, `.data.rel.ro`, `.data`, and `.bss` sizes now agree.
`.bss` contents also agree. The complete executable still differs.

The comparison confirmed exact SHA-256 equality for the processed Skim,
PRQL, Polyglot, and PCO archives. Wasmtime, Delta Kernel FFI, and chdig now
have identical ordered archive member names, but their processed archives
differ by -890, +1,152, and +20,852 bytes respectively. The native command
audit in this run stopped before producing its report because the separate
NASM and C++ aquery results both contained the final link. This does not
alter the ELF hashes above. The audit now coalesces duplicate actions only
when the Bazel action key, mnemonic, label, outputs, command, environment and
resolved input filenames agree.

PR #1 was subsequently rewritten at
`29281120601f767e8e828f57ec9baf35ca898674`. The branch contains that PR head.
Its five-file update changes Bazel's LLVM flags and Zstd headers; no native
ClickHouse source files or pinned submodules changed from `4213ffa205c`.
The original CMake executable SHA-256 above remains the reference. A
[focused Rust comparison](https://app.buildbuddy.io/invocation/db441cd8-f203-43c1-901c-66d8e47eb08e)
confirmed equal archive member names and ordering for the three remaining
archives. All remaining Delta members have identical allocated sections; only
Ring debug filenames differ. Wasmtime and chdig retain different paths to
generated Rust files in Cranelift and perfetto_protos. Corrosion's Cargo
directory calculation has now been reproduced.

On PR head `292811206`,
[Bazel invocation 1143155c](https://app.buildbuddy.io/invocation/1143155c-1259-4055-8ecd-dcd8f831362d)
built successfully after 12,382 remote executions. The original CMake hash and
both compiler hashes were reverified. All 59 generated LLVM headers matched.
The full Bazel executable SHA-256 is:

```text
008c496b0bca4f4e936a40b93748c90d672dc46f313329aee2522239e94875de
```

`cmp` on the complete executables returned 1. Both ELF files contain 46
sections; Bazel's `.text` is 20,544 bytes smaller and `.rodata` is 4,096
bytes smaller. Generated Cargo paths changed the full hash but did not change
these section-size differences. Those differences concern code and read-only
data, so stripping debug information alone cannot remove them.

The requested completion criterion is now matching stripped executables.
Run `bash bazel/run_bazel_parity.sh --stripped` on the reference host. It
preserves the original file hashes and uses the same
`llvm-strip --strip-all -o COPY ORIGINAL` from the saved LLVM installation
for both files. `summary.txt`, `stripped.sha256`, `stripped-cmp.log`, and
`stripped-elf-sections.json` contain the measured result. Full executable
equality is not required in this mode. Action-graph parity alone does not
establish equal stripped hashes.

The first stripped comparison on that PR head is
[BuildBuddy invocation 82a328c8](https://app.buildbuddy.io/invocation/82a328c8-c943-4a0a-923e-21b63a3d07d6).
The unstripped CMake and Bazel hashes remain the hashes above. After
`llvm-strip --strip-all -o COPY ORIGINAL`:

| Input | Stripped executable SHA-256 |
| --- | --- |
| CMake reference | `6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe` |
| Bazel PR 292811206 | `fe6d2a4f996d03e4bc4f61911e11f793d9caed8a15b0fdc259121ba91b6c06bc` |

The stripped `cmp` returned 1. Both stripped files have 43 ELF sections and
retain the 20,544-byte `.text` difference. The complete symbol-size comparison
reports 777,259 equal function-size multisets and 65 unequal ones; all 65 are
LZ4 functions. The Rust symbols absent on one side have matching sizes and
differ in LLVM-generated local symbol suffixes. The symbol report and original
ELF files diagnose the remaining difference; they do not establish equality.

The CMake link now compiles `@clickhouse_lz4//:_lz4__compile_0` from the
four pinned `contrib/lz4/lib` source files. The CMake flags include
`LZ4_FAST_DEC_LOOP=1` and omit the BCR-only `XXH_NAMESPACE=LZ4_`.
`lz4hc.c` also receives `lz4.c` as a declared compilation input.
[Bazel invocation bf4791f5](https://app.buildbuddy.io/invocation/bf4791f5-3eb7-44a3-b16d-0e0653dacd6c)
completed with 5 remote executions and 24,754 remote cache hits. Its hashes are:

| Input | SHA-256 |
| --- | --- |
| Bazel full executable | `c263799280b379235b90596b8005d0e8b799e62e88c805f52fa8454c6b4b2a3f` |
| Bazel `llvm-strip --strip-all` executable | `73db3edaac7436ef7f07e06175d74ad33c37dd7219d12bd896610d79a7f51ad8` |

The selected compiler still matched `52a7d0fd…6262ce`. The stripped hash
does not yet equal the reference hash. Run `bash bazel/run_stripped_hash.sh`
to reproduce the check on a Linux BuildBuddy worker even if that worker
does not have the original CMake build directory.
