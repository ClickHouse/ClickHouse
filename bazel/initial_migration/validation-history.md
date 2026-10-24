# ClickHouse Bazel validation history

This archive preserves the build commands, binary comparisons, and dependency audits
recorded in [`e46b82964`](https://github.com/dzbarsky/clickhouse/commit/e46b829649be5dc711d95388fd8644925cb50a0b).
For current build instructions and the latest verified hashes, see the
[Bazel build guide](../../README.bazel.md). Commands in this archive use the
repository root as their working directory.

This port uses Bazel 9.3.0, hermetic-llvm, and `rules_rs`. Bazel fetches pinned
third-party sources, so a Bazel build does not require initialized Git
submodules. The compiler and LLVM libraries come from ClickHouse's LLVM fork
at `86b7fe802669a4513da4931d909c36f4728729b9`; the compiler is built from source.

The ClickHouse source and the CMake reference are pinned to upstream commit
`6bb427c9a81459c2b9e7369ef6ed8a6e8680b0da`. The Bazel branch descends from
that upstream commit. Comparisons against upstream must use that merge base;
rebasing onto a different ClickHouse version requires recording a new CMake
reference and repeating both binary comparisons.

```sh
bazel build --config=x86_64 //:clickhouse
bazel build --config=x86_64 //...
bazel test --config=x86_64 //:unit_tests_dbms
```

The `aarch64` configuration selects Linux ARM64. For native ARM64 compilers,
generators, and Cargo build scripts on BuildBuddy, use
`--config=cmake_match_aarch64`.

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
  --host_jvm_args=-Xmx24g build --config=remote --config=x86_64 //:clickhouse
```

`remote` selects `//bazel:buildbuddy_linux_x86_64`, which inherits
`@llvm//:rbe_linux_x86_64` and uses BuildBuddy's Ubuntu 20.04 execution
image; `x86_64` selects the ClickHouse target platform. LLVM,
Rust, and code generators execute on Linux even when Bazel runs on macOS.
Bazel's `common:remote` configuration sets `--jobs=500` for remote builds
and enables the same remote configuration for `aquery`.
Bazel still evaluates repository rules and fetches source dependencies on
the client, so the client needs free disk space for the fetched repositories.
Remote builds use `--remote_download_outputs=minimal`. BuildBuddy's invocation
artifacts record the output's SHA-256 and size; checking them does not require
downloading ClickHouse. Bazel still downloads inputs needed for actions that
execute locally. To materialize the executable when you need it, run:

```sh
bazel build --config=remote --config=x86_64 --remote_download_outputs=toplevel //:clickhouse
```

On macOS, the result is a Linux ELF executable and cannot be run directly.

Keep API keys out of version control. Existing BuildBuddy credentials in
`~/.bazelrc` also work; no additional local file is required in that case.

Dependency ownership, generation, and action-audit details are in
[`bazel/README.md`](../README.md).

## Compare hashes without downloading executables

`remote` uses `--remote_download_outputs=minimal`. These builds use the
recorded CMake metadata and the recorded native compiler. The normalization
target produces an ELF with `--strip-all --remove-section=.note.gnu.build-id`;
the original `//:clickhouse` remains an explicit output in the same build.

```sh
bazel build --config=cmake_match --build_event_json_file=/tmp/clickhouse-x86.bep \
  //:clickhouse //bazel/initial_migration:clickhouse.normalized
python3 bazel/initial_migration/read_binary_metadata.py --arch x86_64 /tmp/clickhouse-x86.bep

bazel build --config=cmake_match_aarch64 --build_event_json_file=/tmp/clickhouse-arm.bep \
  //:clickhouse //bazel/initial_migration:clickhouse.normalized
python3 bazel/initial_migration/read_binary_metadata.py --arch aarch64 /tmp/clickhouse-arm.bep
```

The reader requires successful build and target completions, then compares the
normalized ELF's remote SHA-256 and size from Bazel's named file sets. It
also records the original ELF's digest; that digest is not the stripped
CMake comparison. Exit status 0 means the normalized hash matches, 1 means
it differs, and 2 means the metadata is missing or invalid. The reader never
downloads either ELF. This checks the recorded CMake hashes without rebuilding
CMake; it does not claim a new byte-for-byte comparison with a newly built
CMake executable.

## Check C++ dependencies

`depend_on_what_you_use` is a development-only Bazel module. The `dwyu`
configuration checks for missing direct dependencies, unused dependencies,
and public `deps` used only in private sources. Run it on the library targets
you are changing, for example:

```sh
bazel build --config=cmake_match --config=dwyu \
  //base/readpassphrase @clickhouse_libuv//:_uv
bazel build --config=cmake_match_aarch64 --config=dwyu \
  //base/readpassphrase @clickhouse_libuv//:_uv
```

The aspect checks the requested C++ libraries and keeps going after findings,
which fail the command. Read the reported include sites before changing
`deps`: public headers need public dependencies; includes used only by source
files belong in `implementation_deps`. Libraries needed for linking can be
required even if no headers are included. The custom `//:clickhouse` final
link is not a C++ library; request its underlying libraries individually.
Rebuild and compare `//:clickhouse` on both CPUs after changing the graph.

The checker uses Clang to preprocess C and C++ with the target's real
defines and include directories. Its positive and negative tests passed on
[`efc95a005`](https://app.buildbuddy.io/invocation/fd412377-53b1-42a6-b463-f76de205432a),
both with and without system include treatment on the test libraries.
Recompile each library after removing reported unused dependencies: a
dependency can supply an include directory for a header owned by another
dependency. DWYU does not diagnose that separately.
`//src/Common:cares_ptr_resolver` now owns its source and header and publishes
the `Common/...` include prefix. Its real compile and both strict DWYU reports
for it and `//base/readpassphrase` passed on
[`eb7b5b543`](https://app.buildbuddy.io/invocation/3368dac8-1546-4214-966d-8537faf7e80c).
The actual compile and strict DWYU reports for
`//base/poco/NetSSL_OpenSSL:_poco_net_ssl__compile_0`,
`//base/poco/NetSSL_OpenSSL:secure_socket`, and the six bech32, cityhash,
consistent-hashing, farmhash, metrohash and MurmurHash libraries also passed
on [7f243010](https://app.buildbuddy.io/invocation/a946eb68-b890-4f5d-842d-2efbb1b3901c).
The same run found remaining DWYU findings in other libraries, so the overall
DWYU command exited 1. The verified reports above each have no findings.
Textual C++ fragments, preprocessed assembly and forced includes are checked
in the context of the files that actually include them. Regression fixtures
preserve real missing-private, missing-public and unused-dependency failures.
The textual and toolchain regression fixtures passed on both
[x86-64](https://app.buildbuddy.io/invocation/3d11a99b-d91a-4965-af52-d663f49ee3bb)
and [ARM64](https://app.buildbuddy.io/invocation/9afcc10c-9ff8-46c5-b299-3245ae3a2458).
The additional forced-include fixtures passed on
[x86-64](https://app.buildbuddy.io/invocation/99601363-4f40-440a-9f78-b1f066d9ee02).
A check of 75 affected libraries found 41 passing, 27 with dependency findings,
and 7 with missing reports or preprocessing failures
([94975f322](https://app.buildbuddy.io/invocation/3b6069e2-fa6d-4dfc-bedf-61cbaa72edda)).
The compile failures in Config and DoubleConverter were fixed in the binary
verification snapshot below. These checks do not establish repository-wide
DWYU cleanliness.

The `ce9d63d1b` gRPC/Google Cloud checks compared native builds and actual
DWYU CPP reports before/after on
[x86-64 before](https://app.buildbuddy.io/invocation/9c7e9923-af9e-4c04-bbd6-d0192e934c4f),
[x86-64 after](https://app.buildbuddy.io/invocation/b8dcef6a-a6da-4f4b-bfa8-3e5f5427f665),
[ARM64 before](https://app.buildbuddy.io/invocation/a523061d-4891-4df1-9e45-adb1ae826863)
and [ARM64 after](https://app.buildbuddy.io/invocation/0980fdcc-ff90-4cea-a30b-3513c7e3207a).
Exactly ten unused findings disappeared in eight compiled owners; all other
findings, unverified link inputs and direct owned link symbols were unchanged.
All 2,040 measured ordinary compilations per CPU had no new inputs; 85 lost
the expected generated proto headers. Both ClickHouse and unit-test links and
ELFs stayed equal. Twelve of these 15 CPP reports pass. Address sorting,
gRPC++, and the ClickHouse gRPC proto report retain unverified link inputs.
The check does not cover the whole repository or score source-free wrappers.

The linker check also inventories the actual configured libraries. It credits an
ordinary strong definition only when its provider and direct dependency are
unique. Imported static archives can rule out a competing definition but do
not earn object ownership credit. Known `alwayslink` objects participate in
that comparison; dependencies required only for `alwayslink` remain held.
Unknown linker inputs, malformed symbol output and unreadable files remain
unverified. `link_input_reasons` records the actual file, owner and configured
dependency for those decisions.
`unverified_link_reasons` also records why each direct dependency remains
held: ambiguous definitions, a forwarded input, multiple configured routes,
retained `alwayslink` inputs, or unverified linker inputs. The auditor validates
each reported reason against the dependency and configured input. A duplicate
route does not by itself prove the declared dependency is a Bazel alias.

Scoped development-checker verification on `70b583641` → `c551c5f34`
preserved every pre-existing field for 25 genuine CPP/link reports per CPU
and passed 68 schema checks per CPU. Fresh canonical-label Boost checks
replaced two incorrectly labelled ineligible checks; the ineligible checks
remain unscored. All 33 DBMS link holds remain: none was cleared merely by
classifying an `alwayslink` input. This checker change leaves the production
BUILD files and normalized binary hashes unchanged.

The `1c8eb247a` development-checker verification covered native linker and CPP
controls and `//src:dbms__compile_0` on both CPUs. The DBMS report retains six
existing public-header misses and now holds 33 link dependencies instead of
29: libfiu, LLVM Support, lber and simdjson have ambiguous providers. All
other real header findings remain unchanged. One old LLVM Support privacy
recommendation disappears because it depended on the now-unproved symbol
credit. The configured Arrow helper removed above is absent from both reports.
This development-only change does not alter the production BUILD files or
the binary hashes recorded for `e7d519725`. DWYU is not fully clean.

### Partial compiler coverage

DWYU records each required owned original that Clang did not enter in
`unentered_owned_files`, including whether it is public or private.
It records all direct `deps` and `implementation_deps` in
`reserved_compilation_deps`, without counting a reserved dependency as
used. Unused and privatization suggestions are unavailable for that
target; actually observed missing includes and independent linker holds
remain in the report. Incomplete coverage fails the strict audit even
when the target has no dependencies. A failed `DwyuCompilerPreprocess`
action still fails without a report. Missing includes apply only to the
tested preprocessing contexts; DWYU does not infer additional macro
environments.

Scoped verification used frozen sources `c2ca52ad9` → `1807332d2`:
[x86-64 before](https://app.buildbuddy.io/invocation/2a45dc68-84bb-4569-8a83-964e76807998),
[x86-64 after](https://app.buildbuddy.io/invocation/0ce9c29c-9b09-4d70-bdff-d24e5979fd8f),
[ARM64 before](https://app.buildbuddy.io/invocation/1bbd0434-3916-47b4-ae44-bc7f3c4fbfa8)
and [ARM64 after](https://app.buildbuddy.io/invocation/d14bbab3-4ed1-4fba-9110-ed4a427e6f28).
Each run has 58/58 native and CPP records, including 29 successful
native builds and seven passing positive controls per CPU. Sixteen
existing complete fixtures retain every prior scoring field; 36 schema
checks per CPU reject malformed reports. Nine incomplete fixtures now
produce partial reports, all still failing strict DWYU. Parser and
System compile_1 retain their linker holds, Boost Graph retains its
passing report, and Msgpack preprocessing still fails without a report.
The whole repository has not passed DWYU. These runs changed only the
development checker, audit and regression fixtures; they did not measure
a new ClickHouse or CMake binary hash. The existing binary-comparison
record below is preserved.

Compiler-mode `DwyuAnalyzeTarget` now consumes preprocessing JSON,
owned-header manifests, assembly outputs and symbol/archive inputs. Raw
sources and headers remain inputs to `DwyuCompilerPreprocess`. Other DWYU
modes retain their inputs.

Frozen sources `22dde2bdd` → `7ba337afe` verify the combined checker in
[x86-64](https://app.buildbuddy.io/invocation/86051695-ecd5-4acd-9c7d-4e9acde02eee)
and [ARM64](https://app.buildbuddy.io/invocation/5c158e52-9766-4563-ac59-f15a7fa8e9e5).
Both original runs validate failed-preprocess, partial, linker and assembly
controls and before/after reports for selected production libraries. The
SDK tests pass 11/11, GatherUtils passes 8/8, and libarchive's Zstd test
round-trips 1 MiB on both architectures. DBMS test registrations remain
29,940 on x86-64 and 29,917 on ARM64; external symbol names, types and bindings
match. The unit binaries themselves changed. This is scoped DWYU validation;
20 selected production reports still have findings or holds on x86-64,
19 on ARM64, and Msgpack preprocessing remains unscored.

### Audit the configured C++ owners

On a Linux host configured for BuildBuddy, run from the workspace root:

```sh
python3 bazel/initial_migration/audit_full_dwyu.py --config cmake_match --out /tmp/clickhouse-dwyu-x86
python3 bazel/initial_migration/audit_full_dwyu.py --config cmake_match_aarch64 --out /tmp/clickhouse-dwyu-arm
```

Each output directory must be new. `manifest.json` records owners by
canonical label and native configuration checksum. It retains the original
configured query, action query and build event protocol (BEP) files. The audit
matches each original `DwyuAnalyzeTarget` action and BEP to its owner before
using a report. A clean owner requires the actual report bytes, no missing or
unused dependency findings, and no unverified assembly or link dependencies.
Missing reports and untested tool configurations do not pass.
For successful actions, the auditor accepts either explicit BEP digest/length
fields or a canonical `bytestream://` blob URI containing SHA-256 and size.
Both must match the actual report bytes; contradictory or malformed metadata
is rejected. The report must still satisfy all current CPP and linker checks.
An earlier x86 full audit ran out of Bazel Java heap. Its 286 recovered URI
outputs match their recorded bytes, but lack the newer linker fields and
remain unproved; they do not establish current repository cleanliness.

The default root `//:clickhouse` covers the binary's configured graph. It
does not cover every target in the repository. Repeat `--seed` for additional
roots; include `--seed //:clickhouse` to keep the binary when specifying other
roots. `--enumerate-only` and `--audit-target` collect partial results;
eligible untested owners remain incomplete. `--config=dwyu` retains its
existing behavior for explicitly requested targets.

## Binary comparison

Bazel snapshot `b54899c28` built all 47 requested targets on both CPUs. `//src:unit_tests_dbms` linked on both; these builds did not run the tests. The build-event SHA-256 and byte count for each remotely stripped ClickHouse ELF match the recorded native CMake reference. The unstripped SHA-256 identifies the Bazel output.

| Native Bazel build | Unstripped SHA-256 | Bytes | Stripped SHA-256 (matches CMake) | Bytes |
| --- | --- | ---: | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/a07177bd-f2c7-4e54-8096-1418e94801eb) | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929619680 | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710693736 |
| [ARM64](https://app.buildbuddy.io/invocation/47b385af-39c4-4578-b6a2-61b2aeb726f7) | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907067440 | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637316696 |

The [x86-64 DWYU run](https://app.buildbuddy.io/invocation/28038fb9-34db-428f-9dea-056a48dea3ae) and [ARM64 DWYU run](https://app.buildbuddy.io/invocation/84ed0103-9876-414f-b65c-ca24d279a7c4) failed. Each recorded 802 C++ target results and passed all 16 C++ controls. The separate producer control `proto_negative_0` rejected an unexpected Bazel action mnemonic, and the generated-file audit could not identify a unique `google_api_annotations_proto` report. The original publication check rejected this snapshot. DWYU remains incomplete.

Bazel cleanup checks build `//:clickhouse` and compare the normalized
ELF with the recorded native CMake hashes. The `7ba337afe` check uses
`--remote_download_outputs=minimal` and compares actual build-event
SHA-256/size metadata for the original and remotely stripped ELF. No
ClickHouse binary is downloaded by the verifier. Earlier checks below
include the original x86-64 stripped-byte comparisons against saved CMake.

| Verified commit | Change | x86-64 | ARM64 |
| --- | --- | --- | --- |
| `d1246980e` | Move common/base and harmful into `//base`, publish Poco headers from their libraries and compact first-party header globs | [2e6522eb](https://app.buildbuddy.io/invocation/2e6522eb-2fa8-46f7-ad32-c49903ffe3b8) | [614507cd](https://app.buildbuddy.io/invocation/614507cd-3d59-43d7-be52-4e7bd2556e68) |
| `2c17235f4` | Private web assets only at `//src:dbms__compile_0`; protobuf headers from producer libraries; remove unused C++ compiler inputs; factor CPU-dependent options and deps | [2ed56ae3](https://app.buildbuddy.io/invocation/2ed56ae3-2803-498c-a21b-037b21cdf21e) | [d3f7f72b](https://app.buildbuddy.io/invocation/d3f7f72b-d81c-4e5b-b4e1-20144e230e76) |
| `2dd816e17` | Remove inherited include directories across native and external libraries; simplify empty/default attributes; verify every header glob during migration | [4a00d9df](https://app.buildbuddy.io/invocation/4a00d9df-e8ac-414e-871f-5a51c886d28a) | [cfa9c744](https://app.buildbuddy.io/invocation/cfa9c744-f6c3-4c7b-9660-3cdd91d52bed) |
| `f6171366a` | Factor shared headers, include directories and dependency lists across 121 BUILD files | [a32378ce](https://app.buildbuddy.io/invocation/a32378ce-2d66-46b2-8da7-2e4803ee4d35) | [1a640ec9](https://app.buildbuddy.io/invocation/1a640ec9-4d35-4d19-ad8b-f69311334c98) |
| `e13fce440` | Remove another 103 inherited include directories on the supported CPUs, including 49 in System | [65fd36c3](https://app.buildbuddy.io/invocation/65fd36c3-9f55-4e1c-a124-ac612fc1053c) | [51192ad4](https://app.buildbuddy.io/invocation/51192ad4-c4c4-4c4c-89c5-3c1aaaec0af5) |
| `ddd1b7a50` | Use ordered source globs across Arrow and ten other external libraries | [5df10cd9](https://app.buildbuddy.io/invocation/5df10cd9-7d05-4618-9858-fccb3e1ebee3) | [285f173c](https://app.buildbuddy.io/invocation/285f173c-04f1-4745-a8c9-602820a86133) |
| `726df523f` | Use configured outputs for 839 generated sources and resources, including System build options; remove CPU names from generated labels | [980425b9](https://app.buildbuddy.io/invocation/980425b9-6ee9-4027-875d-6d7ed41ca914) | [996943ee](https://app.buildbuddy.io/invocation/996943ee-02cf-44d0-b55f-d90a5f9c29a7) |
| `c33ef80bb` | Publish include directories from 74 existing C++ owners and remove 2,246 inherited include entries across 145 BUILD files | [2da6ce75](https://app.buildbuddy.io/invocation/2da6ce75-4313-4f73-9f08-d5d940f9fb9c) | [4d3214e6](https://app.buildbuddy.io/invocation/4d3214e6-6429-4c9c-a2ce-c334c716c617) |
| `16eb12020` | Audit all 277 BUILD files; simplify C++ source inputs, public header ownership, private header globs and exports in 64 BUILD files | [25eb6582](https://app.buildbuddy.io/invocation/25eb6582-895f-4b94-95be-5ce171677d3e) | [86e31142](https://app.buildbuddy.io/invocation/86e31142-df80-45c5-ad8b-188762b7569c) |
| `200715e40` | Refresh the LLVM archive checksum after GitHub changed `export-subst` in Python bindings metadata; compiler sources verified unchanged | [8d6284cc](https://app.buildbuddy.io/invocation/8d6284cc-6f87-4ee3-b04a-94d41c20391f) | [9c38d891](https://app.buildbuddy.io/invocation/9c38d891-5fe5-412d-9e4e-920157433329) |
| `fd232ec86` | Build ClickStack Next resources from pinned HyperDX sources with `rules_js`; remove prebuilt asset exports, preserve native ARM execution and resource timestamps | [683e25a2](https://app.buildbuddy.io/invocation/683e25a2-7979-40d5-8f57-be184464276d) | [655f9e64](https://app.buildbuddy.io/invocation/655f9e64-2939-45fc-9111-f33bfd4c1950) |
| `46418055d` | Use actual source owners in Cargo, cbindgen, OpenSSL and LLVM compilation and link inputs | [c4aedb00](https://app.buildbuddy.io/invocation/c4aedb00-e11a-4698-baa6-820b0c7d1555) | [21144ab6](https://app.buildbuddy.io/invocation/21144ab6-66c6-416c-9c04-fa9aa67a855d) |
| `9c641d5e7` | Remove 9,325 inherited public C++ dependencies in base, Poco and program targets | [36e5297d](https://app.buildbuddy.io/invocation/36e5297d-7dcf-402e-97db-588938211dce) | [75336c4b](https://app.buildbuddy.io/invocation/75336c4b-041d-4835-9779-9a129517fed0) |
| `a0f0fc551` | Use Parquet, Cassandra, SASL and libxml header owners; remove unused Protobuf compiler inputs; share timezone inputs and common external-library sources | [6e75d9ca](https://app.buildbuddy.io/invocation/6e75d9ca-212c-4264-9ff4-751850a0f9f0) | [d04af095](https://app.buildbuddy.io/invocation/d04af095-58eb-4857-a817-166cde47f9f0) |
| `edcf793d2` | Remove 1,429 inherited pure-header dependencies and 462 unused compiler inputs from first-party source libraries | [8cd4d93b](https://app.buildbuddy.io/invocation/8cd4d93b-730a-4229-82d8-e6be2cce51a8) | [250fe733](https://app.buildbuddy.io/invocation/250fe733-bab9-4da0-91b6-95a6987ee7e7) |
| `d190c4589` | Share LLVM runtime CPU settings; use actual TableGen source inputs; collapse 25 equivalent guarded CPU selections | [4b1cc6ad](https://app.buildbuddy.io/invocation/4b1cc6ad-3ea5-416b-8a8a-acadea21462b) | [c8ba4265](https://app.buildbuddy.io/invocation/c8ba4265-4a1e-4d89-a65a-5485bc546691) |
| `0f056047a` | Remove 216 local include entries already provided by external header owners or earlier identical include directories | [1c2757f0](https://app.buildbuddy.io/invocation/1c2757f0-011f-49f6-a2e2-babbd72601cc) | [17aa7d79](https://app.buildbuddy.io/invocation/17aa7d79-99d6-4f4b-bbb0-75962de23dbc) |
| `7c4942663` | Factor common BUILD options and simplify configured target attributes | [ffcc3031](https://app.buildbuddy.io/invocation/ffcc3031-86bd-43e4-a99a-65e6b9614e2f) | [3916d923](https://app.buildbuddy.io/invocation/3916d923-3017-4ced-97e1-2985962d4bd9) |
| `b53a1f0ef` | Use exact ordered source globs for ClickHouse and external libraries | [3b3ce979](https://app.buildbuddy.io/invocation/3b3ce979-38b9-47b8-849e-68997f669b3f) | [a551cecb](https://app.buildbuddy.io/invocation/a551cecb-1d51-45a2-9489-6e9402263f84) |
| `b2a8c3c90` | Receive generated headers through their public C++ dependencies | [79256c87](https://app.buildbuddy.io/invocation/79256c87-438a-48af-ac56-e1e6d0b97324) | [e11c7df8](https://app.buildbuddy.io/invocation/e11c7df8-954a-4e5c-a743-ea2614c808f1) |
| `ad9ecc63e` | Name shared C++ build attributes after their actual owners | [1701a1f8](https://app.buildbuddy.io/invocation/1701a1f8-c9c1-423e-a458-1a0b4fd6a6a4) | [0a28ce2e](https://app.buildbuddy.io/invocation/0a28ce2e-349b-4e1a-8f0f-9bb6ad5e42f8) |
| `8566b1dbe` | Inherit tool headers and published include directories; simplify guarded program options | [65d3acf3](https://app.buildbuddy.io/invocation/65d3acf3-bc62-4fee-add3-074cc0e4210d) | [e5ff17a7](https://app.buildbuddy.io/invocation/e5ff17a7-af86-4028-ab19-ca0b2ef02c4f) |
| `9f3a2b2b9` | Make redundant public dependencies private and simplify program platform selections | [e1921a90](https://app.buildbuddy.io/invocation/e1921a90-0bd0-4acc-87c2-e0f9aa91ad73) | [23b43b04](https://app.buildbuddy.io/invocation/23b43b04-46ea-425a-9aad-aa15269596d3) |
| `122b68f39` | Remove unused glibc includes and inherit readpassphrase headers | [67d34b17](https://app.buildbuddy.io/invocation/67d34b17-0a87-48d9-bc86-442158ba20e6) | [cc3d0bed](https://app.buildbuddy.io/invocation/cc3d0bed-8924-4343-af32-855966283a24) |
| `68cb1a885` | Share Poco C compiler options and inherit generated and external headers | [3a860b85](https://app.buildbuddy.io/invocation/3a860b85-30d4-4e8c-a179-3be5c4daa7aa) | [cb9c0bab](https://app.buildbuddy.io/invocation/cb9c0bab-3a60-466d-b4c2-c2c621a51f5c) |
| `ba14ec929` | Use source globs for Arrow, Parquet, Avro, cppkafka and other external libraries, preserving CMake archive order | [ede33140](https://app.buildbuddy.io/invocation/ede33140-b8f8-418c-8b1c-1264cd36beda) | [ff1d6800](https://app.buildbuddy.io/invocation/ff1d6800-9f0e-44db-9206-524a9c05e768) |
| `7030218f2` (verification snapshot) | Replace raw include options with header dependencies across ClickHouse and external libraries; use protobuf 36.1 and the upstream Delta Cargo.lock | [990aaa99](https://app.buildbuddy.io/invocation/990aaa99-3fcd-4b07-97ab-ec7f82fbc252) | [09ba3815](https://app.buildbuddy.io/invocation/09ba3815-1d12-4485-a088-d5f9734a6a90) |
| `af5de0992` (verification snapshot) | Use native C++ proto rules for all 65 Google Cloud protos and typed LLVM runtime includes, retaining protobuf archive order | [e1b2cf7a](https://app.buildbuddy.io/invocation/e1b2cf7a-8453-4e39-8f11-b03f7cf95366) | [3047e868](https://app.buildbuddy.io/invocation/3047e868-4a34-42a6-97ce-8931c754ddf7) |
| `9b13f2efd` (verification snapshot) | Add development-only DWYU checks; remove 10 redundant header wrappers; make allocator, Access/Common, Poco and external dependencies private or direct at their owners | [8dfb9f12](https://app.buildbuddy.io/invocation/8dfb9f12-d2fc-499c-987a-2ba548367ee0) | [78d74d65](https://app.buildbuddy.io/invocation/78d74d65-3465-4f77-9ff9-674397a5b3c5) |
| `15a331cc2` (verification snapshot) | Put Poco headers on their libraries; correct Poco dependency directions and remove 29 redundant header wrappers across all consumers; simplify equivalent Bazel labels repository-wide; fix public and private providers for c-ares, cityhash, sorting, and Poco | [904cb2e1](https://app.buildbuddy.io/invocation/904cb2e1-282c-4a8c-b491-5d938e61e1db) | [1f513462](https://app.buildbuddy.io/invocation/1f513462-5698-4a61-89f7-f87b05aff800) |
| `8a53eb4fb` (verification snapshot) | Stop publishing the entire Linux sysroot in every native library’s headers and remove implicit LLVM API header dependencies | [0b00df52](https://app.buildbuddy.io/invocation/0b00df52-f1ef-4819-a5bf-c0659491e127) | [1e8ba352](https://app.buildbuddy.io/invocation/1e8ba352-3618-45d3-8202-ce8ce9bec696) |
| `2003dfd8b` (verification snapshot) | Put OpenSSL, IO, Google Cloud and AWS headers on their compiled owners, keep first-party implementation headers private, and remove Poco NetSSL and Foundation header cycles | [d402dea2](https://app.buildbuddy.io/invocation/d402dea2-7edb-4a62-9f15-2b5be0729a56) | [0662ba9d](https://app.buildbuddy.io/invocation/0662ba9d-43f5-4225-95e0-30183193b590) |
| `efc95a005` (verification snapshot) | Complete NetSSL and IO header ownership; put Abseil, gRPC, Kafka, Wasmtime and libpqxx headers on their compiled owners; remove inherited header and runtime dependencies; normalize Bazel labels; use Clang in DWYU | [e414861c](https://app.buildbuddy.io/invocation/e414861c-295a-4a5a-926b-67ff5d5a29db) | [a3557b7c](https://app.buildbuddy.io/invocation/a3557b7c-8477-43eb-b681-d54bd9a72d66) |
| `eb7b5b543` (verification snapshot) | Use the compiled Cares, farmhash, metrohash, MurmurHash and bech32 libraries as header owners; keep every IO archive member in its original position; check standalone exported macros in DWYU | [1f816158](https://app.buildbuddy.io/invocation/1f816158-633c-42e4-a617-accce026ef07) | [bb3f2893](https://app.buildbuddy.io/invocation/bb3f2893-a634-4682-97fe-fc19082bb8b8) |
| `064407cd4` (verification snapshot) | Get Mongo and libssh generated headers from their compiled owners instead of listing them in 21 first-party header sets; remove the empty TimeSeries and UniqTheta header targets | [467a5e89](https://app.buildbuddy.io/invocation/467a5e89-e915-4074-b1aa-160aae9c158b) | [76b13055](https://app.buildbuddy.io/invocation/76b13055-e8b7-4064-b98c-4e4b8a22e956) |
| `b82e9b694` (verification snapshot) | Give Common, PromQL grammar and consistent hashing compiled header owners; remove NetSSL's remaining Common header dependencies, remove six unused LLVM header sets, and assign CRoaring C and C++ headers to their compiled libraries | [8a31eb38](https://app.buildbuddy.io/invocation/8a31eb38-7c4f-45ba-b1b3-bd281d93b095) | [3484d6a6](https://app.buildbuddy.io/invocation/3484d6a6-a321-4685-8e31-01192bb7c62f) |
| `18908441f` (verification snapshot) | Fix Abseil, Protobuf and gRPC direct dependencies, simplify program and benchmark libraries, publish ZooKeeper and DoubleConverter headers from their compiled owners, and check textual and forced includes in DWYU | [2ae786c6](https://app.buildbuddy.io/invocation/2ae786c6-a98e-4363-bf08-43db93868729) | [464b86d1](https://app.buildbuddy.io/invocation/464b86d1-5f64-4dd8-b85c-34f33ba10385) |
| `140e24015` (verification snapshot) | Remove unused root headers and their dependencies, keep PCG as its genuine header-only library, publish Base C++ headers from their compiled owner, and remove inactive Abseil/Protobuf private headers | [119f4191](https://app.buildbuddy.io/invocation/119f4191-46fd-47ab-9ae4-65f3d9390f8c) | [b25fb956](https://app.buildbuddy.io/invocation/b25fb956-1b83-4188-9b4f-843664d4ec83) |
| `c86b592b6` (verification snapshot) | ConfigProcessor and ConfigReloader supplied by compiled Config; use direct Config dependencies in 13 consumers; trim 37 redundant benchmark deps, three Abseil deps and an inactive gRPC private header | [749ccc62](https://app.buildbuddy.io/invocation/749ccc62-6b79-4fe6-abfb-f943e3afadc1) | [5be37f6c](https://app.buildbuddy.io/invocation/5be37f6c-e2b2-4e9a-86a6-dc45501f44be) |
| `88858ec61` (verification snapshot) | Supply LLVM headers and textual fragments from compiled LLVM libraries, remove llvm_source_headers, assign 65 gRPC generated APIs to their compiled libraries, and fix DWYU's handling of Bazel's implicit textual .inc headers | [ee34dbbb](https://app.buildbuddy.io/invocation/ee34dbbb-ce82-4169-94dd-03a293be1745) | [f45a3965](https://app.buildbuddy.io/invocation/f45a3965-ad6a-4660-b8c2-d706f08a47f9) |
| `b179d4ba0` (verification snapshot) | Publish Common IO APIs from their compiled IO library, replace IO header dependencies with public or implementation deps, and restrict Arrow C, Arrow C++, Flight and Parquet headers to their appropriate consumers | [a6c58fc4](https://app.buildbuddy.io/invocation/a6c58fc4-0040-4f76-a702-5ef328be9b40) | [cb22a76f](https://app.buildbuddy.io/invocation/cb22a76f-5a28-45ea-a124-1f84d9b56637) |
| `f4cd46f3b` (verification snapshot) | Publish the Config API from compiled Config; fix LLVM textual-header ownership, remove uncompiled BuzzHouse headers, and assign the Google Cloud IAM credentials stub to its compiled library | [b1b75616](https://app.buildbuddy.io/invocation/b1b75616-ec67-4b29-915a-01a4851cd81a) | [b461c6b1](https://app.buildbuddy.io/invocation/b461c6b1-67a6-463f-b9d6-b63d5524d676) |
| `43e4f4de2` (verification snapshot) | Publish the DBMS API once from its compiled owner; remove the 1,693 duplicated headers and broad source-header dependencies from DBMS splits 1–4; give unit_tests_dbms its direct fast_hex dependency | [b145879c](https://app.buildbuddy.io/invocation/b145879c-5b4b-4b74-be4e-be0aa45e8223) | [8a931a75](https://app.buildbuddy.io/invocation/8a931a75-5234-49bc-816c-39e3cee8c0b1) |
| `e098677a4` (verification snapshot) | Reduce DBMS compile splits 1–4 to their directly used private dependencies; publish the AI SDK dependency required by DBMS client headers | [341f820c](https://app.buildbuddy.io/invocation/341f820c-e358-4c39-9a33-ffb1c86d251d) | [4067668d](https://app.buildbuddy.io/invocation/4067668d-fe4a-46c2-8783-354de2fb5cd9) |
| `ac22626e9` (verification snapshot) | Publish actual DBMS and Common public-header dependencies, remove inactive DBMS/Arrow headers, and handle a C translation unit that includes its own implementation | [6d19c7f6](https://app.buildbuddy.io/invocation/6d19c7f6-e609-4c42-b7b4-5eb6912e78c6) | [ffb7eef7](https://app.buildbuddy.io/invocation/ffb7eef7-46be-4f4f-aee8-c768fd51aefd) |
| `637108a03` (verification snapshot) | Reduce unused Abseil dependencies; make private dependencies private and retain only active source headers across 25 checked targets | [669eba2d](https://app.buildbuddy.io/invocation/669eba2d-c274-4417-b5c2-980759c14b85) | [c5fe3d6f](https://app.buildbuddy.io/invocation/c5fe3d6f-8e87-4392-954c-20f8224398eb) |
| `5c029cef8` (verification snapshot) | AWS: direct public dependencies, private implementation dependencies, active Linux source headers and no unused libc++ defaults in 11 overlays | [4bf078c3](https://app.buildbuddy.io/invocation/4bf078c3-9ef2-4b06-8819-0073be7f0d2a) | [3dc13d65](https://app.buildbuddy.io/invocation/3dc13d65-9e4e-4244-844d-acb3c8fa1b6f) |
| `e4984d411` (verification snapshot) | gRPC: direct Abseil public dependencies and omit source headers for unbuilt TLS and telemetry implementations | [63213d8f](https://app.buildbuddy.io/invocation/63213d8f-a3f2-4444-be21-75919fb7a48d) | [8ef864af](https://app.buildbuddy.io/invocation/8ef864af-1e23-4f31-9e34-9b876c353453) |
| `0964b777b` (verification snapshot) | LLVM: use implementation dependencies in 14 compiled libraries; DWYU expands compiler Make variables to inspect CCTZ embedded inputs | [7a810b1f](https://app.buildbuddy.io/invocation/7a810b1f-f74a-49f3-9cb6-3101775b488e) | [57baa8b8](https://app.buildbuddy.io/invocation/57baa8b8-d98c-400f-ad92-cf14a539b7b5) |
| `95dd32354` (verification snapshot) | gRPC unsecure: compiled Abseil owners for its public and private includes; c-ares implementation dependency | [7c7ac607](https://app.buildbuddy.io/invocation/7c7ac607-6df0-424d-bc80-7574d02383e1) | [bd0d4ccc](https://app.buildbuddy.io/invocation/bd0d4ccc-e41d-43df-9bd0-bcfebcf7b452) |
| `31ca0af80` (verification snapshot) | glibc/OpenSSL assembly: actual native assembly dependency checks and ARM-only OpenSSL header | [2fcde940](https://app.buildbuddy.io/invocation/2fcde940-2f99-4e6c-b79e-d77f171c58a9) | [97d13d35](https://app.buildbuddy.io/invocation/97d13d35-2517-4090-b8fe-e51e9e363364) |
| `025dd9050` (verification snapshot) | Generated headers from their Rust/gRPC owners; Google Cloud public/private headers and protobuf dependencies | [5222418a](https://app.buildbuddy.io/invocation/5222418a-fcd0-43d4-9097-18317049969b) | [3eaeb1fc](https://app.buildbuddy.io/invocation/3eaeb1fc-bb3b-40bf-90c0-b7a0e4788adb) |
| `bca4a5ef2` (verification snapshot) | Remove C-only libraries' unused libc++ headers; private Common implementation dependencies and System fragment | [4f9e9b21](https://app.buildbuddy.io/invocation/4f9e9b21-2cd7-4c6a-ad27-b2fca52fd713) | [a856ced1](https://app.buildbuddy.io/invocation/a856ced1-259b-476a-b3c0-75d1643edc25) |
| `d15ee9fae` (verification snapshot) | Arrow HDFS/zstd and Parquet version/RapidJSON/xsimd header ownership | [b1dba745](https://app.buildbuddy.io/invocation/b1dba745-7289-49f0-b108-0cacb8cae2ac) | [ced8a66f](https://app.buildbuddy.io/invocation/ced8a66f-04e0-40b1-b37f-1e0af117fa70) |
| `e97b28fb6` (verification snapshot) | Poco and program library public headers and implementation dependencies | [1bdd91c3](https://app.buildbuddy.io/invocation/1bdd91c3-a230-4418-9fff-42627e92229c) | [212269c1](https://app.buildbuddy.io/invocation/212269c1-5fdf-46b3-b0e2-290b7d083a47) |
| `b63670f02` (verification snapshot) | LLVM C/assembly runtime inputs and backend public header ownership | [91b5daf6](https://app.buildbuddy.io/invocation/91b5daf6-cf9a-4da5-903b-8e831f767858) | [8f9e6844](https://app.buildbuddy.io/invocation/8f9e6844-7460-43be-b7d2-19d26050eb23) |
| `5568e6f0e` (verification snapshot) | gRPC ownership and compiler-based consumer audits | [fd6e4d56](https://app.buildbuddy.io/invocation/fd6e4d56-63c0-4ccd-9698-884960e46025) | [bc432a7c](https://app.buildbuddy.io/invocation/bc432a7c-2df3-40ed-abf9-067582048607) |
| `48813843b` (verification snapshot) | Remove 20 redundant FlatBuffers generated-header references | [6c14921a](https://app.buildbuddy.io/invocation/6c14921a-ceb6-418f-b03f-77b375a3e994) | [218d91af](https://app.buildbuddy.io/invocation/218d91af-8695-4b00-9e3c-82ae80a4c1fc) |
| `1727c9490` (verification snapshot) | DWYU Clang guarded-header attribution and consumer checks | [6e83876d](https://app.buildbuddy.io/invocation/6e83876d-27cb-4314-b3c4-0d5cef4b660d) | [350cdec5](https://app.buildbuddy.io/invocation/350cdec5-eeaf-4af6-92c6-3047187d8b02) |
| `081e42f92` (verification snapshot) | Remove four GCP dependencies verified by native isolated links | [93141dec](https://app.buildbuddy.io/invocation/93141dec-b288-485b-9e0f-fa1601abfa81) | [5649d2b1](https://app.buildbuddy.io/invocation/5649d2b1-0450-4495-b813-aecd5757e09f) |
| `cc5b05740` (verification snapshot) | C++ library ownership: Cap'n Proto, gtest, ANTLR, Minizip, WordNet and other dependencies | [0fbd854d](https://app.buildbuddy.io/invocation/0fbd854d-aa33-44bb-92a7-b92f1dc5ccc1) | [858f9a68](https://app.buildbuddy.io/invocation/858f9a68-849b-475f-8e01-54666a79367e) |
| `fd40501a2` (verification snapshot) | Native C++ header ownership; Poco Foundation/XML and program deps; Avro public/private deps | [8edb9851](https://app.buildbuddy.io/invocation/8edb9851-f92a-4ae4-b8fd-06fef6688e42) | [818fcf79](https://app.buildbuddy.io/invocation/818fcf79-ae68-4ec8-8ad5-165c79f83cfa) |
| `b2c047c57` (verification snapshot) | Access and Parser public headers from compiled libraries; direct owning dependencies in DBMS and IO | [e29e417a](https://app.buildbuddy.io/invocation/e29e417a-bfcf-4b57-bfa1-1905e0df6848) | [be4edfb7](https://app.buildbuddy.io/invocation/be4edfb7-76c2-4195-8344-b394a8770669) |
| `7e75a6d00` (verification snapshot) | Base public headers in common, Poco XML/Expat ownership, complete REST dependencies and LLVM Atomic owner | [0f4d2fc4](https://app.buildbuddy.io/invocation/0f4d2fc4-61f4-4692-a166-37a9575cadcc) | [30935dd1](https://app.buildbuddy.io/invocation/30935dd1-15e2-4e75-9894-9e26567bb370) |
| `e70a4f987` (verification snapshot) | Remove unrelated headers and 138 dependencies from the generated System licenses source | [64bfaf5b](https://app.buildbuddy.io/invocation/64bfaf5b-4645-4eb9-a11c-cfa5224cb9d2) | [32f8ffef](https://app.buildbuddy.io/invocation/32f8ffef-e681-414c-b65a-c4a4e72247d0) |
| `f6b06dbce` (verification snapshot) | Select the 125 ARM-only compiler-rt outline atomic targets on ARM | [4da44d66](https://app.buildbuddy.io/invocation/4da44d66-718e-488a-8fab-12971e70e25e) | [2b778ee0](https://app.buildbuddy.io/invocation/2b778ee0-c6f9-41f5-b6a4-3b4c0a6d1ff5) |
| `ebef76675` (verification snapshot) | Correct Abseil header ownership and Linux inputs; remove redundant Arrow dependencies | [0f7b59a5](https://app.buildbuddy.io/invocation/0f7b59a5-eb82-4090-affc-c07036ecb7a5) | [b7a72f38](https://app.buildbuddy.io/invocation/b7a72f38-fa50-45af-815f-ec63cce8d946) |
| `21fc69dbe` (verification snapshot) | Remove Arrow header aggregate, give DBMS direct Arrow/Parquet owners, and remove unused Parquet Boost regex | [6fa26f91](https://app.buildbuddy.io/invocation/6fa26f91-ddda-4b63-8f04-8d123608ba19) | [c824f631](https://app.buildbuddy.io/invocation/c824f631-dc12-4dec-97be-fcab22810c16) |
| `0f42b2607` (verification snapshot) | Remove the redundant Common dependency from Google Cloud REST; retain Common through Poco TLS | [42d3c7aa](https://app.buildbuddy.io/invocation/42d3c7aa-b12b-485c-84ae-2ee07afdbdd4) | [9b8f8ef4](https://app.buildbuddy.io/invocation/9b8f8ef4-ceb1-4e71-847c-6aa8289fe6cf) |
| `97b2d9a3e` (verification snapshot) | Correct Linux private headers and dependencies for AI SDK, CCTZ, CRC32C, lemmagen, NuRaft, replxx, SILK and ZXC | [4810ce74](https://app.buildbuddy.io/invocation/4810ce74-5c61-4347-9a8e-b6ba1af6e0b6) | [1bd31480](https://app.buildbuddy.io/invocation/1bd31480-0e1c-453e-9ec9-025ae693bf5e) |
| `35609a001` (verification snapshot) | Give OpenSSL, HDFS and Snappy headers to compiled owners; preserve private crypto assembly and HDFS implementation deps | [06b8e9e4](https://app.buildbuddy.io/invocation/06b8e9e4-9195-4ea8-b2c0-fe6fead6d473) | [55a6e9ae](https://app.buildbuddy.io/invocation/55a6e9ae-c112-44dc-8f29-8db49e64a4de) |
| `cba24794c` (verification snapshot) | Give libpq only its 49 private Linux headers and three public API headers | [c80af064](https://app.buildbuddy.io/invocation/c80af064-b110-4d96-b39b-77f8b360c97c) | [24f4bb87](https://app.buildbuddy.io/invocation/24f4bb87-4ace-4672-bab1-1be3fb388f3d) |
| `f05573ccb` (verification snapshot) | Give SASL its two public API headers and eight private Linux headers | [b600961e](https://app.buildbuddy.io/invocation/b600961e-221a-442d-9e7e-440deab28b33) | [fe01e4e0](https://app.buildbuddy.io/invocation/fe01e4e0-0061-4d3f-b172-1701a9714641) |
| `2cb60420e` (verification snapshot) | Give libxml2 and jemalloc their configured public headers; remove unused private, C++ runtime and disabled zlib inputs | [c6e27cc7](https://app.buildbuddy.io/invocation/c6e27cc7-329a-4937-a305-01903f32574f) | [171dfda4](https://app.buildbuddy.io/invocation/171dfda4-247b-416f-9be2-f0cf93e01788) |
| `826781f96` (verification snapshot) | Remove unused direct NuRaft dependencies from Compression and ZooKeeper; retain the required ZooKeeper base dependency | [e2cb9c03](https://app.buildbuddy.io/invocation/e2cb9c03-d73a-4759-bd37-5669ed9be2b8) | [1493e7ca](https://app.buildbuddy.io/invocation/1493e7ca-b75b-4118-b902-3cbdfd0abef9) |
| `125dea6d8` (verification snapshot) | Remove 722 copied dependency entries from TableFunctions, TimeSeries and GatherUtils; add their direct private C++ imports | [1d0669af](https://app.buildbuddy.io/invocation/1d0669af-6566-4ba5-a1e7-23a9d4d1e093) | [4c01c0ca](https://app.buildbuddy.io/invocation/4c01c0ca-e55e-47c9-a80e-0620663695bf) |
| `ea65909ca` (verification snapshot) | Shorten default-package labels in migration output and the contrib documentation; keep source verification and canonical dependency identities | [1c20801e](https://app.buildbuddy.io/invocation/1c20801e-08b4-4856-bd08-37b77dac7cfe) | [74ca0038](https://app.buildbuddy.io/invocation/74ca0038-64a2-4b79-935c-892e2a03c1fb) |
| `56218e4c9` (verification snapshot) | Give Kerberos and librdkafka their included C files as private headers; retain Linux headers and direct implementation dependencies | [2ff05a4e](https://app.buildbuddy.io/invocation/2ff05a4e-67e4-46a5-9d25-c092d1dc5b3a) | [e68724a3](https://app.buildbuddy.io/invocation/e68724a3-9e24-4f7f-bab2-fa62e5002309) |
| `f130f2e94` (verification snapshot) | Give H3, libarchive, libdeflate and XZ their owned C headers and libarchive its private compression dependencies | [2e5b7a65](https://app.buildbuddy.io/invocation/2e5b7a65-4ab0-409e-8a11-fbc88f6c0713) | [647685df](https://app.buildbuddy.io/invocation/647685df-00ff-4b39-b820-b6a42957ff80) |
| `d7867522c` (verification snapshot) | Declare OpenSSL's used Linux headers and private included C files; keep the crypto assembly and compiler builtins dependencies | [f78509e7](https://app.buildbuddy.io/invocation/f78509e7-2ebe-4120-9093-7eeb21e3a254) | [01500365](https://app.buildbuddy.io/invocation/01500365-a053-4d91-ac1c-d87d7c523611) |
| `2a8623ca3` (verification snapshot) | Make RangeHashed templates private to their instantiations and give URL lookup its own header; three DWYU reports recovered per CPU. Five existing Dictionaries header findings and unused deps remain | [fd1065eb](https://app.buildbuddy.io/invocation/fd1065eb-0d16-40e9-b45a-52d576d17955) | [9c3bfa49](https://app.buildbuddy.io/invocation/9c3bfa49-1f14-46b8-ae45-ce421f945a41) |
| `97fe6288d` (verification snapshot) | Give Functions and array headers one compiled owner; remove 166 redundant forwarding deps and three unused jemalloc deps. All 13 compiled-owner reports available across both CPUs; source-free forwarders are native-only, main Functions findings remain | [883e5567](https://app.buildbuddy.io/invocation/883e5567-2a9a-45c3-932c-a7907b6ba071) | [04df3d76](https://app.buildbuddy.io/invocation/04df3d76-1f62-460b-a07e-2c5e79220ca2) |
| `323af11bf` (verification snapshot) | Build DWYU's three C++ executables with the complete exec toolchain; collect actual reports in original native configurations | [08174423](https://app.buildbuddy.io/invocation/08174423-de64-4542-8a56-eb9481c96229) | [ae0b631e](https://app.buildbuddy.io/invocation/ae0b631e-4068-4e2d-83fd-a51332d7ec1c) |
| `299a7e955` (verification snapshot) | Fix Poco Foundation, XML and JSON header ownership; trim unused pdjson and thread_pool deps and fix Expat link counts | [80c62074](https://app.buildbuddy.io/invocation/80c62074-bc23-48fd-8bbc-2d4d919bf418) | [00f370bc](https://app.buildbuddy.io/invocation/00f370bc-440a-4244-9e2d-ae2366248a12) |
| `2ba054d0e` (verification snapshot) | Declare the existing owners used by Functions' public headers; remove repeated specialized Functions and array deps. Missing direct includes are fixed in both main owners; other unused deps remain. Separate checker323 run on this graft; earlier97f→7a5 is not a checker323 run | [95378b90](https://app.buildbuddy.io/invocation/95378b90-0999-46af-82e0-f64a3318f386) | [c4ba44e7](https://app.buildbuddy.io/invocation/c4ba44e7-dcbe-4571-b6ec-564f0ac8b5cf) |
| `24266c4eb` (verification snapshot) | Keep DBMS and IO implementation-only Common headers private, exclude two unused Linux headers, and remove secure_socket's unused IO dependency. Four Common missing-public-include pairs are fixed; other Common and DBMS findings remain. Original d786→ff47 before/after scope and separate checker323 graft scope | [b80b1203](https://app.buildbuddy.io/invocation/b80b1203-cb77-4b2f-bee2-1668388e0044) | [90816327](https://app.buildbuddy.io/invocation/90816327-9392-49b7-b69c-d695ad3e3320) |
| `d50270c3e` (verification snapshot) | Declare PCG, Abseil and ClickHouse config dependencies for five Dictionaries headers; remove unused Dictionaries and URL library dependencies. Remaining unused-dependency reports are still recorded | [30ff2c5f](https://app.buildbuddy.io/invocation/30ff2c5f-6d18-4241-a58c-d48a5a1611d2) | [d2c10f54](https://app.buildbuddy.io/invocation/d2c10f54-9809-4534-934d-a4d5048102fe) |
| `7719fe1f8` (verification snapshot) | Remove unused Functions and array dependencies with retained compiled owners; delete the obsolete generated dep and proto lists. Original scoped DWYU and native checks on 2ba→7cc, remaining BUILD statements identical after unused-assignment cleanup; original full binary checks on this snapshot | [3b32e9f2](https://app.buildbuddy.io/invocation/3b32e9f2-50bd-44c2-aaca-5ad575e18181) | [9a77a441](https://app.buildbuddy.io/invocation/9a77a441-f291-4e9a-9417-dde91c382877) |
| `4958dc478` (verification snapshot) | Make Poco Net private to base common; remove unused compiler-rt and configured pcg_random edges | [87bb8489](https://app.buildbuddy.io/invocation/87bb8489-0b12-4e85-8274-7c87b0547468) | [7b53dd43](https://app.buildbuddy.io/invocation/7b53dd43-8f29-4ce5-826c-4eb48861927c) |
| `937b54ef5` (verification snapshot) | Keep Poco PCRE implementation headers in both Foundation owners' srcs. Actual d50 before/after DWYU checks and DBMS links pass without a new consumer finding; full binary hashes verified on this graft | [8969cc88](https://app.buildbuddy.io/invocation/8969cc88-202e-407b-9446-6f906035507f) | [c0fa03e2](https://app.buildbuddy.io/invocation/c0fa03e2-b829-449b-8ed6-184b7ee9e46b) |
| `d25b982ba` (verification snapshot) | Make seven LLVM implementation .def includes private in their six owning libraries; preserve shared VecFuncs.def | [1880e740](https://app.buildbuddy.io/invocation/1880e740-765b-48c6-ae85-57dfc4b3e877) | [e2995ad0](https://app.buildbuddy.io/invocation/e2995ad0-0ab4-4657-9d14-f36697c1938e) |
| `a97f72c16` (verification snapshot) | Keep private implementation headers in Parquet and Capnproto srcs; keep Arrow, IDNA, GoogleTest, Jieba and aws-c-http implementation includes in private_hdrs | [29bbb794](https://app.buildbuddy.io/invocation/29bbb794-8ba1-4a5c-8439-ea867e745aec) | [28a7ef5d](https://app.buildbuddy.io/invocation/28a7ef5d-8498-436c-acb1-9deec2f19a60) |
| `91780ad58` (verification snapshot) | Remove 171 unused direct Programs dependencies; replay actual Programs CPP, keeper and DBMS native builds; preserve previous Rust archive reachability and recorded ordinary-link limitations | [74cf395d](https://app.buildbuddy.io/invocation/74cf395d-46cd-4a5d-b040-82cfc896d080) | [945f8bda](https://app.buildbuddy.io/invocation/945f8bda-e07d-4965-bafe-fa3a92254224) |
| `ff49dbe3e` (verification snapshot) | Declare Client SQL keywords in DBMS private srcs; remove three public declarations without reordering BUILD attributes. Actual current Programs and baseline DBMS DWYU findings unchanged, controls verified, native DBMS link arguments identical to the current Programs commit. | [865f1f9e](https://app.buildbuddy.io/invocation/865f1f9e-dff9-45b7-b075-ce6dc00170f7) | [39b05fd0](https://app.buildbuddy.io/invocation/39b05fd0-3918-4801-b33a-b8ad121eba6f) |
| `99e04b15e` (verification snapshot) | Keep generated .pb.cc private in GRPC, Prometheus, and ORC; let Prometheus own its public .pb.h and remove its unused bridge | [40f39eeb](https://app.buildbuddy.io/invocation/40f39eeb-c622-4831-b9ba-25895dfa2d2f) | [387afbab](https://app.buildbuddy.io/invocation/387afbab-0d5a-4359-a283-cbd56fa9c29d) |
| `500b8d6af` (verification snapshot) | Publish Loggers and Daemon API headers through their compiled libraries and declare the direct Poco Net implementation import. Native CPP and keeper/DBMS links verified on the SQL source; the exact two-file graft on the protobuf source also passes owner/keeper CPP and compiler/link argument checks | [9c55a39b](https://app.buildbuddy.io/invocation/9c55a39b-b49c-43cb-8564-70352ce673b6) | [e60e6f98](https://app.buildbuddy.io/invocation/e60e6f98-29df-4218-a75f-7b4fcbe6a7aa) |
| `e8e4f7b8c` (verification snapshot) | Remove 37 unused Functions and array deps; declare Wagyu's public Mapbox dependency and sparsehash on src:headers. Current CPP and eight DBMS/ClickHouse links passed. Five existing findings owners and Mapbox's unavailable CPP report remain | [528e3df4](https://app.buildbuddy.io/invocation/528e3df4-7578-4feb-bfc6-111d825e5267) | [07fd4622](https://app.buildbuddy.io/invocation/07fd4622-de7b-4d87-b95b-6ae6b0d14450) |
| `06c669086` (verification snapshot) | Declare 15 libpqxx implementation gates and wait.hxx in srcs; retain public contextual libpqxx headers | [07dd46db](https://app.buildbuddy.io/invocation/07dd46db-48a4-48ee-952a-7a3b8b80d6f2) | [f7151b4a](https://app.buildbuddy.io/invocation/f7151b4a-95b7-4464-a4b7-6f1fab243b73) |
| `34e24dcf0` (verification snapshot) | Publish Parser API headers through clickhouse_parsers and declare its public Poco and configuration dependencies. Current native CPP and DBMS links verified on both architectures; remaining DWYU findings are recorded | [a4a1a1af](https://app.buildbuddy.io/invocation/a4a1a1af-9a9b-47b5-9c4d-b558c7a76be6) | [99f65593](https://app.buildbuddy.io/invocation/99f65593-9f44-46c8-835b-4e82f649a4dd) |
| `753cdf2d8` (verification snapshot) | Declare LLVM glibc/kernel installed headers and private LLVM runtime inputs; limit glibc/kernel public include paths to the bootstrap toolchain. Current DBMS/LZ4/base header traces and CPP controls pass; LLVM ABI/sysroot DWYU reports remain unproved | [f6bb3b48](https://app.buildbuddy.io/invocation/f6bb3b48-cafb-4f4a-b49d-255e0d1187a2) | [dc1ca73d](https://app.buildbuddy.io/invocation/dc1ca73d-4afb-4638-8406-7c56ca3c4f2d) |
| `bfcded04a` (verification snapshot) | Publish Common and Access headers from their compiled owners, remove measured unused dependencies, and provide absl_function_ref through src headers. Original and current x86/ARM native builds, DBMS links and DWYU controls pass with no new findings. Existing findings remain. Both full ELF and normalized CMake hashes match | [e286693e](https://app.buildbuddy.io/invocation/e286693e-519b-455c-b280-c5ba0e2c1014) | [8893d0b5](https://app.buildbuddy.io/invocation/8893d0b5-effe-4ca0-825e-16d29980b693) |
| `3c6652f45` (verification snapshot) | Correct declared private headers and non-header inputs in glibc-compatibility and ten external BUILD files. Native 13/14-target builds and both sets of DWYU controls pass. All 10/11 edited compiled owners now produce real reports (2 clean, 8/9 with remaining findings). BSON, Wasmtime, lz4_frame and LLVM runtime failures remain. Both full ELF and normalized CMake hashes match | [cb362962](https://app.buildbuddy.io/invocation/cb362962-8ba9-4243-9929-0eafe0c44dea) | [144f7554](https://app.buildbuddy.io/invocation/144f7554-67f2-4fd8-8c1b-969f998d6031) |
| `bde761350` (verification snapshot) | Make src headers provide DoubleConverter.h and SettingsEnums.h dependencies. //src/Common:Common now has clean original compiler-backed DWYU reports on both CPUs. Owner/consumer native builds and DBMS links pass; the three positive and two negative controls remain valid. Other recorded findings remain. Both ELF and normalized CMake hashes match | [a705198b](https://app.buildbuddy.io/invocation/a705198b-2876-4fe0-8807-87ea43817282) | [286bd6b8](https://app.buildbuddy.io/invocation/286bd6b8-a805-4f7a-9d41-4e6a332c7325) |
| `84ce41171` (verification snapshot) | Publish AggregateFunctions and Compression headers from their compiled owners; move private dependencies to implementation_deps and remove unused declarations. Keep the compiled Parser owner for public IAST_fwd.h and private AST includes. Current owner and consumer CPP reports have no new missing includes; existing findings remain. Native //src:unit_tests_dbms links, 3 existing tests, and 3 positive/2 negative DWYU controls pass on both CPUs. Original and normalized ClickHouse ELF hashes remain as recorded below. | [8475769e](https://app.buildbuddy.io/invocation/8475769e-423a-4f25-b206-a41c05e46af6) | [43e54a22](https://app.buildbuddy.io/invocation/43e54a22-defc-418f-94a0-c72153797cb1) |
| `1a296454f` (verification snapshot) | Make FunctionsHashing.h and its hashing dependencies private to their compile owners. UniqTheta now uses five implementation dependencies; its CPP reports clear 242 x86/240 ARM unused dependencies and seven missing include pairs. Functions Obj0's existing findings remain unchanged; Obj1 remains clean. Current native compiler and DBMS links pass on both CPUs. Original and normalized ClickHouse ELF hashes remain as recorded below | [7357cc7c](https://app.buildbuddy.io/invocation/7357cc7c-404f-47c0-99be-4ee930d4289c) | [701db24a](https://app.buildbuddy.io/invocation/701db24a-5af2-4697-a250-0b698a6322f7) |
| `21a7cd54d` (verification snapshot) | Keep 20 generated LLVM backend implementation includes in their four compiled owners; remove seven resulting unused AggregateFunctions, Config and runtime-filter CodeGen deps. Scoped native JIT and DBMS links and original CPP reports have no new findings; existing findings remain | [637f9e63](https://app.buildbuddy.io/invocation/637f9e63-2b90-49fe-b0b3-5ca11e123f5d) | [b8ea76c7](https://app.buildbuddy.io/invocation/b8ea76c7-cdfc-4f1a-9363-788f97b60ea9) |
| `8a196ff8b` (verification snapshot) | Keep generated Flight.grpc.pb.cc private to its Arrow Flight importer. Scoped Arrow, Parquet and DBMS native links and CPP reports match on both architectures; existing DBMS findings remain | [bd96a0c5](https://app.buildbuddy.io/invocation/bd96a0c5-9103-4d8b-8dcf-3419e0053a86) | [d1ca0f01](https://app.buildbuddy.io/invocation/d1ca0f01-9031-458a-b96c-f8f753cec18a) |
| `d47eb3376` (verification snapshot) | Let mysqlxx own its public headers and declare its direct implementation dependencies; remove unused Config and mysqlxx deps. Both libraries are CPP-clean on x86 and ARM; 15 scoped consumer reports and all ordinary DBMS link inputs are unchanged. Full and normalized hashes match | [22008281](https://app.buildbuddy.io/invocation/22008281-772e-479b-b65f-1d40a51cbe4a) | [7d8c7863](https://app.buildbuddy.io/invocation/7d8c7863-170f-4c55-b102-b960a31a4da9) |
| `cf4a6a1ba` (verification snapshot) | Correct direct owners and private includes in six external BUILD files. All edited 5/6 native reports resolve missing includes with no new typed findings; private and unused dependency findings remain. Three positive and two negative controls pass. Normal unit_tests_dbms compiles/links on both CPUs with every edited library's objects in the same link arguments and inputs. Full and normalized CMake hashes match | [09851cb2](https://app.buildbuddy.io/invocation/09851cb2-585e-43fc-94a3-b7b83d300bab) | [f4422e28](https://app.buildbuddy.io/invocation/f4422e28-8211-45b0-9f64-fb692925ae94) |
| `139d2b400` (verification snapshot) | Declare Wasmtime and Sparsehash ordinary public headers in hdrs, exclude unused Sparsehash test headers, and remove zstd's duplicate textual xxhash.c declaration. Required compiled consumer CPP and ClickHouse/DBMS links verified; Mongo's two unavailable CPP reports remain unresolved | [ddcbd179](https://app.buildbuddy.io/invocation/ddcbd179-7ebe-42ab-9e29-c2e636865752) | [14e24e59](https://app.buildbuddy.io/invocation/14e24e59-326c-4853-99b9-efc45029a21f) |
| `fe4a2c681` (verification snapshot) | Remove unused ZooKeeper deps and use real public/private owners. The three libraries retain 2/5/2 unused-dependency findings; none has a missing-include or private-placement finding. Ordinary ZooKeeper tests, consumer CPP, positive and negative controls, and eight link comparisons pass on both CPUs. Both Bazel ELF hashes equal the recorded Bazel hashes; normalized stripped hashes equal native CMake. | [3ad00fc7](https://app.buildbuddy.io/invocation/3ad00fc7-151a-476a-bf02-f9122c50e9d5) | [60f305a0](https://app.buildbuddy.io/invocation/60f305a0-5430-4b44-bf87-c2b7868a9cf4) |
| `0abc83ebf` (verification snapshot) | Keep generated GRPC and Prometheus protobufs in their compiled owners; remove 160 verified unused deps. Both owners have clean compiler-backed DWYU reports, original DBMS links and all five controls pass; other measured findings remain unchanged | [04fde681](https://app.buildbuddy.io/invocation/04fde681-8bf0-4dea-9143-b05c78186897) | [9432a7d5](https://app.buildbuddy.io/invocation/9432a7d5-630e-47c4-8cbd-54e55e1fb1b2) |
| `15296f36c` (verification snapshot) | Make System implementation headers private; remove 128 unused deps and make 15 implementation-only deps private. Publish cppkafka and libdivide dependencies from their real //src:headers owner. Scoped compiler and ordinary DBMS link checks pass on both CPUs with no new current dependency findings; four System DWYU findings remain per CPU | [b085ea1f](https://app.buildbuddy.io/invocation/b085ea1f-ed9d-46e7-b5a2-1e24922f9635) | [2c6805a8](https://app.buildbuddy.io/invocation/2c6805a8-1b2d-46e1-8738-31173e91816f) |
| `7f925ebde` (verification snapshot) | Use selected Linux LDAP configuration headers; put public LDAP headers on actual owners and implementation headers in srcs. Six original LDAP owner CPP reports are clean; the current DBMS compilation and unit_tests_dbms link pass on both CPUs. DBMS retains its existing findings and the compiled _lber dep reported unused by CPP. Full and stripped ClickHouse hashes match the recorded hashes | [ef8a40f7](https://app.buildbuddy.io/invocation/ef8a40f7-20a1-4e04-a1e8-560b48ac9368) | [28e541bf](https://app.buildbuddy.io/invocation/28e541bf-a53d-4fee-85fa-efee90650ba4) |
| `d894f6526` (verification snapshot) | Use the SZ3 generated-header owner in SZ3 and the Roaring header owner in AggregateFunctions; correct private LLVM/libdivide dependencies and remove unused AggregateFunctions/Compression declarations. All applicable AggregateFunctions compiled owners have zero DWYU findings on both CPUs. Compression retains the one pre-existing unused LDAP dependency; no new missing/private findings in checked DBMS/Functions consumers. Both native DBMS links and 3 selected tests pass. Ordinary DBMS test ELF layout differs, with equal global/weak definitions, undefined symbols and dynamic symbols; each ordered library group and its linker options are preserved. Original and normalized ClickHouse hashes remain as recorded below. | [464fe9d3](https://app.buildbuddy.io/invocation/464fe9d3-99a1-4ae5-ba1c-a4e6f61d9fae) | [5f8ed2a8](https://app.buildbuddy.io/invocation/5f8ed2a8-6f96-49a7-9c75-2d99d230d03e) |
| `3486e911f` (verification snapshot) | Remove one unused LDAP dep from Compression and unused LDAP/mysqlxx deps from GatherUtils. Both compiled owners have zero DWYU findings on both CPUs. The old native ClickHouse run failed on Protobuf utf8_validity archive ownership; these full builds verify the corrected link plans. The original and normalized ClickHouse hashes remain as below. Ordinary DBMS links and 3 selected tests pass; unit ELF hashes differ with library order, while inputs, ordered groups, linker flags and global/weak/undefined/dynamic symbol comparisons match. | [0e9fc41b](https://app.buildbuddy.io/invocation/0e9fc41b-b347-4ccb-aa09-0ae45d385fa9) | [5a65d2bb](https://app.buildbuddy.io/invocation/5a65d2bb-e37f-4140-be42-125b9e25a4e9) |
| `680bec100` (verification snapshot) | Remove inherited LLVM/libc++ headers from AWS Checksums, LZ4, and Zstd C/assembler compiles: 14 unused DWYU findings removed on x86-64, 19 on ARM64. LZ4 retains the existing `lz4hc.c`→`lz4.c` finding and required source input; no new missing headers. Five regression controls and two Abseil controls pass. The fa5 baseline had genuine Keeper and Abseil unit failures; these corrected full builds pass the native compiler, ELF and stripped hashes recorded below. Current ordinary unit binaries, exact links and symbols match; all 3 selected tests pass before/after on both CPUs. | [a97b251f](https://app.buildbuddy.io/invocation/a97b251f-ee10-4233-82f2-baa76ee6825e) | [3bd8459d](https://app.buildbuddy.io/invocation/3bd8459d-5eda-4409-9bd2-47cbf010b326) |
| `f1de61866` (verification snapshot) | Classify three LLVM configuration headers, libpqxx libpq-forward.hxx, and jemalloc_defs.h as public hdrs. Ordinary libraries, compiled consumers, ClickHouse and unit_tests_dbms pass on both architectures. The scored CPP reports have no new findings; existing LLVM preprocessing failures remain unscored. Original and normalized native CMake hashes match | [7cacdf6e](https://app.buildbuddy.io/invocation/7cacdf6e-c186-476c-bc37-61947e221245) | [8cc2b54f](https://app.buildbuddy.io/invocation/8cc2b54f-c895-45bf-a02a-93755c999672) |
| `cc9d4621e` (verification snapshot) | Remove 10 private Common IO dependencies supplied by its declared header owners. Current Common IO, consumer and control CPP has no new findings; IO0 retains 6 x86 and 5 ARM findings. Native ClickHouse and unit_tests_dbms pass with unchanged normal link inputs and arguments; original and normalized native CMake hashes match | [5f8748dc](https://app.buildbuddy.io/invocation/5f8748dc-f955-4ecc-bd0a-df867bcbd94f) | [1355aeba](https://app.buildbuddy.io/invocation/1355aeba-b75a-44fa-b906-e79a0d0bdd6c) |
| `2df176c78` (verification snapshot) | Declare only pinned Linux c-ares and libuv headers; move used private headers to srcs. Libuv DWYU clean; c-ares retains 2 unused libc++ header deps. NATS still fails CPP and is not counted (8/9 available). ARM scoped [first attempt](https://app.buildbuddy.io/invocation/9a2356a6-16c7-43a5-9af9-b6a431484250) skipped both links after Substrait download Premature EOF; [same-source retry](https://app.buildbuddy.io/invocation/45d8c4fe-e6f1-4bed-8959-44392d8fd157) passes both. x86/ARM native ClickHouse and DBMS unit links match baseline arguments; original ELF and normalized CMake hashes match | [f925b108](https://app.buildbuddy.io/invocation/f925b108-40b9-4734-8508-6a0b55241940) | [d6df2fd4](https://app.buildbuddy.io/invocation/d6df2fd4-353e-4983-8bbc-1635666f9871) |

System retains four measured DWYU findings per CPU:
`@clickhouse_llvm_project//:LLVMX86CodeGen`,
`@clickhouse_llvm_project//:LLVMX86Desc`,
`@clickhouse_llvm_project//:LLVMX86Info` on x86, the corresponding
`@clickhouse_llvm_project//:LLVMAArch64CodeGen`,
`@clickhouse_llvm_project//:LLVMAArch64Desc`,
`@clickhouse_llvm_project//:LLVMAArch64Info` on ARM, and
`@clickhouse_nuraft//:_nuraft` on both.
The CodeGen findings were already present in both current before and after
compiler reports; the earlier three-finding System audit preceded the LLVM
CodeGen private-header cleanup. Unchecked and untested repository targets are
still unknown. Replacing the remaining System public header target with its
compiled owner still creates a dependency cycle.

The ordinary `//src:unit_tests_dbms` links on both CPUs. It has different
normal ELF hashes after the dependency reordering; it is not the ClickHouse
hash comparison below. Its before and after global, dynamic and undefined
symbol sets match; the ARM-only local-symbol differences are AArch64 thunks
and their mapping symbols. Ten existing BloomFilter and Storage gtests pass
before and after on both CPUs.
| `2942389ee` (verification snapshot) | Use compiled Functions, DBMS and GatherUtils header owners for Array; put private Array headers in srcs and private dependencies in implementation_deps. Both Functions owners, all four Array owners and UniqTheta have zero CPP dependency findings on both CPUs. GatherUtils retains exactly the parent's two unused dependencies, OpenLDAP _ldap and mysqlxx; its complete report is byte-identical before and after. Ordinary compiler and DBMS link checks pass. Original and normalized ClickHouse ELF hashes remain as recorded below | [41d25a76](https://app.buildbuddy.io/invocation/41d25a76-f4b1-4a40-a412-3b303e571048) | [074d4871](https://app.buildbuddy.io/invocation/074d4871-d666-4bf1-a750-2d47c22d1af9) |
| `b7abacacf` (verification snapshot) | Remove 12 unused public dependencies from the three Dictionaries compile targets and two unused Embedded dependencies. Embedded is CPP-clean; three implementation-dependency findings per Dictionaries target remain because native compilation requires them. Current native and CPP checks match with no new consumer findings; both original and normalized hashes match | [c6b93972](https://app.buildbuddy.io/invocation/c6b93972-6f55-4f7c-b196-f896ebeb9ba1) | [c253fae1](https://app.buildbuddy.io/invocation/c253fae1-ec81-42d2-aa64-039b2214f121) |
| `6357668b9` (verification snapshot) | Mongo C's four disabled backend headers omitted; three OpenSSL-private headers and the included utf8proc source declared private; direct crypto and zstd compiler dependencies. Mongo C now has an actual report with no missing includes; 3 unused findings and unavailable BSON C++ retained. Compiled consumers and both links verified | [202687e8](https://app.buildbuddy.io/invocation/202687e8-ece6-4f3b-aa1d-61961f7f0d0a) | [1447ac74](https://app.buildbuddy.io/invocation/1447ac74-d57c-4a1c-b5a8-1afd5acbcb69) |
| `e5a56524d` (verification snapshot) | Give StringZilla its implemented C/C++ APIs and apply gnu11 to C only; give xxHash the two headers implemented by its library; remove unused LLVM header deps. Both owners and Common pass CPP checks on x86 and ARM. USearch retains its pre-existing CPP failure. Full and normalized hashes match | [dc2bb68f](https://app.buildbuddy.io/invocation/dc2bb68f-98b0-4f92-8881-5580d82bef6c) | [d0a170ec](https://app.buildbuddy.io/invocation/d0a170ec-5a39-47b4-a7c7-f23da9365b52) |
| `595953208` (verification snapshot) | Declare libprotobuf’s public and private Abseil owners. Separate the UTF8 C implementation from its C++ API and use the C archive owner in both final-link plans. Three changed owners pass all CPP fields on both CPUs; libprotobuf-lite retains its 36 public / 19 private findings. Ordinary DBMS links, full ELF and normalized hashes match | [3e9df4bb](https://app.buildbuddy.io/invocation/3e9df4bb-fd7e-4e14-ad45-dcf826b3dc44) | [492ff82d](https://app.buildbuddy.io/invocation/492ff82d-aa23-4ccb-8c58-1c55ebcd3f76) |
| `44503381d` (verification snapshot) | BSON C++ public enum compatibility checked in existing types.cpp; all BSON C++ DWYU fields clean, genuine changed-enum native compile failure, current fetched input hashes and ClickHouse/DBMS links verified | [b8a7215a](https://app.buildbuddy.io/invocation/b8a7215a-c823-4b48-9b6c-e7b12a354e68) | [72566ab9](https://app.buildbuddy.io/invocation/72566ab9-adcd-4dbd-a2b1-32859cc380fb) |
| `ac1b85a6b` (verification snapshot) | Parser test headers in unit-test srcs, direct Azure/Abseil providers on DBMS owners; x86 103→0 unused deps, ARM 102→0; remaining Parser public/private include findings 0/5 x86, 0/5 ARM. `//src/Parsers:headers` remains unscored | [88a76f82](https://app.buildbuddy.io/invocation/88a76f82-99f0-40ef-a965-6ad6d8afc4e8) | [45c1a381](https://app.buildbuddy.io/invocation/45c1a381-9c06-4e7e-adf9-caaf08990f07) |
| `6cb5711f3` (verification snapshot) | Remove duplicate Loggers/Daemon headers and unused deps; declare Hive, SQLite, NuRaft and DBMS-test Abseil at their actual owners. Scoped native consumers and ordinary DBMS links pass; original DWYU residuals remain (Loggers 8+1 unused, 3 private; Daemon 2+1 unused, 7 private). Both full ELF and normalized CMake hashes match | [8d359b06](https://app.buildbuddy.io/invocation/8d359b06-7cec-4d11-8662-42f35a0582d6) | [848e7cd0](https://app.buildbuddy.io/invocation/848e7cd0-9e7a-4f96-943a-4ad85aa85dfc) |
| `7c3ed66e1` (verification snapshot) | Give URLHierarchy the compiled owner of FunctionTokens.h and Regexps.h; use implementation_deps for JSONPath's private DBMS headers and remove unused direct dependencies in URL, JSONPath, TimeSeries and TableFunctions. Both URL owners, JSONPath, TimeSeries and TableFunctions have zero CPP dependency findings on both CPUs. Ordinary C++ builds and both DBMS and ClickHouse links pass. DBMS object order and local symbols differ within the same linker groups; its public symbols and linker inputs match, and all nine existing PortFunction tests pass before and after. ClickHouse ELF files match the parent. Original and normalized ClickHouse ELF hashes remain as recorded below | [09ff150d](https://app.buildbuddy.io/invocation/09ff150d-4ea1-4756-b554-847761a14b94) | [1fe38611](https://app.buildbuddy.io/invocation/1fe38611-e25f-4d36-a2d8-ad007f80acdf) |
| `c18aa6a72` (verification snapshot) | Boost, FlatBuffers, DMLC, S2, UTF8 and LZ4 implementation headers and direct providers; S2 public/private missing-dep files 91/63 x86, 91/63 ARM; S2 still reports unused deps @clickhouse_openssl//:ssl/@clickhouse_openssl//:ssl (removal/link necessity not established here). CPP unavailable for 5 x86 and 6 ARM targets; no whole-repo DWYU claim | [a7164f72](https://app.buildbuddy.io/invocation/a7164f72-e12b-4b76-8b9d-46ce74ceae67) | [d321ceb3](https://app.buildbuddy.io/invocation/d321ceb3-27ef-4b15-b1e1-2df95334dcd9) |
| `e4adb0c97` (verification snapshot) | DBMS public and implementation deps, USearch FP16 provider and test SZ3 dep; DBMS compiler CPP counts (public missing/private missing/unused public/unused private/should-be-private): x86 72/56/40/23/18→1/1/20/17/2, arm 71/57/40/22/18→0/1/20/16/2. CPP unavailable for 2 x86 and 4 ARM checked targets. Eight SZ3, Compression and Arrow unit tests passed per CPU; no whole-repo DWYU claim | [2be50ddc](https://app.buildbuddy.io/invocation/2be50ddc-8634-4777-a4c4-329001cd3556) | [f3d6fa11](https://app.buildbuddy.io/invocation/f3d6fa11-fae4-4f9c-9ac6-b3bee7e01953) |
| `8ba64e980` (verification snapshot) | Give Vectorscan its four public API headers and private source headers per compiled library. DWYU recognizes another compiled source of the same rule while still rejecting a foreign private compiled source. All 3 Vectorscan and URL CPP reports are clean on both CPUs (parent Vectorscan reports unavailable); existing and new controls pass. ClickHouse/DBMS ELF bytes and all link/archive arguments equal the parent; original and normalized native CMake hashes match | [60a5e8dd](https://app.buildbuddy.io/invocation/60a5e8dd-7c01-4c61-a167-e05fd9a77846) | [d4df7977](https://app.buildbuddy.io/invocation/d4df7977-1339-4059-86b7-1b8c0be23067) |
| `6845aa964` (verification snapshot) | Remove S2's unused OpenSSL dependency; keep Abseil random distributions private. Give Boost.Asio's paired compiler-pragmas headers one owner and audit the pair in the actual NuRaft consumer. S2 and Boost still have unresolved public-header findings | [0b0cb184](https://app.buildbuddy.io/invocation/0b0cb184-fdb2-4c20-8659-38e7312f649b) | [10815749](https://app.buildbuddy.io/invocation/10815749-d454-4aad-ad43-a6be2ba98d18) |
| `383bbbd16` (verification snapshot) | Declare 18 complete LLVM Intrinsics headers as public; give four libc++ ABI support headers a restricted owner and keep libc++abi implementation headers private. CPP checks retain five bootstrap dependencies; libc++, LLVMSupport and llvm_headers reports remain unscored. Original and normalized native CMake hashes match | [663e836d](https://app.buildbuddy.io/invocation/663e836d-6637-4992-b006-dfc67397c39a) | [af35ce6c](https://app.buildbuddy.io/invocation/af35ce6c-95ad-4fd4-9df2-83adec1d4688) |
| `1b1c530db` (verification snapshot) | Give Boost.Config its Linux Clang headers, private suffix and paired ABI fragments. The real Boost.Iostreams consumer and both native/CPP comparisons pass. Boost.Config standalone and boost_headers remain unscored. Recorded ELF hashes and normalized native CMake comparisons match | [e71ed905](https://app.buildbuddy.io/invocation/e71ed905-6701-4860-8631-6096076cdeaa) | [e5920097](https://app.buildbuddy.io/invocation/e5920097-75f6-4323-a89b-a270e76df409) |
| `c5b234267` (verification snapshot) | Programs: use the compiled owners of Parsers, AggregateFunctions, Dictionaries, Functions, Access/Common and Storages/System headers privately, and the existing public owner where no compiled direct owner exposes the header. Remove 11 unused GatherUtils deps. All 26 program CPP reports are clean on both CPUs (16 previously reported findings), with 3 positive and 2 negative controls. ClickHouse and unit_tests_dbms ELF hashes and native link/archive inputs match the parent. All srcs and other non-dependency attributes are preserved; native CMake hashes match. | [394849ae](https://app.buildbuddy.io/invocation/394849ae-bdeb-42e2-946d-daa93da7eafc) | [09a3b596](https://app.buildbuddy.io/invocation/09a3b596-fb79-4bab-bd4a-3134eed878ec) |
| `9ac0761e6` (verification snapshot) | Remove libc++ header inputs from four C libraries, make libuv/Minizip/sparsehash private to Cassandra, and remove redundant Minizip SSL and WordNet Boost graph deps. All 18 x86/12 ARM audited libraries have clean CPP reports; 5 controls pass per CPU. ClickHouse ELF and native link match; changed DBMS links retain complete groups and pass tests bound to the exact executable. Both recorded CMake hashes match | [09f8f50e](https://app.buildbuddy.io/invocation/09f8f50e-0577-4024-8e1b-8a22cb94b5b6) | [0a08ec5c](https://app.buildbuddy.io/invocation/0a08ec5c-e0e3-4fed-be0a-eab925f50546) |
| `69c080d4b` (verification snapshot) | Remove redundant mysqlxx:headers and depend on the mysqlxx library in src, Dictionaries and Functions. mysqlxx's real CPP reports pass on x86 and ARM, with the existing 5 controls; unavailable src header reports remain unproved. ClickHouse ELF and ordered native link/archive inputs match the parent. Changed unit_tests_dbms links preserve full groups and symbols, with 17 tests passing against each exact executable. Both native CMake hashes match | [2979d6bd](https://app.buildbuddy.io/invocation/2979d6bd-b739-4d39-bb08-2f651e1f950f) | [1dde2d81](https://app.buildbuddy.io/invocation/1dde2d81-25ae-4fe3-ac1e-a570a70c44fe) |
| `14c822622` (verification snapshot) | Remove four unused libcxxabi providers, keep libcxx bootstrap headers only on bootstrap toolchains, and publish shared src includes from abi_support. libcxxabi and abi_support CPP reports are clean on both architectures; libcxx retains its recorded frozen-C++03 preprocessing failure. ClickHouse and unit ELF hashes and ordered links match, and both native CMake hashes match | [8ab06a9e](https://app.buildbuddy.io/invocation/8ab06a9e-5ffb-4ffe-a12d-894aa3704805) | [30ab29a9](https://app.buildbuddy.io/invocation/30ab29a9-d586-489d-b2e9-d2b5b6a02874) |
| `59ac2cdbf` (verification snapshot) | Make Arrow Flight's compiled-and-included FlightSql.pb.cc a private compiler input; retain its compiled source and all generated public headers. Arrow Flight and URL pass DWYU on both CPUs; DBMS has no new findings and both binaries and link arguments match the parent. Original ELF and normalized native CMake hashes match | [cc7b0cc9](https://app.buildbuddy.io/invocation/cc7b0cc9-a856-457a-815f-5735c16435c5) | [505ddef6](https://app.buildbuddy.io/invocation/505ddef6-b92b-4ac4-987d-32b0d72bc92e) |
| `06c1701b9` (verification snapshot) | Keep four Foundation UNIX/POSIX implementation headers in srcs; retain public Poco headers and the included .cpp fragments. All 11 compiled Poco targets pass DWYU on both CPUs; 382 compile commands, native links/archives, ClickHouse and DBMS ELF bytes match the parent. Original ELF and normalized native CMake hashes match | [de71bcd2](https://app.buildbuddy.io/invocation/de71bcd2-b442-4778-a870-20f06fac7192) | [b1d564ea](https://app.buildbuddy.io/invocation/b1d564ea-3ea5-48b4-920c-ff9997522285) |
| `070778461` (verification snapshot) | Declare the existing Abseil header owners directly in S2's public or implementation dependencies. S2's 91 public and 63 private DWYU findings reach zero on both CPUs; direct-consumer findings and unscored reports do not increase | [f8b0a1dc](https://app.buildbuddy.io/invocation/f8b0a1dc-0339-4cc3-a0a0-3160a27c37a1) | [d6428a8c](https://app.buildbuddy.io/invocation/d6428a8c-c915-4c82-adbf-486d7ec8cad5) |
| `89cb5dfc3` (verification snapshot) | Remove the unused LLVMCGData dependency from LLVMipo. LLVMipo CPP has no findings on either CPU; LLVMCoroutines and LLVMCGData still report their link dependencies. The current ClickHouse and unit binaries match and native ClickHouse link actions are unchanged; native CMake hashes match | [47a4aa75](https://app.buildbuddy.io/invocation/47a4aa75-4330-494e-b50d-6e959392ca8e) | [e9b43e67](https://app.buildbuddy.io/invocation/e9b43e67-22cb-4074-bc7c-b24376f95f26) |
| `c1196bea6` (verification snapshot) | AWS SDK publishes its pinned CRT and tracing APIs and declares AWS common, auth, and HTTP compiled owners. AWS CPP passes on both CPUs; real consumers have no new CPP findings. ClickHouse and DBMS test link inputs/groups, full binaries and nonlocal symbols match the current parent; other DBMS/IO/Config findings remain as recorded. Original ELF and normalized CMake hashes match | [26b37f0f](https://app.buildbuddy.io/invocation/26b37f0f-e7f7-45bf-b227-b27e007e6786) | [03dfd28e](https://app.buildbuddy.io/invocation/03dfd28e-4094-460c-87e4-e6ec0de5561b) |
| `ab138d2c3` (verification snapshot) | Put gRPC's paired codegen macro headers on the actual generated-code libraries; keep Arrow's use private. Both compiled gRPC libraries now have zero CPP findings, and all 83 other compiled targets retain their existing findings. Ordinary ClickHouse and unit-test binaries match on both CPUs | [d1e21860](https://app.buildbuddy.io/invocation/d1e21860-1a09-41d0-9be1-fe76847839db) | [88f3c520](https://app.buildbuddy.io/invocation/88f3c520-eb8f-47ef-bbdd-d52ab0763689) |
| `649805fbe` (verification snapshot) | Give WKB.h to the compiled IO library and GeoBbox.h/HilbertUtils.h to the compiled DBMS library; remove Common's unused Functions header dependency and shorten the two remaining repeated-basename labels. Common and Functions CPP pass on both CPUs, with no new IO/DBMS findings; src header CPP remains unproved. Ordered ClickHouse/unit links and full ELF hashes match; normalized native CMake hashes match | [23905897](https://app.buildbuddy.io/invocation/23905897-1ccb-4f0c-86cb-217e888e639d) | [7c617bb3](https://app.buildbuddy.io/invocation/7c617bb3-4fe5-4708-ae46-438722ed7f8e) |
| `a27c99e84` (verification snapshot) | Fix the frozen libc++ C++03 assertion handler and exception ABI. Native C++03/C++23 exception and allocation tests pass on both CPUs; the explicitly requested wrong-layout test fails as expected. Whole-libc++ CPP and default C++03 errors remain recorded. ClickHouse and unit ELF bytes and ordered links, selected compiler hashes and both normalized native CMake hashes match | [675f89a7](https://app.buildbuddy.io/invocation/675f89a7-7aba-4349-96dd-889b746d387d) | [b078407d](https://app.buildbuddy.io/invocation/b078407d-3211-404d-b221-d3b069fc8349) |
| `a0bd8bfb0` (verification snapshot) | Check direct object symbols in DWYU and keep LLVMCFGuard's LLVMCore dependency private. ClickHouse and unit binaries and ordered link actions match. Restoring LLVMipo's unused LLVMCGData dependency still fails; 15 other LLVM targets still have findings. Native Bazel and normalized CMake hashes match | [58413607](https://app.buildbuddy.io/invocation/58413607-0908-4cce-9693-988c96aa4677) | [29455853](https://app.buildbuddy.io/invocation/29455853-370e-463d-ad23-f9d5527cd53f) |
| `c24aa5499` (verification snapshot) | Remove duplicate target LLVM dependencies from the System compiler target; retain the target LLVM libraries used for linking. System is not DWYU clean: all remaining findings and unverified link dependencies are unchanged. Only the three removed LLVM dependencies were proved unused. Five controls per CPU pass. Ordered ClickHouse/DBMS links and complete executables match; both recorded CMake hashes match | [3b5170e1](https://app.buildbuddy.io/invocation/3b5170e1-f84c-459d-84f0-9ae1b502f5a2) | [e72204a5](https://app.buildbuddy.io/invocation/e72204a5-bc8e-4110-b17a-2230e1114f6a) |
| `39c84725a` (verification snapshot) | Use one XGBoost source/header glob and remove all 169 additional compiler inputs; keep the eight CPU-included .cu files private. XGBoost passes CPP DWYU. The Dictionary remains unverified for LLVMSupport and glibc-compatibility; both original CPP reports and all findings match before/after on both CPUs. 158 compile commands, ordered archives/links and both ClickHouse and DBMS ELF bytes match. Original Bazel ELF hashes match their recorded values; stripped Bazel/CMake hashes match | [ebd50786](https://app.buildbuddy.io/invocation/ebd50786-0cbe-41fd-9acd-001b04f72e86) | [16c18dca](https://app.buildbuddy.io/invocation/16c18dca-2cc1-4334-9472-5592ed3e0780) |
| `7c91c9caf` (verification snapshot) | Remove flatc’s whole-repository compiler input glob; keep the binary’s sysroot. All five DWYU controls pass on both CPUs. The compiled FlatBuffers library has no header findings but its unchanged builtins link remains unscored; flatc’s existing unused private-header failure also remains unscored. The six generated Arrow headers, flatc, ClickHouse, and DBMS test binaries and ordered links match the current parent; normalized native CMake hashes match | [0c20fcff](https://app.buildbuddy.io/invocation/0c20fcff-e459-45e3-8767-37f220e2351b) | [cef299a1](https://app.buildbuddy.io/invocation/cef299a1-812d-4ab5-b839-dce4a238219a) |
| `98b710c3b` (verification snapshot) | Remove the redundant cppkafka and libhdfs3 additional compiler inputs; leave libhdfs3 proto and assembly files on their actual generation actions. All five current owner/caller CPP reports, including direct symbols and held unverified link inputs, are unchanged. Full ClickHouse/unit ELF bytes and ordered links match; both normalized native CMake hashes match | [c36f067b](https://app.buildbuddy.io/invocation/c36f067b-5272-409f-91fe-95a497ded588) | [f9df8ee3](https://app.buildbuddy.io/invocation/f9df8ee3-6973-400c-9c0c-370ded2523a3) |
| `f4ce83a27` (verification snapshot) | Remove the ORC and programs additional compiler inputs that duplicate five files compiled by their actual owners. Only the 45 owning compile actions lose those inputs; all other compiler inputs, commands and outputs match. The four original ORC, programs, Arrow and DBMS CPP reports match byte for byte, including their findings and unverified link dependencies; all four remain held, not DWYU clean. All five controls per CPU behave as expected. Full ClickHouse/unit ELF bytes, ordered links and normalized native CMake hashes match | [6e4899c9](https://app.buildbuddy.io/invocation/6e4899c9-cc53-4510-b04d-c977f96ec095) | [7edb7143](https://app.buildbuddy.io/invocation/7edb7143-a7df-4790-9cc3-a66622a5ebe1) |
| `5b5482f77` (verification snapshot) | Remove duplicate OpenLDAP compiler inputs and restrict simdjson compiler inputs to included implementation fragments for the current CPU. Consumer compile inputs are unchanged. OpenLDAP and LZ4 have actual zero-finding DWYU reports on both CPUs; four positive and four negative controls pass. Existing simdjson wrong-CPU preprocessing errors and unchanged Compression, Config, and DBMS unverified-link reports remain unscored. ClickHouse and DBMS binaries and ordered links match the current parent; normalized native CMake hashes match | [7562738d](https://app.buildbuddy.io/invocation/7562738d-5500-47e2-8f50-2e8276ed5c00) | [5741eee4](https://app.buildbuddy.io/invocation/5741eee4-093f-4949-bf5b-e4eb9ac46793) |
| `c50d1da3e` (verification snapshot) | Restrict public headers for zmij, libbcrypt, MariaDB and NATS. Their four public preprocessing failures are fixed on both CPUs. MariaDB and NATS pass current DWYU; zmij and libbcrypt remain link-unverified for clang_rt_builtins. Their old preprocessing failures supplied no link evidence. All eight reported fields for three callers match before/after; those callers remain held. Simdjson .cpp inputs do not propagate into the seven native compile targets. Five controls, full ClickHouse/DBMS links and executables, and recorded CMake hashes match | [33c4f89c](https://app.buildbuddy.io/invocation/33c4f89c-0a58-459c-a9f4-de691955464d) | [1c5400e9](https://app.buildbuddy.io/invocation/1c5400e9-d722-4dba-b035-29043d0bacdf) |
| `67cf44fc5` (verification snapshot) | Remove unused OpenSSL, Vectorscan and MariaDB compiler inputs and empty aws-c-http and LLVM attributes. Retain Vectorscan's compiled and included impl.cpp. On each CPU, seven of eleven C/C++ owners and callers pass DWYU; OpenSSL crypto, Functions and mysqlxx remain link-unverified, and Vectorscan compile_1 retains its finding. All reported fields and the five controls match before/after. Only the selected owning compiler inputs were removed; caller inputs, ordered ClickHouse/unit links and full ELF bytes match. Both normalized native CMake hashes match | [473db04b](https://app.buildbuddy.io/invocation/473db04b-6a21-4465-a9c7-1342829cb927) | [bf17441a](https://app.buildbuddy.io/invocation/bf17441a-e7b3-4647-a03f-be1a6c3dae66) |
| `ce9d63d1b` (verification snapshot) | Remove ten CPP-verified unused dependencies in gpr and seven Google Cloud proto libraries. Fresh native and CPP checks cover 15 compiled owners per CPU; three reports remain held for pre-existing unverified link inputs. | [4e81f431](https://app.buildbuddy.io/invocation/4e81f431-b995-4135-9db8-f2770cbe9b79) | [3d9be540](https://app.buildbuddy.io/invocation/3d9be540-9460-4e30-8324-46fdd906b923) |
| `1f127cf69` (verification snapshot) | Use exact source globs for Keeper bench and AggregateFunctions Combinators; remove 17 x86-64 / 16 ARM64 unused Loggers, ZooKeeper and IO deps and make three Loggers deps private. Fresh CPP reports show no new findings; four positive and ten real negative controls and native ClickHouse/DBMS links pass. Thirteen production reports still have unverified link deps, one IO report still has findings, and the two existing header reports remain unproved; this is not a DWYU-clean claim. Both Bazel and normalized CMake hashes match | [fef46baa](https://app.buildbuddy.io/invocation/fef46baa-a3ac-42db-a3ed-7d34d5f10d66) | [2a86d637](https://app.buildbuddy.io/invocation/2a86d637-3b5f-43bb-8430-9080aed88452) |
| `e2071ccab` (verification snapshot) | Remove six generated files from `additional_compiler_inputs` in `//src:unit_tests_dbms`; their six libraries compile them. Keep the sysroot and the nine .cpp files directly included by unit tests. All 627 unit-test compiles lose exactly those six inputs; googletest's compile and all other inputs, arguments and outputs match. The unit-test and googletest original CPP reports and all eight fields match, including their existing findings and unverified link dependencies; both remain held, not DWYU clean. Three positive and two negative controls per CPU pass. Full ClickHouse/unit ELF bytes, ordered links and native normalized CMake hashes match | [7258f7b2](https://app.buildbuddy.io/invocation/7258f7b2-0ed8-44ee-9ac7-e327bb15a8d0) | [8af72311](https://app.buildbuddy.io/invocation/8af72311-c008-4e46-920c-8914df5eaf03) |
| `fc2ad99e5` (verification snapshot) | Follow resolved toolchains in the configured DWYU closure and accept the LLVM bootstrap's source llvm-nm executable. The actual Rust allocator has successful CPP and ordinary compile/archive reports on both CPUs; three positive and two negative controls validate. Other missing reports and findings remain. Both ClickHouse native and normalized CMake/Bazel hashes match | [c08d3487](https://app.buildbuddy.io/invocation/c08d3487-37d2-4cbb-8720-238645a14acc) | [be3b19df](https://app.buildbuddy.io/invocation/be3b19df-80ca-4b80-818d-42bfc8caf188) |
| `6a07deef3` (verification snapshot) | DWYU records configured dependency include directories separately from header ownership. `@clickhouse_nuraft//:_nuraft` now has zero DWYU findings on both CPUs. Five positive and six negative compiler controls and one real compile-negative pass per CPU. The two callers retain their existing link holds: ZooKeeper has 10, keeper-data-dumper has 4. ZooKeeper also explicitly holds two formerly-unused recommendations for the ambiguous `src` include directory from NuRaft and `//src/Compression:clickhouse_compression`; its unused deps change from 23 to 21. The callers and the repository are not DWYU clean. Ordered ClickHouse/unit links, complete executables, compiler hashes and normalized CMake hashes match | [066099cf](https://app.buildbuddy.io/invocation/066099cf-7078-4e8f-a6cd-e510ff882c7b) | [cdf091dd](https://app.buildbuddy.io/invocation/cdf091dd-0d4a-457d-ae3c-067a6aad2372) |
| `172e4f232` (verification snapshot) | Use @clickhouse_jemalloc//:_jemalloc directly instead of its duplicate headers alias; let //src/Access/Common:headers alone declare SQLSecurityDefs.h and use it for QueryFuzzer.cpp. Affected native builds and compile/archive arguments match on x86/ARM; compiled DWYU mandatory fields, optional-field contents and presence match. Existing unsupported link or include-directory dependencies remain unverified; unit/ClickHouse ordered link actions and full executable hashes match; ClickHouse original and normalized native CMake hashes match | [d113f05d](https://app.buildbuddy.io/invocation/d113f05d-d77f-41a3-8f64-e587f1a7f422) | [10e8f904](https://app.buildbuddy.io/invocation/10e8f904-e3d5-4c40-9d41-61c66350466e) |
| `7357db0e6` (verification snapshot) | Make simdjson included sources private; remove GCP's unused build_info.cc input, declare Parsers and Vectorscan deps, and collapse 9 guarded empty selects. Scoped native actions and CPP reports verified; simdjson's clang_rt_builtins dep and other DWYU holds remain unverified. Native x86/ARM ClickHouse original and normalized CMake hashes match | [1ad7ad76](https://app.buildbuddy.io/invocation/1ad7ad76-5488-4902-a521-a3185dc12593) | [17ecd8df](https://app.buildbuddy.io/invocation/17ecd8df-79c2-4425-9d0a-027db7106d76) |
| `9a0fb1011` (verification snapshot) | Declare AWS MQTT's public AWS common and I/O dependencies, and remove unused HTTP. Both current MQTT CPP reports clear all 12 missing includes; scoped native builds and positive/negative controls pass. Ordinary x86/ARM unit-test ELF bytes are unchanged. Original and normalized ClickHouse hashes match the recorded CMake hashes | [56a20598](https://app.buildbuddy.io/invocation/56a20598-9d3d-4875-931b-2b26d404c606) | [f07b4a75](https://app.buildbuddy.io/invocation/f07b4a75-8c66-4999-8848-c53a722110f0) |
| `5514e0b0b` (verification snapshot) | Remove Boost's two inactive i386 assembly inputs, unused Azure SSL and REST NetSSL dependencies, and C++ header inputs from Mongo C libraries; libxml2 is private to Azure. Component CPP and unit-link checks pass on x86 and ARM; existing Boost, IO and Mongo link holds remain. Combined original and normalized ClickHouse hashes match the recorded native CMake hashes | [5b535d65](https://app.buildbuddy.io/invocation/5b535d65-1232-4620-a2b9-f5ee4aacaefb) | [d580e8c1](https://app.buildbuddy.io/invocation/d580e8c1-b530-474c-8b98-6165de97d746) |
| `d486d6f4b` (verification snapshot) | Supply GNU Linux generator sysroots through the LLVM compiler configuration and remove six generator compiler-input declarations. Make four RocksDB dependencies private and remove three unused Abseil dependencies. Both CPU component checks: seven RocksDB findings removed and 30/31 available sysroot reports unchanged. FlatBuffers' original missing report and the link negative control's unverified result remain unclaimed. Both original and normalized ClickHouse hashes match the recorded native CMake hashes | [6ab1e89e](https://app.buildbuddy.io/invocation/6ab1e89e-01ad-4a13-b555-86966dae1ba6) | [48a902ae](https://app.buildbuddy.io/invocation/48a902ae-b55a-46c5-a626-b7af3fe9488b) |
| `0ae77a591` (verification snapshot) | Remove duplicated sysroot compiler inputs from ch_cc_library and unit_tests_dbms; supply generated Arrow protobuf dependencies and declare the Bitpacking test header as a test source. Component checks on both CPUs: 31 scoped native builds and unit link pass, 30 available DWYU reports stay equal, Arrow's generated headers check clean, and 725 unit missing include pairs are resolved. Existing FlatBuffers and link holds remain. Both combined original and normalized ClickHouse hashes match the recorded native CMake hashes | [9b5c7ec5](https://app.buildbuddy.io/invocation/9b5c7ec5-02e2-44fb-bf97-27f025c4cdf0) | [a89557a5](https://app.buildbuddy.io/invocation/a89557a5-1758-4899-a365-d541c53687eb) |
| `8f5ade875` (verification snapshot) | Remove raw libc++ runtime files from the remaining C library compiler inputs; trim 15 unsupported headers from libdivide, RapidJSON, FP16 and heks. Original component checks on both CPUs remove exactly 9,776 files from each affected C compile, preserve native unit and ClickHouse links, and make all four header owners check clean. The 17 available consumer reports are unchanged; three header-consumer and original C report holds remain. Both combined original and normalized ClickHouse hashes match the recorded native CMake hashes | [c6381da8](https://app.buildbuddy.io/invocation/c6381da8-a71d-4dda-a2a8-038995ee856d) | [e58830bd](https://app.buildbuddy.io/invocation/e58830bd-8b32-463d-b130-8adf380aed8e) |
| `f48ec543e` (verification snapshot) | Let cxx and cxxabi own their runtime object archives; remove two forwarding compile libraries and name their owners in both native link manifests. Original scoped checks on both CPUs preserved full unit ELF hashes and LLVM compiler actions. Runtime symbols are now attributed to their real libraries. New private/unused runtime findings, unavailable DWYU source reports and unverified linker inputs remain open. Both original ClickHouse hashes and the normalized CMake hashes match | [fce3a6cd](https://app.buildbuddy.io/invocation/fce3a6cd-0c1b-41c4-9877-4bd7776daa2b) | [3d35871a](https://app.buildbuddy.io/invocation/3d35871a-a480-4831-b750-8d40dbf9e221) |
| `f617ffdcf` (verification snapshot) | Make cxx and cxxabi private implementation dependencies in ch_cc_library; keep unwind public. All 77 queried compilation providers and ordinary unit/ClickHouse links are preserved on each CPU. DWYU resolves 119 private runtime edges per CPU, while 19 genuinely unused edges and all other unverified findings remain reported; four clean reports include three positive controls. Original ClickHouse and normalized native CMake hashes both match | [3a811a5c](https://app.buildbuddy.io/invocation/3a811a5c-a53e-4370-a117-14fb191bf647) | [1d3a5c4d](https://app.buildbuddy.io/invocation/1d3a5c4d-9392-43ad-ba5c-2f7268407cf8) |
| `0a910fbaa` (verification snapshot) | Keep ICU's compiled data in `@clickhouse_icu//:_icuuc` implementation_deps; retain the icudt78_dat link symbol. On scoped 6a5ecf5bd → 38e05f3a5 checks, ICU and icui18n pass DWYU on both CPUs; all other owner/caller fields and five controls match. ZXC's 7 x86 / 3 ARM private findings and five caller holds remain. The previous row's 19 unused runtime edges were not re-audited. Scoped ordinary ClickHouse/unit ordered links and full ELFs match. Current-source original ClickHouse and normalized native CMake hashes both match | [25d0d64f](https://app.buildbuddy.io/invocation/25d0d64f-b1c1-4be1-997b-9bc7af010e3b) | [1f8c0aec](https://app.buildbuddy.io/invocation/1f8c0aec-7c53-4aa1-889f-24108ef9784d) |
| `08784ef5a` (verification snapshot) | Remove four measured unused libc++/libc++abi implementation dependencies from FarmHash and MetroHash; retain unwind. On scoped 0beb20b38 → 5618e4380 checks, four DWYU reports pass including three positive controls, six reports retain unverified link dependencies, and two negative controls behave as expected on each CPU. CityHash's two runtime edges remain reported: removing them changes the ordinary unit-test link order and binary. Removal of the other 13 runtime edges is untested. Scoped ordinary ClickHouse/unit ordered links and full ELFs match. Current-source original ClickHouse and normalized native CMake hashes both match | [c99456a0](https://app.buildbuddy.io/invocation/c99456a0-83a4-4081-92be-8626917cfb61) | [ca16225e](https://app.buildbuddy.io/invocation/ca16225e-1afc-4d8b-a9b4-8c4ecad07759) |
| `4a043a244` (verification snapshot) | Declare SQLConsoleResources.generated.h in dbms__compile_0.srcs; keep all 22 SQL gzip assets and the existing ClickStack files as compiler inputs. On scoped 6a5ecf5bd → 88a367cae checks, the one SQL private-header missing pair is resolved on both CPUs; the ClickStack pair remains. All three scored targets retain their genuine existing holds and five controls match. //src:dbms has no report and is unscored. Three consumer compiles gain only the private SQL header; all other inputs and arguments match, with no added compilation or dropped data. Current-source original ClickHouse and normalized native CMake hashes both match | [53324cdb](https://app.buildbuddy.io/invocation/53324cdb-b420-4870-a55a-8109b0e720ef) | [b40fa640](https://app.buildbuddy.io/invocation/b40fa640-3868-4623-a74f-a6d0b1d8b44f) |
| `d13a9693d` (verification snapshot) | Declare ClickStackResources.generated.h in dbms__compile_0.srcs and retain the packed assets as compiler inputs. On scoped 88a367cae → c9028fae5 checks, WebUI.cpp's ClickStack missing header is resolved on both CPUs. All three scored targets retain their existing DWYU holds and five controls match; //src:dbms has no report and remains unscored. All 2,351/2,352 DBMS compile input sets and arguments match. Three caller compiles add only the header; their actual native PIC objects and archives are byte-identical. Current-source original ClickHouse and normalized native CMake hashes both match | [f7f0c841](https://app.buildbuddy.io/invocation/f7f0c841-66bd-417e-a991-d4599d61c498) | [393c5786](https://app.buildbuddy.io/invocation/393c5786-eb10-44f8-a276-57a6a27069bf) |
| `6a6e4a097` (verification snapshot) | Remove eight unused libc++/libc++abi implementation dependencies from the three Poco C libraries and readpassphrase; keep unwind. Scoped 2164865b4 → 89d3479de checks cover every Poco macro receiver: 12 reports pass including three positive controls, seven retain unverified link dependencies, and two negative controls are confirmed on each CPU. CityHash's two runtime findings remain; five other runtime removals have not been tested. All 26 C compiles, ordinary ClickHouse/unit ordered links and full ELFs match. Current-source original ClickHouse and normalized native CMake hashes both match | [b6e7678f](https://app.buildbuddy.io/invocation/b6e7678f-db2b-4464-82cc-c05bbea3561b) | [63869dc7](https://app.buildbuddy.io/invocation/63869dc7-3e11-4095-8657-0d453efadd6b) |
| `6f4ee7f17` (verification snapshot) | Remove five unused libc++/libc++abi implementation dependencies from WideChar, MurmurHash and `//src/Common:clickhouse_new_delete`. Keep unwind and the allocator's cxxabi and jemalloc dependencies, which still provide nine exception symbols and six allocation symbols. Scoped 89a07d50d → 6f4ee7f17: x86 has four passing reports including three positive controls; ARM has three passing controls and no report for `//src/Functions:clickhouse_functions_obj__compile_2`. Each CPU retains six unverified link reports and confirms two negative controls. CityHash's two reported unused runtime edges remain because their prior removal changed unit-test links and ELFs. DWYU is not fully clean. All four changed-owner compiles, ordinary ClickHouse/unit ordered links and full ELFs match. Current-source original ClickHouse and normalized native CMake hashes both match | [0c71cb22](https://app.buildbuddy.io/invocation/0c71cb22-cd56-4c32-93e6-25bb3cbf70f1) | [90c4d373](https://app.buildbuddy.io/invocation/90c4d373-1d2b-4936-9c1e-519c72ab53b8) |
| `8c5da9290` (verification snapshot) | Give `//src/Access/Common:headers` acyclic `//src:core_types` and `//src/Common:memory_tracking_api` prerequisites; declare the same APIs on their real consumers. On d13a9693d → 2ffc6a8ee, Access/Common and both APIs produce clean CPP reports on each CPU. Among 34 x86 / 33 ARM consumers, 2 reports remain clean and 23/22 retain their original findings; 9 original preprocessing failures on each CPU remain unscored. Three positive and two negative controls behave as expected. Scoped ordinary ClickHouse/unit links and full ELFs match. Current-source original ClickHouse and normalized native CMake hashes both match | [b2d3f5aa](https://app.buildbuddy.io/invocation/b2d3f5aa-a62c-4914-8e23-e110d73208f4) | [7d5a6d66](https://app.buildbuddy.io/invocation/7d5a6d66-8ff1-496a-968f-e7477c40cf72) |
| `5a9304c95` (verification snapshot) | Keep libcOTP's four implementation headers in `@clickhouse_libcotp//:_libcotp` srcs; export only `src/cotp.h`. On 61fa0ee6c → 5a9304c95, libcOTP passes DWYU on both CPUs, five controls behave as expected, and every measured libcOTP and Access/Common field matches. Access/Common retains ten unverified link dependencies; DWYU is not fully clean. On 17c7e7cb7 → c4f61dfdf, original ClickHouse/unit ordered links and full ELFs matched. Current-source original ClickHouse and normalized native CMake hashes match | [351de9ba](https://app.buildbuddy.io/invocation/351de9ba-25ca-4463-a3b0-7fe4e836b452) | [4cb665b3](https://app.buildbuddy.io/invocation/4cb665b3-95a4-4c5e-9776-2bda8fc2c9ff) |
| `1580f5474` (verification snapshot) | Keep `bzlib_private.h` and generated `bz_version.h` in `@clickhouse_bzip2//:_bzip2` srcs; publish `bzlib.h`. On both CPUs, all measured bzip2, Arrow, libarchive, minizip and IO CPP fields and five controls match before/after. All five reports retain unverified link dependencies; IO retains one unused implementation dep. Ordinary ClickHouse/unit ordered links, ELF hashes and symbols match. DWYU is not fully clean. Original ClickHouse and normalized native CMake hashes both match | [788bf735](https://app.buildbuddy.io/invocation/788bf735-cd4b-47af-916a-63de336e39cc) | [7dbddc79](https://app.buildbuddy.io/invocation/7dbddc79-6a59-4af1-8cde-9a2e92522371) |
| `0fe042bd3` (verification snapshot) | Move 54 Functions and two Dictionaries implementation headers to their compiled owners. On both CPUs, four callers and five controls match; four owners preserve missing/unused findings, held deps and symbols, with nine newly visible private-dep suggestions. Original unit and ClickHouse ELFs, ordered links and symbol tables match. DWYU is not fully clean. Normalized native CMake hashes match | [61a20161](https://app.buildbuddy.io/invocation/61a20161-5c74-4eab-a3eb-8cb69027342d) | [23d4b722](https://app.buildbuddy.io/invocation/23d4b722-3d8e-4cfb-9c5e-bb455d49e667) |
| `55a4ce862` (verification snapshot) | Keep two Snappy implementation headers in `srcs` and remove two test-only headers from the public API. On both CPUs, Snappy and four callers retain their pre-existing missing/unused findings and genuine link holds; ordinary ClickHouse and unit links and ELFs match. Normalized native CMake hashes match | [4945a8ff](https://app.buildbuddy.io/invocation/4945a8ff-39dd-4e61-a3c4-4b84c3f120e7) | [a587d904](https://app.buildbuddy.io/invocation/a587d904-ee63-48bc-9bc7-04f607a0d7be) |
| `120a25cc8` (verification snapshot) | Move ZooKeeper retries, implementation and dispatcher headers onto their compiled owners; give their public Poco and configuration includes public dependencies. Keeper-bench and System use the compiled owners. Scoped CPP confirms ownership and retains existing link holds; Keeper tests pass, and ordinary ClickHouse and unit ELFs and symbol tables match on both CPUs. Normalized native CMake hashes match | [c29c7733](https://app.buildbuddy.io/invocation/c29c7733-9a5a-4041-8f29-929671d314d3) | [5422242b](https://app.buildbuddy.io/invocation/5422242b-6887-4099-932f-c33782079168) |
| `f9f6e40f6` (verification snapshot) | Give Arrow's generated configuration headers to its existing `_arrow` library and remove the separate header target and DBMS edge. Simply dropping that edge selected a different version header and failed both hashes. On both CPUs, scoped CPP preserves caller findings, ordinary links, ClickHouse and unit ELFs; the formerly unscored `_arrow` now has eight genuine unverified link deps. DWYU is not fully clean. Normalized native CMake hashes match | [4013118c](https://app.buildbuddy.io/invocation/4013118c-ddf7-4ead-8719-f636860ff5d2) | [36eacda2](https://app.buildbuddy.io/invocation/36eacda2-3f2b-48e3-a0d9-b00db988a276) |
| `f11e4fc16` (verification snapshot) | Move libfiu's `internal.h` and `wtable.h` into `srcs`; keep its three `fiu*.h` API headers public. On both CPUs, libfiu and Common CPP fields, native link actions, ClickHouse and unit ELFs and symbols match. Four existing link-hold reports remain. Normalized native CMake hashes match | [6eda8b33](https://app.buildbuddy.io/invocation/6eda8b33-c712-4439-9e7c-b812f89fa7ec) | [eb5192c2](https://app.buildbuddy.io/invocation/eb5192c2-2ef2-4b0c-84df-ca00f163c201) |
| `5e06ba36a` (verification snapshot) | Use the compiled LLVM Support library as the owner of its objects. Scoped checks cover 21 native and 19 CPP targets per CPU and retain unproved links. ClickHouse links and ELFs match; unit link groups change, with registrations and bounded tests checked. The full suite did not pass: ARM disk tests also fail on the baseline where executor statvfs reports zero; x86 stopped at an SSL timing failure not confirmed against baseline. Normalized native CMake hashes match | [3ad36664](https://app.buildbuddy.io/invocation/3ad36664-b993-4d89-836e-e1fdd435cf06) | [3b7f647e](https://app.buildbuddy.io/invocation/3b7f647e-7a37-4ca8-9dc3-554115b417fa) |
| `e7d519725` (verification snapshot) | Move the nine Functions/Dictionaries dependencies exposed by header privatization to `implementation_deps`. Four owner CPP reports lose only the nine intended privacy findings; four caller reports match. Ordinary ClickHouse links and ELFs match; the unit FastOps link groups move, changing unit hashes. Full registration lists (29,940 x86; 29,917 ARM) and eight targeted tests match on both CPUs. Existing DWYU link holds remain. Normalized native CMake hashes match | [fcb5f5b6](https://app.buildbuddy.io/invocation/fcb5f5b6-34a4-4c63-8f90-c1c3f0e5c8b4) | [813aa0a4](https://app.buildbuddy.io/invocation/813aa0a4-3fdc-4b3b-adb6-556743acdff0) |
| `4d444db93` (verification snapshot) | Remove the unused compiler-rt headers dependency from `//base:harmful`. It now passes DWYU on both CPUs; its program caller and `//base:common` preserve all actual findings. Ordinary ClickHouse and unit links, ELF bytes and symbol tables match; existing program link holds remain. Normalized native CMake hashes match | [30e249b5](https://app.buildbuddy.io/invocation/30e249b5-a759-448e-926f-c00108d0a00e) | [390e93f7](https://app.buildbuddy.io/invocation/390e93f7-0fca-419b-ae2d-a2fef1cc7dcf) |
| `6ad0f16f9` (verification snapshot) | Remove `_boost_regex` from `_boost_graph`: Graph uses the public Boost.Regex headers, not the separately compiled POSIX Regex APIs. Graph now passes CPP DWYU. ClickHouse/unit ordered links, raw ELF and symbol tables match. The benchmark still has 7 missing private includes, 3 privacy findings and 200 x86 / 197 ARM held link deps; its Graph dep is now reported unused. Both normalized native CMake hashes match | [bd2966d6](https://app.buildbuddy.io/invocation/bd2966d6-3d3b-49b6-8f9a-319f17173bbb) | [1e35dfd0](https://app.buildbuddy.io/invocation/1e35dfd0-640e-4a86-b4e7-d0fd1dbb996f) |
| `d212a22fd` (verification snapshot) | Remove `_cctz`'s separate `tzdata` library dependency, retaining every one of the 598 embedded timezone files. `_cctz` now passes CPP DWYU on both CPUs; the other scoped findings and holds are unchanged. ClickHouse ordered links and raw ELFs match. The unit timezone object group moves; all 1,197 native timezone tests pass before/after on both CPUs. Both normalized native CMake hashes match | [09654456](https://app.buildbuddy.io/invocation/09654456-cbac-4889-909a-a3bd8cbaeb0f) | [347adb40](https://app.buildbuddy.io/invocation/347adb40-d1a2-411a-ac83-8cf2e70c0aa9) |
| `c3cc8393b` (verification snapshot) | Remove repeated builtins dependencies from four Boost libraries; keep runtimes on the executable targets. `_boost_filesystem`, `_boost_program_options` and `_boost_regex` now pass CPP DWYU on both CPUs. All seven ordinary executables, ClickHouse, ten ordinary link actions, six FlatBuffers headers and Protobuf/gRPC outputs match. Five scoped finding/held reports per CPU remain. Both normalized native CMake hashes match | [9eb3720e](https://app.buildbuddy.io/invocation/9eb3720e-eb91-4364-ae4b-56d25efac36d) | [80fa0825](https://app.buildbuddy.io/invocation/80fa0825-f09d-4c17-acd8-d307d5cb256f) |
| `247d58581` (verification snapshot) | Remove six measured unused benchmark deps, keep three implementation deps private and get two Common headers from `//src/Common`. Scoped `6ad0f16f9` → `899ab61bb` CPP has 0 unused/0 private suggestions; 18 Functions include pairs and 200 x86 / 197 ARM link holds remain. Scoped ClickHouse/unit ordered links, ELFs and symbols match; no standalone benchmark executable was tested. Integrated normalized native CMake hashes match | [ecfbfc78](https://app.buildbuddy.io/invocation/ecfbfc78-2049-493e-a5f9-96f2a5feb830) | [4d8d355c](https://app.buildbuddy.io/invocation/4d8d355c-0c11-4366-ae58-cb3ee8aef7b4) |
| `86696f676` (verification snapshot) | Remove redundant compiler-runtime dependencies from 13 Abseil libraries; retain runtime deps at executable owners. Scoped `70b583641` → `575824957` CPP: all 13 owners and 11 callers clean on both CPUs, 3 positive and 2 negative controls preserved. Scoped ClickHouse/unit ordered links, ELFs and symbols match. Five auxiliary tools byte-match; the gRPC plugin ELF differs, with equal complete symbols and all 142 actual generated outputs per CPU. Both integrated normalized native CMake hashes match | [803d661d](https://app.buildbuddy.io/invocation/803d661d-a735-4ecc-af19-2d2896364e43) | [bf0f3694](https://app.buildbuddy.io/invocation/bf0f3694-f796-42c3-bbd6-ceb70af8f98c) |
| `37134ad5a` (verification snapshot) | Make `//src/Loggers:loggers` a private Daemon implementation dependency and remove its unused config-header dependency. Scoped `da1517e06` → `ef164b944` CPP checks keep all caller findings and Daemon's existing link holds; scoped ClickHouse/unit ordered links, ELFs and symbols match. Both integrated normalized native CMake hashes match | [46ca5573](https://app.buildbuddy.io/invocation/46ca5573-0bb4-48c4-adff-f3a43b1b7ce7) | [529d7eb2](https://app.buildbuddy.io/invocation/529d7eb2-8970-4861-94d8-185b57e226d1) |
| `ed45dbac8` (verification snapshot) | Publish DBMS0's existing ZooKeeper base dependency so consumers get its six required public includes. Scoped `da1517e06` → `da34d64ff` CPP checks fix all six on x86/ARM while preserving other findings; ARM DBMS4 remains source-empty with no CPP report. Scoped ordinary ClickHouse/unit ordered links, ELFs and symbols match; both integrated normalized native CMake hashes match | [c34cd772](https://app.buildbuddy.io/invocation/c34cd772-48c7-41dd-8b86-6a891ba5f979) | [2e3dad8b](https://app.buildbuddy.io/invocation/2e3dad8b-f682-48bd-85fc-0965403f2abd) |
| `1ed257475` (verification snapshot) | Remove unused SipHash from DBMS0; make jemalloc, LLVMPasses and Prometheus proto private. Scoped `da34d64ff` → `9f29b135c` resolves four findings, retains 33 link holds and ARM DBMS4's missing CPP report. ClickHouse links, ELFs and symbols match. Unit ELFs differ −64/−24 B; 72 selected tests and all 29,940/29,917 registrations pass before/after. No CHJIT or remote.pb serialization coverage. Both integrated normalized native CMake hashes match | [c2c8eaa1](https://app.buildbuddy.io/invocation/c2c8eaa1-c427-4b66-a2e3-1fd1bf7a4369) | [daccbdbf](https://app.buildbuddy.io/invocation/daccbdbf-fbf7-4120-a15f-fc4d998be41d) |
| `0cc8213ff` (verification snapshot) | Move FunctionFactory and FunctionsLogical headers to their DBMS1 compiled owner; update benchmark_runtime_filter_objects. Scoped `51fc04fec` → `e4bf7508b` resolves 18/7 include-owner findings, preserves unrelated findings, and matches ClickHouse/unit ordered links, ELFs and symbols on both CPUs. Benchmark executable not linked. Both integrated normalized native CMake hashes match | [5605af8f](https://app.buildbuddy.io/invocation/5605af8f-d83d-463b-a516-b7ecb3597a67) | [d8a96fc8](https://app.buildbuddy.io/invocation/d8a96fc8-bcd1-42f8-8357-36c260c788fc) |
| `46395c47c` (verification snapshot) | Correct librseq and liburing contextual headers; declare libssh's C++ wrapper separately from its C headers. Scoped `51fc04fec` → `8a518fc9a` builds all three C libraries and actual callers; missing CPP reports recover on both CPUs with unrelated findings retained. Both integrated normalized native CMake hashes match | [2b3a19f7](https://app.buildbuddy.io/invocation/2b3a19f7-2c3a-454e-8bc3-307366c89aaf) | [6455ec6a](https://app.buildbuddy.io/invocation/6455ec6a-dded-4846-a51c-cb3e020cff74) |
| `7283c18a5` (verification snapshot) | Drop six unused libc++/libc++abi header dependencies of librseq, libssh and liburing and make libssh OpenSSL crypto private. Scoped `8a518fc9a` → `879600c4c`: ordinary libraries and callers build on both CPUs; recovered libssh CPP is clean, remaining linker holds unchanged. Both integrated normalized native CMake hashes match | [f972a7d2](https://app.buildbuddy.io/invocation/f972a7d2-26ee-4452-8baa-78ef364bbe31) | [1d78a233](https://app.buildbuddy.io/invocation/1d78a233-fde4-4a3b-8b8b-e2cd0a8c6b8a) |
| `7e166adb1` (verification snapshot) | Declare JWT's default API and OpenSSL dependencies and the Mapbox primitives used by Wagyu. Scoped `51fc04fec` → `2de9d9d7f` builds 10 ordinary libraries/callers on both CPUs; actual JWT and Mapbox CPP reports are clean and callers unchanged. MessagePack remains unverified. Both integrated normalized native CMake hashes match | [f7d88254](https://app.buildbuddy.io/invocation/f7d88254-5419-4fc2-9a01-e91eac445707) | [9669ca4c](https://app.buildbuddy.io/invocation/9669ca4c-133e-41c6-aec4-fb9f618e28ec) |
| `a5420f79b` (verification snapshot) | Supply Daemon and Loggers public includes from Common, Core and Poco; retain named ConfigProcessor.h at the verified Config/Daemon cycle. Scoped `51fc04fec` → `8dd0625f8`: ordinary owners and callers build, both header CPP reports and five fresh controls are correct on both CPUs. Parsers' original missing report remains held. Both integrated normalized native CMake hashes match | [fbb3df39](https://app.buildbuddy.io/invocation/fbb3df39-a121-402c-801d-745bab24395a) | [09867c48](https://app.buildbuddy.io/invocation/09867c48-7a11-452d-b35f-55588a8f5d8a) |
| `ffcbe7d4f` (verification snapshot) | Declare enabled zlib-ng architecture headers and supply SZ3 with the complete Zstd library including assembly. Scoped `51fc04fec` → `92fc54add`: 11 native targets and five real controls pass on each CPU; missing CPP reports recover. Existing caller findings remain, Compression's prior Zstd files gain the SZ3 route. zlib-ng unused/held and SZ3 forwarded-header findings remain. No unit ELF or execution claim. Both integrated normalized native CMake hashes match | [695aa530](https://app.buildbuddy.io/invocation/695aa530-5ac3-4992-a218-e422feabd730) | [e28d27c8](https://app.buildbuddy.io/invocation/e28d27c8-b461-4403-bab0-32394d31232a) |
| `a7d01efab` (verification snapshot) | Remove unused LZ4 frame wrapper from RocksDB; keep LZ4 block, HC and Zstd. Scoped 51fc04fec to 653878852: 16 native builds, five controls, actual CPP reports and ordinary ClickHouse/unit binaries, symbols and ordered links match on both CPUs. RocksDB's genuine Zstd hold and caller findings remain. Both integrated normalized native CMake hashes match | [edf843fc](https://app.buildbuddy.io/invocation/edf843fc-7b27-4adf-9800-7d87b2be5ad3) | [cf3c1e48](https://app.buildbuddy.io/invocation/cf3c1e48-81e8-4598-b713-3723d43ba751) |
| `9144b1fc2` (verification snapshot) | Remove unneeded C++ runtime dependencies of libstemmer, SHA3IUF and ULID C. Scoped 51fc04fec to 4f4e79408: 24 native builds, 11 measured CPP libraries, real positive/negative controls and ordinary ClickHouse/unit binary equality on both CPUs. Two prior linker holds and one unused dependency resolve; native-only targets remain unscored by CPP. Both integrated normalized native CMake hashes match | [cb57e8ed](https://app.buildbuddy.io/invocation/cb57e8ed-ffa9-4be2-a788-9ff5333b8f08) | [b2cb9ceb](https://app.buildbuddy.io/invocation/b2cb9ceb-7a12-4bc1-a1ff-c3d4057e8f35) |
| `e3eae6a12` (verification snapshot) | Remove both unused libc++ header dependencies of zlib-ng C. Scoped 92fc54add to 264bff05f: both actual CPP findings resolve, SZ3 and caller reports remain byte-identical, every ordinary ClickHouse/unit link action matches, and five real controls pass on both CPUs. zlib-ng's compiler-runtime linker hold remains. Both integrated normalized native CMake hashes match | [72b38aa6](https://app.buildbuddy.io/invocation/72b38aa6-d181-433f-8b2a-8b0e7995fb8c) | [620c974f](https://app.buildbuddy.io/invocation/620c974f-a086-4259-b08a-c4a288e9ea25) |
| `3ed901ef6` (verification snapshot) | Remove two unused libc++ header dependencies from c-ares C. Scoped e3eae6a12 to 3ed901ef6: 13 native targets, five real CPP controls and genuine owner/caller reports pass on both CPUs; only two c-ares findings change. Mongo and DBMS linker holds remain. Ordinary ClickHouse/unit binaries, symbols and all ordered links match. Both normalized native CMake hashes match | [52ad556b](https://app.buildbuddy.io/invocation/52ad556b-37b7-4905-ae3e-9d94877b7565) | [731ffb3b](https://app.buildbuddy.io/invocation/731ffb3b-4410-4de2-8485-94d7d0b07454) |
| `7ba337afe` (verification snapshot) | Default to minimal remote downloads and verify ELF metadata; narrow DWYU analyzer inputs and retain all compiler/linker holds; remove redundant headers and dependencies in programs, GatherUtils, AI SDK, Abseil, XGBoost, libarchive and libhdfs3. Both scoped CPP and native checks pass; recorded normalized CMake hashes match | [86051695](https://app.buildbuddy.io/invocation/86051695-ecd5-4acd-9c7d-4e9acde02eee) | [5c158e52](https://app.buildbuddy.io/invocation/5c158e52-9766-4563-ac59-f15a7fa8e9e5) |
| `8ad25273c` (verification snapshot) | Assign private headers to compiled owners, restore the required GatherUtils/IO header dependencies, select ISA-L headers per CPU, and separate optional Msgpack providers. Scoped CPP had no new missing includes; SDK, GatherUtils, libarchive, Mongo and Msgpack checks pass on both CPUs. Raw hashes are unchanged; normalized hashes match the recorded CMake hashes. | [c573ba62](https://app.buildbuddy.io/invocation/c573ba62-915a-4228-8db6-549a2ba49317) Raw `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` (929619680 B); normalized `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` (710693736 B) | [d2ae3839](https://app.buildbuddy.io/invocation/d2ae3839-0f48-4b03-b57b-5d034e1a768c) Raw `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` (907067440 B); normalized `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` (637316696 B) |
| `180c42ca3` (verification snapshot) | Consolidate program definitions, headers and build macros; give affected programs, benchmark libraries and Loggers direct header owners. Remove redundant config exports and DBMS-embedded imports. The scoped CPP comparison and native SDK/Gather/Zstd/Mongo/Msgpack tests pass on both CPUs. All 5 selected benchmark libraries built; benchmarks and the whole unit suite were not run. The whole repository has not passed DWYU. Raw hashes are unchanged; normalized hashes match recorded CMake. | [99bff251](https://app.buildbuddy.io/invocation/99bff251-6469-486d-a50a-b14a36b8c4b8) Raw `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` (929619680 B); normalized `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` (710693736 B) | [7267a0d8](https://app.buildbuddy.io/invocation/7267a0d8-3db0-4342-811d-a3fe3c19670e) Raw `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` (907067440 B); normalized `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` (637316696 B) |
| `a8ef9573d` (verification snapshot) | Simplify external sources and private header ownership, preserving the repaired direct owners. Scoped CPP and native SDK/Gather/Zstd/Mongo/Msgpack checks pass; Boost checks exercised actual local query, help and XML config selection on both CPUs. All selected benchmark libraries built; benchmarks and the whole unit suite were not run. The whole repository has not passed DWYU. Raw hashes remain equal; normalized hashes match recorded CMake. | Run [ce8186f9](https://app.buildbuddy.io/invocation/ce8186f9-4a77-4374-a37b-f3122ac4c51d); baseline native [5eed10d7](https://app.buildbuddy.io/invocation/5eed10d7-6549-45d8-b4e5-22d790af4076); candidate native [6576dab3](https://app.buildbuddy.io/invocation/6576dab3-d5ed-4fd0-9d1c-56c9eb51093b); raw `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` (929619680 B); normalized `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` (710693736 B) | Run [a05decdc](https://app.buildbuddy.io/invocation/a05decdc-cc79-42da-8809-cefbb94ee91e); baseline native [64534173](https://app.buildbuddy.io/invocation/64534173-7fce-4633-86bf-e331293795f9); candidate native [2d48d6a7](https://app.buildbuddy.io/invocation/2d48d6a7-bb7a-4b58-bbd4-6a8552288565); raw `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` (907067440 B); normalized `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` (637316696 B) |

A separate source-only audit on `08511a780`, unchanged by `1580f5474`, found 13 production `additional_compiler_inputs` declarations for 661 embedded assets (counting the ClickStack packed tree once) and 12 C/C++ files that are both included and compiled. XGBoost declares no `additional_compiler_inputs`; this source audit does not establish CPP or native cleanliness.

The DWYU cleanup rebuilds above reproduced both normalized CMake hashes:
`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`
(x86-64, `cmp` 0) and
`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`
(native ARM64). The original Bazel ELF hashes also remained
`0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372`
and `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744`,
respectively.

On `6845aa964`, the current S2/DMLC check compiled all 117 source actions and built `//:clickhouse` and `//src:unit_tests_dbms` on both native architectures. Both ordinary ELFs and the DBMS link arguments, parameters and compiled inputs match the preceding verified source. Actual S2 reports retain 91 public and 63 private include findings; DMLC's report is clean. AggregateFunctions and Dictionaries had no scored report on either architecture, and `//src:dbms__compile_4` had no scored ARM report. They are not treated as clean. Boost.Asio's pragma headers were verified only together in NuRaft; Boost's full header report is still unavailable. Scoped negative controls continued to report findings. The native `FunctionsStress.stress` run discovered all six requested S2 functions but uses random inputs; the run does not guarantee successful execution of all six.

For these checks, `llvm-strip --strip-all --remove-section=.note.gnu.build-id`
produced SHA-256 `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`
on x86-64 and `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`
on ARM64. The selected compilers and original Bazel ELFs retained the SHA-256
values recorded in their respective tables.

On the final `9f3a2b2b9` build, the original Bazel SHA-256 values remained
`0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372`
(x86-64) and
`2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744`
(ARM64). The selected compiler SHA-256 values remained
`52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce`
(x86-64) and
`3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f`
(ARM64).

The `46418055d` through `9f3a2b2b9` builds each reproduced these hashes.
The x86 check verified the saved CMake ELF SHA-256
`cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc`
and returned 0 from both the stripped-binary `cmp` and `PARITY_STATUS`.
The ARM check matched the recorded native CMake normalized hash and returned
`ARM_STRIPPED_STATUS=0`; it did not run a fresh ARM CMake build.
Only completed successful runs appear in the table.

The 50 BUILD checks from `3ddb640b7` covered all 277 tracked first-party
and external BUILD files per check (13,850 file checks). They covered header
ownership, pinned-file source globs, public and private dependencies, flags
and include directories, CPU selections, compiler inputs, exports, and
shared settings. The six commits above apply the proven reductions; source
globs preserve the original translation-unit order on both CPUs. Checks
that would add sources, change include precedence, or remove a platform
guard were rejected. All changed production files are BUILD files.

ClickStack's source build reproduced all 162 pinned release assets on both
CPUs. Its source manifest SHA-256 was
`1dc9e12c91fdf7e62c97eb87329d8d241d07634e5ed88713aa3d6cfcfe9d799c`.
The 163-file compressed-resource manifest was
`f1eba11ef01b2d65a209413ce6976815ac71d7803ee1d13d708367c88349b958`
(x86-64) and
`b575f49150d3c4be233723802259b22016bc05ec5fff99e2a994af7a7a2fb078`
(ARM64); these differ because the native CMake reference builds recorded
different resource timestamps. Both manifests reproduced after disabling
remote cache acceptance in the
[x86-64 replay](https://app.buildbuddy.io/invocation/330a160d-406e-420b-8144-0e727ee1c41d)
and [ARM64 replay](https://app.buildbuddy.io/invocation/5969731e-e831-428f-873f-2e5266c14bcb).

The audit on `2dd816e17` ran 50 BUILD predicates for each of x86-64 and ARM64:
100 inspections covering 117 main packages, 159 external overlays and 814
C/C++ targets. It found 92 inspections with complete static evidence. The
other eight retained unresolved generated-proto/translation-unit inputs and
13 dynamic export labels; the audit does not claim those can be removed.
The comparison against upstream `6bb427c9a` has no non-BUILD changes in
`src`, `base`, `programs`, `contrib`, `cmake`, `tests`, or `utils`.

The subsequent `16eb12020` audit inspected all 277 tracked BUILD files:
116 first-party BUILD files, 159 dependency overlays, the dependency template
package, and the Rust Perfetto protobuf overlay. It changed 64 BUILD files,
removing unused compiler inputs, redundant exports and header exclusions,
and obtaining shared headers from their C++ dependencies. All 22 removed
exports were checked against references in BUILD, Starlark, and migration
metadata. Three external source archives were unavailable for source-inventory
proof; their files were inspected and left unchanged. The source-file order
checks cover both supported CPUs.

### Linux x86-64

On a Linux x86-64 host with BuildBuddy remote execution configured, build the
CMake reference with the compiler selected by this repository:

```sh
bash bazel/initial_migration/run_cmake_reference.sh
python3 bazel/initial_migration/refresh_cmake_metadata.py --arch=x86_64 \
  --build-dir="$PWD/build_bazel_reference_x86_64"
python3 bazel/initial_migration/refresh_cmake_metadata.py --check --arch=x86_64 \
  --build-dir="$PWD/build_bazel_reference_x86_64"
```

`bazel/initial_migration/run_cmake_reference.sh` builds `//bazel:cmake_toolchain_files`,
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
bazel build --config=cmake_match //:clickhouse
```

For the stripped x86-64 comparison, run on a BuildBuddy Linux hosted runner:

```sh
bash bazel/initial_migration/run_stripped_hash.sh
```

`--config=cmake_match` expands to `--config=remote --config=x86_64
--//bazel:cmake_metadata`. This uses `common:remote` (`--jobs=500`), verifies that the Clang and Clang++
SHA-256 values match the recorded CMake toolchain, and builds `//:clickhouse`.
After Bazel finishes, it verifies the original CMake artifact in a temporary
directory outside the checkout. It first verifies the original
`llvm-strip --strip-all` hash, then compares copies made with:

```sh
llvm-strip --strip-all --remove-section=.note.gnu.build-id -o OUTPUT INPUT
```

Both copies must equal the recorded SHA-256
`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`
and pass `cmp`. The comparison exits
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

| SHA-256 | CMake | Bazel |
| --- | --- | --- |
| Selected Clang and Clang++ | `52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce` | `52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce` |
| Original | `cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc` | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` |
| `llvm-strip --strip-all` | `6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe` | `2ca4dbdd6bfd4511189c0547ebc62cb505cb1b760aa5346c553175ded61e5c93` |
| `llvm-strip --strip-all --remove-section=.note.gnu.build-id` | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` |

`cmp` of the last row returned 0, and `PARITY_STATUS=0`. This is stripped
binary equality; the original binaries and ordinary stripped build IDs differ.

The native-rule and dependency BUILD cleanup was compared with native CMake
after each batch. Every comparison below returned `PARITY_STATUS=0` and
`PARITY_STRIPPED_CMP_STATUS=0`. The normalized SHA-256 values were
`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` on
x86-64 and
`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` on
ARM64. The ARM64 CMake and Bazel compiler and tool verification is described
under [Linux ARM64 (native)](#linux-arm64-native).

| Cleanup batch | x86-64 comparison | Native ARM64 comparison |
| --- | --- | --- |
| Remove `cmake_native_compile_flags`; default native flags and features in `ch_cc_library` | [6d0f655f](https://app.buildbuddy.io/invocation/6d0f655f-2094-41f4-adb4-7e4c85298eb2) | [b897bd67](https://app.buildbuddy.io/invocation/b897bd67-c016-49a0-90c7-2a97d3df8e92) |
| Declare Boost, fmt, FlatBuffers and libuv public headers once through their owning dependencies | [0cce2c2a](https://app.buildbuddy.io/invocation/0cce2c2a-44d8-4ef2-a5ea-cedb599d06b5) | [166537b6](https://app.buildbuddy.io/invocation/166537b6-4b94-43d5-82d0-48773f9e3c83) |
| Collapse 414 single compile-group wrappers and preserve CMake archive and object order | [93c41ed5](https://app.buildbuddy.io/invocation/93c41ed5-3c29-4653-a4ac-616c612f27b6) | [888890f7](https://app.buildbuddy.io/invocation/888890f7-5597-4a4e-9097-c81f065910a1) |
| Deduplicate Arrow and dependency public headers, compact header globs and remove unused exported inputs | [f5bc803d](https://app.buildbuddy.io/invocation/f5bc803d-21d0-45a4-9004-e2c46ef0ca0e) | [6aae0e32](https://app.buildbuddy.io/invocation/6aae0e32-7f28-4e2f-afd5-584b6b695cc9) |
| Compile AWS, ORC, DMLC and ICU sources in their owning repositories, with public headers and the original object order | [abed9878](https://app.buildbuddy.io/invocation/abed9878-5651-4c75-9111-48896e062ff4) | [93f97ae5](https://app.buildbuddy.io/invocation/93f97ae5-982c-47fb-a6ef-79b79a288f86) |
| Declare 23 native header libraries, consume existing Arrow/librdkafka headers, and remove unreferenced external exports | [0637569e](https://app.buildbuddy.io/invocation/0637569e-2bd2-4310-940f-59172a308a1d) | [5f3aeea0](https://app.buildbuddy.io/invocation/5f3aeea0-8d18-4cee-b54d-83a2ca9f83da) |

In the final batch the original CMake and Bazel hashes, ordinary stripped
hashes, and selected compiler hashes also remained equal to their respective
values in the tables above and under [Linux ARM64 (native)](#linux-arm64-native).
All 159 external BUILD files were audited: the remaining export-only
repositories supply licenses, web resources or compressed language data;
their configured consumers do not compile C/C++ sources from them.

LLVM registration, Rust code generation, initial migration, Abseil, and
Buildifier were verified again as separate batches. Each x86-64 run below
executes `bazel/initial_migration/run_stripped_hash.sh`: it verifies the reference compiler and
CMake executable, verifies both normalized hashes, and requires `cmp=0` and
`PARITY_STATUS=0`. All x86-64 hashes in the table above are unchanged.

The ARM64 runs use `--config=cmake_match_aarch64`. For LLVM and protoc,
BuildBuddy's completed `ClickHouseNativeLink` action reports the original
Bazel ELF SHA-256 and size. The other ARM64 runs download the entire Bazel ELF
and require `sha256sum --check` to match the original Bazel ELF from the
native CMake comparison under [Linux ARM64 (native)](#linux-arm64-native).
Every original ARM64 Bazel ELF equals
`2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744`
(907,067,440 bytes). Equality of the entire original ELF preserves the
native-CMake-verified normalized ARM64 hash
`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`.
These ARM64 checks verify the Bazel output against that recorded comparison;
they do not depend on rerunning the unchanged CMake build for every Bazel edit.

| Cleanup batch | x86-64 normalized CMake comparison | ARM64 original Bazel ELF check |
| --- | --- | --- |
| Register `@llvm//toolchain:all` | [c6c9cffe](https://app.buildbuddy.io/invocation/c6c9cffe-7891-4d3e-acb2-c2c705d0bc38) | [01d39e22 (link action)](https://app.buildbuddy.io/invocation/01d39e22-5cd1-4751-a2e1-add29680579e) |
| Use `@protobuf//:protoc` in Perfetto | [ff56d81c](https://app.buildbuddy.io/invocation/ff56d81c-9a81-40a0-bced-87f8850fe831) | [6670b3d4 (link action)](https://app.buildbuddy.io/invocation/6670b3d4-febd-4c04-9edb-6aa312f62ba7) |
| Share native code generation records and use one `rust_cbindgen` rule | [92803c27](https://app.buildbuddy.io/invocation/92803c27-60af-463e-b49b-bf4885365a2f) | [424772b8](https://app.buildbuddy.io/invocation/424772b8-366a-49d6-9f66-cd5278a13d4e) |
| Move initial BUILD generation and recorded data into `bazel/initial_migration/` | [39c6a80a](https://app.buildbuddy.io/invocation/39c6a80a-e17d-45f4-a5b3-90b18c993bee) | [80d5f9c7](https://app.buildbuddy.io/invocation/80d5f9c7-824c-48d9-9536-db091a8b2467) |
| Use Abseil's `cc_library` headers and remove unused exports | [839891fa](https://app.buildbuddy.io/invocation/839891fa-ded0-4d6d-927a-716eed347b26) | [0497b824](https://app.buildbuddy.io/invocation/0497b824-cabc-421a-bb6f-20ea0496ac69) |
| Format all generated BUILD files with Buildifier while retaining compilation order | [5669c7cd](https://app.buildbuddy.io/invocation/5669c7cd-22a9-487c-814a-6239dc692edf) | [5a76e188](https://app.buildbuddy.io/invocation/5a76e188-d4ee-4fee-9906-c1fead7f51d5) |
| Use native globs with actual header suffixes; remove empty patterns, unused exclusions and `allow_empty` in all dependencies | [91311f81](https://app.buildbuddy.io/invocation/91311f81-c3dd-4d70-b12e-8947c7abd5f3) | [c90cccaf](https://app.buildbuddy.io/invocation/c90cccaf-388f-4cde-a680-578688bc17fe) |
| Remove package BUILD file exports from dependencies | [338499e5](https://app.buildbuddy.io/invocation/338499e5-4d1b-425e-b3b5-e2c825be278d) | [ff92e91d](https://app.buildbuddy.io/invocation/ff92e91d-dbf1-474f-bd18-00ddaf9df837) |
| Inherit public headers and Mongo textual inputs through existing C++ deps; locate Rust repositories through real Cargo manifests | [e4e24fe5](https://app.buildbuddy.io/invocation/e4e24fe5-073e-4cbd-b79e-c8a40877e7d1) | [d92d3d1f](https://app.buildbuddy.io/invocation/d92d3d1f-3be6-4266-95fd-54e2494872c4) |
| Supply private Postgres OpenSSL and libcxxabi includes with `implementation_deps` | [426e77d3](https://app.buildbuddy.io/invocation/426e77d3-7692-4480-8f6c-8042391c8ad7) | [a8badca3](https://app.buildbuddy.io/invocation/a8badca3-c103-43f9-a434-aaff9a690b01) |
| Remove unused translation units, build files and directories from compiler inputs (before changing the global include feature) | [ff50469b](https://app.buildbuddy.io/invocation/ff50469b-d885-47d4-8abb-a62a45a8a16f) | [4282634b](https://app.buildbuddy.io/invocation/4282634b-3942-4253-863f-08ca850a2de9) |
| Set `common --features=system_include_paths`; remove per-library feature attributes | [b26876e4](https://app.buildbuddy.io/invocation/b26876e4-3642-4bd7-b74a-2a0f710da365) | [b7b065d2](https://app.buildbuddy.io/invocation/b7b065d2-83ee-4d0c-b321-b37cc10ed342) |
| Replace exported Postgres/libpqxx source headers with `postgres_headers` and `libpqxx_headers` library dependencies | [599bc775](https://app.buildbuddy.io/invocation/599bc775-52eb-495c-b525-ce53c1e797a5) | [e2b5aede](https://app.buildbuddy.io/invocation/e2b5aede-f072-4727-b347-90bc20cf93e1) |
| Supply libpqxx extensionless wrappers in `textual_hdrs`; remove unneeded PostgreSQL inputs from libpqxx | [f61afa2c](https://app.buildbuddy.io/invocation/f61afa2c-bd39-4817-8c97-ba76ac0090a7) | [04be19aa](https://app.buildbuddy.io/invocation/04be19aa-5f84-4423-9f0f-7cecd40e1897) |
| Remove remaining PostgreSQL/libpqxx native compiler inputs and export only licenses | [32d0583d](https://app.buildbuddy.io/invocation/32d0583d-6340-47d3-adff-75e70f8aa2c1) | [3889a2c1](https://app.buildbuddy.io/invocation/3889a2c1-e9df-46be-aeba-c0a4399b9fdb) |
| Remove PostgreSQL source filename colons, unused local data, and OpenSSL template compiler inputs | [11f3af33](https://app.buildbuddy.io/invocation/11f3af33-3a06-49f4-873e-d3a40477dc99) | [ec8840bb](https://app.buildbuddy.io/invocation/ec8840bb-6bf5-4ef7-85c8-ad37194aba43) |
| Simplify source filenames in all externals and use 316 order-identical globs in 45 repositories | [edac5f3d](https://app.buildbuddy.io/invocation/edac5f3d-36a4-4ddf-a56a-80019a822873) | [12f2d76e](https://app.buildbuddy.io/invocation/12f2d76e-dd34-4f6a-8248-6fd4249f9271) |
| Remove 17,431 raw header references supplied by public or own implementation dependencies across all external owners | [e52e9900](https://app.buildbuddy.io/invocation/e52e9900-f1e2-46a5-bf5f-b81f133c5a77) | [836ec5d5](https://app.buildbuddy.io/invocation/836ec5d5-0a99-4db5-bceb-df1083421947) |
| Remove 41,115 captured data-file inputs from native libraries; declare all Sparsehash extensionless headers on their library | [f8f92940](https://app.buildbuddy.io/invocation/f8f92940-11fa-4d2d-9cd7-cebfd177bd72) | [f00daa53](https://app.buildbuddy.io/invocation/f00daa53-c3bb-40ab-9a1a-1752d59ce241) |
| Inherit OpenSSL public include directories through reachable library dependencies in 21 external repositories | [4b04bcc0](https://app.buildbuddy.io/invocation/4b04bcc0-991a-45be-862d-f2f3b57308ad) | [08e21950](https://app.buildbuddy.io/invocation/08e21950-02d9-4a2e-93b5-415689aced47) |
| Supply `libcxxabi_headers` through `implementation_deps` in 57 external repositories | [54a9bf39](https://app.buildbuddy.io/invocation/54a9bf39-85ad-4290-ba29-5a3ab6cea9ce) | [ffd6c028](https://app.buildbuddy.io/invocation/ffd6c028-a56c-41c0-af53-a37329fb6f20) |
| Remove native data unreachable from compiled sources and published headers; prune 6,027 unused exports (`dbef9b19a`) | [228e87ab](https://app.buildbuddy.io/invocation/228e87ab-f161-4d44-8f70-baa31de7cc13) | [5110ccb0](https://app.buildbuddy.io/invocation/5110ccb0-83fe-45c4-9117-37dd84bea1dd) |
| Use local directory names for the 36 same-repository include expressions (`1793e1fa3`) | [61415215](https://app.buildbuddy.io/invocation/61415215-0c82-4ef5-8af1-4b908b6c5bf2) | [d4593dc6](https://app.buildbuddy.io/invocation/d4593dc6-a400-4dbb-851f-5d6d82ae4545) |
| Publish AWS CRT public headers and includes through library dependencies; remove captured foreign inputs (`0e0832918`) | [2dfd2546](https://app.buildbuddy.io/invocation/2dfd2546-fb33-4c2c-aa26-b4071cc47001) | [99f08d57](https://app.buildbuddy.io/invocation/99f08d57-c4bd-4651-97dc-b4fa86722c54) |

The external dependency ownership cleanup was also verified directly with
normalized binaries on both platforms. The ARM comparison ran LLVM's strip
tool on an ARM64 executor against the recorded native CMake result.

| Cleanup batch | x86-64 normalized SHA-256 | ARM64 normalized SHA-256 |
| --- | --- | --- |
| Native header ownership, private Asio, Arrow generation order, unused compiler inputs and banners (`66945d072`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/34e4eac9-d6c5-4499-9850-78f61eeba610) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/9ef5527b-d761-4f49-aff6-04d304a03c55) |
| Remove unused NuRaft source inputs, inherit liburing generated headers, and remove unused BUILD imports | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/7538721e-11ac-4734-9be5-1958be61135e) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/f7b1b89a-4500-4659-bc34-dc7e697db713) |
| Inherit duplicate public header globs; make Benchmark internal headers private; remove unused ICU data libc++abi inputs (`9d2391a9c`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/80a15c31-222d-4c9e-a33a-d10599ad481b) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/37fd4325-f7a0-4a08-8491-2981f0a23ec9) |
| Generate Google Cloud protos in configured Bazel output directories; remove unused Boost/Zstd/OpenSSL includes and make AI SDK, Cassandra, and Silk internal headers private (`7bbf9beb6`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/fa2ee870-e09c-4142-a37e-558a153ad250) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/43c5316a-714a-42c6-a427-df3fb5dc5963) |
| Inherit transitive native dependencies; supply pinned sysroot inputs in `ch_cc_library`; order LLVM math before glibc compatibility in tool links (`ea59298f6`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/d719df32-51c2-427d-a882-57f75f3ed8c0) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/ec0d8a08-6749-4e83-b4c9-9c4ae1cfb53e) |
| Remove copied source and CMake data from MeCab and other native compiler inputs; remove compiled sources from published generated headers (`7a6a69a2b`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/09c7cec2-cd43-4317-ad65-add19b754241) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/2f19e552-852a-4619-a220-716c609e7e55) |
| Use configured external headers and code generation outputs; share zlib-ng labels in Minizip (`9e57d5fbc`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/e72f5480-fabe-434d-97b3-5a541f90d48e) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/5bfe2f5b-4ed1-4114-8401-028382499e18) |
| Use configured LLVM runtime and TableGen outputs and retain architecture guards for ARM-only generators (`5897416a0`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/27f5abfe-bb95-47db-b88c-38879f820613) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/4caf665d-1c9c-479a-b9c3-f782147f5f74) |
| Move ELF comparison, audit and CMake reference scripts into `bazel/initial_migration` (`ce068c5ec`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/5a194734-bc8e-4475-a4d9-7c8322c15126) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/9b339263-77cc-4bc3-ae56-5bb39ab2920e) |
| Move production rules, code generation inventories, native link plans and build tools into `bazel/rules` (`0b64e208a`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/2f96e64f-bdd4-4e06-b705-a154d4000e84) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/6b6792ef-fe7f-4cde-8b10-19de95b4d386) |
| Move native rules into source, program and in-tree dependency packages; retain template includes and remove repeated `-w` (`913d21c59`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/c4a7112c-c9fc-49fb-a9c7-5f3588f566e7) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/b60d46a5-8bf1-4fe8-845c-f389b03750de) |
| Use exact DMLC, ORC and libcotp header globs, Abseil public headers, private external includes, shared options, fewer FastOps rules and C++ defaults, and generated Arrow FlatBuffers headers (`8efcb12c0`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/b170352f-1048-435d-878f-7fef9889cb64) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/46286939-2d68-4b2d-92ef-b102c47ac94d) |
| Inherit eight dependencies' published includes, share Arrow and AWS substitutions, remove unused Arrow compiler inputs, and inherit 449 existing generated or external public headers (`45276d8c3`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/3d7fb7dc-1d13-4930-a37c-f1687a005951) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/84a65e09-6587-4612-ab15-1d30aed2a88b) |
| Inherit first-party headers, remove unused first-party compiler inputs and defaults, and use configured output names for GitHash, config and version (`f8b61cbe4`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/b8c4b4f3-3587-42e9-8e3d-4fecc9c26c1e) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/103487d0-a768-47f9-bdbf-86111a7aee9e) |
| Convert first-party source `-I` directories to `local_includes` and inherit public dependency includes (`573c1a784`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/675b44ee-022b-460d-9ea8-853f58908dd6) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/4f7b1327-4b1b-4fcd-84af-06f0770b446e) |
| Remove 4,762 unused first-party exports, inherit 1,762 generated source declarations, use 179 ordered globs, share own-code C++ defaults, and remove 258 compiler-input references (`0c285c5d8`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/0da6f376-ab20-4205-9a97-9ee14de85553) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/31eb9ea1-1d3d-4cca-a2ec-119dee5712b7) |
| Supply LLVM headers, C libc++ inputs and applicable `cxx`/`cxxabi`/`unwind` dependencies through `ch_cc_library` (`ddcb5a721`) | [`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`](https://app.buildbuddy.io/invocation/33ee925f-5f62-40d9-8742-506da967a93c) | [`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03`](https://app.buildbuddy.io/invocation/3ce75088-45d2-411a-a113-fca669c85918) |

Every x86-64 comparison passed `cmp` and `PARITY_STATUS=0`; every ARM64
comparison reported `ARM_STRIPPED_STATUS=0`. Each batch also reproduced
the exact original Bazel ELF hashes from the preceding tables on both CPUs.
The MeCab, configured external output, and LLVM output batches also built `@clickhouse_mecab//:_mecab`,
`@clickhouse_minizip_ng//:_minizip`,
`@clickhouse_google_cloud_cpp//:google_cloud_cpp_storage_protos`, and
`@clickhouse_google_benchmark//:google_benchmark_main` on both CPUs.
The Benchmark/ICU batch also built
`@clickhouse_google_benchmark//:google_benchmark_main` on both CPUs, and
reproduced both original Bazel ELF hashes.
The Google Cloud batch also built
`@clickhouse_google_cloud_cpp//:google_cloud_cpp_storage_protos` and
`@clickhouse_google_benchmark//:google_benchmark_main` on both CPUs,
and reproduced both original Bazel ELF hashes.
The native dependencies batch built both of those targets and
`@clickhouse_thrift//:_thrift` on both CPUs. Both original Bazel ELF
and selected Clang hashes were unchanged.
The `8efcb12c0` batch also built
`@clickhouse_arrow//:parquet_dump_schema`,
`@clickhouse_arrow//:parquet_reader`, and
`@clickhouse_arrow//:parquet_scan` on both CPUs. The
[ARM Parquet checks](https://app.buildbuddy.io/invocation/aa413ce0-b9c4-4370-bff6-4b5d03e67580)
passed before the ARM ClickHouse run was retried for a transient remote-cache
input-transfer error; the successful hash comparison is linked above.
The `45276d8c3` and `f8b61cbe4` batches built the same three Parquet tools
on both CPUs in the linked invocations.

After `0c285c5d8`, `d9e835140` joins adjacent literal-list expressions
in first-party C++ attributes. An ordered AST comparison of all 111 source
BUILD files confirmed identical expressions under every configuration;
`select`, `glob`, and comprehension expressions were not joined.
`c4e709f54` removes only the runtime-filter benchmark's use of a shared
textual-header list. The benchmark retains all 44 headers on each CPU
through public dependencies; both complete public and compiler-input sets
and ordered source lists matched across 1,638 configured library comparisons.
The four database compilations that use that list remain unchanged. The
benchmark is absent from the ClickHouse `cmake_link` references on both CPUs.
Neither change modifies the ClickHouse compile or link inputs verified above.

`ddcb5a721` includes those two changes in complete builds on both CPUs.
All original ELF, normalized ELF, and selected compiler hashes above remain
unchanged. x86-64 also compares the two normalized files directly
(`PARITY_STRIPPED_CMP_STATUS=0`). The macro supplies the ClickHouse LLVM
header dependencies and include directories by default for ClickHouse code;
`llvm_headers` opts other libraries in or preserves an exceptional dependency
order. `link_runtime` opts in only existing direct users of `cxx`, `cxxabi`,
and `unwind`. `clang_rt_builtins` remains explicit to avoid a dependency
cycle through `glibc-rseq`. The macro also supplies the pinned libc++ inputs
for C; assembly inputs remain explicit where required.

`@clickhouse_sysroot` provides the target glibc headers, Linux headers,
startup objects, and link libraries that CMake used. The x86 reference uses
glibc 2.27; the LLVM module starts at 2.28 and supplies different startup
objects on both CPUs. Those differences survive stripping. LLVM's pinned
libc++ headers still come from the toolchain; `-nostdinc++` in
`ch_cc_library` prevents Clang from also searching GCC's implicit C++
directories when it selects the ClickHouse C sysroot.

`@chdig_crates//:perfetto_protos-0.51.1` also compiled using the registered
`@protobuf//:protoc` on both
[x86-64](https://app.buildbuddy.io/invocation/1b34c694-6e96-459d-a72b-b408f9796fa9)
and [ARM64](https://app.buildbuddy.io/invocation/fc2b1cb6-2efa-4451-9eab-a9a428811df6).
Both runs returned `PERFETTO_BUILD_STATUS=0`.

After using the recorded CMake compiler version in Arrow, Mongo C and Mongo
C++ generated headers, the same x86-64 hashes and `cmp=0` were verified in
[BuildBuddy 49f16579](https://app.buildbuddy.io/invocation/49f16579-c602-4eb5-8570-111469b2417a).

Moving `-O3`, `-DNDEBUG`, and `-gz=zlib` into `.bazelrc` and using
`ch_cc_library` for target-platform CPU flags and ClickHouse XRay/warnings
preserved every x86-64 hash in the table above. The full comparison in
[BuildBuddy b8794a76](https://app.buildbuddy.io/invocation/b8794a76-e24b-4778-8090-a04db10f8fa7)
([Bazel build 86eecebf](https://app.buildbuddy.io/invocation/86eecebf-47e3-4263-9f12-9f3e84f2d341))
returned `PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

Using common `--copt` for shared C/C++ flags and `-DOS_LINUX`, and
collapsing redundant CPU selections, preserved every x86-64 hash in the
table. [BuildBuddy d21148b0](https://app.buildbuddy.io/invocation/d21148b0-7ae9-4305-8e90-ba1fe0b9a758)
([Bazel build 9404f5b7](https://app.buildbuddy.io/invocation/9404f5b7-849b-4157-bb5b-6c8503712177))
returned `PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The `local_includes` and direct header/input cleanup preserved every
x86-64 hash in the table.
[BuildBuddy 3437e0b1](https://app.buildbuddy.io/invocation/3437e0b1-34fb-4abc-a822-96b01516a54a)
([Bazel build b7a16ab7](https://app.buildbuddy.io/invocation/b7a16ab7-5c75-4d7d-b581-f77247e39d6a))
returned `PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The simplified generated configuration was verified in
[BuildBuddy invocation c972df98](https://app.buildbuddy.io/invocation/c972df98-7d2d-4b65-ac42-5ebf5117b5a3)
([Bazel build](https://app.buildbuddy.io/invocation/7bbae2ed-c019-4a04-b07f-746a6570c815)).
The same compiler, full binary, plain-stripped and normalized hashes in the
table above all matched; `PARITY_STRIPPED_CMP_STATUS=0`, `PARITY_STATUS=0`.

The ARM64 execution and Cargo support preserved all x86-64 hashes in
[BuildBuddy invocation 2cc0b2e2](https://app.buildbuddy.io/invocation/2cc0b2e2-a9f5-49ea-ab98-706597095b65)
([Bazel build](https://app.buildbuddy.io/invocation/8a5fdc01-b656-4b65-8269-c064679fce4f)).
Both normalized files still passed `cmp`.

The common compile flags were moved to `.bazelrc` and removed from generated
native `cc_library.copts`. Clang C/C++ flags use `--conlyopt` and
`--cxxopt`, so PIE does not affect assembly. Cargo, NASM, and the recorded
LLVM runtime rules keep their flags because their custom actions do not read
`--copt`. ClickHouse's complete x86-64 and ARM64 Bazel binaries were unchanged
by the flag cleanup. These ARM64 checks preceded the native CMake ClickStack
and SQLConsole resource metadata update:
[x86-64 13-flag build](https://app.buildbuddy.io/invocation/fae9a5a7-fb8e-411f-bfe9-e19b133e0b51)
and [ARM64 13-flag build](https://app.buildbuddy.io/invocation/dceb77aa-cc78-42b9-8fd7-f3d8cf044972).
The x86-64 build again passed `cmp` against normalized CMake, with every
hash in the table above unchanged.

After moving C/C++ `-Werror` to `.bazelrc` and sharing the repeated
ClickHouse warning list,
[x86-64](https://app.buildbuddy.io/invocation/92c74fe8-b381-45f1-b8bb-eed31254ee80)
again preserved every compiler and binary hash in the table above and passed
`cmp` against normalized CMake (`PARITY_STRIPPED_CMP_STATUS=0`).
[ARM64](https://app.buildbuddy.io/invocation/f5560883-a6a5-40db-b2a8-2e7665b28859)
preserved the then-current complete Bazel binary hash
`da5a37c6f1f40b8026b577ba3fd36e8e26a7cc4bd3ba073e77f007077b02d52a`.
The two glibc compatibility C compilations retain their CMake `-Wno-error`
exception. Contrib `-w` stays on targets that already disable warnings.

LLVM's stage0 startup objects clear application C flags in their existing
transition. That restores the three ARM `crtbegin*.o` files to
`d923570ce208352e2b654f2784c31ff8a7e3aa4ea9b039a9f86f977850028f03`
without changing the complete ARM Bazel binary:
[ARM64 compiler, CRT and binary checks](https://app.buildbuddy.io/invocation/e24a290d-9898-43b5-9d31-2e4df4a31067).
The [x86-64 CMake comparison](https://app.buildbuddy.io/invocation/09b3baad-f52d-49c7-8332-bef11ddadeac)
again preserved all hashes in the table and passed `cmp`.

On the original Linux x86-64 reference host, run the complete audit with:

```sh
bash bazel/initial_migration/run_bazel_parity.sh
```

Use `bash bazel/initial_migration/run_bazel_parity.sh --stripped` for the same normalized
comparison against a CMake build retained on that host. Both entry points use
`bazel/initial_migration/verify_stripped_hash.py` and record plain-stripped hashes separately.

`bazel/initial_migration/run_bazel_parity.sh` verifies the saved reference hashes and metadata,
rebuilds `//bazel:cmake_toolchain_files` and confirms that Bazel selected the
recorded CMake compiler. It also compares all embedded LLVM generated headers
against the CMake headers. It then builds the Bazel executable and seven Rust
link archives, records both complete ELF hashes, the `cmp` result and all
seven native/Bazel archive hashes,
and compares the CMake File API and Ninja commands to Bazel `aquery` with
EngFlow/any2bazel. It retains the reports in the CMake build directory and
uploads the reports on a BuildBuddy hosted runner. Its status distinguishes a
binary or command difference from a failed build or failed audit.

### Linux ARM64 (native)

For ARM64, run these commands on a BuildBuddy Linux hosted runner:

```sh
bash bazel/initial_migration/run_native_reference.sh --configure-only
bash bazel/initial_migration/run_native_reference.sh
python3 bazel/initial_migration/refresh_cmake_metadata.py --arch=aarch64 --full-reference \
  --build-dir="$PWD/bazel-bin/bazel/cmake_reference_aarch64"
bash bazel/initial_migration/run_native_reference.sh --compare
```

`run_native_reference.sh` prepares the current Git checkout, its pinned submodules, and Ubuntu ARM64
tools as declared Bazel inputs. The checkout uses each Git commit's timestamp
so ClickHouse's embedded gzip resources are reproducible on another worker.
`//bazel:cmake_reference_aarch64` runs ClickHouse's original CMake on a native
ARM64 Linux BuildBuddy executor with the same source-built compiler as Bazel.
CMake builds its TableGen tools on ARM64 and the reference checks that the
compiler, Rust tools, TableGen tools, and executable are all ARM64 ELF files.
It records the original, plain-stripped, and normalized CMake hashes and the
compiler hash in `cmake-reference-summary.json`.

The native ARM64 reference inputs below were measured at source commit
`9906f9142ae9b16d8d1da7354907dd9727758dd7`, on
`refs/heads/bazel-buildbuddy`, with the Bazel changes under validation.
The source archive includes the real Git metadata read by ClickHouse CMake.
On later commits, refresh the CMake metadata before comparison as shown above;
the newly embedded Git commit can change the binary hashes. These input hashes
are recorded separately from binary validation:

| Input | SHA-256 |
| --- | --- |
| Source archive at `9906f9142` | `45fc5be13cbb7a94e4e2c0a99cd8e11a6f088c1df057af4c0cdfee0700e56cf3` |
| ARM64 tools archive | `be979691db973446e80c1ba08d4fa0843fcd64fe8a02b1bc377208b0cc69d3af` |
| Source-built ARM64 compiler | `3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f` |
| Ubuntu 22.04 OCI image index | `0a9887931b0e38af2e3487ed39e29436b0535c4f3f52e57745baaf79d1898956` |
| Ubuntu 22.04 Linux ARM64 OCI manifest | `88881eb36da489f9ff8746ece8048ca16cc94f80e8cd3ae7e8335d8822631591` |

The archive hashes were checked by
[BuildBuddy f91755ca](https://app.buildbuddy.io/invocation/f91755ca-f16c-4fc9-96ff-bf44cbe32ae5);
the compiler hash was checked in
[BuildBuddy e24a290d](https://app.buildbuddy.io/invocation/e24a290d-9898-43b5-9d31-2e4df4a31067).
The ARM image pin preserves the entire native CMake input tree: old action
`118da31bd42d0b12ca605d11c859ef8a29d6bc5f6c31651d76cb0790692afcd9`
and new action
`a8da5e91e8557207dd2a01e6ae3ea94b977d3c687d84d835a137647a91548bc0`
both use input root
`1017566ce6eaeff728f2f3a658124a66f647afdd3f4b58abfa5c0e282c48b8e7`.
This includes the native Clang, Rust, source archive, and utility archive.
These hashes do not establish binary parity.

The full native CMake reference completed in
[BuildBuddy 12df76fc](https://app.buildbuddy.io/invocation/12df76fc-3a68-46da-8af4-1e01c1dd4b4b);
its status is `{"status":0,"phase":"built","configure_only":false}`. The ARM64
comparison in [BuildBuddy 7a855e69](https://app.buildbuddy.io/invocation/7a855e69-ff30-4b33-92b0-7c61ed6b6517)
([Bazel build 77a7cbb6](https://app.buildbuddy.io/invocation/77a7cbb6-6430-42fd-8f84-cef21677368e))
measured the following hashes. Its `ClickHouseNativeLink` action
`c3edb5788f2bebe5a5c5cb321126bef5ef14addf019a94abb19672eb004efdbb`
also records the final Bazel executable's SHA-256 and size.

| ARM64 SHA-256 | CMake | Bazel | Bytes |
| --- | --- | --- | --- |
| LLVM compiler / `llvm-strip` | `3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f` | `3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f` | Both 221,377,856 |
| Original ELF | `cd204de2504bbe77c5e2b0babce701943133374e77490f36ad8b679d18b56646` | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | Both 907,067,440 |
| `llvm-strip --strip-all` | `5180f5d1ee2918cccf2e872ec9c92a2e8de8c1021e75972ad1ba2df53097b88a` | `5dbc0eee809acefa84bf3095453cbd684b5ce8803a8dee77ce19704887413c5a` | — |
| `llvm-strip --strip-all --remove-section=.note.gnu.build-id` | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | Both 637,316,696 |

The normalized copies passed `cmp`: `PARITY_STRIPPED_CMP_STATUS=0` and
`PARITY_STATUS=0`. Both are 637,316,696 bytes and match byte for byte.
Clang, Clang++, Rust, Cargo and the reference TableGen tools are native ARM64
ELFs. The original and plain-stripped binaries differ because their build IDs
still include information from discarded sections.

The common `--copt`, `-DOS_LINUX` and CPU selection cleanup preserved
every ARM64 hash in the table.
[BuildBuddy 7978c989](https://app.buildbuddy.io/invocation/7978c989-9762-46ce-96e7-956313e499fe)
([Bazel build 26aa0801](https://app.buildbuddy.io/invocation/26aa0801-f625-4be7-9cdb-f4196ebb15ab))
rechecked the native CMake reference and returned
`PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The `local_includes` and direct header/input cleanup also preserved
every ARM64 compiler, original ELF, plain-stripped and normalized hash
in the table.
[BuildBuddy 9539db98](https://app.buildbuddy.io/invocation/9539db98-6d61-483b-8796-d775d898288e)
([Bazel build 079cc9b2](https://app.buildbuddy.io/invocation/079cc9b2-600f-49c4-950a-1a18c326e531))
verified the native CMake reference and returned
`PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The subsequent `ch_cc_library` and common optimization flags also preserved
all ARM64 compiler, original ELF, plain-stripped, and normalized hashes in
the table above. The full native comparison in
[BuildBuddy fd0db6c9](https://app.buildbuddy.io/invocation/fd0db6c9-0b22-4a3e-b2cd-83a0b9a0dacc)
([Bazel build d92f2e6c](https://app.buildbuddy.io/invocation/d92f2e6c-4a81-4674-8f56-74bcf2d18f68))
reused and checked the recorded native source and tools archives, then returned
`PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

<details>
<summary>Earlier ARM64 measurements and the compiler metadata fix</summary>

Before refreshing the MongoDB compiler version, the Bazel executable in
[BuildBuddy f91755ca](https://app.buildbuddy.io/invocation/f91755ca-f16c-4fc9-96ff-bf44cbe32ae5)
had original SHA-256 `696185b7d3351b5e171cdc14917d39fa99bec07b4812445b67e01572a00d1c3b`
(907,067,440 bytes), plain-stripped SHA-256
`291a3ceb0cd0e93e8742f3a7ff8283b3475f6a0265469ac362b34e56073ca879`
(637,316,784 bytes), and normalized SHA-256
`469ba31fac67a8e3f4f5c3f48e974dcc6b09c557a1b37249a1d4a170818aed9f`
(637,316,696 bytes). This first comparison returned `PARITY_STATUS=1`.
The [ARM strip action](https://app.buildbuddy.io/invocation/f647cac4-6d5d-4feb-8ccb-41c3dc80ead5)
independently verified that earlier binary and `llvm-strip` on ARM64. Its
hosted runner retained the measured
`bazel-arm64-stripped.json` in
[BuildBuddy dbff081e](https://app.buildbuddy.io/invocation/dbff081e-ea3b-41fc-b9f6-9585da096dbd).

That earlier Bazel executable also passed a native ARM64 runtime check in
[BuildBuddy 326c1056](https://app.buildbuddy.io/invocation/326c1056-fdbd-4f24-abd2-d3a6767a7b26)
([ARM64 action](https://app.buildbuddy.io/invocation/793dae40-7204-4329-876c-8aa3ca021ef3)).
The ARM64 executor rechecked that earlier SHA-256 and byte count.
`clickhouse local --version` printed `ClickHouse local version 26.11.1.1.`,
and `clickhouse local --path=<scratch> --query='SELECT 1'` printed exactly
`1\n`; both exited 0. This check predates the final compiler-metadata fix.

The first mismatch started at byte 105,961 in `.rela.dyn`. The only
`.rodata` content differences were two MongoDB strings recording Clang
`21.1.8` instead of native CMake's `22.1.1`. All 161,684 differing
relocations referenced the same strings at addresses shifted by those
compiler strings. Mongo C, Mongo C++ and Arrow now obtain the C++ compiler
version from `bazel/initial_migration/cmake_metadata.bzl`; `bazel/initial_migration/generate_build.py` preserves
that behavior on regeneration.

The first ARM64 Bazel build completed 17,334 remote actions in
[BuildBuddy invocation 8952f682](https://app.buildbuddy.io/invocation/8952f682-f072-4073-b1e4-bb9c88abbfa2)
([Bazel build](https://app.buildbuddy.io/invocation/9da0fe08-e021-4171-8c38-b840fa023626)).
Its original executable SHA-256 was
`ccef0e7abc7d89b76b5c9b5d8680c6fefc86edc3b19d2280503e96fdfacff173`.
After refreshing the compiler, Git, and license metadata from native ARM64
CMake, and before refreshing the ClickStack and SQLConsole resource timestamps,
Bazel built the original executable
`da5a37c6f1f40b8026b577ba3fd36e8e26a7cc4bd3ba073e77f007077b02d52a`
in [BuildBuddy invocation 9124c824](https://app.buildbuddy.io/invocation/9124c824-09d1-4f34-b553-e77d79569197)
([Bazel build](https://app.buildbuddy.io/invocation/42467401-4875-450b-b712-9bf686f51fdc)).
Those two values record earlier Bazel binaries; they do not establish
stripped equality with the completed native ARM64 CMake reference.
</details>

`--compare` uses `//bazel:stripped_hash_aarch64` to verify the recorded
hashes and compare normalized copies of the actual CMake and Bazel binaries.
The CMake executable is an input only to the comparison, never to
`//:clickhouse`. The ARM64 configuration retains `common:remote` and
`--jobs=500`; CMake's Ninja build uses 12 jobs on its ARM executor.

### Embedded CMake metadata

`--//bazel:cmake_metadata` uses the values recorded by
`bazel/initial_migration/generate_build.py` or refreshed by `bazel/initial_migration/refresh_cmake_metadata.py`
from the configured CMake reference. It also maps generated source filenames
to the corresponding CMake build directory, so compiler-generated filenames
such as `__FILE__` match that reference. The reference must match the CMake
build being compared. Without the flag, `system.build_options` reports the
actual Bazel toolchain and flags, and generated filenames use Bazel directories.
The metadata generation has been executed in both modes; with the flag, its
output matches the configured CMake metadata source byte for byte.

<details>
<summary>Earlier Linux x86-64 reference and build history</summary>

These results record the earlier builds that led to the verified x86-64
normalized comparison above. Earlier unequal results are not the current
comparison result.

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
`configure-command.json`; `bash bazel/initial_migration/run_cmake_reference.sh` reproduces
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

The next build replaced six imported bootstrap archives with the exact CMake
`cxx`, `cxxabi`, `unwind`, `clang_rt_builtins`, `clang_rt_xray`, and
`libllvmlibc` compile groups. The focused
[Bazel runtime build](https://app.buildbuddy.io/invocation/48f1710d-578c-4a16-b7d9-074f65a06c9a)
succeeded with 364 remote compiles. Its
[command audit](https://app.buildbuddy.io/invocation/8d49c7d6-3122-45c6-a2c0-5f21f6c65f03)
verified all six CMake source counts, both selected compiler SHA-256 hashes,
and absence of bootstrap libc++ include directories in those compile actions.
The 125 AArch64 outline-atomic sources are generated from the `lse.S`
symlinks verified against the saved Ninja commands. That check validated runtime
inputs and did not compare the complete executable.

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
with 45 differing table entries or contents; that build did not reproduce the
CMake binary.

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

The first plain-stripped comparison on that PR head was
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

The selected compiler still matched `52a7d0fd…6262ce`. This earlier
plain-stripped hash differed from the reference hash. The current normalized
comparison and its reproduction commands are recorded above.

</details>


### Native build and hash verification for Bazel snapshot `c607864d1`

Both architectures passed the 43 native targets, including the registered SQL and CRC32C checks. Raw and normalized binary SHA256 values and lengths match the native CMake references. This is a native build and hash verification only. DWYU is still a work in progress: scoped CPP and full-production CPP have not qualified. Full-census llvm-nm controls have not qualified. No failed CPP preprocessing action is counted as a success.

The original collector rejected `source_stages_incomplete: CPP/native source overlay differs in baseline_native`: it compared the 43-target build against the earlier 41-target fixture. A separate replay authenticated the originally submitted 43-target SQL/CRC fixture, both original Runs and their native status artifacts. The original rejection and strict CPP qualification remain unchanged. The CPP numbers below are diagnostic counts from the original scoped stage reports, not a CPP or whole-repository qualification.

| CPU | Run submitted | BuildBuddy | Raw SHA256 (bytes) | Normalized SHA256 (bytes) | Remaining CPP diagnostic |
| --- | --- | --- | --- | --- | --- |
| x86-64 | 2026-10-22 18:13:45 UTC | [8388606c](https://app.buildbuddy.io/invocation/8388606c-bc18-4105-bf85-cffd5a6fa0bc) | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` (929619680 B) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` (710693736 B) | 2551 failed CPP preprocess actions on 14 reported owners; 222 rows with findings |
| ARM64 | 2026-10-22 18:13:42 UTC | [6921aee2](https://app.buildbuddy.io/invocation/6921aee2-d0d4-4f38-b009-1fcd24086a16) | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` (907067440 B) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` (637316696 B) | 2557 failed CPP preprocess actions on 15 reported owners; 211 rows with findings |


### Native ClickHouse verification for Bazel snapshot `f8a1ee593`

The native Bazel build on each CPU completed all 43 selected targets in both baseline and candidate. The cleanup, Boost, post-census, benchmark, registered SQL and CRC32C status files were checked against the original successful native child invocations. Raw and normalized SHA256 values and sizes match the native CMake references below. Binary and library downloads were 0 B.

The parent Run status is reported separately. The f8 producer collector failed after the native build because an original configured-query JSON exceeded its 64 MiB file limit. In the submitted script, the producer collector ran inside the candidate-native marker, so a producer failure suppressed the later marker. A separate reader authenticated the complete original logs and submitted scripts and verified the native children independently. It did not mark the failed parent Runs successful.

DWYU, LLVM and protobuf producer controls, ordinary producer linking, full-census llvm-nm, and scoped or full-production CPP remain unqualified. No failed action is cached as success.

| CPU | Run submitted | BuildBuddy | Parent status | Raw SHA256 (bytes) | Normalized SHA256 (bytes) |
| --- | --- | --- | --- | --- | --- |
| x86-64 | 2026-10-22 19:48:58 UTC | [4bb0356c](https://app.buildbuddy.io/invocation/4bb0356c-9977-43aa-9282-9bce8cefdfc2) | Failed (Failed) | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` (929619680 B) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` (710693736 B) |
| ARM64 | 2026-10-22 19:49:01 UTC | [c446f1ab](https://app.buildbuddy.io/invocation/c446f1ab-007b-4dda-ac35-9ffe6db0bbd1) | Failed (Failed) | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` (907067440 B) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` (637316696 B) |

## 234b7aab0

Bazel source `234b7aab0` was published in [`d0eb31f46`](https://github.com/dzbarsky/clickhouse/commit/d0eb31f4604052ebca55716ba405f8cfbb53b7e0). Its two native builds completed all 51 requested targets and linked `//:clickhouse` and `//src:unit_tests_dbms`. On each CPU, the original test reports confirm that the selected 26 GoogleTests and six SQL cases passed. The full unit test suite was not run. Separate compile checks built the five WebUI objects for `//src:dbms_embedded`.

| Native Bazel build | Normalized SHA-256 (matches CMake) | Bytes |
| --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/4e5a002f-86ec-4495-a0f5-8277177058dc) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710693736 |
| [ARM64](https://app.buildbuddy.io/invocation/b4894636-e1ad-4524-8d05-c18b179abcc7) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637316696 |

The [x86-64](https://app.buildbuddy.io/invocation/7f45e761-87d8-4a4b-8ada-9c12f9c121a9) and [ARM64](https://app.buildbuddy.io/invocation/9cbfef68-d0ce-41f1-9ae6-0b1b8b4adc56) workflows also ran DWYU. The audit reported missing or unused dependencies on 45 non-fixture targets for x86-64 and 41 for ARM64.

## 292683117

The [x86-64](https://app.buildbuddy.io/invocation/08450517-5bbd-4115-9cba-dac4b3fa6f43) and [ARM64](https://app.buildbuddy.io/invocation/701aee05-1018-4c83-9abe-742e08044df3) builds stopped while Bazel was applying the protobuf patch. Neither build produced a ClickHouse binary or a new hash. Source `d67a4cc67` adds the missing Git file headers to the patch.

## d67a4cc67

| Native Bazel build | Normalized SHA-256 (matches CMake) | Bytes |
| --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/995630b4-f956-477e-84cb-fe0b65fa4d8a) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710693736 |
| [ARM64](https://app.buildbuddy.io/invocation/41a64355-fe01-437b-b673-5408b139a0d7) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637316696 |

The BuildBuddy workflows for [x86-64](https://app.buildbuddy.io/invocation/408c3f79-50e3-43ed-8b4b-4a8c908aa3e4) and [ARM64](https://app.buildbuddy.io/invocation/d733c8c0-7aa0-4711-b55b-c046eaa26eb2) reported failure after Bazel exited successfully. The result reader expected `boost_mysql_compiled` to use the executable’s configuration; Bazel retained its test configuration because the target declares `testonly = True`. The build events and test reports from the two Bazel invocations above confirm the results independently of that reader.

Both native Bazel invocations built all 55 requested targets. Their default output groups contain the six embedded object files, the Boost.MySQL static archive, and two CcInfo reports for `//src:dbms_embedded` and `//src:clickhouse_common_io_embedded`. The selected 26 GoogleTests and six SQL cases passed on each CPU. These builds did not run DWYU.

The direct dependencies on `//src:dbms_embedded` decreased from 135 to 114, and those on `//src:clickhouse_common_io_embedded` from 44 to 39. On both CPUs, the two CcInfo comparisons found identical ordered compilation and linking fields before and after those removals.

## 4e214b47a

Bazel source `4e214b47a` built all 57 requested targets on each CPU. The normalized ClickHouse ELF matched the SHA-256 and size from native CMake:

| Native Bazel build | Normalized SHA-256 | Bytes |
| --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/50d792e9-3a3b-4916-8844-1a1d563162ce) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
| [ARM64](https://app.buildbuddy.io/invocation/88071a32-86d0-406b-b5d4-6dcb6ed6702e) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |

Both the raw ClickHouse ELF and `//src:unit_tests_dbms` also matched the [previous published build](https://github.com/dzbarsky/clickhouse/commit/a67536970816be10181b3054b25178ac3d0f026c). Each CPU passed 38 GoogleTests and six SQL cases. The comparison used BuildBuddy output metadata without downloading the executables.

The [x86-64 parent](https://app.buildbuddy.io/invocation/f9488607-2e75-4e91-81d6-c63ecafc03cc) and [ARM64 parent](https://app.buildbuddy.io/invocation/829eaf8e-97c2-49a6-8773-bee434d177a7) both failed after their native Bazel children passed. Python 3.8 rejected `zip(..., strict=True)` in the worker's `emit_local_originals` function. The linked child invocations provide the original configured targets and output SHA-256 values. The native child durations were 420.9 s on x86-64 and 491.8 s on ARM64.

Restoring the JSONPath include order fixed the earlier unit-test ELF mismatch. All seven JSONPath objects on each CPU now match their inputs to the previous published unit-test link. The configured output groups also verified six embedded C++ objects and the Boost.MySQL archive per CPU. The current CcInfo JSON files matched their FileWrite digests. All 17 ordered compilation and linking fields matched when comparing 134 versus 113 direct dependencies for `//src:dbms_embedded` and 44 versus 39 for `//src:clickhouse_common_io_embedded`. The actual source change removed one DBMS dependency (114 to 113); the 134-entry reference separately checks 21 prior removals.

The five original status artifacts per CPU verified 26 prior GoogleTests, seven MergeTree tests, five header-ownership tests, and six SQL cases. The DWYU command never started: zero of 784 declared production owners ran on either CPU, and neither DWYU control ran. Prior standalone-header failures, direct dependency findings, and unverified textual, generated, and Rust dependencies remain outstanding. The generator reproduced the changed owners in two passes and Buildifier accepted all 300 BUILD files; the full dependency replay retains one inherited difference in the unchanged `src/Storages/System/BUILD.bazel`.

## 541b6bcee

Bazel source [541b6bcee](https://github.com/dzbarsky/clickhouse/commit/541b6bcee6a6d7e0913765d1aeec12a6cb9dc46c) moved 197 headers from `//src:headers` to their compiled ClickHouse libraries and removed 104 `llvm/lib/` headers from `@clickhouse_llvm_project//:llvm_headers`. Buildifier accepted 300 BUILD files. Running the dependency rewrites twice left all 300 files unchanged. The ClickHouse and LLVM generators were also replayed against the recorded native CMake inputs for both architectures.

All 57 requested native Bazel targets built on each CPU. The normalized ELF matched native CMake; the raw ClickHouse and unit-test ELFs matched [963d3f431](https://github.com/dzbarsky/clickhouse/commit/963d3f43154b2583088dd0aa5f07ebdbf4c4d567).

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/9ddbebcc-4f7c-4767-9e4c-64391a97d921) | ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
| [x86-64](https://app.buildbuddy.io/invocation/9ddbebcc-4f7c-4767-9e4c-64391a97d921) | ClickHouse, normalized | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
| [x86-64](https://app.buildbuddy.io/invocation/9ddbebcc-4f7c-4767-9e4c-64391a97d921) | unit_tests_dbms | `bc523ef4436f034fe89df4a85f84b6056aa61b54825fe405bc51f7a9f5683398` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/87d17df9-724a-41c6-9d2c-f6ea5a5ec642) | ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
| [ARM64](https://app.buildbuddy.io/invocation/87d17df9-724a-41c6-9d2c-f6ea5a5ec642) | ClickHouse, normalized | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
| [ARM64](https://app.buildbuddy.io/invocation/87d17df9-724a-41c6-9d2c-f6ea5a5ec642) | unit_tests_dbms | `24603ddfc53c3eae1efbcea14eaab78ead22de00f2195aa345c9ec1b9df2f4a8` | 795,060,896 |

BuildBuddy’s configured outputs verified six embedded object files and the Boost.MySQL archive per CPU. For `//src:dbms_embedded` and `//src:clickhouse_common_io_embedded`, all 17 ordered compilation and linking fields of `CcInfo` matched between the reference and reduced dependency lists (134 versus 113 and 44 versus 39, respectively). Each CPU passed 26 selected GoogleTests, seven MergeTree GoogleTests, five header-ownership GoogleTests, and six SQL cases.

The [x86-64 parent](https://app.buildbuddy.io/invocation/e4b548f5-8428-49b8-8678-6dd57c676b13) and [ARM64 parent](https://app.buildbuddy.io/invocation/14a0b6ef-3744-4e63-845a-3f6cbbcb8a8f) both failed after the native builds and all eight native output checks succeeded. The focused DWYU worker exited 2 with `ValueError: ordinary actions refer to unrequested owner/configuration` while mapping Bazel compile actions to the requested targets. Neither the ordinary DWYU build nor the strict DWYU build started. Of the 119 x86-64 and 117 ARM64 production targets selected from 784 per architecture, zero received a DWYU verdict. The workers reported no dependency findings and did not run the controls. Earlier dependency findings remain unresolved.

## ea022a835

[ea022a835](https://github.com/dzbarsky/clickhouse/commit/ea022a835034c61e89eb4c0170570bd6a9e23990) moved 17 DBMS and eight Common I/O headers from `//src:headers` to the libraries that compile their implementations. The Linux Boost declarations omitted 33 headers for other operating systems, classified eight MPL fragments as textual headers, and exposed two Boost.Config helpers. The Linux LLVM declarations omitted the z/OS `string.h` wrapper. The DWYU auditor now associates requested targets with their own configured actions. No application source files changed.

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/33948636-32f4-4285-9848-f0b102d250e8) | ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
| [x86-64](https://app.buildbuddy.io/invocation/33948636-32f4-4285-9848-f0b102d250e8) | ClickHouse, normalized | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
| [x86-64](https://app.buildbuddy.io/invocation/33948636-32f4-4285-9848-f0b102d250e8) | unit_tests_dbms | `bc523ef4436f034fe89df4a85f84b6056aa61b54825fe405bc51f7a9f5683398` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/ae47daf3-e35a-435d-9b62-bbe6e3b5a6af) | ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
| [ARM64](https://app.buildbuddy.io/invocation/ae47daf3-e35a-435d-9b62-bbe6e3b5a6af) | ClickHouse, normalized | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
| [ARM64](https://app.buildbuddy.io/invocation/ae47daf3-e35a-435d-9b62-bbe6e3b5a6af) | unit_tests_dbms | `24603ddfc53c3eae1efbcea14eaab78ead22de00f2195aa345c9ec1b9df2f4a8` | 795,060,896 |

Each CPU completed all 58 requested native targets and all nine native output checks. The normalized ELF matched native CMake; both the raw ClickHouse and unit-test ELFs matched [541b6bcee](https://github.com/dzbarsky/clickhouse/commit/541b6bcee6a6d7e0913765d1aeec12a6cb9dc46c). Each CPU passed 26 selected GoogleTests, seven MergeTree GoogleTests, five header-ownership GoogleTests, and six SQL cases. BuildBuddy verified the expected Boost header membership and all 17 ordered `CcInfo` fields for the DBMS and Common I/O embedded libraries.

[x86-64 hosted run](https://app.buildbuddy.io/invocation/0bd95668-6bc8-417a-a8f4-502d7fde62fe): [ordinary DWYU](https://app.buildbuddy.io/invocation/1e3616d7-6f46-4b60-866b-0d2a53395e3c) (Bazel exit 0) and [strict DWYU](https://app.buildbuddy.io/invocation/85513c6d-89e7-44e1-be33-10fe31efbca3) (Bazel exit 1). The scoped worker reported `findings_or_unverified`. Of 784 production targets, 123 were selected and 661 omitted.
Production outcomes: 17 `clean`, 4 `failed`, 19 `findings`, 6 `header_pp_not_verified`, 1 `link_extra_lib_only`, 9 `no_cpp`, 2 `unentered_owned_files`, 65 `unverified_link_deps`. Control fixture outcomes: 1 `findings`.
The recorded outcomes for the three changed header owners were `//src:clickhouse_common_io__compile_0` `findings`, `//src:dbms__compile_0` `findings`, `//src:headers` `failed`.
The auditor reported actionable dependency findings for `//programs/keeper-client:clickhouse-keeper-client-lib`, `//programs/keeper-converter:clickhouse-keeper-converter-lib`, `//programs/packed-io:clickhouse-packed-io-lib`, `//src/AggregateFunctions:clickhouse_aggregate_functions__compile_1`, `//src/Common/mysqlxx:mysqlxx`, `//src/Functions/GatherUtils:clickhouse_functions_gatherutils`, `//src/Functions/array:clickhouse_functions_array__compile_0`, `//src/Functions:clickhouse_functions_obj__compile_1`, `//src/Functions:clickhouse_functions_obj__compile_2`, `//src/Storages/System:clickhouse_storages_system__compile_1`, `//src/Storages/System:clickhouse_storages_system_information_schema`, `//src/Storages/System:headers`, `//src/TableFunctions:clickhouse_table_functions`, `//src:benchmark_runtime_filter_objects`, `//src:clickhouse_common_io__compile_0`, `//src:dbms__compile_0`, `//src:dbms__compile_4`, `//src:dbms_embedded`, `//src:unit_tests_dbms`.

[ARM64 hosted run](https://app.buildbuddy.io/invocation/bf7a9f58-f6cc-4840-9df9-f2b170d12723): [ordinary DWYU](https://app.buildbuddy.io/invocation/7bcabbbf-fb6c-4aa1-b7cf-d901b70cd1cf) (Bazel exit 0) and [strict DWYU](https://app.buildbuddy.io/invocation/bbb14870-ccec-444c-a42a-5a65372880ec) (Bazel exit 1). The scoped worker reported `findings_or_unverified`. Of 784 production targets, 121 were selected and 663 omitted.
Production outcomes: 14 `clean`, 4 `failed`, 17 `findings`, 6 `header_pp_not_verified`, 1 `link_extra_lib_only`, 12 `no_cpp`, 2 `unentered_owned_files`, 65 `unverified_link_deps`. Control fixture outcomes: 1 `findings`.
The recorded outcomes for the three changed header owners were `//src:clickhouse_common_io__compile_0` `findings`, `//src:dbms__compile_0` `findings`, `//src:headers` `failed`.
The auditor reported actionable dependency findings for `//programs/keeper-client:clickhouse-keeper-client-lib`, `//programs/keeper-converter:clickhouse-keeper-converter-lib`, `//programs/packed-io:clickhouse-packed-io-lib`, `//src/Common/mysqlxx:mysqlxx`, `//src/Functions/GatherUtils:clickhouse_functions_gatherutils`, `//src/Functions/array:clickhouse_functions_array__compile_0`, `//src/Functions:clickhouse_functions_obj__compile_1`, `//src/Storages/System:clickhouse_storages_system__compile_1`, `//src/Storages/System:clickhouse_storages_system_information_schema`, `//src/Storages/System:headers`, `//src/TableFunctions:clickhouse_table_functions`, `//src:benchmark_runtime_filter_objects`, `//src:clickhouse_common_io__compile_0`, `//src:dbms__compile_0`, `//src:dbms__compile_3`, `//src:dbms_embedded`, `//src:unit_tests_dbms`.

## 698fea052

Source: [698fea052](https://github.com/dzbarsky/clickhouse/commit/698fea052686835908931fce25669b703b220b84). DBMS and Common I/O now publish their own 21 and 6 headers, respectively. The LZMA buffer headers use Common I/O’s existing public `@clickhouse_xz//:_liblzma` dependency. `LocalTime.h` and `Logger_fwd.h` now belong to `//src/Common:parser_support_api`. Azure, DBMS and the unit tests drop duplicate libxml2, HDFS3 and LZ4 dependencies. 11 first-party targets now declare the dependencies used by their public headers or implementations. Boost classifies 25 FunctionTypes and MultiIndex fragments as textual headers and drops 2 Wave templates. The Rust `cc_import` audit reads `unentered_owned_files` as a filename-to-visibility map and accepts a missing `unverified_link_deps` field as an empty list. The commit changes only BUILD files and migration scripts or templates.

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/a9e28792-5995-4653-8a89-5b83662a61da) | ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
|  | ClickHouse, normalized | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
|  | `unit_tests_dbms` | `bc523ef4436f034fe89df4a85f84b6056aa61b54825fe405bc51f7a9f5683398` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/f72461ad-2f14-4662-8c8a-b1451e6fd30b) | ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
|  | ClickHouse, normalized | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
|  | `unit_tests_dbms` | `24603ddfc53c3eae1efbcea14eaab78ead22de00f2195aa345c9ec1b9df2f4a8` | 795,060,896 |

All 58 native Bazel targets passed on each CPU. The normalized ClickHouse ELF matched native CMake; the raw ClickHouse and unit-test ELFs matched [ea022a835](https://github.com/dzbarsky/clickhouse/commit/ea022a835034c61e89eb4c0170570bd6a9e23990). Each CPU passed the same 26 selected, seven MergeTree and five header-ownership GoogleTests, plus the same six SQL cases. The two dependency lists compared for DBMS and Common I/O produced equal values for all 17 ordered CcInfo fields. The configured Boost provider report matched all 12 expected fields on both CPUs.

| Hosted run | Hosted status | Ordinary DWYU build | Strict DWYU build |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/5482e71d-a45e-45c3-a329-1590e84ce16a) | `Failed` | [ordinary DWYU](https://app.buildbuddy.io/invocation/8746a52b-cfb6-4417-9775-7bbe752b0b60) (exit 0) | [strict DWYU](https://app.buildbuddy.io/invocation/6a833bd4-047b-4bfa-880d-0a8cae0322a7) (exit 1) |
| [ARM64](https://app.buildbuddy.io/invocation/531aa66e-0aaa-410f-b5f9-3b96aafd2b17) | `Failed` | [ordinary DWYU](https://app.buildbuddy.io/invocation/fbca6e12-a89d-4168-8438-735163fbdf39) (exit 0) | [strict DWYU](https://app.buildbuddy.io/invocation/af1abe63-88d9-4b6b-9c7e-be8fa30e8718) (exit 1) |

| Production DWYU | Selected | Compiled clean | Findings | Failed | Other outcomes | Selected without verified records | Omitted |
| --- | --- | --- | --- | --- | --- | --- | --- |
| x86-64 | 42 | 4 | 9 | 4 | 25 | 0 | 742 |
| ARM64 | 40 | 4 | 7 | 4 | 25 | 0 | 744 |

x86-64 other outcomes: 1 `header_pp_not_verified`, 1 `link_extra_lib_only`, 1 `unentered_owned_files`, 22 `unverified_link_deps`. Poco JSON passed; the intentionally negative fixture reported its unused implementation dependency. The 11 first-party targets selected for changes to header owners or dependency visibility reported 1 `findings`, 10 `unverified_link_deps`.

ARM64 other outcomes: 1 `header_pp_not_verified`, 1 `link_extra_lib_only`, 1 `unentered_owned_files`, 22 `unverified_link_deps`. Poco JSON passed; the intentionally negative fixture reported its unused implementation dependency. The 11 first-party targets selected for changes to header owners or dependency visibility reported 1 `findings`, 10 `unverified_link_deps`.

Rust import reports for C++ targets: x86-64 0 of 5 reports verified (`observed`; 5 `held`); ARM64 0 of 5 reports verified (`observed`; 5 `held`). x86-64 produced 5 reports with no import findings whose C++ owners failed strict DWYU; ARM64 produced 5 reports with no import findings whose C++ owners failed strict DWYU. These reports do not qualify any Rust target or confer a compiled DWYU pass.

An omitted target has no DWYU result for this source. Only the compiled clean column counts a successful compile and strict dependency report.

## ab0569eff

Source: [ab0569eff](https://github.com/dzbarsky/clickhouse/commit/ab0569eff320a3f4676bc2c6645f4e845e2d60b0). Eleven Dictionary implementation headers move from public header lists into the source lists of the compile groups that read them. Abseil Status owns optional_ref.h; CRoaring's roaring.c owns its forced alignment header. These changes remove two header libraries. ISA-L's C compiler no longer receives libc++ ABI headers. The unused Bazel header library and its migration declarations are removed. DBMS embedded and AggregateFunctions drop four and five redundant dependencies, respectively. The commit changes only BUILD files and migration scripts or templates.

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/2cc9151c-2416-4150-a2f3-fe1070dad84b) | ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
|  | ClickHouse, normalized | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
|  | `unit_tests_dbms` | `bc523ef4436f034fe89df4a85f84b6056aa61b54825fe405bc51f7a9f5683398` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/13f1da26-75a7-417a-9d98-2781c93a86d3) | ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
|  | ClickHouse, normalized | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
|  | `unit_tests_dbms` | `24603ddfc53c3eae1efbcea14eaab78ead22de00f2195aa345c9ec1b9df2f4a8` | 795,060,896 |

All 59 native Bazel targets passed on each CPU. The normalized ClickHouse ELF matched native CMake; the raw ClickHouse and unit-test ELFs matched [698fea052](https://github.com/dzbarsky/clickhouse/commit/698fea052686835908931fce25669b703b220b84). Each CPU passed the same 26 selected, seven MergeTree and five header-ownership GoogleTests, plus the same six SQL cases. The two dependency lists compared for DBMS and Common I/O produced equal values for all 17 ordered CcInfo fields. The configured Boost provider report matched all 12 expected fields on both CPUs. In each native CRoaring archive, roaring_bitmap_and_cardinality resides in an executable section aligned to at least 4096 bytes, at an offset divisible by 4096.

| Hosted run | Hosted status | Ordinary DWYU build | Strict DWYU build |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/ae9d0a52-ee8b-417a-87a7-b07c60efa4d9) | `Failed` | [ordinary DWYU](https://app.buildbuddy.io/invocation/87ed04b2-994c-479e-9f17-020e3ebf04b7) (exit 0) | [strict DWYU](https://app.buildbuddy.io/invocation/3a018e8d-e1cc-447f-bda6-e636e607e366) (exit 1) |
| [ARM64](https://app.buildbuddy.io/invocation/e3797e46-403f-4354-a66e-295184fc5d73) | `Failed` | [ordinary DWYU](https://app.buildbuddy.io/invocation/888b85ea-967e-47ac-821b-299848e625d6) (exit 0) | [strict DWYU](https://app.buildbuddy.io/invocation/e3d2dc45-f505-4091-b210-949b6eb65f29) (exit 1) |

| Production DWYU | Selected | Compiled clean | Findings | Failed | Other outcomes | Selected without verified records | Omitted |
| --- | --- | --- | --- | --- | --- | --- | --- |
| x86-64 | 23 | 4 | 13 | 4 | 2 | 0 | 758 |
| ARM64 | 19 | 4 | 11 | 4 | 0 | 0 | 762 |

x86-64 other outcomes: 1 `unchecked_link_inputs`, 1 `unentered_owned_files`. Poco JSON passed; the intentionally negative fixture reported its unused implementation dependency.

ARM64 other outcomes: none. Poco JSON passed; the intentionally negative fixture reported its unused implementation dependency.

An omitted target has no DWYU result for this source. Only the compiled clean column counts a successful compile and strict dependency report.

## e10eaf426

Source: [e10eaf426](https://github.com/dzbarsky/clickhouse/commit/e10eaf426d559c60ad5be706011e95fe97be5d6f). All seven changes are in the BUILD files and migration scripts. No application source changed.

- OpenLDAP keeps 24 internal header inputs in the two libraries that compile them. `@clickhouse_openldap//:_lber` and `@clickhouse_openldap//:_ldap` each expose six public headers.
- `//programs/keeper-client:clickhouse-keeper-client-lib` declares its implementation dependency on `//src:dbms__compile_0`.
- `//src:benchmark_runtime_filter_objects` removes unused direct ICU data, libcpuid and simdcomp dependencies.
- `//src:dbms__compile_0` owns `StorageSet.h`, `StorageJoin.h`, `StorageSQLite.h`; the declarations also expose the SQLite headers used by `StorageSQLite.h`.
- `//src:dbms__compile_0` owns four PostgreSQL headers under `src/Core/PostgreSQL` and `src/Processors/Sources/PostgreSQLSource.h`. The dictionary implementation depends on that owner.
- `src/Dictionaries/BUILD.bazel` moves eight implementation dependencies out of public `deps` and removes the unused sparsehash dependency from `//src/Dictionaries:headers`.
- `//src/Common:int_exp` compiles `intExp.cpp` and publishes `intExp.h` and `exp10_i32.h`. The CMake link plan keeps the object at member 218 (zero-based) of `src/libclickhouse_common_io.a`.

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/18572b65-8f54-42aa-89e0-1f200cadc4b6) | Normalized ClickHouse | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
|  | Raw ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
|  | `unit_tests_dbms` | `2a6934b1d4523406f0c1e45b68b515914ba14a4167ffb6c05c0668a5ffb11260` | 811,968,280 |
|  | `unit_tests_dbms` at [ab0569eff](https://github.com/dzbarsky/clickhouse/commit/ab0569eff320a3f4676bc2c6645f4e845e2d60b0) | `bc523ef4436f034fe89df4a85f84b6056aa61b54825fe405bc51f7a9f5683398` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/f1a0f860-b020-4631-89cd-43e746bbd81e) | Normalized ClickHouse | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
|  | Raw ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
|  | `unit_tests_dbms` | `d489d1b0fd52b57389f107e5650cb58c5f6a33689d9c6090a5198cf0bdef2176` | 795,060,896 |
|  | `unit_tests_dbms` at [ab0569eff](https://github.com/dzbarsky/clickhouse/commit/ab0569eff320a3f4676bc2c6645f4e845e2d60b0) | `24603ddfc53c3eae1efbcea14eaab78ead22de00f2195aa345c9ec1b9df2f4a8` | 795,060,896 |

Each native build completed the same 52 build targets, three measurement targets and four verification targets as [ab0569eff](https://github.com/dzbarsky/clickhouse/commit/ab0569eff320a3f4676bc2c6645f4e845e2d60b0). The normalized ClickHouse executable matched the native CMake build on each CPU; the raw ClickHouse executable matched the previous Bazel build. The unit-test ELF changed on x86-64 and ARM64. The original compiler and linker records verified that the same `intExp.pic.o` moved in the unit-test link.

x86-64 `unit_tests_dbms` changed from `bc523ef4436f034fe89df4a85f84b6056aa61b54825fe405bc51f7a9f5683398` (811,968,280 bytes) to `2a6934b1d4523406f0c1e45b68b515914ba14a4167ffb6c05c0668a5ffb11260` (811,968,280 bytes). The original `CppCompile` actions produced the same `intExp.pic.o` (`90e7e8b2cfdc8910f00023141c089be71cafc3e48e590b551bef0f8d229eb346`, 12,248 bytes). Its zero-based position in the `CppLink` response changed from 7610 to 9998 among 16,293 objects. The other ordered response arguments and other linker inputs matched.

ARM64 `unit_tests_dbms` changed from `24603ddfc53c3eae1efbcea14eaab78ead22de00f2195aa345c9ec1b9df2f4a8` (795,060,896 bytes) to `d489d1b0fd52b57389f107e5650cb58c5f6a33689d9c6090a5198cf0bdef2176` (795,060,896 bytes). The original `CppCompile` actions produced the same `intExp.pic.o` (`cc3fe719d6681a5491378635ad9e70e6ac118c39fe65e75007604fbb8b9937c9`, 4,376 bytes). Its zero-based position in the `CppLink` response changed from 7591 to 9967 among 16,221 objects. The other ordered response arguments and other linker inputs matched.

Each CPU passed 26 selected GoogleTests, seven MergeTree GoogleTests and five header-ownership GoogleTests. The six SQL cases were `gather_arrays`, `table_format`, `time_series_range`, `time_series_tags`, `transposed_distance`, `vector_array`. A separate SELECT in the same SQL action checked the positive and negative overflow boundary for Decimal32, Decimal64, Decimal128 and Decimal256 (16 assertions, one result row).

The original unit comparison rejected the removed compiler argument `-Ibazel-out/k8-opt/bin/bazel/_virtual_includes/clickhouse_config_headers`. A separate check compared the original and current compile commands, action results, complete linker response files and linker input directories on both CPUs. The removed include option did not change the object bytes. The database unit-test executable was compared with the previous Bazel executable; CMake supplied the reference for normalized ClickHouse.

| CPU | CcInfo target | Direct dependency counts compared |
| --- | --- | --- |
| x86-64 | `//src:clickhouse_common_io_embedded` | 44 → 39 |
| x86-64 | `//src:dbms_embedded` | 134 → 113; 113 → 109 |
| ARM64 | `//src:clickhouse_common_io_embedded` | 44 → 39 |
| ARM64 | `//src:dbms_embedded` | 134 → 113; 113 → 109 |

The two CcInfo reports on each CPU found all 17 ordered fields equal between the dependency lists tested at `e10eaf426`. Their separate comparison to `ab0569eff` returned `other_difference`: it predicted one new system include directory and measured three. For `//src:clickhouse_common_io_embedded` the count was 195 → 198 (196 expected); for `//src:dbms_embedded` it was 309 → 312 (310 expected). Both original classifications remain recorded. The Boost report matched all 12 fields, and CRoaring passed the 4,096-byte alignment check on both CPUs.

The CcInfo reports explicitly name the virtual include directory for `//src/Common:int_exp`. The OpenLDAP BUILD file and the pinned `rules_cc` rule account for the other two entries: `external/+contrib+clickhouse_openldap/include` and its generated counterpart under `bazel-out/k8-opt/bin`. The original aggregate reports recorded counts, not those two directory names. The actual compiler commands for the unchanged `programs/packed-io/PackedIO.cpp` used both directories on both CPUs. On x86-64 its system include count changed from 301 to 304, adding just those two directories and the int_exp virtual directory. On ARM64 it already had both OpenLDAP directories and changed from 316 to 317.

DWYU ran for the following selection. A compiled target counts as clean only when its ordinary compile and strict dependency report succeeded.

| CPU | Native build | Ordinary DWYU | Strict DWYU | Parent exit | Selected | Declared | Unverified outside selection |
| --- | --- | --- | --- | --- | --- | --- | --- |
| x86-64 | [Native](https://app.buildbuddy.io/invocation/18572b65-8f54-42aa-89e0-1f200cadc4b6) | [Ordinary](https://app.buildbuddy.io/invocation/170dde4d-1113-4ccd-bd11-9ebc0fa83d3e) | [Strict](https://app.buildbuddy.io/invocation/aab77ae1-eac9-4c3b-84a7-c9cd24b47b0d) | [Failed](https://app.buildbuddy.io/invocation/d9569984-351b-4afd-8af2-f3a3f52de0b9) | 28 | 782 | 754 |
| ARM64 | [Native](https://app.buildbuddy.io/invocation/f1a0f860-b020-4631-89cd-43e746bbd81e) | [Ordinary](https://app.buildbuddy.io/invocation/fce10d96-2952-4f65-a48b-330a7f15f82b) | [Strict](https://app.buildbuddy.io/invocation/6b3d7d6b-c194-446b-886c-e887b169feab) | [Failed](https://app.buildbuddy.io/invocation/e9e9f309-9330-4c39-bfef-60a0f7ac3bd1) | 26 | 782 | 756 |

| CPU | Production class | Selected | `clean` | `failed` | `findings` | `header_pp_not_verified` | `no_cpp` | `unverified_link_deps` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| x86-64 | `compiled_native` | 20 | 2 | 0 | 9 | 0 | 0 | 9 |
| x86-64 | `header_only_native` | 7 | 0 | 4 | 0 | 2 | 0 | 1 |
| x86-64 | `native_forwarder_no_sources` | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| ARM64 | `compiled_native` | 18 | 2 | 0 | 7 | 0 | 0 | 9 |
| ARM64 | `header_only_native` | 7 | 0 | 4 | 0 | 2 | 0 | 1 |
| ARM64 | `native_forwarder_no_sources` | 1 | 0 | 0 | 0 | 0 | 1 | 0 |

| CPU | Fixture | Class | Outcome |
| --- | --- | --- | --- |
| x86-64 | `//bazel/initial_migration/dwyu_regressions/guarded_toolchain:negative_duplicate_i_system` | `compiled_native` | `findings` |
| ARM64 | `//bazel/initial_migration/dwyu_regressions/guarded_toolchain:negative_duplicate_i_system` | `compiled_native` | `findings` |

`negative_duplicate_i_system` reported exactly one unused implementation dependency, `@@//bazel/initial_migration/dwyu_regressions/guarded_toolchain:shadow`.

Distinct failed `DwyuCompilerPreprocess` outputs in each selected strict run: x86-64: 545 (previous selection: 602); ARM64: 545 (previous selection: 602). The configured PostgreSQL report identified all five public headers; it did not execute a separate compiler action per header. The two commits select different targets. These counts do not measure the entire repository. Nonselected labels remain unverified.

## 93d4235b8

[93d4235b8](https://github.com/dzbarsky/clickhouse/commit/93d4235b8ae2c096a83af390ad69868b28603c4a): `Reduce ClickHouse compile dependencies and ISA-L assembler inputs`. The raw ClickHouse and `unit_tests_dbms` executables matched [e10eaf426](https://github.com/dzbarsky/clickhouse/commit/e10eaf426d559c60ad5be706011e95fe97be5d6f). Each CPU passed 38 GoogleTests, six SQL cases and 16 Decimal boundary checks. The hashes and sizes come from BuildBuddy output metadata.

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/7d1bb0b6-8913-4a93-bd56-99ad65359abe) | Normalized ClickHouse | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
|  | Raw ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
|  | `unit_tests_dbms` | `2a6934b1d4523406f0c1e45b68b515914ba14a4167ffb6c05c0668a5ffb11260` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/e57fb26c-484f-4cd1-bcd1-d8fa7eb2b312) | Normalized ClickHouse | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
|  | Raw ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
|  | `unit_tests_dbms` | `d489d1b0fd52b57389f107e5650cb58c5f6a33689d9c6090a5198cf0bdef2176` | 795,060,896 |

Native Bazel targets built: x86-64: 60, ARM64: 60. Both normalized binaries matched native CMake. Both embedded CcInfo comparisons matched all 17 ordered fields and their source constraints. The Boost report matched its 12 fields, and CRoaring passed the 4,096-byte alignment check on each CPU.

x86-64: 51 independent translation units produced objects, one per selected public header. Original compiler action metadata was not collected for these translation units.

ARM64: 51 independent translation units produced objects, one per selected public header. Original compiler action metadata was not collected for these translation units.

| CPU | Ordinary DWYU | Strict DWYU | Selected | Omitted | Production outcomes | Failed preprocessing outputs |
| --- | --- | --- | --- | --- | --- | --- |
| x86-64 ([parent Failed](https://app.buildbuddy.io/invocation/fc1bc8f6-d3c3-49e9-90fb-179e3b3f3e4a)) | [exit 0](https://app.buildbuddy.io/invocation/029d81a9-3aac-4bbe-a1a0-5a68e89dfd48) | [exit 1](https://app.buildbuddy.io/invocation/690b0944-5804-4b70-8717-f55df8f0f364) | 36 | 746 | 9 `clean`, 4 `failed`, 9 `findings`, 1 `unchecked_link_inputs`, 13 `unverified_link_deps` | 499 |
| ARM64 ([parent Failed](https://app.buildbuddy.io/invocation/68cd08a8-43f9-4f59-973c-db5cb643966e)) | [exit 0](https://app.buildbuddy.io/invocation/21845cc6-3485-4b59-85fe-d6bb87fc3ee3) | [exit 1](https://app.buildbuddy.io/invocation/76e30117-b659-4b13-ace3-37bb310b68ad) | 32 | 750 | 9 `clean`, 4 `failed`, 7 `findings`, 12 `unverified_link_deps` | 499 |

Each CPU ran the expected negative fixture and reported exactly its unused implementation dependency. Nonselected labels are unverified. A configured header declaration alone does not verify a compiler's include resolution.

DWYU checked the selected declarations in `cmake_match` and `cmake_match_aarch64`. Includes from textual consumer contexts were outside this selection.

The unpublished `a392b4c0f` build failed on `h24_SetSerialization.cpp` on both CPUs ([x86-64](https://app.buildbuddy.io/invocation/deefa67c-7403-471e-af16-6b02d4ab4184), [ARM64](https://app.buildbuddy.io/invocation/980bccf1-e776-4c61-bb99-6008c72f8804)). [src/Interpreters/SetSerialization.h](https://github.com/ClickHouse/ClickHouse/blob/6bb427c9a81459c2b9e7369ef6ed8a6e8680b0da/src/Interpreters/SetSerialization.h) uses `std::list` without including `<list>`. The correction kept `src/Interpreters/SetSerialization.h` on `//src:headers`. The current 51 downstream checks omit that unsolved header. The failed sibling sets the BuildBuddy submission interval; the published [e10eaf426](https://github.com/dzbarsky/clickhouse/commit/e10eaf426d559c60ad5be706011e95fe97be5d6f) supplies all three ELF references on each CPU.

## a38921cb8

[a38921cb8](https://github.com/dzbarsky/clickhouse/commit/a38921cb8dd22a3f8c942b0e6249d076ad4e2e04): `Declare ClickHouse header owners and narrow private dependencies`. The raw ClickHouse and `unit_tests_dbms` executables matched [93d4235b8](https://github.com/dzbarsky/clickhouse/commit/93d4235b8ae2c096a83af390ad69868b28603c4a). Each CPU passed 38 GoogleTests, six SQL cases and 16 Decimal boundary checks. The hashes and sizes come from BuildBuddy output metadata.

| Native Bazel build | ELF | SHA-256 | Bytes |
| --- | --- | --- | --- |
| [x86-64](https://app.buildbuddy.io/invocation/9036d423-cb65-4737-b0da-09162af52825) | Normalized ClickHouse | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
|  | Raw ClickHouse | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` | 929,619,680 |
|  | `unit_tests_dbms` | `2a6934b1d4523406f0c1e45b68b515914ba14a4167ffb6c05c0668a5ffb11260` | 811,968,280 |
| [ARM64](https://app.buildbuddy.io/invocation/a0be3d16-e7d4-40cf-ad54-b9e8f2e0b630) | Normalized ClickHouse | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |
|  | Raw ClickHouse | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | 907,067,440 |
|  | `unit_tests_dbms` | `d489d1b0fd52b57389f107e5650cb58c5f6a33689d9c6090a5198cf0bdef2176` | 795,060,896 |

Native Bazel targets built: x86-64: 60, ARM64: 60. Both normalized binaries matched native CMake. Both embedded CcInfo comparisons matched all 17 ordered fields and their source constraints. The Boost report matched its 12 fields, and CRoaring passed the 4,096-byte alignment check on each CPU.

x86-64: 51 independent translation units produced objects, one per selected public header. Original compiler action metadata was not collected for these translation units.

ARM64: 51 independent translation units produced objects, one per selected public header. Original compiler action metadata was not collected for these translation units.

| CPU | Ordinary DWYU | Strict DWYU | Selected | Omitted | Production outcomes | Failed preprocessing outputs |
| --- | --- | --- | --- | --- | --- | --- |
| x86-64 ([parent Failed](https://app.buildbuddy.io/invocation/dc1fc957-f137-422d-bd06-5cb531a96edc)) | [exit 0](https://app.buildbuddy.io/invocation/2e607c79-4004-4f1f-9365-9f332ba6fe7b) | [exit 1](https://app.buildbuddy.io/invocation/5e29b7cb-ff8c-49db-a06c-b6c437de25b7) | 21 | 761 | 4 `clean`, 3 `failed`, 4 `findings`, 1 `header_pp_not_verified`, 1 `no_cpp`, 8 `unverified_link_deps` | 486 |
| ARM64 ([parent Failed](https://app.buildbuddy.io/invocation/36949da9-fb22-429e-af36-049e25ee66af)) | [exit 0](https://app.buildbuddy.io/invocation/f2bc3633-9173-46d8-920b-ee2d2c54f2a2) | [exit 1](https://app.buildbuddy.io/invocation/04b401a7-7fe0-4438-bf88-97bb050298a7) | 19 | 763 | 4 `clean`, 3 `failed`, 3 `findings`, 1 `header_pp_not_verified`, 8 `unverified_link_deps` | 486 |

Each CPU ran the expected negative fixture and reported exactly its unused implementation dependency. Nonselected labels are unverified. A configured header declaration alone does not verify a compiler's include resolution.

DWYU checked the selected declarations in `cmake_match` and `cmake_match_aarch64`. Includes from textual consumer contexts were outside this selection.

The eight changes cover public and private DBMS headers, OpenLDAP, benchmark dependencies, the Arrow BUILD file, `IThrottler`, Arrow’s ISA-L dependencies and `ASTSystemQuery.h`. The eight SDK consumers included OpenLDAP C and C++ callers, `StoragePostgreSQL.h`, `parseColumnsListForTableFunction.h`, `IThrottler.h` and `ASTSystemQuery.h` with `USE_XRAY=1`. All eight produced the expected object on both CPUs. BuildBuddy recorded all ten benchmark archive SHA-256 values and sizes, including `//src:benchmark_runtime_filter_objects`; all ten matched [93d4235b8](https://github.com/dzbarsky/clickhouse/commit/93d4235b8ae2c096a83af390ad69868b28603c4a). Individual benchmark object equality was not inferred from an archive.

`src/Interpreters/SetSerialization.h` still belongs to `//src:headers`. The [previous standalone build](bazel/initial_migration/validation-history.md#93d4235b8) found that this upstream header uses `std::list` without including `<list>`. It is outside the 51 current checks.

The 17 ordered CcInfo fields compare equivalent current BUILD expressions. Counts can change from the previous source; these are the measured changes that matched the declared dependencies and named include directories.

| CPU | Embedded target | Field counts: 93d4235b8 → current |
| --- | --- | --- |
| x86-64 | `//src:clickhouse_common_io_embedded` | `headers`: 24392 → 24368, `system_includes`: 198 → 199 |
| x86-64 | `//src:dbms_embedded` | `headers`: 29719 → 29695, `linker_inputs`: 444 → 439, `system_includes`: 312 → 313 |
| ARM64 | `//src:clickhouse_common_io_embedded` | `headers`: 24389 → 24365, `system_includes`: 198 → 199 |
| ARM64 | `//src:dbms_embedded` | `headers`: 29716 → 29692, `system_includes`: 312 → 313 |

| CPU | Production labels queried | Mandatory labels requested | Optional selection | Optional labels requested | Production DWYU reports | Requested without a DWYU report | Public-header objects matching 93d4235b8 (of 51) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| x86-64 | 45 | 21 | mandatory_only | 0/24 | 17 | 4 | 51 |
| ARM64 | 43 | 19 | mandatory_only | 0/24 | 16 | 3 | 51 |

The requested target counts come from the original ordinary and strict invocations. The production DWYU reports contain the original per-target JSON with a verified SHA-256. A requested target without a report has no inferred clean or failed report. Prior failed preprocessing outputs are not assumed fixed.

First attempt at the same commit ([x86-64](https://app.buildbuddy.io/invocation/adb7bdd3-8932-4ebe-aa26-b4e987b0461d), [ARM64](https://app.buildbuddy.io/invocation/b2c7b667-d9c4-48de-b475-db2d149813ae)): both native invocations built 59 of 61 targets. `@clickhouse_openldap//:_api` had been removed, and four OpenLDAP C/C++ tests in `//bazel/fixtures/compiled_sdk_consumers:objects` used `__has_include` to test whether a dependency exported a header. Discoverability includes compiler search directories, so it cannot establish which dependency owns a public header. The three executable hashes matched on each CPU, and the four embedded CcInfo reports matched all 17 fields. Neither invocation passed the native build; GoogleTest, SQL and DWYU did not run.

The retry kept the same source tree. It removed `@clickhouse_openldap//:_api` from the requested targets and required `@clickhouse_openldap//:_lber` to export four public headers and `@clickhouse_openldap//:_ldap` to export two more and provide the four BER headers. Bazel compared their direct public, private and textual headers. The four downstream C and C++ checks used header guards to test what their public includes actually introduced. All eight SDK object files were present on each CPU. Local Bazel controls also rejected a private header promoted to public and a public header hidden from its owner.

The ordinary DWYU invocations passed; both strict invocations failed. On each CPU, the 24 optional targets were withheld because the configured analyzer inputs did not match. DWYU ran on 21 of 782 production targets on x86-64 and 19 on ARM64. Four compiled targets were clean on both: `//base/poco/JSON:_poco_json`, `//src:dbms__compile_1`, `@clickhouse_openldap//:_lber`, `@clickhouse_openldap//:_ldap`. The selected headers produced 486 failed preprocessing actions per CPU. Of the 499 header failures at [93d4235b8](https://github.com/dzbarsky/clickhouse/commit/93d4235b8ae2c096a83af390ad69868b28603c4a), 495 were in the current selection. 466 had identical stderr, 20 had a different first diagnostic, and nine no longer appeared as failures. The last count does not establish nine successful compiles.
