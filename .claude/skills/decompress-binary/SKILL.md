---
name: decompress-binary
description: Extract the inner ELF from a ClickHouse self-extracting `clickhouse` binary, including when its architecture differs from the host (e.g. to load an aarch64 CI core dump on an x86 workstation). Use when gdb/lldb needs real symbols from a downloaded CI/release binary, or when self-extraction by running the binary is not possible because of an architecture mismatch.
argument-hint: <clickhouse-self-extracting> [output.elf]
disable-model-invocation: false
allowed-tools: Bash(python3:*), Bash(zstd:*), Bash(llvm-objdump:*), Bash(llvm-nm:*), Bash(file:*), Bash(curl:*), Bash(ls:*), Bash(stat:*), Read, Write
---

# Decompress a ClickHouse Binary (Cross-Architecture)

ClickHouse release and CI `clickhouse` binaries are **self-extracting**: a small
decompressor stub, followed by the `zstd`-compressed real ELF and a trailer.

The normal way to decompress is to **run the binary once**: it extracts the inner
ELF in place and re-`exec`s it. That works on the binary's **own architecture**,
and also on a foreign architecture **if `qemu` user-mode emulation for it is
installed** (e.g. `qemu-aarch64` to run an aarch64 binary on x86). When `qemu` for
the target is not available, you cannot run the binary at all and must extract the
payload offline.

This skill extracts the inner ELF without executing anything, on any host.

## When to use

- You downloaded a CI/release `clickhouse` and `gdb`/`lldb` shows no real symbols
  (it only sees the decompressor stub's tiny symbol table).
- The binary's architecture differs from the host (cannot self-extract by running).
- You need the inner ELF to load a core dump (see `ci/decrypt-cores.md` for the
  matching core-dump decryption procedure).

## Format

See `utils/self-extracting-executable/types.h`:

```
[ decompressor ELF ]
[ compressed file blobs ]
[ FileData[] ]                 # one per packed file, each followed by its name
[ MetaData (16 bytes) @ EOF ]

MetaData { uint64 number_of_files; uint64 start_of_files_data; }
FileData { uint64 start, end, name_length, uncompressed_size, umask; bool exec; }
```

`MetaData` sits at the very end of the file. `start_of_files_data` points at the
`FileData` array; each 48-byte `FileData` is followed by the file name. The
compressed bytes for a file are `input[start:end]` (`zstd`, possibly multi-frame).
The packed `clickhouse` ELF is the entry with `exec = true`.

## Steps

1. Download the binary from the build job for the exact commit, for example:

   ```bash
   curl -s "https://clickhouse-builds.s3.amazonaws.com/PRs/<pr>/<sha>/build_<arch>_<sanitizer>/clickhouse" -o clickhouse.sfx
   ```

   Find the precise URL in the build job's `artifact_report_build_*.json`, or via
   `.claude/tools/fetch_ci_report.js "<pr-url>"`. Download in the foreground (a
   killed/resumed `curl` can append garbage past EOF and break the trailer; verify
   the size matches `Content-Length`).

2. Extract the inner ELF:

   ```bash
   python3 .claude/skills/decompress-binary/extract_self_extracting.py clickhouse.sfx clickhouse.elf
   ```

3. Verify it is the right build and has symbols:

   ```bash
   file clickhouse.elf                                          # ELF ..., not stripped, with debug_info
   llvm-objdump -s -j .note.gnu.build-id clickhouse.elf | tail  # must match the core's build id
   ```

   The build id must equal the one in the crash report / core. A mismatched
   binary yields unusable backtraces.

4. Use it with the core dump:

   ```bash
   gdb clickhouse.elf core.<pid>      # or: lldb clickhouse.elf -c core.<pid>
   ```

   `gdb` and `lldb` read foreign-architecture cores fine for backtraces and memory
   inspection (you are not executing the target).

## Notes

- A truncated or corrupted download is the most common failure: if the script
  reports an implausible `number_of_files`, re-download cleanly and check the size.
- The inner ELF is large (several GB for sanitizer builds, unstripped). Make sure
  there is enough disk.
- Shortcut when you can run the binary: if the host matches the binary's
  architecture, or `qemu` user-mode emulation for it is installed, just run
  `./clickhouse` once to self-extract in place. This skill is for the case where
  neither is possible.
