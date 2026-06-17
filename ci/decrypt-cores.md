# Decrypting Core Dumps from CI Artifacts

When a ClickHouse server crashes inside a CI job, the resulting core dump is
compressed with `zstd` and encrypted before being uploaded as a job artifact.
This document explains how to retrieve the original core file from those
artifacts.

## Background

Core dumps may contain memory contents that should not be exposed publicly, so
they are encrypted with a hybrid scheme:

1. A fresh 32-byte AES-256 key is generated per job.
2. The core file is compressed with `zstd` and encrypted with
   `AES-256-CBC` (PBKDF2 key derivation, password = the raw AES key bytes).
3. The AES key itself is wrapped with `RSA-OAEP` (SHA-256) using the public
   key checked into the repo at `ci/defs/public.pem`.

The corresponding private key (`private-cores.pem`) is held off-repo by the CI
maintainers and is required for decryption.

## Artifacts Produced by a Job

When a job collects core dumps, it attaches the following files to the run:

- `aes.key.rsa` — the RSA-OAEP-wrapped AES key for that job.
- One or more `core.<pid>.zst.enc` files — at most three, taken from
  `ci/tmp/run_r*/core.*`.

You will also need the matching `clickhouse` binary (download it from the build
job for the same commit) to actually analyze the core in `gdb`.

## Prerequisites

- `openssl` (1.1 or newer).
- `zstd`.
- The private key `private-cores.pem`, obtained from a CI maintainer.

## Decryption Steps

Assuming the downloaded artifacts are in the current directory and the private
key is at `~/private-cores.pem`:

```bash
# 1. Unwrap the per-job AES key.
openssl pkeyutl -decrypt \
    -inkey ~/private-cores.pem \
    -in aes.key.rsa \
    -out aes.key \
    -pkeyopt rsa_padding_mode:oaep \
    -pkeyopt rsa_oaep_md:sha256

# 2. Decrypt the compressed core.
openssl enc -d -aes-256-cbc \
    -in core.<pid>.zst.enc \
    -out core.<pid>.zst \
    -pbkdf2 \
    -pass file:aes.key

# 3. Decompress to obtain the raw core file.
zstd -d core.<pid>.zst -o core.<pid>
```

Repeat steps 2 and 3 for each `core.<pid>.zst.enc` artifact. The same
`aes.key` works for every core file in the same job.

## Loading the Core in gdb

```bash
gdb /path/to/clickhouse core.<pid>
```

The `clickhouse` binary must come from the exact build that produced the core;
mismatched binaries yield unusable backtraces.

## Cleanup

Delete the unwrapped `aes.key` and any decrypted core files once the
investigation is complete — they may contain sensitive data from the test run.

## References

- Pull request that introduced the scheme: [ClickHouse/ClickHouse#97342](https://github.com/ClickHouse/ClickHouse/pull/97342)
- Encryption helper: `Utils.encrypt` in `ci/praktika/utils.py`
- Core collection: `ClickHouseProc._collect_core_dumps` in `ci/jobs/scripts/clickhouse_proc.py`
- Public key: `ci/defs/public.pem`
