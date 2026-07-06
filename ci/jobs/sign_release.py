#!/usr/bin/env python3
import hashlib
import os
from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

TEMP_PATH = Path(f"{Utils.cwd()}/ci/tmp")

GPG_BINARY_SIGNING_KEY = os.getenv("GPG_BINARY_SIGNING_KEY")
GPG_BINARY_SIGNING_PASSPHRASE = os.getenv("GPG_BINARY_SIGNING_PASSPHRASE")

# Release package artifacts (downloaded via job `requires`) and the
# self-extracting binary are the inputs to sign.
PACKAGE_SUFFIXES = (".deb", ".rpm", ".tgz")
BINARY_NAMES = ("clickhouse",)


def hash_file(file_path: Path) -> Path:
    block_size = 65536
    file_hash = hashlib.sha512()
    with open(file_path, "rb") as f:
        block = f.read(block_size)
        while block:
            file_hash.update(block)
            block = f.read(block_size)
    hash_file_path = Path(f"{file_path}.sha512")
    hash_file_path.write_text(file_hash.hexdigest(), encoding="utf-8")
    return hash_file_path


def collect_files_to_sign():
    files = []
    for f in sorted(TEMP_PATH.iterdir()):
        if not f.is_file():
            continue
        if f.suffix in PACKAGE_SUFFIXES or f.name in BINARY_NAMES:
            files.append(f)
    return files


def main():
    stopwatch = Utils.Stopwatch()

    assert (
        GPG_BINARY_SIGNING_KEY and GPG_BINARY_SIGNING_PASSPHRASE
    ), "GPG_BINARY_SIGNING_KEY and GPG_BINARY_SIGNING_PASSPHRASE must be set"

    # Pass the passphrase via a file so it does not leak into command logs.
    passphrase_file = TEMP_PATH / "gpg_passphrase"
    passphrase_file.write_text(GPG_BINARY_SIGNING_PASSPHRASE, encoding="utf-8")
    priv_key_file = TEMP_PATH / "priv.key"
    priv_key_file.write_text(GPG_BINARY_SIGNING_KEY, encoding="utf-8")

    gpg_common = (
        "gpg --batch --yes --pinentry-mode=loopback "
        f"--passphrase-file {passphrase_file}"
    )

    results = []
    try:
        Shell.check(f"{gpg_common} --import {priv_key_file}", verbose=True, strict=True)

        for file_path in collect_files_to_sign():
            hashed = hash_file(file_path)
            signed = Path(f"{hashed}.gpg")
            results.append(
                Result.from_commands_run(
                    name=signed.name,
                    command=f"{gpg_common} -o {signed} --sign {hashed}",
                )
            )
    finally:
        priv_key_file.unlink(missing_ok=True)
        passphrase_file.unlink(missing_ok=True)

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
