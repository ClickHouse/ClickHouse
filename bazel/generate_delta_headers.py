#!/usr/bin/env python3
"""Generate Delta's C/C++ bindings using its pinned offline Cargo sources."""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    for name in ['cargo', 'rustc', 'cbindgen', 'output', 'manifest', 'vendor', 'config']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.output) as scratch:
        scratch = Path(scratch).resolve()
        config = args.config.read_text()
        config = config[config.index('[source.vendored-sources]'):]
        config = config.replace('@RUST_VENDOR_DIR@', str(args.vendor.resolve()))
        (scratch / 'config.toml').write_text(config)
        env = os.environ | {
            'CARGO_HOME': str(scratch / 'cargo-home'),
            'CARGO_NET_OFFLINE': 'true',
            'RUSTC': str(args.rustc.resolve()),
        }
        crate = args.manifest.resolve().parent
        metadata = scratch / 'metadata.json'
        with metadata.open('w') as stream:
            subprocess.run([str(args.cargo.resolve()), 'metadata', '--all-features', '--locked', '--offline',
                            '--format-version=1', '--manifest-path', str(crate / 'Cargo.toml'),
                            '--config', str(scratch / 'config.toml')], env=env, stdout=stream, check=True)
        for language, extension in [('c++', 'hpp'), ('c', 'h')]:
            subprocess.run([str(args.cbindgen.resolve()), str(crate), '--crate', 'delta_kernel_ffi',
                            '--config', str(crate / 'cbindgen.toml'), '--metadata', str(metadata),
                            '--lang', language, '--output', str(args.output / ('delta_kernel_ffi.' + extension))],
                           env=env, check=True)


if __name__ == '__main__':
    main()
