#!/usr/bin/env python3
"""Run cbindgen with Cargo metadata from declared offline vendor sources."""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    for name in ['cargo', 'rustc', 'cbindgen', 'manifest', 'vendor', 'config', 'cargo-config']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--crate', required=True)
    parser.add_argument('--header', nargs=2, metavar=('LANGUAGE', 'OUTPUT'), action='append', required=True)
    args = parser.parse_args()
    output_dir = Path(args.header[0][1]).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output_dir) as scratch:
        scratch = Path(scratch).resolve()
        config = args.cargo_config.read_text()
        config = config[config.index('[source.vendored-sources]'):]
        config = config.replace('@RUST_VENDOR_DIR@', str(args.vendor.resolve()))
        (scratch / 'config.toml').write_text(config)
        env = os.environ | {
            'CARGO_HOME': str(scratch / 'cargo-home'),
            'CARGO_NET_OFFLINE': 'true',
            'RUSTC': str(args.rustc.resolve()),
        }
        metadata = scratch / 'metadata.json'
        with metadata.open('w') as stream:
            subprocess.run([str(args.cargo.resolve()), 'metadata', '--all-features', '--locked', '--offline',
                            '--format-version=1', '--manifest-path', str(args.manifest.resolve()),
                            '--config', str(scratch / 'config.toml')], env=env, stdout=stream, check=True)
        for language, output in args.header:
            subprocess.run([str(args.cbindgen.resolve()), str(args.manifest.resolve().parent), '--crate', args.crate,
                            '--config', str(args.config.resolve()), '--metadata', str(metadata),
                            '--lang', language, '--output', output], env=env, check=True)


if __name__ == '__main__':
    main()
