#!/usr/bin/env python3
"""Apply ClickHouse's canonical Rust symbol passes to a declared output copy."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    for name in ['source', 'output', 'ar', 'objcopy', 'nm', 'ld', 'localize_script', 'strip_script']:
        parser.add_argument('--' + name.replace('_', '-'), type=Path, required=True)
    parser.add_argument('--reference', type=Path, action='append', required=True)
    parser.add_argument('--keep', action='append', default=[])
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(args.source, args.output)
    output = str(args.output.resolve())
    # hermetic-llvm dispatches its multicall binary by the symlink's name.
    # Make paths absolute for the scripts' chdir calls without resolving it.
    tools = {name: str(getattr(args, name).absolute()) for name in ['ar', 'objcopy', 'nm', 'ld']}
    with tempfile.TemporaryDirectory(dir=args.output.parent) as temporary:
        env = os.environ | {'TMPDIR': str(Path(temporary).resolve())}
        subprocess.run(['bash', str(args.localize_script.resolve()), output,
                        tools['ar'], tools['objcopy'], tools['nm'],
                        *(str(p.resolve()) for p in args.reference)], env=env, check=True)
        if args.keep:
            subprocess.run(['bash', str(args.strip_script.resolve()), output,
                            tools['ar'], tools['objcopy'], tools['ld'], *args.keep], env=env, check=True)


if __name__ == '__main__':
    main()
