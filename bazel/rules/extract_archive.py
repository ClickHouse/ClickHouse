"""Extract explicitly declared schema files from ClickHouse's pinned archive."""

import argparse
from pathlib import Path, PurePosixPath
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--strip-prefix', required=True)
    parser.add_argument('members', nargs='+')
    args = parser.parse_args()
    with tarfile.open(args.archive) as archive:
        for name in args.members:
            relative = PurePosixPath(name)
            if relative.is_absolute() or '..' in relative.parts:
                raise ValueError('Non-relative archive member: ' + name)
            member = archive.getmember(args.strip_prefix + '/' + name)
            if not member.isfile():
                raise ValueError('Expected regular schema file: ' + name)
            destination = args.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(archive.extractfile(member).read())


if __name__ == '__main__':
    main()
