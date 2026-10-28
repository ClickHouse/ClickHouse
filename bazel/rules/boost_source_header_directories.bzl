"""Stage complete Boost header directories without maintaining a list of their directories."""

load(":source_header_directories.bzl", "source_header_directories")

def boost_source_header_directories(public_headers, textual_headers):
    """Group maximal boost/ directories with at least 16 unconditional public or textual headers."""
    published = set(public_headers).symmetric_difference(textual_headers)
    entries = native.glob(["boost/**"], exclude_directories = 0)
    counts = {path.rpartition("/")[0]: 0 for path in entries if path != "boost"}
    for path in entries:
        if path == "boost" or path in counts:
            continue
        parent = path.rpartition("/")[0]
        if path not in published:
            # This also excludes empty directories, which have no headers.
            counts[parent] = -1
        elif counts[parent] >= 0:
            counts[parent] += 1

    # A glob stops at BUILD files; a source directory would include the subpackage.
    for package in native.subpackages(include = ["boost/**"], allow_empty = True):
        counts[package.rpartition("/")[0]] = -1

    for directory in sorted(counts, reverse = True):
        if directory == "boost":
            continue
        parent = directory.rpartition("/")[0]
        if counts[directory] < 0:
            counts[parent] = -1
        elif counts[parent] >= 0:
            counts[parent] += counts[directory]

    return source_header_directories(sorted([
        directory
        for directory, count in counts.items()
        if count >= 16 and counts.get(directory.rpartition("/")[0], -1) < 16
    ]))
