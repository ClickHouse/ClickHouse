#!/usr/bin/env python3
"""Report the object lifecycle of every S3 key named in a "No such key" match set.

A `Code: 499 ... The specified key does not exist` match line names the failing read and
nothing else, so the artifact records the victim and never the culprit: it does not say
when the object was written, whether it was deleted, by which thread, or whether the
delete was a batch removal. Those lines are in the same server logs, keyed by the same
string (`ReadBufferFromS3::key`, the `Writing blob for path` key and the
`deleteFileFromS3` key are all the object's `remote_path`), and are simply never selected.

Usage:
    s3_key_lifecycle.py <matches-file> <log-dir>

The report is written to stdout, so a caller can append it next to the matches it
explains. Finding nothing is a normal outcome and exits 0. A genuine failure (an
unreadable log, a grep error) exits non-zero with a traceback, so a caller can mark its
report incomplete instead of publishing an empty one as clean.
"""

import re
import subprocess
import sys
from pathlib import Path

# Only ReadBufferFromS3 adds "while reading key:", so a match line raised elsewhere
# contributes no key and no group.
KEY_PATTERN = re.compile(r"while reading key: ([^\s,]+)")

# A log line opens with "<date> <time>", whose text sorts chronologically. grep reports one
# file at a time, so file order is not time order.
TIMESTAMP_PATTERN = re.compile(r"\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+")

# Lines are selected by LOGGER NAME rather than by message wording, so rewording
# "Writing blob for path" or "were removed from S3" cannot silently empty the report.
LIFECYCLE_LOGGERS = ("DiskObjectStorageTransaction", "deleteFileFromS3")

LOG_FILE_PATTERN = "clickhouse-server*.log"

# Every distinct key always gets a header; only the line expansion is bounded, so no key
# can vanish from the report even when the match set is huge.
MAX_EXPANDED_KEYS = 200
MAX_LINES_PER_KEY = 50

NO_LIFECYCLE = (
    "no lifecycle line found for this key (the upload line is logged at level 'test', "
    "which only the stress phase enables)"
)


def extract_keys(matches_file):
    """The keys named by the match lines, de-duplicated, in first-seen order."""
    text = Path(matches_file).read_text(encoding="utf-8", errors="replace")
    keys = []
    seen = set()
    for key in KEY_PATTERN.findall(text):
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def find_logs(log_dir):
    # Resolved here: subprocess does no shell expansion, and grep given a literal glob
    # fails rather than matching.
    return sorted(str(path) for path in Path(log_dir).glob(LOG_FILE_PATTERN))


def collect_lifecycle_lines(keys, logs):
    """Log lines emitted by a lifecycle logger that mention one of `keys`."""
    # One pass for all keys, and -F so a key is never read as a regular expression.
    grep = subprocess.run(
        ["grep", "-a", "-H", "-F", "-f", "-", "--", *logs],
        input="\n".join(keys),
        capture_output=True,
        text=True,
        errors="replace",
        check=False,
    )
    # grep exits 1 for "no match" and above 1 when a log could not be read, which would
    # otherwise be indistinguishable from a key having no lifecycle.
    if grep.returncode > 1:
        raise RuntimeError(
            f"grep exited {grep.returncode} over {len(logs)} log file(s): {grep.stderr.strip()}"
        )
    return [line for line in grep.stdout.splitlines() if any(logger in line for logger in LIFECYCLE_LOGGERS)]


def timestamp(line):
    found = TIMESTAMP_PATTERN.search(line)
    return found.group(0) if found else ""


def build_report(keys, lines):
    report = [f"--- object lifecycle of the keys above, from {LOG_FILE_PATTERN}"]
    for key in keys[:MAX_EXPANDED_KEYS]:
        report.append(f"--- key: {key}")
        # A batch delete lists its keys as "[k1, k2, ...]", so the join is by substring.
        matched = sorted((line for line in lines if key in line), key=timestamp)
        if not matched:
            report.append(NO_LIFECYCLE)
            continue
        report.extend(matched[:MAX_LINES_PER_KEY])
        if len(matched) > MAX_LINES_PER_KEY:
            omitted = len(matched) - MAX_LINES_PER_KEY
            report.append(f"... {omitted} more lifecycle line(s) omitted (per-key line cap {MAX_LINES_PER_KEY})")
    for key in keys[MAX_EXPANDED_KEYS:]:
        report.append(f"--- key: {key}")
        report.append(f"not expanded: per-report key cap {MAX_EXPANDED_KEYS} reached")
    return report


def report_for(matches_file, log_dir):
    """The report for `matches_file`, or no lines at all when there is nothing to say."""
    keys = extract_keys(matches_file)
    if not keys:
        return []
    logs = find_logs(log_dir)
    if not logs:
        return []
    return build_report(keys, collect_lifecycle_lines(keys[:MAX_EXPANDED_KEYS], logs))


def main(argv):
    if len(argv) != 3:
        raise SystemExit(f"usage: {Path(argv[0]).name} <matches-file> <log-dir>")
    report = report_for(argv[1], argv[2])
    # One write, so a failure mid-scan appends nothing and the caller's failure marker
    # cannot land under a half-written report.
    if report:
        sys.stdout.write("\n".join(report) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
