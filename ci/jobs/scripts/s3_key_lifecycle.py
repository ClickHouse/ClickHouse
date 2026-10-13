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
explains. An empty match set produces nothing, because the caller's PASS verdict is that
the file this output is appended to stayed empty. A non-empty one always produces at
least one line: "there is nothing to report" and "the report could not be built" must not
look alike. A genuine failure (an unreadable log, a grep error) exits non-zero with a
traceback, so a caller can mark its report incomplete instead of publishing an empty one
as clean.
"""

import re
import subprocess
import sys
import tempfile
from pathlib import Path

# An S3 key may hold a space or a comma, so it runs to the emitter's own ", from bucket:"; the
# first one on the line is the emitter's, because the client-controlled query text follows it.
KEY_PATTERN = re.compile(r"while reading key: (.+?), from bucket:")

# The formatter closes the client's unescaped query id with "} " before the level, so the first
# "} <level> " is the line's own slot unless the id holds one as well: the cut can land early,
# inside a hostile id, but never inside the message, which is where the query text is.
LEVEL_PATTERN = re.compile(r"\} <\w+> ")

# A log line opens with "<date> <time>", whose text sorts chronologically. grep reports one
# file at a time, so file order is not time order.
TIMESTAMP_PATTERN = re.compile(r"\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+")

# Lines are selected by LOGGER NAME rather than by message wording, so rewording
# "Writing blob for path" or "were removed from S3" cannot silently empty the report.
LIFECYCLE_LOGGERS = ("DiskObjectStorageTransaction", "deleteFileFromS3")

# The logger opens the message, so the name is looked for there and nowhere else on the line:
# a query whose text replicates a whole "<level> logger: " slot would otherwise select its own
# failure line, which then holds a key's line cap against that key's write and delete lines.
LIFECYCLE_MESSAGE_PATTERN = re.compile(
    r"(?:" + "|".join(re.escape(logger) for logger in LIFECYCLE_LOGGERS) + "): "
)

# The characters the emit sites put on either side of a key: "key <K>," / "[<K1>, <K2>]" /
# "path <K> was removed" / "for blob <K>"<EOL>. A closed set, unlike the key alphabet, so a
# key character nobody foresaw cannot read as a boundary.
KEY_SEPARATORS = " ,[]"

LOG_FILE_PATTERN = "clickhouse-server*.log"

# Every distinct key always gets a header; only the line expansion is bounded, so no key
# can vanish from the report even when the match set is huge.
MAX_EXPANDED_KEYS = 200
MAX_LINES_PER_KEY = 50

NO_LIFECYCLE = (
    "no lifecycle line found: the upload line is logged at level 'test', which only the stress "
    "phase enables, and no deleteFileFromS3 line recorded a delete of this object on this server"
)

SUBSTRING_ONLY = "matched as a substring only (no delimited occurrence found)"


def message_in(log_line):
    """What the emitter wrote on a log line, without the fields that precede the level."""
    level = LEVEL_PATTERN.search(log_line)
    # No slot at all (a continuation line of a multi-line message) leaves the whole line: a
    # shape this parser cannot dissect must not silently name no key.
    return log_line[level.end() :] if level else log_line


def keys_in(match_line):
    """The keys one match line names, in the order the line names them."""
    return KEY_PATTERN.findall(message_in(match_line))


def extract_keys(matches_text):
    """The keys named by the match lines, de-duplicated, in rank-within-line order.

    Every line's first key comes before any line's second, so one line naming many keys
    cannot spend the report's key cap before another line's failing read is reached.
    """
    ranked = sorted(
        (rank, position, key)
        for position, line in enumerate(matches_text.splitlines())
        for rank, key in enumerate(keys_in(line))
    )
    keys = []
    seen = set()
    for _, _, key in ranked:
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def find_logs(log_dir):
    # Resolved here: subprocess does no shell expansion, and grep given a literal glob
    # fails rather than matching.
    return sorted(str(path) for path in Path(log_dir).glob(LOG_FILE_PATTERN))


def timestamp(line):
    found = TIMESTAMP_PATTERN.search(line)
    return found.group(0) if found else ""


def _in_time_order(lines, total):
    ordered = sorted(lines, key=timestamp)
    if total > len(ordered):
        omitted = total - len(ordered)
        ordered.append(f"... {omitted} more lifecycle line(s) omitted (per-key line cap {MAX_LINES_PER_KEY})")
    return ordered


class _KeyLifecycle:
    """One key's retained lines, kept as they arrive so the caps bound memory too.

    A line belongs to the key when a separator (or a message edge) flanks the key on both
    sides, which is what keeps a key out of the group of a key that merely contains it. A
    line that names the key with no separator is kept separately and reported only when
    there is no separated line at all: a reworded message must not empty the group silently.
    """

    __slots__ = ("key", "pattern", "lines", "total", "loose", "loose_total")

    def __init__(self, key):
        self.key = key
        # A negated complement, so a line edge satisfies the lookaround as a separator does.
        self.pattern = re.compile(
            f"(?<![^{re.escape(KEY_SEPARATORS)}]){re.escape(key)}(?![^{re.escape(KEY_SEPARATORS)}])"
        )
        self.lines = []
        self.total = 0
        self.loose = []
        self.loose_total = 0

    def offer(self, line, message):
        # Matched in the emitter's own text, so a key the client put in a field before the
        # level cannot pull an event of a different object into this key's history.
        if self.key not in message:
            return
        if self.pattern.search(message):
            self.total += 1
            if len(self.lines) < MAX_LINES_PER_KEY:
                self.lines.append(line)
            # The fallback is unreachable once a separated line exists.
            self.loose.clear()
            self.loose_total = 0
        elif not self.total:
            self.loose_total += 1
            if len(self.loose) < MAX_LINES_PER_KEY:
                self.loose.append(line)

    def report(self):
        if self.total:
            return _in_time_order(self.lines, self.total)
        if self.loose_total:
            return [SUBSTRING_ONLY] + _in_time_order(self.loose, self.loose_total)
        return [NO_LIFECYCLE]


def collect_lifecycle_lines(keys, logs):
    """Each key's retained lifecycle lines, from one grep pass over `logs`."""
    groups = {key: _KeyLifecycle(key) for key in keys}
    with tempfile.TemporaryFile("w+", errors="replace") as grep_errors:
        # -F so a key is never read as a regular expression, and one pass for all keys.
        grep = subprocess.Popen(
            ["grep", "-a", "-H", "-F", "-f", "-", "--", *logs],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=grep_errors,
            text=True,
            errors="replace",
        )
        # The key list is a few kilobytes at most, so it fits the pipe buffer and can be
        # written before stdout is read. grep's stderr goes to a file for the same reason
        # inverted: a pipe nobody drains until the end can fill up and deadlock.
        grep.stdin.write("\n".join(keys))
        grep.stdin.close()
        # Filtered and retained line by line, so the caps bound what is held in memory too.
        for line in grep.stdout:
            line = line.rstrip("\n")
            message = message_in(line)
            if not LIFECYCLE_MESSAGE_PATTERN.match(message):
                continue
            for group in groups.values():
                group.offer(line, message)
        grep.stdout.close()
        returncode = grep.wait()
        grep_errors.seek(0)
        errors_text = grep_errors.read()
    # grep exits 0 for a match and 1 for none; anything else - 2 for an unreadable log, a
    # NEGATIVE value when a signal killed it - must not read as "this key has no lifecycle".
    if returncode not in (0, 1):
        raise RuntimeError(
            f"grep exited {returncode} over {len(logs)} log file(s): {errors_text.strip()}"
        )
    return groups


def build_report(keys, groups):
    report = [f"--- object lifecycle of the keys above, from {LOG_FILE_PATTERN}"]
    for key in keys[:MAX_EXPANDED_KEYS]:
        report.append(f"--- key: {key}")
        report.extend(groups[key].report())
    for key in keys[MAX_EXPANDED_KEYS:]:
        report.append(f"--- key: {key}")
        report.append(f"not expanded: per-report key cap {MAX_EXPANDED_KEYS} reached")
    return report


def report_for(matches_file, log_dir):
    """The report for `matches_file`, or no lines at all when it holds no match."""
    matches_text = Path(matches_file).read_text(encoding="utf-8", errors="replace")
    if not matches_text.strip():
        return []
    keys = extract_keys(matches_text)
    if not keys:
        count = len(matches_text.splitlines())
        return [
            f"--- no S3 key found in the {count} match line(s) above "
            '(only ReadBufferFromS3 adds "while reading key:")'
        ]
    logs = find_logs(log_dir)
    if not logs:
        return [f"--- no {LOG_FILE_PATTERN} file in {log_dir}, so no lifecycle could be collected"]
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
