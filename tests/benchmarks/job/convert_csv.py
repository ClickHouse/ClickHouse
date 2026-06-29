#!/usr/bin/env python3
"""Re-encode IMDB/JOB CSV to RFC 4180 CSV that ClickHouse can parse.

The JOB dataset is Postgres-COPY CSV with ESCAPE '\\': a backslash escapes the
quote character ONLY inside a quoted field (e.g. "5' 9\\"" -> 5' 9"). Outside
quoted fields a backslash is a literal character (e.g. an unquoted value may end
with `\\` right before the delimiter). Python's csv module applies escapechar
everywhere, which corrupts the latter case, so we parse with a small state
machine that matches Postgres semantics and re-emit standard doubled-quote CSV.

Empty unquoted fields are preserved as empty, which ClickHouse maps to NULL for
Nullable columns.

Fast path: any physical line containing neither '"' nor '\\' is already valid
CSV (it cannot hold an open quote or an escape), so it is forwarded verbatim.
"""
import csv
import sys


def parse_record(s: str):
    """Parse one record from ``s`` (Postgres CSV semantics).

    Returns ``(fields, True)`` when a complete record was parsed, or
    ``(None, False)`` when more input is needed (an open quoted field, or a
    trailing backslash whose escaped char has not arrived yet).
    """
    fields = []
    buf = []
    in_quotes = False
    at_field_start = True
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if in_quotes:
            if ch == "\\":
                if i + 1 < n:
                    nxt = s[i + 1]
                    if nxt == '"' or nxt == "\\":
                        buf.append(nxt)
                        i += 2
                        continue
                    buf.append("\\")
                    i += 1
                    continue
                return None, False
            if ch == '"':
                in_quotes = False
                i += 1
                continue
            buf.append(ch)
            i += 1
            continue
        if ch == '"' and at_field_start:
            in_quotes = True
            at_field_start = False
            i += 1
            continue
        if ch == ",":
            fields.append("".join(buf))
            buf = []
            at_field_start = True
            i += 1
            continue
        if ch == "\n":
            fields.append("".join(buf))
            return fields, True
        if ch == "\r":
            i += 1
            continue
        buf.append(ch)
        at_field_start = False
        i += 1
    if in_quotes:
        return None, False
    fields.append("".join(buf))
    return fields, True


def main() -> int:
    out = sys.stdout
    writer = csv.writer(
        out,
        delimiter=",",
        quotechar='"',
        doublequote=True,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    pending = None
    for line in sys.stdin:
        if pending is None and "\\" not in line and '"' not in line:
            out.write(line)
            continue
        pending = line if pending is None else pending + line
        fields, complete = parse_record(pending)
        if complete:
            writer.writerow(fields)
            pending = None
    if pending is not None:
        fields, complete = parse_record(pending)
        if not complete:
            sys.stderr.write(
                "error: incomplete final record at end of input "
                "(unterminated quoted field or trailing escape); "
                "input is likely truncated or corrupted\n"
            )
            return 1
        writer.writerow(fields)
    return 0


if __name__ == "__main__":
    sys.exit(main())
