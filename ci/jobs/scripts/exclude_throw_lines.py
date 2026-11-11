#!/usr/bin/env python3
"""
Drop the lines of exception-throwing statements from an LCOV tracefile.

A `throw` statement is the error exit of a check whose condition is on the line
before it, and the condition line stays in the report. Counting the `throw`
itself measures how many error messages the tests trigger, not how much logic
they reach. lcov's own `lcov_excl_line` option cannot express this because it
matches one line only, while ClickHouse formats a throw as

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Message {}", argument);

so here the whole statement is removed, up to the `;` that closes it.

Removed: `DA:` and `BRDA:` records of those lines, both covered and uncovered.
`LF:`/`LH:`/`BRF:`/`BRH:` are recomputed. Function records are kept as is.

Usage:
    python3 exclude_throw_lines.py input.info [output.info]

If output.info is omitted, modifies input.info in place (via a temp file).
"""

import os
import re
import sys
import tempfile

THROW_START = re.compile(
    r"\bthrow\s+(DB::)?Exception\b"
    r"|\bthrow\s+(DB::)?ErrnoException\b"
    r"|\bErrnoException::throw"
    r"|\bthrowFromErrno"
    r"|\bthrow\s+std::"
    r"|\bUNREACHABLE\s*\("
    r"|\bstd::terminate\s*\("
)
STRING_LITERAL = re.compile(r'"(\\.|[^"\\])*"' + r"|'(\\.|[^'\\])*'")
LINE_COMMENT = re.compile(r"//.*$")
# A statement longer than this is not a throw formatted over several lines but
# a parsing mistake (an unbalanced parenthesis in a raw string, a macro). Stop
# there rather than swallowing unrelated code.
MAX_STATEMENT_LINES = 40


def throw_lines(source: list[str]) -> set[int]:
    """1-based numbers of the lines that belong to a throwing statement.

    A statement runs from the match to the first `;` outside parentheses, so
    `{ throw Exception(...); }` on one line and a throw inside a macro body end
    where the throw ends, not at the end of the line.
    """
    result: set[int] = set()
    i = 0
    while i < len(source):
        # Match code only: a comment or a string that mentions `throw Exception`
        # must not start a statement and swallow the code that follows it.
        code = LINE_COMMENT.sub("", STRING_LITERAL.sub('""', source[i]))
        match = THROW_START.search(code)
        if not match:
            i += 1
            continue
        depth = 0
        j = i
        pos = match.start()
        ended = False
        while j < len(source) and j - i < MAX_STATEMENT_LINES:
            result.add(j + 1)
            for ch in code[pos:]:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                elif ch == ";" and depth <= 0:
                    ended = True
                    break
            if ended:
                break
            j += 1
            if j < len(source):
                code = LINE_COMMENT.sub("", STRING_LITERAL.sub('""', source[j]))
                pos = 0
        i = j + 1
    return result


def process_section(lines: list[str], missing_sources: list[str]) -> list[str]:
    # A section may start with `TN:` (the test name) before `SF:`.
    source_path = next((l[3:] for l in lines if l.startswith("SF:")), None)
    if source_path is None or not os.path.isfile(source_path):
        if source_path is not None:
            missing_sources.append(source_path)
        return lines

    with open(source_path, encoding="utf-8", errors="replace") as f:
        excluded = throw_lines(f.read().split("\n"))
    if not excluded:
        return lines

    kept: list[str] = []
    lf = lh = brf = brh = 0
    for line in lines:
        if line.startswith("DA:"):
            line_no, count = line[3:].split(",")[:2]
            if int(line_no) in excluded:
                continue
            lf += 1
            lh += int(count) > 0
        elif line.startswith("BRDA:"):
            fields = line[5:].split(",")
            if int(fields[0]) in excluded:
                continue
            brf += 1
            brh += fields[3] not in ("-", "0")
        elif line.startswith(("LF:", "LH:", "BRF:", "BRH:")):
            continue
        elif line == "end_of_record":
            kept += [f"BRF:{brf}", f"BRH:{brh}", f"LF:{lf}", f"LH:{lh}"]
        kept.append(line)
    return kept


def main() -> None:
    if len(sys.argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    in_path = sys.argv[1]
    in_place = len(sys.argv) == 2
    if in_place:
        fd, out_path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(in_path)))
        os.close(fd)
    else:
        out_path = sys.argv[2]

    lines_before = lines_after = 0
    missing_sources: list[str] = []
    with open(in_path, encoding="utf-8", errors="replace") as fin, open(
        out_path, "w", encoding="utf-8"
    ) as fout:
        section: list[str] = []
        for raw in fin:
            line = raw.rstrip("\n")
            section.append(line)
            if line != "end_of_record":
                continue
            lines_before += sum(1 for l in section if l.startswith("DA:"))
            section = process_section(section, missing_sources)
            lines_after += sum(1 for l in section if l.startswith("DA:"))
            fout.write("\n".join(section) + "\n")
            section = []
        if section:
            fout.write("\n".join(section) + "\n")

    if in_place:
        os.replace(out_path, in_path)

    print(f"Excluded {lines_before - lines_after} of {lines_before} lines in throwing statements")
    if missing_sources:
        # Such a file keeps its throw lines, so its coverage can only be understated.
        print(f"Source not found for {len(missing_sources)} file(s), left unfiltered:")
        for path in missing_sources[:20]:
            print(f"  {path}")


if __name__ == "__main__":
    main()
