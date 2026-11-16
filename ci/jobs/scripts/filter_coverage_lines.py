#!/usr/bin/env python3
"""
Drop lines that carry no logic from an LCOV tracefile.

Two kinds of lines are removed, both decided from the source text alone, so
that the same source always yields the same set of lines whatever the tests
executed. `lcov --filter brace` is not used for that reason: it keeps or drops
a brace depending on the hit counts, and newly_covered_lines.py compares the
line sets of a baseline and the current run exactly.

1. Brace-only lines: a line holding only `{`, or only `}` (with `;`, `,`, `)`).
   ClickHouse puts every brace on its own line. A closing brace after a
   `return` or `throw` is never executed, and an opening brace only repeats
   the count of the line that opens its block.

2. Throwing statements: `throw Exception`, `throw ErrnoException`,
   `throwFromErrno`, `throw std::`, `UNREACHABLE`, `std::terminate`, up to the
   `;` that ends the statement. Counting them measures how many error messages
   the tests trigger rather than how much logic they reach. The check before
   the throw stays: a line with other code in front of the throw, such as
   `if (x) throw Exception(...);`, is kept with its branches, and only the
   following lines of the statement are removed. lcov's own `lcov_excl_line`
   cannot express this because it matches one line only, while ClickHouse
   formats a throw as

       throw Exception(ErrorCodes::BAD_ARGUMENTS,
           "Message {}", argument);

Removed: `DA:` and `BRDA:` records of those lines, both covered and uncovered.
`LF:`/`LH:`/`BRF:`/`BRH:` are recomputed. Function records are kept as is.

Usage:
    python3 filter_coverage_lines.py input.info [output.info]

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
BRACE_ONLY_LINE = re.compile(r"^\s*(\{|\}[\s;,)]*)\s*$")
# What may precede a throw on its first line for the line to be removed: block
# punctuation, `else`, and `case`/`default` labels. Anything else is a check.
NO_CODE_BEFORE_THROW = re.compile(r"^[\s{}]*(else\b[\s{]*)?((case\b[^:]*|default)\s*:[\s{]*)?$")
# A statement longer than this is not a throw formatted over several lines but
# a parsing mistake (an unbalanced parenthesis in a raw string, a macro). Stop
# there rather than swallowing unrelated code.
MAX_STATEMENT_LINES = 40


def mask_comments_and_strings(source: list[str]) -> list[str]:
    """The source with comments and the contents of string and character
    literals replaced by spaces, line by line, so line numbers and columns are
    kept. A comment or a message that mentions `throw Exception` or holds a
    brace must not be taken for code. `/* */` comments may span lines."""
    masked: list[str] = []
    in_block_comment = False
    for line in source:
        out = []
        i = 0
        quote = None
        while i < len(line):
            ch = line[i]
            nxt = line[i + 1] if i + 1 < len(line) else ""
            if in_block_comment:
                if ch == "*" and nxt == "/":
                    in_block_comment = False
                    out.append("  ")
                    i += 2
                    continue
                out.append(" ")
            elif quote:
                if ch == "\\":
                    out.append("  ")
                    i += 2
                    continue
                if ch == quote:
                    quote = None
                    out.append(ch)
                else:
                    out.append(" ")
            elif ch == "/" and nxt == "/":
                break
            elif ch == "/" and nxt == "*":
                in_block_comment = True
                out.append("  ")
                i += 2
                continue
            elif ch in "\"'":
                quote = ch
                out.append(ch)
            else:
                out.append(ch)
            i += 1
        masked.append("".join(out))
    return masked


def brace_lines(code: list[str]) -> set[int]:
    """1-based numbers of the lines that hold only an opening or a closing
    brace. `code` is the output of `mask_comments_and_strings`."""
    return {i + 1 for i, line in enumerate(code) if BRACE_ONLY_LINE.match(line)}


def throw_lines(code: list[str]) -> set[int]:
    """1-based numbers of the lines that belong to a throwing statement.
    `code` is the output of `mask_comments_and_strings`.

    A statement runs from the match to the first `;` outside parentheses, so
    `{ throw Exception(...); }` on one line and a throw inside a macro body end
    where the throw ends, not at the end of the line.
    """
    result: set[int] = set()
    i = 0
    while i < len(code):
        match = THROW_START.search(code[i])
        if not match:
            i += 1
            continue
        depth = 0
        j = i
        pos = match.start()
        ended = False
        while j < len(code) and j - i < MAX_STATEMENT_LINES:
            if j > i or NO_CODE_BEFORE_THROW.match(code[i][:pos]):
                result.add(j + 1)
            for ch in code[j][pos:]:
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
        code = mask_comments_and_strings(f.read().split("\n"))
    excluded = brace_lines(code) | throw_lines(code)
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
            # lcov may write a large summed count in scientific notation.
            lh += float(count) > 0
        elif line.startswith("BRDA:"):
            fields = line[5:].split(",")
            if int(fields[0]) in excluded:
                continue
            brf += 1
            brh += fields[3] != "-" and float(fields[3]) > 0
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

    print(f"Excluded {lines_before - lines_after} of {lines_before} lines: brace-only lines and throwing statements")
    if missing_sources:
        # Such a file keeps its throw lines, so its coverage can only be understated.
        print(f"Source not found for {len(missing_sources)} file(s), left unfiltered:")
        for path in missing_sources[:20]:
            print(f"  {path}")


if __name__ == "__main__":
    main()
