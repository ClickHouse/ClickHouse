#!/usr/bin/env python3
"""Filter out style-check hits that fall inside embedded documentation.

The heuristic C++ style checks (tabs, trailing whitespace, indentation, spacing)
must not apply to the verbatim Markdown documentation embedded into the format
source files as `R"DOCS_MD( ... )DOCS_MD"` raw-string literals: that text
legitimately contains literal tabs (`TabSeparated`/`TSV` examples), result
tables indented by one to three spaces (`Pretty` formats), trailing whitespace
inherited from the Markdown pages, and so on.

This reads `path:lineno:...` lines (as produced by `rg -n` / `grep -n`) from
stdin and prints only those whose line is NOT inside such a raw-string literal.
It exits with code 0 if at least one line was printed (i.e. a real violation
remains), and 1 otherwise, so callers can keep the usual

    <check producing path:lineno hits> | filter_embedded_docs.py && echo "violation"

idiom working unchanged.
"""

import sys

OPEN = 'R"DOCS_MD('
CLOSE = ')DOCS_MD"'

_cache = {}


def embedded_doc_lines(path):
    """Return the set of 1-based line numbers covered by DOCS_MD raw strings."""
    if path in _cache:
        return _cache[path]
    exempt = set()
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            in_raw = False
            for lineno, line in enumerate(fh, start=1):
                if not in_raw:
                    idx = line.find(OPEN)
                    if idx != -1:
                        exempt.add(lineno)
                        # may also close on the same line
                        if CLOSE in line[idx + len(OPEN):]:
                            pass  # single-line raw string, stays closed
                        else:
                            in_raw = True
                else:
                    exempt.add(lineno)
                    if CLOSE in line:
                        in_raw = False
    except OSError:
        pass
    _cache[path] = exempt
    return exempt


def main():
    printed = False
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        # Expect "path:lineno:..." — keep lines we cannot parse (fail open: report).
        parts = line.split(":", 2)
        if len(parts) >= 2 and parts[1].isdigit():
            path, lineno = parts[0], int(parts[1])
            if lineno in embedded_doc_lines(path):
                continue
        sys.stdout.write(raw)
        printed = True
    sys.exit(0 if printed else 1)


if __name__ == "__main__":
    main()
