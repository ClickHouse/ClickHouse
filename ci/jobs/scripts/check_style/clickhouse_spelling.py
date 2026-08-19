"""Utilities shared by checks that validate `ClickHouse` spelling."""

import re


# The conventional lowercase and uppercase forms are accepted in identifiers and tools.
CLICKHOUSE_CORRECT_SPELLINGS = ("ClickHouse", "clickhouse", "CLICKHOUSE")

# Keep the space-separated form word-anchored so that, for example, `click Household` is not
# considered a product-name spelling. Other forms deliberately match inside identifiers.
CLICKHOUSE_ANY_SPELLING = (
    r"[Cc][Ll][Ii][Cc][Kk][_-]?[Hh][Oo][Uu][Ss][Ee]"
    r"|(?<![A-Za-z])[Cc][Ll][Ii][Cc][Kk] [Hh][Oo][Uu][Ss][Ee](?![A-Za-z])"
)
CLICKHOUSE_ANY_SPELLING_RE = re.compile(CLICKHOUSE_ANY_SPELLING)

# URLs refer to external resources whose spelling cannot be changed. Replace them with
# equal-length spaces to preserve word boundaries around the URL.
URL_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"'`)\]}<>]*")


def clickhouse_misspellings(text: str) -> list[str]:
    """Return non-canonical product-name spellings in text, excluding URLs."""
    text = URL_RE.sub(lambda match: " " * len(match.group(0)), text)
    return [
        match.group(0)
        for match in CLICKHOUSE_ANY_SPELLING_RE.finditer(text)
        if match.group(0) not in CLICKHOUSE_CORRECT_SPELLINGS
    ]
