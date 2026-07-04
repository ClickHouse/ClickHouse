#!/usr/bin/env python3
"""
Coverage checks for domains whose hand-written pages are richer than the
short summaries embedded in the source (dictionary layouts, dictionary
sources, aggregate function combinators): generating those pages would
destroy content, so instead every item exposed by the system table must map
to an existing page (or heading anchor). An unmapped item fails the check --
a newly added layout/source/combinator cannot ship undocumented.

Once the embedded documentation is enriched upstream to full-page bodies,
these domains can move to the catalog generators.
"""

import os
import re

import introspect

# Dictionary layouts -> pages under statements/create/dictionary/layouts/.
# The complex_key_* variants and the polygon/hashed refinements are documented
# on their base layout's page.
LAYOUT_PAGES = {
    "flat": "flat",
    "hashed": "hashed",
    "sparse_hashed": "hashed",
    "complex_key_hashed": "hashed",
    "complex_key_sparse_hashed": "hashed",
    "hashed_array": "hashed-array",
    "complex_key_hashed_array": "hashed-array",
    "range_hashed": "range-hashed",
    "complex_key_range_hashed": "range-hashed",
    "cache": "cache",
    "complex_key_cache": "cache",
    "ssd_cache": "ssd-cache",
    "complex_key_ssd_cache": "ssd-cache",
    "direct": "direct",
    "complex_key_direct": "direct",
    "ip_trie": "ip-trie",
    "polygon": "polygon",
    "polygon_simple": "polygon",
    "polygon_index_each": "polygon",
    "polygon_index_cell": "polygon",
    "regexp_tree": "regexp-tree",
}

# Dictionary sources -> pages under statements/create/dictionary/sources/.
SOURCE_PAGES = {
    "file": "local-file",
    "executable": "executable-file",
    "executable_pool": "executable-pool",
    "http": "http",
    "clickhouse": "clickhouse",
    "mysql": "mysql",
    "postgresql": "postgresql",
    "mongodb": "mongodb",
    "redis": "redis",
    "cassandra": "cassandra",
    "odbc": "odbc",
    "null": "null",
    "ytsaurus": "ytsaurus",
    "yamlregexptree": "yamlregexptree",
}

# Known pre-existing documentation gaps (bridge-based sources with no page).
# Listed so the check passes today while any NEW gap still fails; writing
# these pages removes the entry.
KNOWN_UNDOCUMENTED_SOURCES = {"library", "jdbc"}

LAYOUTS_DIR = "reference/statements/create/dictionary/layouts"
SOURCES_DIR = "reference/statements/create/dictionary/sources"
COMBINATORS_PAGE = "reference/functions/aggregate-functions/combinators.mdx"


def _check_pages(docs_dir, rows, mapping, rel_dir, kind, known_gaps=()):
    problems = []
    for row in rows:
        name = row["name"]
        page = mapping.get(name)
        if page is None:
            if name in known_gaps:
                continue
            problems.append(
                f"{kind} '{name}' has no documentation page under {rel_dir}/"
            )
            continue
        if not os.path.isfile(os.path.join(docs_dir, rel_dir, page + ".mdx")):
            problems.append(
                f"{kind} '{name}' maps to missing page {rel_dir}/{page}.mdx"
            )
    return problems


def run_checks(binary, docs_dir):
    """Returns a list of problem strings (empty when coverage is complete)."""
    problems = []

    problems += _check_pages(
        docs_dir,
        introspect.fetch_rows(
            binary, "SELECT name FROM system.dictionary_layouts ORDER BY name"
        ),
        LAYOUT_PAGES, LAYOUTS_DIR, "dictionary layout",
    )

    problems += _check_pages(
        docs_dir,
        introspect.fetch_rows(
            binary, "SELECT name FROM system.dictionary_sources ORDER BY name"
        ),
        SOURCE_PAGES, SOURCES_DIR, "dictionary source",
        known_gaps=KNOWN_UNDOCUMENTED_SOURCES,
    )

    # Combinators are documented as `## -If {#-if}` sections of one page.
    combinators_path = os.path.join(docs_dir, COMBINATORS_PAGE)
    with open(combinators_path, encoding="utf-8") as f:
        anchors = set(re.findall(r"\{#([^}]+)\}", f.read()))
    for row in introspect.fetch_rows(
        binary,
        "SELECT name FROM system.aggregate_function_combinators"
        " WHERE NOT is_internal ORDER BY name",
    ):
        if f"-{row['name'].lower()}" not in anchors:
            problems.append(
                f"aggregate function combinator '-{row['name']}' has no"
                f" section anchor in {COMBINATORS_PAGE}"
            )
    return problems
