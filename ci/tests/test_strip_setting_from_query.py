import os
import sys

import pytest

# `strip_setting_from_query` lives next to the perf-test runner. `perf.py`
# itself executes its whole body on import (argparse, scipy, a server
# connection), so the scanner was factored into an import-safe sibling module
# to make it testable in isolation.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "tests", "performance", "scripts"),
)

from perf_create_query_utils import (  # noqa: E402
    create_query_engine,
    is_mergetree_create_query,
    strip_setting_from_query,
)

SETTING = "optimize_row_order_if_no_order_by"

# Each case is (input CREATE TABLE, expected output after stripping SETTING).
#
# `strip_setting_from_query` is a correctness-critical baseline rewrite: when a
# newly added MergeTree setting is missing on the older baseline server, the
# perf harness removes it from the CREATE TABLE and retries so both sides of
# the A/B comparison build the same table. A scanner-state regression here
# would silently rewrite the baseline DDL while the PR side keeps the original,
# invalidating the comparison instead of failing fast. The edge cases below are
# exactly the parser-state bugs found while developing the scanner: first /
# middle / last / only-setting cleanup, commas and the setting name inside
# string / comment / bracket / brace literals, and trailing `AS SELECT` /
# `COMMENT` clauses that terminate the SETTINGS clause.
CASES = [
    # Only setting: the whole SETTINGS clause (and its leading whitespace) goes.
    (
        "only_setting",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()",
    ),
    # Only setting, terminated by a semicolon: the `;` is kept.
    (
        "only_setting_semicolon",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1;",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple();",
    ),
    # First of two: the setting and its trailing comma separator are removed.
    (
        "first_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # Last of two: the setting and its preceding comma separator are removed.
    (
        "last_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, {SETTING} = 0",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # Middle of three: neighbours and their separators stay intact.
    (
        "middle_of_three",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, {SETTING} = 0, min_bytes_for_wide_part = 0",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0",
    ),
    # A comma inside a quoted value must not be mistaken for a setting separator.
    (
        "value_with_commas_in_string",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS storage_policy = 'a,b,c', {SETTING} = 1",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS storage_policy = 'a,b,c'",
    ),
    # Commas inside a bracketed (tuple) value of the following setting.
    (
        "value_tuple_brackets",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1, some_tuple_setting = (1, 2, 3)",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS some_tuple_setting = (1, 2, 3)",
    ),
    # Commas inside a brace (map) value of the preceding setting.
    (
        "value_brace_map",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS m = {{'x,y':1, 'z':2}}, {SETTING} = 1",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS m = {'x,y':1, 'z':2}",
    ),
    # `''` is an escaped quote inside a single-quoted value, not its end.
    (
        "escaped_quote_in_value",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS c = 'it''s, ok', {SETTING} = 1",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS c = 'it''s, ok'",
    ),
    # A block comment sitting in the comma separator must not orphan the comma.
    (
        "block_comment_separator",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 /* sep */, {SETTING} = 1",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 /* sep */",
    ),
    # A trailing `AS SELECT` terminates the SETTINGS clause and is preserved.
    (
        "trailing_as_select",
        f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1 AS SELECT number AS a FROM numbers(10)",
        "CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() AS SELECT number AS a FROM numbers(10)",
    ),
    # A trailing `COMMENT '...'` clause terminates the SETTINGS clause.
    (
        "trailing_comment_clause",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1 COMMENT 'my table'",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() COMMENT 'my table'",
    ),
    # First-of-two with a trailing clause: the clause stays, the other kept.
    (
        "trailing_as_select_first_of_two",
        f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1, index_granularity = 8192 AS SELECT 1 AS a",
        "CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 AS SELECT 1 AS a",
    ),
    # The SETTINGS keyword is matched case-insensitively.
    (
        "case_insensitive_settings_kw",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() settings {SETTING} = 1, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() settings index_granularity = 8192",
    ),
    # Comments are allowed anywhere whitespace is: a block comment between the
    # setting name and `=` must not defeat the match.
    (
        "block_comment_between_name_and_eq",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} /* keep */ = 0, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # A `--` line comment between the name and `=` (the `=` on the next line).
    (
        "line_comment_between_name_and_eq",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} -- keep\n = 1, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # A block comment between `=` and the value.
    (
        "block_comment_between_eq_and_value",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = /* keep */ 1, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
]


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_strip_setting_exact_output(case):
    _name, query, expected = case
    assert strip_setting_from_query(query, SETTING) == expected


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_strip_setting_is_idempotent(case):
    # After the setting is gone, stripping it again must be a no-op. This guards
    # against a scanner that keeps eating characters when its target is absent.
    _name, query, _expected = case
    once = strip_setting_from_query(query, SETTING)
    assert strip_setting_from_query(once, SETTING) == once


def test_no_settings_clause_is_unchanged():
    query = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a"
    assert strip_setting_from_query(query, SETTING) == query


def test_absent_setting_is_unchanged():
    # The setting is not present, but another setting is: the query must be
    # returned byte-for-byte so an unrelated baseline DDL is never rewritten.
    query = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING) == query


def test_setting_only_after_as_select_is_unchanged():
    # The setting is absent from the table's own SETTINGS clause and appears
    # only in a query-level SETTINGS after `AS SELECT`. The name scan must stop
    # at the `AS` boundary (as the value scan does) and leave the query
    # byte-for-byte unchanged, so `perf.py` re-raises the original error and
    # fails fast. Before the name scan honoured trailing clauses, it ran past
    # `AS SELECT` and cut from a comma in the SELECT column list, silently
    # rewriting `SELECT a, b FROM src ...` down to `SELECT a`.
    query = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 AS SELECT a, b FROM src SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == query


def test_setting_in_table_settings_wins_over_occurrence_after_as():
    # The real setting is in the table SETTINGS and the name also appears in the
    # SELECT after `AS`. Only the table setting is stripped; the SELECT (commas
    # and all) is preserved intact.
    query = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0, index_granularity = 8192 AS SELECT a, b, c FROM src SETTINGS {SETTING} = 0"
    expected = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 AS SELECT a, b, c FROM src SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == expected


def test_setting_name_inside_comment_literal_is_preserved():
    # The setting name appears both inside a column COMMENT literal and as the
    # real setting. Only the real setting must be removed; the literal text
    # (which mentions the setting and even a comma) is part of the table schema
    # and must survive unchanged.
    query = f"CREATE TABLE t (a UInt64 COMMENT 'set {SETTING} = 1 here, ok') ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1, index_granularity = 8192"
    expected = f"CREATE TABLE t (a UInt64 COMMENT 'set {SETTING} = 1 here, ok') ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING) == expected


def test_settings_keyword_inside_comment_literal_is_not_matched():
    # A literal containing the word `SETTINGS` before the real clause must not
    # be picked up as the clause to edit.
    query = f"CREATE TABLE t (a UInt64 COMMENT 'SETTINGS {SETTING} = 9') ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1"
    expected = f"CREATE TABLE t (a UInt64 COMMENT 'SETTINGS {SETTING} = 9') ENGINE = MergeTree ORDER BY tuple()"
    assert strip_setting_from_query(query, SETTING) == expected


def test_no_table_settings_only_query_settings_after_as_select_is_unchanged():
    # There is no table-level SETTINGS clause; the only SETTINGS is a
    # query-level clause after `AS SELECT`. The initial keyword search must stop
    # at the top-level `AS` and report "no table SETTINGS" instead of latching
    # onto the query-level clause. Otherwise the helper would silently strip a
    # query-level clause, letting `perf.py` continue with the PR side on the new
    # default and the baseline on the old one -- invalidating the comparison
    # instead of surfacing the fixture bug. The query must be byte-for-byte.
    query = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() AS SELECT a, b FROM src SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == query


def test_as_source_table_trailing_settings_is_table_level():
    # `CREATE TABLE t AS src SETTINGS ...`: `ParserStorage` accepts a bare
    # `SETTINGS` clause as the storage of the new table (`SHOW CREATE` places
    # it in the table's own SETTINGS), so the scanner strips it. `perf.py`
    # still fails fast on this shape, because without an `ENGINE` clause
    # `create_query_engine` cannot know the engine inherited from `src`.
    query = f"CREATE TABLE t AS src SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == "CREATE TABLE t AS src"
    assert not is_mergetree_create_query(query)


def test_storage_after_as_source_table_is_stripped():
    # `CREATE TABLE dst AS src ENGINE = MergeTree ... SETTINGS ...`: in the
    # no-column-list branch of `ParserCreateQuery.cpp` the storage clause may
    # follow `AS [db.]source_table`, so this SETTINGS belongs to the new
    # table and must be stripped (`tests/performance/polymorphic_parts_*.xml`
    # use this shape).
    query = f"CREATE TABLE dst AS src ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0"
    expected = "CREATE TABLE dst AS src ENGINE = MergeTree ORDER BY tuple()"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == expected
    assert is_mergetree_create_query(query)


def test_storage_after_as_source_table_keeps_other_settings():
    query = f"CREATE TABLE dst AS db.src ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, {SETTING} = 0"
    expected = "CREATE TABLE dst AS db.src ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == expected


def test_as_parenthesized_subquery_is_query_level():
    # `AS (` starts a parenthesized select query, so a SETTINGS after it is
    # query-level: the query must be byte-for-byte unchanged.
    query = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() AS (SELECT a FROM src) SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == query


def test_as_with_cte_select_is_query_level():
    # `AS WITH ... SELECT ...` also starts the select query; the trailing
    # SETTINGS is query-level and must not be edited.
    query = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() AS WITH c AS (SELECT 1) SELECT * FROM c SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == query


def test_clone_as_terminates_settings_clause():
    # `CREATE TABLE dst ENGINE = ... SETTINGS ... CLONE AS src`: `CLONE` is a
    # post-`SETTINGS` boundary (`ParserCreateQuery.cpp` parses it after the
    # storage clause), so the only setting is stripped and `CLONE AS src`
    # survives untouched.
    query = f"CREATE TABLE dst ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 CLONE AS src"
    expected = "CREATE TABLE dst ENGINE = MergeTree ORDER BY tuple() CLONE AS src"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == expected
    assert is_mergetree_create_query(query)


def test_clone_as_with_preceding_setting():
    query = f"CREATE TABLE dst ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, {SETTING} = 0 CLONE AS src"
    expected = "CREATE TABLE dst ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 CLONE AS src"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == expected


def test_clone_as_enabled_value_is_unchanged():
    # The value scan must stop at `CLONE`, so the value compares as `1` (not
    # `1 CLONE`) against the allowlist and the query is left unchanged for
    # the fail-fast path.
    query = f"CREATE TABLE dst ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1 CLONE AS src"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == query


def test_empty_as_source_table_settings_after_as_is_stripped():
    # `EMPTY AS src` followed by a storage clause: like the plain `AS src`
    # form, the SETTINGS after the source table belongs to the new table.
    query = f"CREATE TABLE dst EMPTY AS src ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0"
    expected = "CREATE TABLE dst EMPTY AS src ENGINE = MergeTree ORDER BY tuple()"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == expected


def test_no_table_settings_empty_as_select_query_settings_is_unchanged():
    # `EMPTY AS SELECT` also terminates the table definition; a SETTINGS after it
    # is query-level and must not be stripped.
    query = f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() EMPTY AS SELECT a FROM src SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == query


def test_no_table_settings_query_settings_after_table_comment_is_unchanged():
    # A top-level `COMMENT '...'` ends the engine definition, so the SETTINGS
    # after it is a query-level clause, not the table's own (this is exactly
    # what `03234_enable_secure_identifiers.sql` relies on). Stripping it would
    # silently rewrite the baseline DDL, so the query must be left unchanged
    # and `perf.py` must fail fast on the misplaced setting.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() COMMENT 'note' SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == query
    assert strip_setting_from_query(query, SETTING) == query


def test_no_table_settings_query_settings_after_table_comment_with_as_select_is_unchanged():
    # The same shape with a source `AS src` before the table comment: the
    # `COMMENT` still ends the engine definition.
    query = f"CREATE TABLE dst AS src ENGINE = MergeTree ORDER BY tuple() COMMENT 'note' SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == query


def test_table_settings_before_table_comment_is_still_stripped():
    # The `COMMENT` boundary must only apply *before* a table-level SETTINGS
    # clause: when the table has its own SETTINGS followed by a table comment,
    # the setting is still stripped and the comment survives.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0, index_granularity = 8192 COMMENT 'note'"
    expected = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 COMMENT 'note'"
    assert strip_setting_from_query(query, SETTING, {"0", "false"}) == expected


def test_table_settings_after_column_comment_is_still_found():
    # A column-level COMMENT inside the schema parens must not be mistaken for a
    # top-level trailing clause that ends the search early: the real table-level
    # SETTINGS still follows and its target setting must be stripped.
    query = f"CREATE TABLE t (a UInt64 COMMENT 'note') ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1, index_granularity = 8192"
    expected = "CREATE TABLE t (a UInt64 COMMENT 'note') ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING) == expected


# `allowed_values` makes the strip value-aware. Stripping is only
# semantics-preserving when the fixture pins the setting to a value equivalent
# to the baseline server's default; `perf.py` passes {"0", "false"} for
# `optimize_row_order_if_no_order_by`. An enabled value (`= 1`) would build an
# unoptimized baseline table while the PR side uses the optimized layout, so the
# helper must leave the query unchanged and let `perf.py` re-raise
# UNKNOWN_SETTING (fail fast) instead of silently comparing different layouts.
ALLOWED = {"0", "false"}

# (name, value_literal, should_strip)
VALUE_AWARE_CASES = [
    ("zero", "0", True),
    ("false_lower", "false", True),
    ("false_upper", "FALSE", True),
    ("one", "1", False),
    ("true_lower", "true", False),
    ("true_upper", "TRUE", False),
]


@pytest.mark.parametrize("case", VALUE_AWARE_CASES, ids=[c[0] for c in VALUE_AWARE_CASES])
def test_value_aware_only_strips_baseline_default(case):
    _name, value, should_strip = case
    base = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()"
    query = f"{base} SETTINGS {SETTING} = {value}"
    result = strip_setting_from_query(query, SETTING, ALLOWED)
    if should_strip:
        assert result == base
    else:
        # Not a baseline-default value: query must be byte-for-byte unchanged.
        assert result == query


@pytest.mark.parametrize("case", VALUE_AWARE_CASES, ids=[c[0] for c in VALUE_AWARE_CASES])
def test_value_aware_first_of_two(case):
    # The value-aware guard must also apply when the setting is not the only
    # entry: a non-default value leaves the whole SETTINGS clause intact.
    _name, value, should_strip = case
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = {value}, index_granularity = 8192"
    result = strip_setting_from_query(query, SETTING, ALLOWED)
    if should_strip:
        assert result == ("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192")
    else:
        assert result == query


def test_value_aware_none_strips_regardless_of_value():
    # Without `allowed_values` the value is not inspected, so an enabled value
    # is still stripped. This preserves the default (name-only) behavior.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1"
    expected = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()"
    assert strip_setting_from_query(query, SETTING) == expected


# Regression: a baseline-default value followed by a SQL comment must still be
# recognized as a baseline default and stripped. The value scan breaks only at
# the next top-level comma / trailing clause, so a comment between the value
# and that boundary lands inside the extracted value text (`0 /* keep */`,
# `false -- keep`). The value-aware guard must normalize comments out before
# comparing against `allowed_values`; otherwise `perf.py` re-raises
# `UNKNOWN_SETTING` on the baseline even though the fixture pinned the
# baseline-default value.
COMMENTED_VALUE_STRIP_CASES = [
    # Only setting, trailing block comment: the comment goes with the clause.
    (
        "block_comment_after_zero_only_setting",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 /* keep */",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()",
    ),
    # Only setting, trailing `--` line comment.
    (
        "line_comment_after_false_only_setting",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = false -- keep\n",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()",
    ),
    # First of two, block comment between the value and the separator comma.
    (
        "block_comment_after_zero_first_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 /* keep */, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # First of two, `#` line comment between the value and the separator comma.
    (
        "hash_comment_after_zero_first_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 # keep\n, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
]


@pytest.mark.parametrize(
    "case",
    COMMENTED_VALUE_STRIP_CASES,
    ids=[c[0] for c in COMMENTED_VALUE_STRIP_CASES],
)
def test_value_aware_strips_commented_baseline_default(case):
    _name, query, expected = case
    assert strip_setting_from_query(query, SETTING, ALLOWED) == expected


def test_value_aware_commented_enabled_value_is_unchanged():
    # A commented *enabled* value must still fail the guard: comments are
    # normalized out, leaving `1`, which is not a baseline default, so the
    # query is returned byte-for-byte and `perf.py` fails fast.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1 /* keep */"
    assert strip_setting_from_query(query, SETTING, ALLOWED) == query


def test_value_aware_comment_markers_inside_string_value_are_kept():
    # Comment markers inside a quoted value are part of the value, not real
    # comments, so the normalizer must keep them: the value `'0 /* x */'` (a
    # string) normalizes to `'0 /* x */'`, which is not a baseline default, so
    # the query is returned unchanged rather than mis-stripped as a bare `0`.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = '0 /* x */'"
    assert strip_setting_from_query(query, SETTING, ALLOWED) == query


def test_value_aware_comment_between_name_and_eq_is_stripped():
    # Regression: a comment between the setting name and `=` used to be a hard
    # mismatch (the name scan only skipped whitespace before `=`), so the query
    # was returned unchanged and `perf.py` re-raised `UNKNOWN_SETTING` on the
    # baseline even though the fixture pinned a baseline-equivalent value.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} /* keep */ = 0, index_granularity = 8192"
    expected = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING, ALLOWED) == expected


def test_value_aware_comment_between_eq_and_value_is_stripped():
    # Same regression on the other side of `=`: a comment between `=` and the
    # value must not shift `value_start` onto the comment text, which would
    # make the baseline-default value miss the allowlist.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = -- keep\n 0, index_granularity = 8192"
    expected = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING, ALLOWED) == expected


# Regression: the scanner must accept the full comment grammar of
# `src/Parsers/Lexer.cpp`, not a reduced one. It used to miss `//` line
# comments and nesting inside `/* ... */` block comments, so a valid fixture
# using either shape came back unchanged and `perf.py` re-raised
# `UNKNOWN_SETTING` on the baseline instead of stripping the
# baseline-equivalent assignment.
LEXER_COMMENT_STRIP_CASES = [
    # `//` line comment between the value and the separator comma.
    (
        "slash_slash_comment_after_zero_first_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()\nSETTINGS {SETTING} = 0 // keep\n, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()\nSETTINGS index_granularity = 8192",
    ),
    # `//` line comment after the only setting.
    (
        "slash_slash_comment_after_false_only_setting",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = false // keep\n",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()",
    ),
    # Nested block comment (SQL standard) between the value and the comma.
    (
        "nested_block_comment_after_zero_first_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 /* keep /* nested */ still */, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # Nested block comment between the setting name and `=`.
    (
        "nested_block_comment_between_name_and_eq",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} /* a /* b */ c */ = 0, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
    # `#!` line comment (shebang form) between the value and the comma.
    (
        "hash_bang_comment_after_zero_first_of_two",
        f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 #! keep\n, index_granularity = 8192",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
    ),
]


@pytest.mark.parametrize(
    "case",
    LEXER_COMMENT_STRIP_CASES,
    ids=[c[0] for c in LEXER_COMMENT_STRIP_CASES],
)
def test_lexer_comment_grammar_is_mirrored(case):
    _name, query, expected = case
    assert strip_setting_from_query(query, SETTING, ALLOWED) == expected


def test_slash_slash_inside_string_value_is_kept():
    # `//` inside a quoted value is part of the value, not a comment: the
    # value normalizes to the whole string literal, misses the allowlist, and
    # the query is returned unchanged so `perf.py` fails fast.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = '0 // x'"
    assert strip_setting_from_query(query, SETTING, ALLOWED) == query


def test_bare_hash_is_not_a_comment():
    # `Lexer.cpp` recognizes `#` as a comment only when followed by a space or
    # `!` (`#hello` is an error token). The scanner mirrors that: `0 #tag` is
    # not `0` followed by a comment, so the value misses the allowlist and the
    # query is returned unchanged for `perf.py` to fail fast on the (invalid)
    # fixture instead of stripping it on the baseline side only.
    query = f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0 #tag\n, index_granularity = 8192"
    assert strip_setting_from_query(query, SETTING, ALLOWED) == query


# `perf.py` only strips a `MergeTree` setting from a `CREATE TABLE` of the
# `MergeTree` family. On any other engine an `UNKNOWN_SETTING` means the
# fixture itself is wrong: the setting cannot affect that table, so silently
# rewriting the query would let a broken fixture benchmark the wrong setup on
# both sides instead of failing fast.
MERGETREE_ENGINE_CASES = [
    ("plain", "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()", True),
    (
        "replacing",
        "CREATE TABLE t (a UInt64) ENGINE = ReplacingMergeTree ORDER BY tuple()",
        True,
    ),
    (
        "replicated_with_args",
        "CREATE TABLE t (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t', 'r1') ORDER BY tuple()",
        True,
    ),
    (
        "shared",
        "CREATE TABLE t (a UInt64) ENGINE = SharedMergeTree ORDER BY tuple()",
        True,
    ),
    ("no_spaces", "CREATE TABLE t (a UInt64) ENGINE=MergeTree ORDER BY tuple()", True),
    (
        "comment_before_engine_name",
        "CREATE TABLE t (a UInt64) ENGINE = /* engine */ MergeTree ORDER BY tuple()",
        True,
    ),
    ("memory", "CREATE TABLE t (a UInt64) ENGINE = Memory", False),
    ("log", "CREATE TABLE t (a UInt64) ENGINE = Log", False),
    ("null_engine", "CREATE TABLE t (a UInt64) ENGINE = Null", False),
    (
        "distributed_over_mergetree",
        "CREATE TABLE t AS src ENGINE = Distributed(cluster, currentDatabase(), src_mergetree)",
        False,
    ),
    (
        "engine_name_only_in_a_literal",
        "CREATE TABLE t (a UInt64 COMMENT 'ENGINE = MergeTree') ENGINE = Memory",
        False,
    ),
    ("no_engine_clause", "CREATE VIEW v AS SELECT 1", False),
]


@pytest.mark.parametrize(
    "query,expected",
    [(q, e) for _, q, e in MERGETREE_ENGINE_CASES],
    ids=[name for name, _, _ in MERGETREE_ENGINE_CASES],
)
def test_is_mergetree_create_query(query, expected):
    assert is_mergetree_create_query(query) is expected


def test_create_query_engine_ignores_engine_inside_a_column_comment():
    # The engine scan must not be fooled by the schema parentheses: an
    # `ENGINE` written inside a column `COMMENT` literal is not the table's
    # engine, and mistaking it for one would re-enable the baseline rewrite
    # for a non-`MergeTree` fixture.
    query = "CREATE TABLE t (a UInt64 COMMENT 'ENGINE = MergeTree') ENGINE = Memory"
    assert create_query_engine(query) == "Memory"


def test_non_mergetree_fixture_is_not_rewritten_by_the_scanner():
    # End-to-end shape of the rejected case: even though the scanner *could*
    # cut the assignment out of the SETTINGS clause, `perf.py` never calls it
    # for a non-`MergeTree` engine, so the baseline keeps failing fast with
    # `UNKNOWN_SETTING` on such a fixture.
    query = f"CREATE TABLE t (a UInt64) ENGINE = Memory SETTINGS {SETTING} = 0"
    assert not is_mergetree_create_query(query)
