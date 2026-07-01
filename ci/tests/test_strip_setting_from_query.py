import os
import sys

import pytest

# `strip_setting_from_query` lives next to the perf-test runner. `perf.py`
# itself executes its whole body on import (argparse, scipy, a server
# connection), so the scanner was factored into an import-safe sibling module
# to make it testable in isolation.
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "tests", "performance", "scripts"
    ),
)

from perf_create_query_utils import strip_setting_from_query  # noqa: E402

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
    query = (
        "CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS index_granularity = 8192 "
        f"AS SELECT a, b FROM src SETTINGS {SETTING} = 0"
    )
    assert strip_setting_from_query(query, SETTING) == query


def test_setting_in_table_settings_wins_over_occurrence_after_as():
    # The real setting is in the table SETTINGS and the name also appears in the
    # SELECT after `AS`. Only the table setting is stripped; the SELECT (commas
    # and all) is preserved intact.
    query = (
        f"CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 0, "
        f"index_granularity = 8192 AS SELECT a, b, c FROM src SETTINGS {SETTING} = 0"
    )
    expected = (
        "CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() SETTINGS "
        f"index_granularity = 8192 AS SELECT a, b, c FROM src SETTINGS {SETTING} = 0"
    )
    assert strip_setting_from_query(query, SETTING) == expected


def test_setting_name_inside_comment_literal_is_preserved():
    # The setting name appears both inside a column COMMENT literal and as the
    # real setting. Only the real setting must be removed; the literal text
    # (which mentions the setting and even a comma) is part of the table schema
    # and must survive unchanged.
    query = (
        f"CREATE TABLE t (a UInt64 COMMENT 'set {SETTING} = 1 here, ok') "
        f"ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1, index_granularity = 8192"
    )
    expected = (
        f"CREATE TABLE t (a UInt64 COMMENT 'set {SETTING} = 1 here, ok') "
        "ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
    )
    assert strip_setting_from_query(query, SETTING) == expected


def test_settings_keyword_inside_comment_literal_is_not_matched():
    # A literal containing the word `SETTINGS` before the real clause must not
    # be picked up as the clause to edit.
    query = (
        f"CREATE TABLE t (a UInt64 COMMENT 'SETTINGS {SETTING} = 9') "
        f"ENGINE = MergeTree ORDER BY tuple() SETTINGS {SETTING} = 1"
    )
    expected = (
        f"CREATE TABLE t (a UInt64 COMMENT 'SETTINGS {SETTING} = 9') "
        "ENGINE = MergeTree ORDER BY tuple()"
    )
    assert strip_setting_from_query(query, SETTING) == expected


def test_no_table_settings_only_query_settings_after_as_select_is_unchanged():
    # There is no table-level SETTINGS clause; the only SETTINGS is a
    # query-level clause after `AS SELECT`. The initial keyword search must stop
    # at the top-level `AS` and report "no table SETTINGS" instead of latching
    # onto the query-level clause. Otherwise the helper would silently strip a
    # query-level clause, letting `perf.py` continue with the PR side on the new
    # default and the baseline on the old one -- invalidating the comparison
    # instead of surfacing the fixture bug. The query must be byte-for-byte.
    query = (
        "CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() "
        f"AS SELECT a, b FROM src SETTINGS {SETTING} = 0"
    )
    assert strip_setting_from_query(query, SETTING) == query


def test_no_table_settings_as_other_table_query_settings_is_unchanged():
    # `CREATE TABLE t AS src SETTINGS ...`: the trailing SETTINGS is query-level
    # (there is no table-level clause), so the helper must leave it untouched.
    query = f"CREATE TABLE t AS src SETTINGS {SETTING} = 0"
    assert strip_setting_from_query(query, SETTING) == query


def test_no_table_settings_empty_as_select_query_settings_is_unchanged():
    # `EMPTY AS SELECT` also terminates the table definition; a SETTINGS after it
    # is query-level and must not be stripped.
    query = (
        "CREATE TABLE t ENGINE = MergeTree ORDER BY tuple() "
        f"EMPTY AS SELECT a FROM src SETTINGS {SETTING} = 0"
    )
    assert strip_setting_from_query(query, SETTING) == query


def test_table_settings_after_column_comment_is_still_found():
    # A column-level COMMENT inside the schema parens must not be mistaken for a
    # top-level trailing clause that ends the search early: the real table-level
    # SETTINGS still follows and its target setting must be stripped.
    query = (
        "CREATE TABLE t (a UInt64 COMMENT 'note') ENGINE = MergeTree "
        f"ORDER BY tuple() SETTINGS {SETTING} = 1, index_granularity = 8192"
    )
    expected = (
        "CREATE TABLE t (a UInt64 COMMENT 'note') ENGINE = MergeTree "
        "ORDER BY tuple() SETTINGS index_granularity = 8192"
    )
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


@pytest.mark.parametrize(
    "case", VALUE_AWARE_CASES, ids=[c[0] for c in VALUE_AWARE_CASES]
)
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


@pytest.mark.parametrize(
    "case", VALUE_AWARE_CASES, ids=[c[0] for c in VALUE_AWARE_CASES]
)
def test_value_aware_first_of_two(case):
    # The value-aware guard must also apply when the setting is not the only
    # entry: a non-default value leaves the whole SETTINGS clause intact.
    _name, value, should_strip = case
    query = (
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() "
        f"SETTINGS {SETTING} = {value}, index_granularity = 8192"
    )
    result = strip_setting_from_query(query, SETTING, ALLOWED)
    if should_strip:
        assert result == (
            "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() "
            "SETTINGS index_granularity = 8192"
        )
    else:
        assert result == query


def test_value_aware_none_strips_regardless_of_value():
    # Without `allowed_values` the value is not inspected, so an enabled value
    # is still stripped. This preserves the default (name-only) behavior.
    query = (
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() "
        f"SETTINGS {SETTING} = 1"
    )
    expected = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()"
    assert strip_setting_from_query(query, SETTING) == expected
