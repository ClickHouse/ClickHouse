#!/usr/bin/env python3
"""Patch the SQLancer++ source tree for use against ClickHouse:

1. Replace the legacy `ru.yandex.clickhouse:clickhouse-jdbc:0.3.2` Maven
   dependency in `pom.xml` with the `com.clickhouse:clickhouse-jdbc` shaded-all
   artifact, pinned to the same version as the SQLancer job's
   `com.clickhouse:client-v2` (0.9.8). At 0.9.x the driver's default
   implementation (`com.clickhouse.jdbc.ClickHouseDriver`) is the client-v2
   backed one - V1 is only used when `clickhouse.jdbc.v1=true` - so SQLancer++
   reaches the server over the same client-v2 transport as the SQLancer job.
   The legacy driver's HTTP response parser can't handle current ClickHouse
   server output.
2. Extend the `CLICKHOUSE.url` template in `dbconfigs/jdbc.properties` to carry
   `user` and `password` query parameters and to turn ClickHouse-framed response
   compression off. SQLancer++'s generic provider only passes the URL string (no
   JDBC Properties) into `DriverManager.getConnection`, so the new driver - which
   requires credentials - needs them embedded here.
3. Replace the `IS TRUE` postfix in `GeneralNoRECOracle.java` with `!= 0`.
   NoREC's row-counter must agree with `WHERE <expr>`, which ClickHouse
   evaluates by truthiness (any non-zero numeric is included, `NULL` is
   excluded). `<expr> != 0` reproduces that exactly: non-zero -> 1,
   zero -> 0, `NULL` -> `NULL` (counted as not matching), matching `WHERE`.
   Neither `<expr> IS TRUE` nor `<expr> = TRUE` works here: ClickHouse parses
   `IS TRUE` as `<expr> <=> true`, i.e. strict equality with `1`, so a truthy
   but non-`1` value such as `5` or `LN(x)` would compare unequal and produce
   a false-positive oracle mismatch. (`IS TRUE` itself is only accepted since
   https://github.com/ClickHouse/ClickHouse/pull/99997; before that the
   unpatched oracle was a guaranteed `SYNTAX_ERROR`.)
4. Stop `GeneralQueryPartitioningWhere.check` from turning an `IgnoreMeException`
   into an `AssertionError`. When the WHERE-stripped baseline query hits an
   expected (whitelisted) error, `ComparatorHelper.getResultSetFirstColumnAsString`
   sees a `null` result set and throws `IgnoreMeException` - a "skip this
   iteration" control-flow signal, not a bug. The upstream oracle catches it in
   `catch (Exception e)` and, for simple queries (no joins, <= 2 tables),
   re-throws it as `AssertionError: null` ("You probably triggered an error in
   the DBMS ..."). Against ClickHouse that fires constantly, so the WHERE oracle
   (and the QUERY_PARTITIONING composite, which contains it) is a guaranteed
   false-positive failure. Let the `IgnoreMeException` propagate instead; genuine
   unexpected errors (a re-thrown `SQLException`, or an `AssertionError` with a
   message) are unaffected and still reported.
5. Read the view with `SELECT sum(ignore(*)) FROM <view>` in
   `GeneralProvider.checkViewsAreValid`'s per-view validity probe, instead of
   `SELECT * FROM <view>`. That probe goes through `SQLQueryAdapter.execute`,
   which closes the statement without draining the result set, so an error
   ClickHouse reports mid-stream is invisible: `execute` returns `true` and
   neither `dropView` branch fires. An aggregate cannot emit its row before it
   has consumed all input, and `ignore(*)` takes every view column as an
   argument and is never constant-folded, so the projection cannot be pruned.
   An outer predicate (`... WHERE NOT ignore(*)`) is not equivalent: it is
   pushed into the view and can filter rows before the failing projection runs.
   The existing `if (!execute()) -> dropView` / `catch (Throwable) -> dropView`
   branches then drop exactly the unreadable view, so it no longer reaches the
   cross-join size probe, which is called outside the per-oracle
   `catch (AssertionError)` and so killed the run.
"""

import argparse
import pathlib
import re
import sys

# Pin the JDBC driver to the same version the SQLancer job uses for
# `com.clickhouse:client-v2` (see ci/docker/sqlancer-test), so both jobs talk to
# the server over the same client-v2 transport and the build stays reproducible.
# clickhouse-jdbc >= 0.8 defaults to the client-v2-backed implementation.
DEFAULT_JDBC_VERSION = "0.9.8"

LEGACY_DEP = re.compile(
    r"\s*<dependency>\s*<groupId>ru\.yandex\.clickhouse</groupId>"
    r"\s*<artifactId>clickhouse-jdbc</artifactId>"
    r"\s*<version>[^<]+</version>\s*</dependency>",
    re.DOTALL,
)

CLICKHOUSE_URL = re.compile(
    r"^CLICKHOUSE\.url=.*$",
    re.MULTILINE,
)
# clickhouse-jdbc 0.9.8+ rejects URLs with empty `user=` or `password=` query
# parameters, so we must populate both. Also note that SQLancer++'s
# `GeneralJdbcConfigLoader.getUser/getPassword` only honour `--username` /
# `--password` when they differ from the built-in defaults (`sqlancer`), so
# even with explicit CLI flags those defaults fall through to the properties
# file - meaning the entries below need real values.
# `compress=false` is required: client-v2 requests ClickHouse-framed responses by
# default and decodes only the LZ4 method byte (0x82), while the server frames
# them with its default codec, ZSTD (0x90).
NEW_CLICKHOUSE_LINES = (
    "CLICKHOUSE.url=jdbc:clickhouse://{host}:{port}/{database}"
    "?user={user}&password={password}&compress=false\n"
    "CLICKHOUSE.user=sqlancer\n"
    "CLICKHOUSE.password=sqlancer"
)


def patch_pom(repo: pathlib.Path, version: str) -> None:
    pom = repo / "pom.xml"
    text = pom.read_text()
    replacement = (
        "\n    <dependency>\n"
        "      <groupId>com.clickhouse</groupId>\n"
        "      <artifactId>clickhouse-jdbc</artifactId>\n"
        f"      <version>{version}</version>\n"
        "      <classifier>all</classifier>\n"
        "    </dependency>"
    )
    new, count = LEGACY_DEP.subn(replacement, text, count=1)
    if count != 1:
        sys.exit(f"failed to locate legacy clickhouse-jdbc dependency in {pom}")
    pom.write_text(new)


def patch_jdbc_properties(repo: pathlib.Path) -> None:
    props = repo / "dbconfigs" / "jdbc.properties"
    text = props.read_text()
    new, count = CLICKHOUSE_URL.subn(NEW_CLICKHOUSE_LINES, text, count=1)
    if count != 1:
        sys.exit(f"failed to locate CLICKHOUSE.url in {props}")
    props.write_text(new)


def patch_norec_oracle(repo: pathlib.Path) -> None:
    src = repo / "src" / "sqlancer" / "general" / "oracle" / "GeneralNoRECOracle.java"
    text = src.read_text()
    old_postfix = '" IS TRUE "'
    # ClickHouse's `WHERE <expr>` accepts any non-zero numeric as truthy and
    # rejects NULL. `<expr> != 0` reproduces that exact semantics for the
    # row-counter; a naive `= TRUE` would be strict equality with 1 and
    # produce false-positive oracle mismatches for things like LN()/SIN().
    new_postfix = '" != 0 "'
    if old_postfix not in text:
        sys.exit(f"failed to locate {old_postfix} in {src}")
    if text.count(old_postfix) != 1:
        sys.exit(f"expected exactly one {old_postfix} occurrence in {src}")
    src.write_text(text.replace(old_postfix, new_postfix))


def patch_where_oracle(repo: pathlib.Path) -> None:
    src = (
        repo / "src" / "sqlancer" / "general" / "oracle"
        / "GeneralQueryPartitioningWhere.java"
    )
    text = src.read_text()
    # The baseline (WHERE-stripped) query's catch block escalates any exception
    # to an AssertionError for simple queries. An IgnoreMeException here is the
    # benign "expected error, skip this iteration" signal and must propagate
    # instead - otherwise every whitelisted error on a simple select becomes a
    # false-positive `AssertionError: null`.
    anchor = (
        "        } catch (Exception e) {\n"
        "            if (select.getJoinList().size() == 0 && select.getFromList().size() <= 2) {\n"
    )
    replacement = (
        "        } catch (Exception e) {\n"
        "            // ClickHouse patch: an IgnoreMeException means the baseline query hit an\n"
        "            // expected/whitelisted error and was skipped (null result set); it is a\n"
        "            // skip-this-iteration signal, not a DBMS bug, so propagate it instead of\n"
        "            // escalating it into a false-positive AssertionError below.\n"
        "            if (e instanceof sqlancer.IgnoreMeException) {\n"
        "                throw e;\n"
        "            }\n"
        "            if (select.getJoinList().size() == 0 && select.getFromList().size() <= 2) {\n"
    )
    if anchor not in text:
        sys.exit(f"failed to locate the WHERE-oracle catch block in {src}")
    if text.count(anchor) != 1:
        sys.exit(f"expected exactly one WHERE-oracle catch block in {src}")
    src.write_text(text.replace(anchor, replacement))


def patch_view_validity_probe(repo: pathlib.Path) -> None:
    src = repo / "src" / "sqlancer" / "general" / "GeneralProvider.java"
    text = src.read_text()
    # `SELECT * FROM <view>` cannot tell this probe that the view is unreadable:
    # `execute` never drains the result set, so ClickHouse's mid-stream error is
    # invisible and the view survives into the cross-join size probe, where the
    # same error is fatal. An aggregate over `ignore(*)` reads the view the way
    # that size probe does: every column is a required input, and no predicate
    # sits above the view where it could drop rows before the projection runs.
    anchor = (
        '            SQLQueryAdapter q = new SQLQueryAdapter("SELECT * FROM " + view.getName(),'
        " new ExpectedErrors(), false,\n"
        "                    globalState.getOptions().canonicalizeSqlString());\n"
    )
    replacement = (
        "            // ClickHouse patch: an aggregate makes the server finish the view before it answers, and\n"
        "            // ignore(*) requires every column, so the view's projection cannot be pruned away.\n"
        '            SQLQueryAdapter q = new SQLQueryAdapter("SELECT sum(ignore(*)) FROM " + view.getName(),\n'
        "                    new ExpectedErrors(), false, globalState.getOptions().canonicalizeSqlString());\n"
    )
    if anchor not in text:
        sys.exit(f"failed to locate the view-validity probe in {src}")
    if text.count(anchor) != 1:
        sys.exit(f"expected exactly one view-validity probe in {src}")
    src.write_text(text.replace(anchor, replacement))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", help="Path to the SQLancer++ checkout")
    parser.add_argument(
        "--jdbc-version",
        default=None,
        help=f"clickhouse-jdbc version (default: {DEFAULT_JDBC_VERSION}, matching the SQLancer job's client-v2)",
    )
    args = parser.parse_args()

    version = args.jdbc_version or DEFAULT_JDBC_VERSION
    print(f"swap-clickhouse-jdbc: using com.clickhouse:clickhouse-jdbc:{version}")

    repo = pathlib.Path(args.repo)
    patch_pom(repo, version)
    patch_jdbc_properties(repo)
    patch_norec_oracle(repo)
    patch_where_oracle(repo)
    patch_view_validity_probe(repo)


if __name__ == "__main__":
    main()
