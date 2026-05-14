#!/usr/bin/env python3
"""Patch the SQLancer++ source tree for use against ClickHouse:

1. Replace the legacy `ru.yandex.clickhouse:clickhouse-jdbc:0.3.2` Maven
   dependency in `pom.xml` with the latest `com.clickhouse:clickhouse-jdbc`
   shaded-all artifact (version is queried from Maven Central at build time
   so we always ship the most recent driver). The legacy driver's HTTP
   response parser can't handle current ClickHouse server output.
2. Extend the `CLICKHOUSE.url` template in `dbconfigs/jdbc.properties` to carry
   `user` and `password` query parameters. SQLancer++'s generic provider only
   passes the URL string (no JDBC Properties) into `DriverManager.getConnection`,
   so the new driver - which requires credentials - needs them embedded here.
3. Replace the `IS TRUE` postfix in `GeneralNoRECOracle.java` with `= TRUE`.
   ClickHouse's parser doesn't implement the SQL-standard `IS TRUE` /
   `IS FALSE` postfix, so without this patch every NoREC oracle query is a
   guaranteed `SYNTAX_ERROR` and NoREC reports zero oracle pairs. `<expr>
   = TRUE` is what ClickHouse accepts; precedence and JDBC NULL handling are
   equivalent for the row-counter.
"""

import argparse
import pathlib
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET

# Maven Central's `maven-metadata.xml` is the canonical source for the latest
# release of an artifact - it lists every published version plus an explicit
# <release>/<latest> pointer. The `solrsearch` API has stale-index issues and
# returned 0.9.0 long after 0.9.8 was published, so don't go through it.
MAVEN_METADATA_URL = (
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/maven-metadata.xml"
)

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
NEW_CLICKHOUSE_LINES = (
    "CLICKHOUSE.url=jdbc:clickhouse://{host}:{port}/{database}"
    "?user={user}&password={password}\n"
    "CLICKHOUSE.user=sqlancer\n"
    "CLICKHOUSE.password=sqlancer"
)


def resolve_latest_jdbc_version() -> str:
    with urllib.request.urlopen(MAVEN_METADATA_URL, timeout=30) as resp:
        root = ET.fromstring(resp.read())
    versioning = root.find("versioning")
    if versioning is None:
        sys.exit(f"Maven metadata at {MAVEN_METADATA_URL} has no <versioning> element")
    # Prefer <release>; fall back to <latest> (older artifacts may omit
    # <release>). <latest> can include snapshots, but com.clickhouse only
    # publishes releases, so it is safe here.
    version = versioning.findtext("release") or versioning.findtext("latest")
    if not version:
        sys.exit(f"Maven metadata at {MAVEN_METADATA_URL} has neither <release> nor <latest>")
    return version


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", help="Path to the SQLancer++ checkout")
    parser.add_argument(
        "--jdbc-version",
        default=None,
        help="Pin a specific clickhouse-jdbc version (default: resolve latest from Maven Central)",
    )
    args = parser.parse_args()

    version = args.jdbc_version or resolve_latest_jdbc_version()
    print(f"swap-clickhouse-jdbc: using com.clickhouse:clickhouse-jdbc:{version}")

    repo = pathlib.Path(args.repo)
    patch_pom(repo, version)
    patch_jdbc_properties(repo)
    patch_norec_oracle(repo)


if __name__ == "__main__":
    main()
