#!/usr/bin/env python3
"""Generate the ClickHouse Cloud unsupported-features documentation catalog.

The catalog is derived from English documentation pages that render a
`CloudNotSupportedBadge`, including badges rendered through imported MDX
snippets. The destination uses dedicated markers so this generator owns and
positions only the catalog section, not the surrounding compatibility guide.
"""

import argparse
import dataclasses
import re
from pathlib import Path


TARGET = Path("products/cloud/guides/cloud-compatibility.mdx")
START_MARKER = "{/* CLOUD_NOT_SUPPORTED_FEATURES_START */}"
END_MARKER = "{/* CLOUD_NOT_SUPPORTED_FEATURES_END */}"
ROADMAP_HEADING = "## Roadmap {#roadmap}"
SECTION_HEADING = "## List of unsupported features {#list-of-unsupported-features}"
SECTION_INTRO = (
    "The following tables list features and self-managed procedures that are "
    "marked as unsupported in ClickHouse Cloud. Each entry names the specific "
    "unsupported capability, even when its source page also covers supported "
    "functionality. Source links are collected weekly from uses of the Cloud "
    "not supported badge; capability names and categories are curated."
)
LOCALES = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}

BADGE_RE = re.compile(r"<CloudNotSupportedBadge\b[^>]*?/?>")
HEADING_RE = re.compile(
    r"^#{1,6}\s+(.+?)(?:\s+\{#([^}]+)\})\s*$", re.MULTILINE
)
TITLE_RE = re.compile(r"^title:\s*(['\"]?)(.*?)\1\s*$", re.MULTILINE)
SNIPPET_IMPORT_RE = re.compile(
    r"^import\s+([A-Za-z_$][\w$]*)\s+from\s+"
    r"['\"](/snippets/[^'\"]+\.mdx)['\"];?\s*$",
    re.MULTILINE,
)
MARKDOWN_LINK_RE = re.compile(r"\[([^]]+)]\([^)]+\)")
HTML_TAG_RE = re.compile(r"<[^>]+>")


@dataclasses.dataclass(frozen=True, order=True)
class Feature:
    label: str
    url: str


@dataclasses.dataclass(frozen=True)
class Reference:
    label: str
    url: str


@dataclasses.dataclass(frozen=True)
class CatalogEntry:
    category: str
    capability: str
    references: tuple[Reference, ...]


CATEGORIES = (
    (
        "Managed infrastructure and operations",
        "unsupported-managed-infrastructure-and-operations",
    ),
    ("Authentication and security", "unsupported-authentication-and-security"),
    ("Interfaces and integrations", "unsupported-interfaces-and-integrations"),
    ("Database and table engines", "unsupported-database-and-table-engines"),
    (
        "Dictionaries and server configuration",
        "unsupported-dictionaries-and-server-configuration",
    ),
    ("Data formats and codecs", "unsupported-data-formats-and-codecs"),
    (
        "Functions and experimental features",
        "unsupported-functions-and-experimental-features",
    ),
    (
        "Administration and access control",
        "unsupported-administration-and-access-control",
    ),
)


def _entry(
    category: str,
    capability: str,
    *references: tuple[str, str],
) -> CatalogEntry:
    return CatalogEntry(
        category,
        capability,
        tuple(Reference(label, url) for label, url in references),
    )


# These descriptions are deliberately curated instead of being inferred from page
# titles. A page can document a broader supported feature while marking only one
# section as unsupported, and generic titles such as "Using Tigris" don't explain
# what the badge applies to.
CATALOG = (
    _entry(
        "Managed infrastructure and operations",
        "Self-managed `ClickHouse Keeper` deployment and configuration",
        ("Deployment", "/guides/oss/deployment-and-scaling/keeper"),
        (
            "Unique paths",
            "/guides/oss/deployment-and-scaling/keeper#configuring-clickhouse-keeper-with-unique-paths",
        ),
        (
            "Dynamic reconfiguration",
            "/guides/oss/deployment-and-scaling/keeper#reconfiguration",
        ),
        (
            "Secure ZooKeeper connections",
            "/guides/oss/deployment-and-scaling/keeper/ssl-zookeeper",
        ),
    ),
    _entry(
        "Managed infrastructure and operations",
        "Manual TLS certificate provisioning and server configuration",
        (
            "ACME provisioning",
            "/concepts/features/security/tls/configuring-tls-acme-client",
        ),
        ("TLS configuration", "/concepts/features/security/tls/configuring-tls"),
    ),
    _entry(
        "Managed infrastructure and operations",
        "Manual hot/warm/cold storage-tier configuration with `TTL`",
        (
            "Guide",
            "/concepts/features/operations/delete/ttl#implementing-a-hotwarmcold-architecture",
        ),
    ),
    _entry(
        "Managed infrastructure and operations",
        "Running self-managed hardware performance tests",
        ("Guide", "/concepts/features/performance/troubleshoot/performance-test"),
    ),
    _entry(
        "Managed infrastructure and operations",
        "Applying self-managed operational recommendations",
        ("Guide", "/guides/oss/best-practices/tips"),
    ),
    _entry(
        "Authentication and security",
        "HTTP external authentication",
        ("Reference", "/concepts/features/security/external-authenticators/http"),
    ),
    _entry(
        "Authentication and security",
        "Kerberos external authentication",
        (
            "Reference",
            "/concepts/features/security/external-authenticators/kerberos",
        ),
    ),
    _entry(
        "Authentication and security",
        "LDAP authentication and role mapping",
        ("Configuration", "/concepts/features/security/configuring-ldap"),
        ("Authenticator", "/concepts/features/security/external-authenticators/ldap"),
    ),
    _entry(
        "Authentication and security",
        "X.509 certificate authentication",
        ("User authentication", "/concepts/features/security/ssl-user-auth"),
        (
            "External authenticator",
            "/concepts/features/security/external-authenticators/ssl-x509",
        ),
    ),
    _entry(
        "Interfaces and integrations",
        "PostgreSQL wire-protocol interface",
        ("Reference", "/concepts/features/interfaces/postgresql"),
    ),
    _entry(
        "Interfaces and integrations",
        "SSH interface with PTY",
        ("Reference", "/concepts/features/interfaces/ssh"),
    ),
    _entry(
        "Interfaces and integrations",
        "`MaterializedPostgreSQL` replication engine",
        (
            "ClickPipes guide",
            "/integrations/clickpipes/postgres/connecting-to-postgresql#using-the-materializedpostgresql-database-engine",
        ),
        (
            "Connector guide",
            "/integrations/connectors/data-sources/postgres#using-the-materializedpostgresql-database-engine",
        ),
        (
            "Database engine",
            "/reference/engines/database-engines/materialized-postgresql",
        ),
        (
            "Table engine",
            "/reference/engines/table-engines/integrations/materialized-postgresql",
        ),
    ),
    _entry(
        "Interfaces and integrations",
        "ODBC integration and the `ODBC` table engine",
        ("Integration guide", "/integrations/connectors/data-ingestion/odbc-with-clickhouse"),
        ("Table engine", "/reference/engines/table-engines/integrations/odbc"),
    ),
    _entry(
        "Interfaces and integrations",
        "MinIO S3-compatible object-storage integration",
        ("Guide", "/integrations/connectors/data-ingestion/s3-minio"),
    ),
    _entry(
        "Interfaces and integrations",
        "Tigris S3-compatible object-storage integration",
        ("Guide", "/integrations/connectors/data-ingestion/s3-tigris"),
    ),
    _entry(
        "Database and table engines",
        "`MySQL` database engine",
        ("Reference", "/reference/engines/database-engines/mysql"),
    ),
    _entry(
        "Database and table engines",
        "`EmbeddedRocksDB` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/embedded-rocksdb"),
    ),
    _entry(
        "Database and table engines",
        "`HDFS` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/hdfs"),
    ),
    _entry(
        "Database and table engines",
        "`Hive` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/hive"),
    ),
    _entry(
        "Database and table engines",
        "`JDBC` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/jdbc"),
    ),
    _entry(
        "Database and table engines",
        "`SQLite` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/sqlite"),
    ),
    _entry(
        "Database and table engines",
        "`TimeSeries` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/time-series"),
    ),
    _entry(
        "Database and table engines",
        "`YTsaurus` table engine",
        ("Reference", "/reference/engines/table-engines/integrations/ytsaurus"),
    ),
    _entry(
        "Database and table engines",
        "`Log`-family table engines (`Log`, `StripeLog`, and `TinyLog`)",
        ("Family overview", "/reference/engines/table-engines/log-family"),
        ("`Log`", "/reference/engines/table-engines/log-family/log"),
        ("`StripeLog`", "/reference/engines/table-engines/log-family/stripelog"),
        ("`TinyLog`", "/reference/engines/table-engines/log-family/tinylog"),
    ),
    _entry(
        "Dictionaries and server configuration",
        "Named collections and the `CREATE NAMED COLLECTION` and `ALTER NAMED COLLECTION` statements",
        ("Overview", "/concepts/features/configuration/server-config/named-collections"),
        ("Create", "/reference/statements/create/named-collection"),
        ("Alter", "/reference/statements/alter/named-collection"),
    ),
    _entry(
        "Dictionaries and server configuration",
        "File-based dictionary configuration",
        (
            "Reference",
            "/reference/statements/create/dictionary#creating-a-dictionary-with-a-configuration-file",
        ),
    ),
    _entry(
        "Dictionaries and server configuration",
        "Embedded geobase dictionaries",
        ("Reference", "/reference/statements/create/dictionary/embedded"),
    ),
    _entry(
        "Dictionaries and server configuration",
        "Local `YAMLRegExpTree` dictionary sources",
        (
            "Layout guide",
            "/reference/statements/create/dictionary/layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-open-source",
        ),
        (
            "Source reference",
            "/reference/statements/create/dictionary/sources/yamlregexptree",
        ),
    ),
    _entry(
        "Dictionaries and server configuration",
        "`YTsaurus` dictionary source",
        ("Reference", "/reference/statements/create/dictionary/sources/ytsaurus"),
    ),
    _entry(
        "Dictionaries and server configuration",
        "Japanese tokenizer dictionaries configured on the server",
        (
            "Reference",
            "/reference/engines/table-engines/mergetree-family/textindexes#japanese-tokenizer-dictionary",
        ),
    ),
    _entry(
        "Data formats and codecs",
        "`CapnProto` format with server-side schema files",
        ("Format reference", "/reference/formats/CapnProto"),
        ("Guide", "/guides/clickhouse/data-formats/binary#capn-proto"),
    ),
    _entry(
        "Data formats and codecs",
        "`Protobuf`, `ProtobufList`, and `ProtobufSingle` formats with server-side schema files",
        ("Guide", "/guides/clickhouse/data-formats/binary#protocol-buffers"),
        ("`ProtobufList`", "/reference/formats/Protobuf/ProtobufList"),
        ("`ProtobufSingle`", "/reference/formats/Protobuf/ProtobufSingle"),
    ),
    _entry(
        "Data formats and codecs",
        "Obsolete `ZSTD_QAT` and `DEFLATE_QPL` codecs",
        ("`ZSTD_QAT`", "/reference/statements/create/table#zstd_qat"),
        ("`DEFLATE_QPL`", "/reference/statements/create/table#deflate_qpl"),
    ),
    _entry(
        "Functions and experimental features",
        "Experimental natural-language processing functions",
        ("Reference", "/reference/functions/regular-functions/nlp-functions"),
    ),
    _entry(
        "Functions and experimental features",
        "Experimental multi-statement transactions with commit and rollback",
        (
            "Guide",
            "/concepts/features/operations/insert/transactions#transactions-commit-and-rollback",
        ),
    ),
    _entry(
        "Functions and experimental features",
        "Experimental transaction introspection functions",
        (
            "`transactionID`",
            "/reference/functions/regular-functions/other-functions#transactionID",
        ),
        (
            "`transactionLatestSnapshot`",
            "/reference/functions/regular-functions/other-functions#transactionLatestSnapshot",
        ),
        (
            "`transactionOldestSnapshot`",
            "/reference/functions/regular-functions/other-functions#transactionOldestSnapshot",
        ),
    ),
    _entry(
        "Functions and experimental features",
        "Time-window functions for `WindowView`",
        ("Reference", "/reference/functions/regular-functions/time-window-functions"),
    ),
    _entry(
        "Functions and experimental features",
        "Experimental `WindowView`",
        ("Reference", "/reference/statements/create/view#window-view"),
    ),
    _entry(
        "Functions and experimental features",
        "WebAssembly user-defined functions",
        ("Overview", "/reference/functions/regular-functions/udf#webassembly-user-defined-functions"),
        ("Guide", "/reference/functions/regular-functions/wasm_udf"),
    ),
    _entry(
        "Functions and experimental features",
        "Driver-based executable user-defined functions",
        (
            "Reference",
            "/reference/functions/regular-functions/udf#driver-based-executable-user-defined-functions",
        ),
    ),
    _entry(
        "Functions and experimental features",
        "`filesystem` table function over server-local files",
        ("Reference", "/reference/functions/table-functions/filesystem"),
    ),
    _entry(
        "Administration and access control",
        "Column-statistics operations with `ALTER TABLE`",
        ("Reference", "/reference/statements/alter/statistics"),
    ),
    _entry(
        "Administration and access control",
        "User impersonation with `EXECUTE AS`",
        ("Reference", "/reference/statements/execute_as"),
    ),
    _entry(
        "Administration and access control",
        "Table-engine-specific grants",
        ("Grant", "/reference/statements/grant#table-engine"),
        (
            "Server setting",
            "/reference/settings/server-settings/settings/other#table_engines_require_grant",
        ),
    ),
    _entry(
        "Administration and access control",
        "The `GRANT ALL` privilege shortcut",
        ("Reference", "/reference/statements/grant#all"),
    ),
    _entry(
        "Administration and access control",
        "Server shutdown with `SYSTEM SHUTDOWN`",
        ("Reference", "/reference/statements/system#shutdown"),
    ),
    _entry(
        "Administration and access control",
        "Starting and stopping background fetches with `SYSTEM` statements",
        ("`SYSTEM STOP FETCHES`", "/reference/statements/system#stop-fetches"),
        ("`SYSTEM START FETCHES`", "/reference/statements/system#start-fetches"),
    ),
    _entry(
        "Administration and access control",
        "Starting and stopping background merges with `SYSTEM` statements",
        ("`SYSTEM STOP MERGES`", "/reference/statements/system#stop-merges"),
        ("`SYSTEM START MERGES`", "/reference/statements/system#start-merges"),
    ),
    _entry(
        "Administration and access control",
        "Starting and stopping background `TTL` merges with `SYSTEM` statements",
        (
            "`SYSTEM STOP TTL MERGES`",
            "/reference/statements/system#stop-ttl-merges",
        ),
        (
            "`SYSTEM START TTL MERGES`",
            "/reference/statements/system#start-ttl-merges",
        ),
    ),
)


def _page_url(relative_path: Path) -> str:
    path = relative_path.with_suffix("").as_posix()
    if path.endswith("/index"):
        path = path[: -len("/index")]
    return f"/{path}"


def _page_title(text: str, relative_path: Path) -> str:
    match = TITLE_RE.search(text)
    if match:
        return match.group(2).strip()
    return relative_path.stem.replace("-", " ").replace("_", " ").title()


def _clean_label(label: str) -> str:
    label = MARKDOWN_LINK_RE.sub(r"\1", label)
    label = HTML_TAG_RE.sub("", label)
    label = label.replace("**", "").replace("__", "")
    return label.replace("[", r"\[").replace("]", r"\]").strip()


def _expand_mdx_snippets(
    text: str, docs_root: Path, stack: tuple[Path, ...] = ()
) -> str:
    imports = {
        component: docs_root / source.removeprefix("/")
        for component, source in SNIPPET_IMPORT_RE.findall(text)
    }
    for component, snippet_path in imports.items():
        if snippet_path in stack:
            chain = " -> ".join(str(path) for path in (*stack, snippet_path))
            raise ValueError(f"recursive MDX snippet import: {chain}")
        if not snippet_path.is_file():
            raise FileNotFoundError(f"MDX snippet does not exist: {snippet_path}")
        snippet = _expand_mdx_snippets(
            snippet_path.read_text(encoding="utf-8"),
            docs_root,
            (*stack, snippet_path),
        )
        invocation = re.compile(rf"<{re.escape(component)}\b[^>]*?\s*/>")
        text = invocation.sub(lambda _match: snippet, text)
    return text


def _features_in_page(path: Path, docs_root: Path) -> list[Feature]:
    relative_path = path.relative_to(docs_root)
    source = path.read_text(encoding="utf-8")
    expanded = _expand_mdx_snippets(source, docs_root)
    page_title = _clean_label(_page_title(source, relative_path))
    page_url = _page_url(relative_path)
    headings = list(HEADING_RE.finditer(expanded))
    features = []

    for badge in BADGE_RE.finditer(expanded):
        preceding = [heading for heading in headings if heading.start() < badge.start()]
        if preceding:
            heading = preceding[-1]
            heading_title = _clean_label(heading.group(1))
            label = (
                heading_title
                if heading_title.casefold() == page_title.casefold()
                else f"{page_title}: {heading_title}"
            )
            anchor = heading.group(2)
            url = f"{page_url}#{anchor}" if anchor else page_url
        else:
            label = page_title
            url = page_url
        features.append(Feature(label=label, url=url))

    return features


def collect_features(docs_root: Path) -> list[Feature]:
    features = set()
    for path in docs_root.rglob("*.mdx"):
        relative_path = path.relative_to(docs_root)
        if relative_path.parts[0] in LOCALES or relative_path.parts[0] == "snippets":
            continue
        if path.name.startswith("_") or relative_path == TARGET:
            continue
        features.update(_features_in_page(path, docs_root))
    return sorted(features, key=lambda feature: (feature.label.casefold(), feature.url))


def render_feature_list(
    features: list[Feature], catalog: tuple[CatalogEntry, ...] = CATALOG
) -> str:
    if not features:
        raise ValueError("no Cloud-not-supported features found")

    category_anchors = dict(CATEGORIES)
    unknown_categories = sorted(
        {entry.category for entry in catalog} - category_anchors.keys()
    )
    if unknown_categories:
        raise ValueError(
            "unsupported-feature catalog has unknown categories: "
            + ", ".join(unknown_categories)
        )

    catalog_urls = []
    for entry in catalog:
        catalog_urls.extend(reference.url for reference in entry.references)
    duplicates = sorted(
        url for url in set(catalog_urls) if catalog_urls.count(url) > 1
    )
    if duplicates:
        raise ValueError(
            "unsupported-feature catalog has duplicate URLs:\n- "
            + "\n- ".join(duplicates)
        )

    features_by_url = {feature.url: feature for feature in features}
    missing = sorted(features_by_url.keys() - set(catalog_urls))
    if missing:
        details = "\n".join(
            f"- {features_by_url[url].label}: {url}" for url in missing
        )
        raise ValueError(
            "Cloud-not-supported badges need curated catalog entries:\n" + details
        )

    sections = []
    for category, anchor in CATEGORIES:
        rows = []
        for entry in catalog:
            if entry.category != category:
                continue
            references = [
                reference
                for reference in entry.references
                if reference.url in features_by_url
            ]
            if not references:
                continue
            links = ", ".join(
                f"[{reference.label}]({reference.url})" for reference in references
            )
            rows.append(f"| {entry.capability} | {links} |")
        if rows:
            sections.append(
                "\n".join(
                    (
                        f"### {category} {{#{anchor}}}",
                        "",
                        "| Unsupported capability | Documentation |",
                        "|---|---|",
                        *rows,
                    )
                )
            )
    return "\n\n".join(sections)


def updated_target_content(
    docs_root: Path, catalog: tuple[CatalogEntry, ...] = CATALOG
) -> tuple[str, int]:
    target = docs_root / TARGET
    content = target.read_text(encoding="utf-8")
    if content.count(START_MARKER) != 1 or content.count(END_MARKER) != 1:
        raise ValueError(
            f"{target} must contain exactly one pair of unsupported-feature markers"
        )
    before_section, remainder = content.split(START_MARKER, 1)
    _old_section, after_section = remainder.split(END_MARKER, 1)
    without_section = f"{before_section.rstrip()}\n\n{after_section.lstrip()}"
    if without_section.count(ROADMAP_HEADING) != 1:
        raise ValueError(f"{target} must contain exactly one roadmap heading")
    before_roadmap, after_roadmap = without_section.split(ROADMAP_HEADING, 1)

    features = collect_features(docs_root)
    generated = render_feature_list(features, catalog)
    section = (
        f"{START_MARKER}\n\n"
        f"{SECTION_HEADING}\n\n"
        f"{SECTION_INTRO}\n\n"
        f"{generated}\n\n"
        f"{END_MARKER}"
    )
    updated = (
        f"{before_roadmap.rstrip()}\n\n"
        f"{section}\n\n"
        f"{ROADMAP_HEADING}{after_roadmap}"
    )
    return updated, len(features)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    target = args.docs_dir / TARGET
    updated, count = updated_target_content(args.docs_dir)
    current = target.read_text(encoding="utf-8")
    if updated == current:
        print(f"Cloud unsupported-feature list is current ({count} entries)")
        return 0
    if args.write:
        target.write_text(updated, encoding="utf-8")
        print(f"Updated Cloud unsupported-feature list ({count} entries)")
        return 0
    print(
        "Cloud unsupported-feature list is out of date; rerun with --write",
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
