#!/usr/bin/env python3
"""Self-contained tests for structured system-table documentation generation."""

import importlib.util
import re
import tempfile
from importlib.machinery import SourceFileLoader
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[4]
GENERATOR = REPO_ROOT / "utils" / "generate-system-tables-docs"
SOURCE_ROOT = REPO_ROOT / "src"

EXPECTED_DOCUMENTATION_COUNT = 168
EXPECTED_SOURCE_COUNT = 161
EXPECTED_COLUMNS_PROVIDER_COUNT = 48
EXPECTED_FIELD_COUNTS = {
    ".description": EXPECTED_DOCUMENTATION_COUNT,
    ".columns_notes": 10,
    ".examples": 105,
    ".see_also": 59,
}
PLACEHOLDERS = {
    "{{PROFILE_EVENTS}}": 1,
    "{{CURRENT_METRICS}}": 1,
    "{{ASYNCHRONOUS_METRICS}}": 1,
}


def load_generator():
    loader = SourceFileLoader("generate_system_tables_docs", str(GENERATOR))
    spec = importlib.util.spec_from_file_location(
        "generate_system_tables_docs", GENERATOR, loader=loader
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    generator = load_generator()

    source_documents = {}
    table_names = []
    registration_re = re.compile(
        r'REGISTER_SYSTEM_TABLE_DOCUMENTATION\(\s*"([^"]+)",'
        r'\s*\.description\s*=\s*R"DOCS_MD\(',
        re.DOTALL,
    )
    for source_file in SOURCE_ROOT.rglob("*.cpp"):
        source = source_file.read_text(encoding="utf-8")
        names = registration_re.findall(source)
        if not names:
            continue
        registrations_offset = source.index("REGISTER_SYSTEM_TABLE_DOCUMENTATION(")
        source_documents[source_file] = source[registrations_offset:]
        table_names.extend(names)
        assert "#include <Common/SystemTableDocumentation.h>" in source

    assert len(table_names) == EXPECTED_DOCUMENTATION_COUNT
    assert len(table_names) == len(set(table_names))
    assert len(source_documents) == EXPECTED_SOURCE_COUNT
    assert "statements" in table_names

    registry = "\n".join(source_documents.values())
    for field, expected_count in EXPECTED_FIELD_COUNTS.items():
        assert registry.count(field + " = R\"DOCS_MD(") == expected_count
    assert ".additional_sections =" not in registry
    assert registry.count(".get_columns = ") == EXPECTED_COLUMNS_PROVIDER_COUNT
    assert "{{SYSTEM_TABLE_COLUMNS}}" not in registry
    assert "SystemTableCloud" not in registry
    assert not re.search(r"^import ", registry, re.MULTILINE)
    assert set(re.findall(r"\{\{[A-Z_]+\}\}", registry)) == set(PLACEHOLDERS)
    for placeholder, expected_count in PLACEHOLDERS.items():
        assert registry.count(placeholder) == expected_count

    availability_requirements = {
        SOURCE_ROOT / "Interpreters" / "TransactionsInfoLog.cpp": (
            "transactions_info_log",
            "allow_experimental_transactions",
        ),
        SOURCE_ROOT / "Interpreters" / "SessionLog.cpp": ("session_log",),
        SOURCE_ROOT / "Interpreters" / "PredicateStatisticsLog.cpp": (
            "predicate_statistics_log",
            "predicate_statistics_sample_rate",
        ),
        SOURCE_ROOT / "Interpreters" / "DeadLetterQueue.cpp": (
            "dead_letter_queue",
            "handle_error_mode",
        ),
        SOURCE_ROOT / "Storages" / "System" / "StorageSystemUserQueryLog.cpp": (
            "query_log.enable_user_query_log",
            "exists but is empty",
        ),
    }
    for source_file, requirements in availability_requirements.items():
        documentation = source_documents[source_file]
        assert "**Availability**" in documentation
        assert "UNKNOWN_TABLE" in documentation
        for requirement in requirements:
            assert requirement in documentation

    for old_registry_file in (
        SOURCE_ROOT / "Storages" / "System" / "SystemTableDocumentation.cpp",
        SOURCE_ROOT / "Storages" / "System" / "SystemTableDocumentation.h",
        SOURCE_ROOT / "Storages" / "System" / "SystemTableDocumentation.inc",
    ):
        assert not old_registry_file.exists()

    assert generator.DOC_FILENAMES == {
        "delta_lake_metadata_log": (
            "delta_metadata_log.mdx",
            "delta_lake_metadata_log.mdx",
        ),
        "warnings": ("system_warnings.mdx", "warnings.mdx"),
    }
    generated_page_names = {
        Path(filename).stem
        for name in table_names
        for filename in generator.documentation_filenames(name)
    }
    documentation_page_names = {
        path.stem
        for path in (REPO_ROOT / "docs" / "reference" / "system-tables").glob("*.mdx")
    }
    expected_page_names = documentation_page_names - {
        "histogram_metric_log",     # Deprecated `system.histogram_metric_log` table which no longer exists.
        "information_schema",       # Overview of a separate database.
        "overview",                 # System-tables overview, not a table page.
    }
    assert generated_page_names == expected_page_names

    frontmatter = """---
description: 'A table.'
title: 'system.example'
---
"""
    preamble = """import SystemTableCloud from '/snippets/_system_table_cloud.mdx';

<SystemTableCloud/>
"""
    old_body = """
## Description {#purpose}

Old prose.

## Columns {#Columns}

{/*AUTOGENERATED_START*/}
- `old` (`UInt8`)
{/*AUTOGENERATED_END*/}

## Example {#example}

Old example.

## See Also {#see-also}

- Old related page.
"""
    generated_body = """## Description {#description}

Structured embedded prose.

## Metric descriptions {#metric-descriptions}

{/*AUTOGENERATED_METRICS_START*/}
### Metric {#metric}

Description.
{/*AUTOGENERATED_METRICS_END*/}

## Columns {#columns}

- `value` ([UInt8](/reference/data-types/int-uint))

## Examples {#examples}

```sql
SELECT value FROM system.example;
```

## See also {#see-also}

- New related page."""
    assert generator.extract_page_summary(generated_body) == "Structured embedded prose."
    assert (
        generated_body.index("## Description")
        < generated_body.index("## Metric descriptions")
        < generated_body.index("## Columns")
        < generated_body.index("## Examples")
        < generated_body.index("## See also")
    )
    preserved_generated_body = (
        generated_body.replace(
            "## Description {#description}", "## Description {#purpose}"
        )
        .replace("## Columns {#columns}", "## Columns {#Columns}")
        .replace("## Examples {#examples}", "## Example {#example}")
        .replace("## See also {#see-also}", "## See Also {#see-also}")
    )
    assert (
        generator.preserve_published_section_headings(old_body, generated_body)
        == preserved_generated_body
    )

    asynchronous_metrics_page = {
        "asynchronous_metrics": """## Metric descriptions {#metric-descriptions}

Source-backed introduction.

### RuntimeOnly {#runtimeonly}

This entry came from live runtime state.

## See also {#see-also}

- Related page.
"""
    }
    metric_count = generator.populate_asynchronous_metrics_catalog(
        asynchronous_metrics_page
    )
    assert metric_count > 200
    asynchronous_metrics_body = asynchronous_metrics_page["asynchronous_metrics"]
    async_metrics_generator = generator.load_async_metrics_generator()
    assert async_metrics_generator.documentation_anchor("jemalloc.epoch") == "jemalloc-epoch"
    assert async_metrics_generator.documentation_anchor("metric_name") == "metric-name"
    try:
        async_metrics_generator.render_markdown(
            [
                ("PlatformMetric", "Linux description", "linux"),
                ("PlatformMetric", "Darwin description", "darwin"),
            ]
        )
    except ValueError as error:
        assert "PlatformMetric" in str(error)
    else:
        raise AssertionError(
            "Conflicting platform descriptions must not be concatenated"
        )
    assert "Source-backed introduction." in asynchronous_metrics_body
    assert "### AsynchronousMetricsUpdateInterval" in asynchronous_metrics_body
    assert "### jemalloc.epoch {#jemalloc-epoch}" in asynchronous_metrics_body
    for metric_name in (
        "MemoryThreadStacksCount",
        "MemoryThreadStacksResident",
        "MemoryThreadStacksVirtual",
    ):
        assert asynchronous_metrics_body.count(f"### {metric_name} ") == 1
    assert "RuntimeOnly" not in asynchronous_metrics_body
    assert asynchronous_metrics_body.count(generator.AUTOGENERATED_METRICS_START) == 1
    assert asynchronous_metrics_body.count(generator.AUTOGENERATED_METRICS_END) == 1
    assert "## See also {#see-also}" in asynchronous_metrics_body

    temporary_root = REPO_ROOT / "tmp"
    temporary_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=temporary_root) as directory:
        page = Path(directory) / "example.mdx"
        page.write_text(frontmatter + preamble + old_body, encoding="utf-8")

        assert generator.update_full_doc_file(page, generated_body)
        expected = (
            frontmatter
            + "\n"
            + preamble
            + "\n"
            + generator.AUTOGENERATED_START
            + "\n"
            + preserved_generated_body
            + "\n"
            + generator.AUTOGENERATED_END
            + "\n"
        )
        assert page.read_text(encoding="utf-8") == expected
        assert not generator.update_full_doc_file(page, generated_body)

        created = Path(directory) / "created.mdx"
        generator.create_doc_file(
            created,
            "created",
            "A newly documented table.",
            generated_body,
        )
        created_text = created.read_text(encoding="utf-8")
        assert generator.AUTOGENERATED_START + "\n" + generated_body in created_text
        assert created_text.endswith(generator.AUTOGENERATED_END + "\n")

        # The generator must use the documentation registration as its source
        # of truth even when `system.tables` has no attached table/comment.
        registered_only = Path(directory) / "registered_only.mdx"
        assert generator.generate_pages(
            directory,
            {
                "registered_only": generated_body,
                "query_log_0": generated_body,
            },
            {},
        ) == (0, 1, 0)
        registered_only_text = registered_only.read_text(encoding="utf-8")
        assert "description: 'Structured embedded prose.'" in registered_only_text
        assert generator.AUTOGENERATED_START + "\n" + generated_body in registered_only_text
        assert not (Path(directory) / "query_log_0.mdx").exists()

        # Both published filenames for a legacy duplicate are refreshed from
        # the same structured registration and remain deterministic.
        assert generator.generate_pages(
            directory,
            {"warnings": generated_body},
            {},
        ) == (0, 2, 0)
        for filename in generator.documentation_filenames("warnings"):
            duplicate_text = (Path(directory) / filename).read_text(encoding="utf-8")
            assert generator.AUTOGENERATED_START + "\n" + generated_body in duplicate_text
        assert generator.generate_pages(
            directory,
            {"warnings": generated_body},
            {},
        ) == (0, 0, 2)

    print(
        f"OK: {len(table_names)} structured system-table documents in "
        f"{len(source_documents)} defining C++ files; generated pages preserve "
        "their MDX preamble and published section headings and update deterministically"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
