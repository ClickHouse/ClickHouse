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
ATTACH_SOURCE = SOURCE_ROOT / "Storages" / "System" / "attachSystemTables.cpp"
SYSTEM_LOG_HEADER = SOURCE_ROOT / "Interpreters" / "SystemLog.h"

EXPECTED_DOCUMENTATION_COUNT = 169
EXPECTED_ATTACH_DOCUMENTATION_COUNT = 139
EXPECTED_SYSTEM_LOG_DOCUMENTATION_COUNT = 30
EXPECTED_FIELD_COUNTS = {
    "description": EXPECTED_DOCUMENTATION_COUNT,
    "columns_notes": 10,
    "examples": 105,
    "see_also": 59,
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

    attach_source = ATTACH_SOURCE.read_text(encoding="utf-8")
    attach_documents = dict(
        re.findall(
            r'attach(?:NoDescription)?<[^;\n]+>\(context,\s*system_database,\s*"([^"]+)",'
            r'\s*R"DOCS_MD\((.*?)\)DOCS_MD"',
            attach_source,
            re.DOTALL,
        )
    )
    assert len(attach_documents) == EXPECTED_ATTACH_DOCUMENTATION_COUNT

    system_log_source = SYSTEM_LOG_HEADER.read_text(encoding="utf-8")
    system_log_constants = dict(
        re.findall(
            r'inline constexpr char SYSTEM_LOG_DOCUMENTATION_([A-Z0-9_]+)\[\] = '
            r'R"DOCS_MD\((.*?)\)DOCS_MD";',
            system_log_source,
            re.DOTALL,
        )
    )
    system_log_names = dict(
        re.findall(
            r'^\s*M\([^,]+,\s*([a-zA-Z0-9_]+)\s*,'
            r'\s*DB::SYSTEM_LOG_DOCUMENTATION_([A-Z0-9_]+)\)',
            system_log_source,
            re.MULTILINE,
        )
    )
    system_log_documents = {
        table_name: system_log_constants[constant_name]
        for table_name, constant_name in system_log_names.items()
    }
    assert len(system_log_documents) == EXPECTED_SYSTEM_LOG_DOCUMENTATION_COUNT

    documents = attach_documents | system_log_documents
    assert len(documents) == EXPECTED_DOCUMENTATION_COUNT
    table_names = list(documents)
    assert len(table_names) == len(set(table_names))
    assert "statements" in table_names

    structured_comments = "\n".join(documents.values())
    for field, expected_count in EXPECTED_FIELD_COUNTS.items():
        assert len(re.findall(rf"(?m)^\.{field}$", structured_comments)) == expected_count
    assert ".additional_sections" not in structured_comments
    assert ".get_columns" not in structured_comments
    assert "{{SYSTEM_TABLE_COLUMNS}}" not in structured_comments
    assert "SystemTableCloud" not in structured_comments
    assert not re.search(r"^import ", structured_comments, re.MULTILINE)
    assert set(re.findall(r"\{\{[A-Z_]+\}\}", structured_comments)) == set(PLACEHOLDERS)
    for placeholder, expected_count in PLACEHOLDERS.items():
        assert structured_comments.count(placeholder) == expected_count

    for source_file in SOURCE_ROOT.rglob("*.cpp"):
        source = source_file.read_text(encoding="utf-8")
        assert "REGISTER_SYSTEM_DOCS_MDUMENTATION" not in source
        assert "Common/SystemTableDocumentation.h" not in source
    assert "REGISTER_SYSTEM_DOCS_MDUMENTATION" not in system_log_source

    availability_requirements = {
        "transactions_info_log": (
            "transactions_info_log",
            "allow_experimental_transactions",
        ),
        "session_log": ("session_log",),
        "predicate_statistics_log": (
            "predicate_statistics_log",
            "predicate_statistics_sample_rate",
        ),
        "dead_letter_queue": (
            "dead_letter_queue",
            "handle_error_mode",
        ),
        "user_query_log": (
            "query_log.enable_user_query_log",
            "exists but is empty",
        ),
    }
    for table_name, requirements in availability_requirements.items():
        documentation = documents[table_name]
        assert "**Availability**" in documentation
        assert "UNKNOWN_TABLE" in documentation
        for requirement in requirements:
            assert requirement in documentation

    for old_registry_file in (
        SOURCE_ROOT / "Common" / "SystemTableDocumentation.cpp",
        SOURCE_ROOT / "Common" / "SystemTableDocumentation.h",
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

        # Every row exposed by system.documentation drives a page, and rotated
        # system-log table names remain excluded.
        documented_only = Path(directory) / "documented_only.mdx"
        assert generator.generate_pages(
            directory,
            {
                "documented_only": generated_body,
                "query_log_0": generated_body,
            },
        ) == (0, 1, 0)
        documented_only_text = documented_only.read_text(encoding="utf-8")
        assert "description: 'Structured embedded prose.'" in documented_only_text
        assert generator.AUTOGENERATED_START + "\n" + generated_body in documented_only_text
        assert not (Path(directory) / "query_log_0.mdx").exists()

        # Both published filenames for a legacy duplicate are refreshed from
        # the same structured comment and remain deterministic.
        assert generator.generate_pages(
            directory,
            {"warnings": generated_body},
        ) == (0, 2, 0)
        for filename in generator.documentation_filenames("warnings"):
            duplicate_text = (Path(directory) / filename).read_text(encoding="utf-8")
            assert generator.AUTOGENERATED_START + "\n" + generated_body in duplicate_text
        assert generator.generate_pages(
            directory,
            {"warnings": generated_body},
        ) == (0, 0, 2)

    print(
        f"OK: {len(documents)} structured system-table comments: "
        f"{len(attach_documents)} attached tables and "
        f"{len(system_log_documents)} system logs; generated pages preserve "
        "their MDX preamble and published section headings and update deterministically"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
