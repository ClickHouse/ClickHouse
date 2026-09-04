#!/usr/bin/env python3
"""Test structured system-table sources and Python page rewriting."""

import importlib.util
import io
import re
import tempfile
from contextlib import redirect_stderr
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest.mock import patch


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[4]
GENERATOR = REPO_ROOT / "utils" / "generate-system-tables-docs"
SOURCE_ROOT = REPO_ROOT / "src"
ATTACH_SOURCE = SOURCE_ROOT / "Storages" / "System" / "attachSystemTables.cpp"
SYSTEM_LOG_HEADER = SOURCE_ROOT / "Interpreters" / "SystemLog.h"
SCHEMA_SPECIFIC_SYSTEM_LOG_DOCUMENTATION_SOURCES = {
    "transposed": SOURCE_ROOT / "Interpreters" / "TransposedMetricLog.h",
    "bucketed": SOURCE_ROOT / "Interpreters" / "BucketedMetricLog.h",
}
NON_LITERAL_ATTACH_DOCUMENTATION_SOURCES = {
    "ASYNCHRONOUS_METRICS_DOCUMENTATION": (
        SOURCE_ROOT / "Storages" / "System" / "StorageSystemAsynchronousMetrics.cpp"
    )
}
ASYNC_METRIC_DOCUMENTATION_CATALOG = (
    SOURCE_ROOT / "Common" / "AsynchronousMetricDocumentation.inc"
)
ASYNC_METRICS_PAGE = (
    REPO_ROOT / "docs" / "reference" / "system-tables" / "asynchronous_metrics.mdx"
)

EXPECTED_DOCUMENTATION_COUNT = 169
EXPECTED_ATTACH_DOCUMENTATION_COUNT = 139
EXPECTED_SYSTEM_LOG_DOCUMENTATION_COUNT = 30
EXPECTED_FIELD_COUNTS = {
    "description": EXPECTED_DOCUMENTATION_COUNT,
    "columns_notes": 10,
    "examples": 105,
    "see_also": 60,
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
    async_metrics_generator = generator.load_async_metrics_generator()

    partial_result = generator.subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout="name\tdescription\nString\tString\npartial\tIncomplete page\n",
        stderr="Code: 999. Partial result",
    )
    complete_result = generator.subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout="name\tdescription\nString\tString\ncomplete\tComplete page\n",
        stderr="",
    )
    with patch.object(
        generator.subprocess,
        "run",
        side_effect=[partial_result, complete_result],
    ) as run:
        with redirect_stderr(io.StringIO()):
            assert generator.run_query("clickhouse", None, "SELECT 1") == [
                {"name": "complete", "description": "Complete page"}
            ]
        assert run.call_count == 2

    large_description = "x" * (128 * 1024)
    large_result = generator.subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=(
            "name\tdescription\n"
            "String\tString\n"
            f"large\t{large_description}\n"
        ),
        stderr="",
    )
    with patch.object(generator.subprocess, "run", return_value=large_result):
        assert generator.run_query("clickhouse", None, "SELECT 1") == [
            {"name": "large", "description": large_description}
        ]

    with patch.object(
        generator.subprocess,
        "run",
        side_effect=[partial_result, partial_result],
    ) as run:
        with redirect_stderr(io.StringIO()):
            try:
                generator.run_query("clickhouse", None, "SELECT 1")
            except SystemExit as error:
                assert error.code == 1
            else:
                raise AssertionError("Repeated partial output must fail")
        assert run.call_count == 2

    attach_source = ATTACH_SOURCE.read_text(encoding="utf-8")
    attach_documents = dict(
        re.findall(
            r'attach(?:NoDescription)?<[^;\n]+>\(context,\s*system_database,\s*"([^"]+)",'
            r'\s*R"DOCS_MD\((.*?)\)DOCS_MD"',
            attach_source,
            re.DOTALL,
        )
    )
    non_literal_attach_documents = dict(
        re.findall(
            r'attach(?:NoDescription)?<[^;\n]+>\(\s*context,\s*system_database,\s*"([^"]+)",'
            r'\s*([A-Z][A-Z0-9_]*_DOCUMENTATION)\b',
            attach_source,
            re.DOTALL,
        )
    )
    assert set(non_literal_attach_documents.values()) == set(
        NON_LITERAL_ATTACH_DOCUMENTATION_SOURCES
    )
    for table_name, constant_name in non_literal_attach_documents.items():
        source = NON_LITERAL_ATTACH_DOCUMENTATION_SOURCES[constant_name].read_text(
            encoding="utf-8"
        )
        documentation = re.search(
            rf'\b{re.escape(constant_name)}\s*=\s*R"DOCS_MD\((.*?)\)DOCS_MD";',
            source,
            re.DOTALL,
        )
        assert documentation is not None
        attach_documents[table_name] = documentation.group(1)
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

    schema_specific_system_log_documents = {}
    for schema, source_file in SCHEMA_SPECIFIC_SYSTEM_LOG_DOCUMENTATION_SOURCES.items():
        source = source_file.read_text(encoding="utf-8")
        documentation = re.search(
            r'\bDOCUMENTATION\s*=\s*R"DOCS_MD\((.*?)\)DOCS_MD";',
            source,
            re.DOTALL,
        )
        assert documentation is not None
        schema_specific_system_log_documents[schema] = documentation.group(1)
    for documentation in schema_specific_system_log_documents.values():
        assert documentation.count(".description") == 1
        assert documentation.count(".examples") == 1
        assert documentation.count(".see_also") == 1

    documents = attach_documents | system_log_documents
    assert len(documents) == EXPECTED_DOCUMENTATION_COUNT
    table_names = list(documents)
    assert len(table_names) == len(set(table_names))
    assert "statements" in table_names

    structured_comments = "\n".join(documents.values())
    for field, expected_count in EXPECTED_FIELD_COUNTS.items():
        assert len(re.findall(rf"(?m)^\.{field}$", structured_comments)) == expected_count
    assert ".see_also" in documents["asynchronous_metric_log"]
    assert "**See Also**" not in documents["asynchronous_metric_log"]
    assert ".additional_sections" not in structured_comments
    assert ".get_columns" not in structured_comments
    assert "{{SYSTEM_TABLE_COLUMNS}}" not in structured_comments
    assert "SystemTableCloud" not in structured_comments
    assert not re.search(r"^import ", structured_comments, re.MULTILINE)
    assert set(re.findall(r"\{\{[A-Z_]+\}\}", structured_comments)) == set(PLACEHOLDERS)
    for placeholder, expected_count in PLACEHOLDERS.items():
        assert structured_comments.count(placeholder) == expected_count

    documentation_input_sources = {
        ATTACH_SOURCE,
        SYSTEM_LOG_HEADER,
        *SCHEMA_SPECIFIC_SYSTEM_LOG_DOCUMENTATION_SOURCES.values(),
        *NON_LITERAL_ATTACH_DOCUMENTATION_SOURCES.values(),
        *(REPO_ROOT / source for source in async_metrics_generator.SOURCE_FILES),
    }
    for source_file in documentation_input_sources:
        source = source_file.read_text(encoding="utf-8")
        assert "REGISTER_SYSTEM_TABLE_DOCUMENTATION" not in source
        assert "Common/SystemTableDocumentation.h" not in source

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
    assert async_metrics_generator.documentation_anchor("jemalloc.epoch") == "jemallocepoch"
    assert async_metrics_generator.documentation_anchor("metric_name") == "metric_name"
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
    generated_cpp_catalog = async_metrics_generator.render_cpp_catalog(
        async_metrics_generator.collect_metrics()
    )
    assert (
        ASYNC_METRIC_DOCUMENTATION_CATALOG.read_text(encoding="utf-8")
        == generated_cpp_catalog
    )
    assert "Source-backed introduction." in asynchronous_metrics_body
    assert "### AsynchronousMetricsUpdateInterval" in asynchronous_metrics_body
    assert "### jemalloc.epoch {#jemallocepoch}" in asynchronous_metrics_body
    published_metric_anchors = dict(
        re.findall(
            r"(?m)^### (.+) \{#([^}\n]+)\}$",
            ASYNC_METRICS_PAGE.read_text(encoding="utf-8"),
        )
    )
    generated_metric_anchors = dict(
        re.findall(r"(?m)^### (.+) \{#([^}\n]+)\}$", asynchronous_metrics_body)
    )
    common_metric_names = published_metric_anchors.keys() & generated_metric_anchors.keys()
    assert len(common_metric_names) > 200
    assert all(
        generated_metric_anchors[name] == published_metric_anchors[name]
        for name in common_metric_names
    )
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
            + generator.add_legacy_columns_generated_region(
                preserved_generated_body
            )
            + "\n"
        )
        assert page.read_text(encoding="utf-8") == expected
        assert not generator.update_full_doc_file(page, generated_body)

        migrated = Path(directory) / "migrated.mdx"
        migrated.write_text(
            frontmatter
            + "\n"
            + preamble
            + "\n"
            + generator.AUTOGENERATED_START
            + "\n"
            + old_body.replace(generator.AUTOGENERATED_START, "").replace(
                generator.AUTOGENERATED_END, ""
            ).strip()
            + "\n"
            + generator.AUTOGENERATED_END
            + "\n",
            encoding="utf-8",
        )
        assert generator.update_full_doc_file(migrated, generated_body)
        assert migrated.read_text(encoding="utf-8") == (
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
        assert not generator.update_full_doc_file(migrated, generated_body)

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
        f"{len(system_log_documents)} system logs; Python page rewrites preserve "
        "their MDX preamble and published section headings and update deterministically"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
