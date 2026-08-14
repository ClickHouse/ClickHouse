import importlib.machinery
import importlib.util
import json
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "utils/generate-system-tables-docs"
)
LOADER = importlib.machinery.SourceFileLoader(
    "generate_system_tables_docs", str(SCRIPT_PATH)
)
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
generate_system_tables_docs = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(generate_system_tables_docs)


def write_navigation(path, pages):
    path.write_text(
        json.dumps(
            {
                "pages": [
                    {
                        "group": "System Tables",
                        "pages": pages,
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def test_add_pages_to_navigation_orders_new_pages_and_is_idempotent(tmp_path):
    navigation_file = tmp_path / "navigation.json"
    write_navigation(
        navigation_file,
        [
            "reference/system-tables/overview",
            "reference/system-tables/azure_queue_log",
            "reference/system-tables/azure_queue_metadata_cache",
            "reference/system-tables/graphite_retentions",
            "reference/system-tables/histogram_metric_log",
            "reference/system-tables/row_policies",
            "reference/system-tables/s3_queue_settings",
        ],
    )

    table_names = ["handlers", "s3_queue_metadata", "azure_queue_metadata"]
    assert generate_system_tables_docs.add_pages_to_navigation(
        navigation_file, table_names
    ) == [
        "reference/system-tables/azure_queue_metadata",
        "reference/system-tables/handlers",
        "reference/system-tables/s3_queue_metadata",
    ]

    pages = json.loads(navigation_file.read_text(encoding="utf-8"))["pages"][0][
        "pages"
    ]
    assert pages == [
        "reference/system-tables/overview",
        "reference/system-tables/azure_queue_log",
        "reference/system-tables/azure_queue_metadata",
        "reference/system-tables/azure_queue_metadata_cache",
        "reference/system-tables/graphite_retentions",
        "reference/system-tables/handlers",
        "reference/system-tables/histogram_metric_log",
        "reference/system-tables/row_policies",
        "reference/system-tables/s3_queue_metadata",
        "reference/system-tables/s3_queue_settings",
    ]

    original_content = navigation_file.read_text(encoding="utf-8")
    assert not generate_system_tables_docs.add_pages_to_navigation(
        navigation_file, table_names
    )
    assert navigation_file.read_text(encoding="utf-8") == original_content


def test_main_adds_a_created_page_to_navigation(tmp_path, monkeypatch):
    binary = tmp_path / "clickhouse"
    binary.touch()
    docs_dir = tmp_path / "system-tables"
    docs_dir.mkdir()
    navigation_file = tmp_path / "navigation.json"
    write_navigation(
        navigation_file,
        [
            "reference/system-tables/overview",
            "reference/system-tables/metrics",
        ],
    )

    def run_query(_binary, _config, query, _use_client=False, _extra_args=None):
        if "FROM system.tables" in query:
            return [{"name": "new_table", "comment": "A newly documented table."}]
        if "default_kind NOT IN" in query:
            return [
                {
                    "table": "new_table",
                    "name": "value",
                    "type": "String",
                    "comment": "Stored value.",
                    "default_kind": "",
                    "default_expression": "",
                }
            ]
        return []

    monkeypatch.setattr(generate_system_tables_docs, "run_query", run_query)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--binary",
            str(binary),
            "--docs-dir",
            str(docs_dir),
            "--navigation-file",
            str(navigation_file),
        ],
    )

    generate_system_tables_docs.main()

    assert (docs_dir / "new_table.mdx").is_file()
    pages = json.loads(navigation_file.read_text(encoding="utf-8"))["pages"][0][
        "pages"
    ]
    assert pages == [
        "reference/system-tables/overview",
        "reference/system-tables/metrics",
        "reference/system-tables/new_table",
    ]


def test_add_pages_to_navigation_requires_the_system_tables_group(tmp_path):
    navigation_file = tmp_path / "navigation.json"
    navigation_file.write_text('{"pages": []}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="System Tables"):
        generate_system_tables_docs.add_pages_to_navigation(
            navigation_file, ["new_table"]
        )
