#!/usr/bin/env python3
"""Focused regression tests for the Mintlify component import policy."""

import importlib.util
import sys
import tempfile
from pathlib import Path


HERE = Path(__file__).resolve().parent


def load_checker():
    spec = importlib.util.spec_from_file_location(
        "snippet_component_imports_check",
        HERE / "snippet_component_imports_check.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    checker = load_checker()
    tmp_root = Path.cwd() / "tmp"
    tmp_root.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory(dir=tmp_root) as directory:
        repository_root = Path(directory)
        docs_root = repository_root / "docs"
        snippets_root = docs_root / "snippets"
        snippets_root.mkdir(parents=True)

        clickhouse_supported = (
            snippets_root
            / "components"
            / "ClickHouseSupported"
            / "ClickHouseSupported.jsx"
        )
        clickhouse_supported.parent.mkdir(parents=True)
        clickhouse_supported.write_text(
            "export const ClickHouseSupportedBadge = () => null;\n"
            "export default ClickHouseSupportedBadge;\n",
            encoding="utf-8",
        )

        settings_info = (
            snippets_root
            / "components"
            / "SettingsInfoBlock"
            / "SettingsInfoBlock.jsx"
        )
        settings_info.parent.mkdir(parents=True)
        settings_info.write_text(
            "const SettingsInfoBlock = () => null;\n"
            "export default SettingsInfoBlock;\n",
            encoding="utf-8",
        )

        page = docs_root / "page.mdx"
        page.write_text(
            'import ClickHouseSupportedBadge from '
            '"/snippets/components/ClickHouseSupported/ClickHouseSupported.jsx";\n',
            encoding="utf-8",
        )

        source = repository_root / "src" / "GeneratedDocs.cpp"
        source.parent.mkdir()
        source.write_text(
            'R"DOCS_MD(\n'
            'import SettingsInfoBlock from '
            '"/snippets/components/SettingsInfoBlock/SettingsInfoBlock.jsx";\n'
            "import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';\n"
            ')DOCS_MD"\n',
            encoding="utf-8",
        )

        assert checker.find_component_export_errors(docs_root) == [
            "snippets/components/SettingsInfoBlock/SettingsInfoBlock.jsx: "
            "component SettingsInfoBlock must have a named export",
        ]
        assert checker.find_component_import_errors(docs_root) == [
            "page.mdx: use a named import for component "
            "ClickHouseSupportedBadge",
            "src/GeneratedDocs.cpp: use a named import for component "
            "SettingsInfoBlock",
            "src/GeneratedDocs.cpp: use a named import for component "
            "CloudOnlyBadge",
        ]

        page.write_text(
            'import { ClickHouseSupportedBadge } from '
            '"/snippets/components/ClickHouseSupported/ClickHouseSupported.jsx";\n',
            encoding="utf-8",
        )
        settings_info.write_text(
            "export const SettingsInfoBlock = () => null;\n"
            "export default SettingsInfoBlock;\n",
            encoding="utf-8",
        )
        source.write_text(
            'R"DOCS_MD(\n'
            'import { SettingsInfoBlock } from '
            '"/snippets/components/SettingsInfoBlock/SettingsInfoBlock.jsx";\n'
            ')DOCS_MD"\n',
            encoding="utf-8",
        )
        assert checker.find_component_export_errors(docs_root) == []
        assert checker.find_component_import_errors(docs_root) == []

    print("OK: components use named exports and named imports in MDX and C++")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
