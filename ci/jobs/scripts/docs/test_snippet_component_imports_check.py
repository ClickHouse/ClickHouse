#!/usr/bin/env python3
"""Focused regression tests for the Mintlify badge import policy."""

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
            'import CloudSupportedBadge from '
            '"/snippets/components/CloudSupportedBadge/CloudSupportedBadge.jsx";\n'
            ')DOCS_MD"\n',
            encoding="utf-8",
        )

        errors = checker.find_default_badge_imports(docs_root)
        assert errors == [
            "page.mdx: use a named import for badge component "
            "ClickHouseSupportedBadge",
            "src/GeneratedDocs.cpp: use a named import for badge component "
            "CloudSupportedBadge",
        ], errors

        page.write_text(
            'import { ClickHouseSupportedBadge } from '
            '"/snippets/components/ClickHouseSupported/ClickHouseSupported.jsx";\n',
            encoding="utf-8",
        )
        source.write_text(
            'R"DOCS_MD(\n'
            'import { CloudSupportedBadge } from '
            '"/snippets/components/CloudSupportedBadge/CloudSupportedBadge.jsx";\n'
            ')DOCS_MD"\n',
            encoding="utf-8",
        )
        assert checker.find_default_badge_imports(docs_root) == []

    print("OK: badge imports are checked by symbol in MDX and C++ docs sources")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
