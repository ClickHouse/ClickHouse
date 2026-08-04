#!/usr/bin/env python3
"""Validate Mintlify component imports and exports.

Every snippet resolves its own imports. A page-level import is not visible to
a snippet imported by that page, and the same rule applies recursively when a
snippet renders another snippet. This check requires every non-built-in MDX
tag to have a local import and prevents snippets from importing the custom
Image component, which can collide with a page-level Image import.

Components must use named imports and provide matching named exports. Mintlify
can render a default-imported component while silently omitting page content.
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


IMPORT_RE = re.compile(
    r"^\s*import\s+(?P<spec>.+?)\s+from\s+['\"](?P<src>[^'\"]+)['\"]\s*;?\s*$",
    re.MULTILINE,
)
TAG_RE = re.compile(r"<\/?([A-Z][A-Za-z0-9_]*)\b")
IMG_SRC_RE = re.compile(r"<img\b[^>]*\bsrc=['\"]([^'\"]+)['\"]", re.IGNORECASE)
DECLARED_EXPORT_RE = re.compile(
    r"\bexport\s+(?:default\s+)?(?:const|let|var|function|class)\s+"
    r"([A-Z][A-Za-z0-9_]*)"
)
DOCS_MD_RE = re.compile(r'R"DOCS_MD\((?P<body>.*?)\)DOCS_MD"', re.DOTALL)
DEFAULT_EXPORT_RE = re.compile(r"\bexport\s+default\b")
DEFAULT_EXPORT_NAME_RE = re.compile(
    r"\bexport\s+default\s+([A-Z][A-Za-z0-9_]*)\s*;"
)
NAMED_EXPORT_RE = re.compile(
    r"\bexport\s+(?:const|let|var|function|class)\s+"
    r"([A-Za-z_$][A-Za-z0-9_$]*)"
)
EXPORT_LIST_RE = re.compile(r"\bexport\s*\{(?P<names>[^}]*)\}")
TRANSLATION_DIRS = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}
MINTLIFY_BUILTINS = {
    "Accordion",
    "CodeBlock",
    "Frame",
    "Info",
    "Note",
    "Step",
    "Steps",
    "Tab",
    "Tabs",
    "Tip",
    "Warning",
}


@dataclass(frozen=True)
class Binding:
    exported: str
    local: str
    source: str
    is_default: bool = False
    is_namespace: bool = False


def without_fenced_code(text: str) -> str:
    """Blank fenced code while retaining line boundaries for import parsing."""
    output: list[str] = []
    fence: str | None = None
    for line in text.splitlines(keepends=True):
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            char = marker.group(1)[0]
            if fence is None:
                fence = char
            elif fence == char:
                fence = None
            output.append("\n" if line.endswith("\n") else "")
        elif fence is None:
            output.append(line)
        else:
            output.append("\n" if line.endswith("\n") else "")
    return "".join(output)


def parse_bindings(text: str) -> list[Binding]:
    bindings: list[Binding] = []
    for match in IMPORT_RE.finditer(without_fenced_code(text)):
        spec = match.group("spec").strip()
        source = match.group("src")
        default_match = re.fullmatch(
            r"(?P<local>[A-Za-z_$][A-Za-z0-9_$]*)"
            r"(?:\s*,\s*(?P<rest>.+))?",
            spec,
        )
        if default_match:
            bindings.append(
                Binding(
                    Path(source).stem,
                    default_match.group("local"),
                    source,
                    is_default=True,
                )
            )
            spec = default_match.group("rest")
            if spec is None:
                continue
            spec = spec.strip()
        if spec.startswith("{") and spec.endswith("}"):
            for piece in spec[1:-1].split(","):
                piece = piece.strip()
                if not piece:
                    continue
                names = re.split(r"\s+as\s+", piece, maxsplit=1)
                bindings.append(Binding(names[0], names[-1], source))
        elif spec.startswith("*"):
            local = re.split(r"\s+as\s+", spec, maxsplit=1)[-1]
            bindings.append(
                Binding(
                    Path(source).stem,
                    local,
                    source,
                    is_namespace=True,
                )
            )
    return bindings


def is_nested_snippet_source(source: str) -> bool:
    return source.startswith("/snippets/") and source.endswith((".md", ".mdx"))


def is_translation_source(source: str) -> bool:
    parts = Path(source.lstrip("/")).parts
    return len(parts) > 1 and parts[0] == "snippets" and parts[1] in TRANSLATION_DIRS


def is_component_source(source: str) -> bool:
    source_parts = Path(source.lstrip("/")).parts
    return (
        len(source_parts) >= 3
        and source_parts[0] == "snippets"
        and "components" in source_parts
        and source.endswith(".jsx")
    ) or source.startswith(
        ("@theme/", "@site/src/components/", "@site/src/theme/")
    )


def named_component_exports(text: str) -> set[str]:
    exports = set(NAMED_EXPORT_RE.findall(text))
    for match in EXPORT_LIST_RE.finditer(text):
        for piece in match.group("names").split(","):
            piece = piece.strip()
            if not piece:
                continue
            names = re.split(r"\s+as\s+", piece, maxsplit=1)
            exported = names[-1]
            if re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*", exported):
                exports.add(exported)
    return exports


def find_component_export_errors(docs_root: Path) -> list[str]:
    """Require every JSX component with a default export to export its name."""
    errors: list[str] = []
    snippets_root = docs_root / "snippets"
    for path in sorted(snippets_root.rglob("*.jsx")):
        relative_path = path.relative_to(docs_root)
        if "components" not in relative_path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        named_exports = named_component_exports(text)
        default_exports = DEFAULT_EXPORT_NAME_RE.findall(text)
        for default_export in default_exports:
            if default_export not in named_exports:
                errors.append(
                    f"{relative_path.as_posix()}: component {default_export} "
                    "must have a named export"
                )
        if DEFAULT_EXPORT_RE.search(text) and not default_exports:
            errors.append(
                f"{relative_path.as_posix()}: anonymous default component "
                "must be replaced by a named export"
            )
        if path.stem[0].isupper() and not named_exports and not default_exports:
            errors.append(
                f"{relative_path.as_posix()}: component module must have a "
                "named export"
            )
    return errors


def find_component_import_errors(docs_root: Path) -> list[str]:
    """Require named component imports in files and C++ documentation blocks."""
    errors: list[str] = []

    exports_cache: dict[Path, set[str]] = {}

    def exports_for(path: Path) -> set[str]:
        if path not in exports_cache:
            exports_cache[path] = named_component_exports(
                path.read_text(encoding="utf-8", errors="ignore")
            )
        return exports_cache[path]

    def check_text(text: str, display_path: str) -> None:
        for binding in parse_bindings(text):
            if not is_component_source(binding.source):
                continue
            if binding.is_default or binding.is_namespace:
                errors.append(
                    f"{display_path}: use a named import for component "
                    f"{binding.local}"
                )
                continue
            if not binding.source.startswith("/snippets/"):
                continue
            target = docs_root / binding.source.lstrip("/")
            if not target.is_file():
                errors.append(
                    f"{display_path}: component import does not exist: "
                    f"{binding.source}"
                )
            elif binding.exported not in exports_for(target):
                errors.append(
                    f"{display_path}: component {binding.source} does not "
                    f"export {binding.exported} by name"
                )

    for path in sorted(docs_root.rglob("*")):
        if path.suffix not in {".md", ".mdx", ".jsx"}:
            continue
        check_text(
            path.read_text(encoding="utf-8", errors="ignore"),
            path.relative_to(docs_root).as_posix(),
        )

    repository_root = docs_root.parent
    source_root = repository_root / "src"
    if source_root.is_dir():
        for path in sorted(source_root.rglob("*")):
            if path.suffix not in {".cpp", ".h"}:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            docs_blocks = DOCS_MD_RE.finditer(text)
            for docs_block in docs_blocks:
                check_text(
                    docs_block.group("body"),
                    path.relative_to(repository_root).as_posix(),
                )

    return errors


def find_page_import_collisions(docs_root: Path) -> list[str]:
    """Find bindings Mint would declare twice when expanding nested snippets."""
    errors: list[str] = []
    bindings_cache: dict[Path, list[Binding]] = {}

    def bindings_for(path: Path) -> list[Binding]:
        if path not in bindings_cache:
            bindings_cache[path] = parse_bindings(
                path.read_text(encoding="utf-8", errors="ignore")
            )
        return bindings_cache[path]

    def collect_bindings(
        path: Path,
        visited: set[Path],
        declarations: dict[str, list[tuple[Path, str]]],
    ) -> None:
        if path in visited or not path.is_file():
            return
        visited.add(path)
        for binding in bindings_for(path):
            declarations[binding.local].append((path, binding.source))
            if is_nested_snippet_source(binding.source):
                collect_bindings(
                    docs_root / binding.source.lstrip("/"),
                    visited,
                    declarations,
                )

    for path in sorted(docs_root.rglob("*")):
        if path.suffix not in {".md", ".mdx"}:
            continue
        relative_path = path.relative_to(docs_root)
        if (
            relative_path.parts[0] in TRANSLATION_DIRS
            or relative_path.parts[0] == "snippets"
        ):
            continue

        declarations: dict[str, list[tuple[Path, str]]] = defaultdict(list)
        collect_bindings(path, set(), declarations)
        for local, occurrences in sorted(declarations.items()):
            if len(occurrences) < 2:
                continue
            details = "; ".join(
                f"{owner.relative_to(docs_root).as_posix()} imports {source}"
                for owner, source in occurrences
            )
            errors.append(
                f"{relative_path.as_posix()}: duplicate import binding {local}: "
                f"{details}"
            )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("docs_root", nargs="?", default=".")
    args = parser.parse_args()

    docs_root = Path(args.docs_root).resolve()
    snippets_root = docs_root / "snippets"
    if not snippets_root.is_dir():
        parser.error(f"No snippets directory under docs root: {docs_root}")

    errors: list[str] = []
    checked = 0
    checked_images = 0

    errors.extend(find_component_export_errors(docs_root))
    errors.extend(find_component_import_errors(docs_root))

    for path in sorted(snippets_root.rglob("*")):
        if path.suffix not in {".md", ".mdx"}:
            continue
        relative_path = path.relative_to(snippets_root)
        if (
            "components" in relative_path.parts
            or relative_path.parts[0] in TRANSLATION_DIRS
        ):
            continue

        checked += 1
        text = path.read_text(encoding="utf-8", errors="ignore")
        visible_text = without_fenced_code(text)
        bindings = parse_bindings(text)
        imported = {binding.local for binding in bindings}
        declared = set(DECLARED_EXPORT_RE.findall(visible_text))

        if any(
            binding.source == "/snippets/components/Image.jsx"
            for binding in bindings
        ):
            errors.append(
                f"{relative_path.as_posix()}: use <Frame><img ... /></Frame> "
                "instead of importing the custom Image component"
            )

        used_tags = set(TAG_RE.findall(visible_text))
        missing = sorted(used_tags - imported - declared - MINTLIFY_BUILTINS)
        if missing:
            errors.append(
                f"{relative_path.as_posix()}: missing local import(s) for "
                + ", ".join(missing)
            )

        for source in IMG_SRC_RE.findall(visible_text):
            if not source.startswith("/"):
                continue
            checked_images += 1
            target = docs_root / source.lstrip("/")
            if not target.is_file():
                errors.append(
                    f"{relative_path.as_posix()}: local image does not exist: {source}"
                )

        for binding in bindings:
            if not is_nested_snippet_source(binding.source):
                continue
            target = docs_root / binding.source.lstrip("/")
            if not target.is_file():
                errors.append(
                    f"{relative_path.as_posix()}: nested snippet import does not exist: "
                    f"{binding.source}"
                )
            if is_translation_source(binding.source):
                errors.append(
                    f"{relative_path.as_posix()}: default-locale snippet imports translated "
                    f"snippet {binding.source}"
                )

    errors.extend(find_page_import_collisions(docs_root))

    if errors:
        print(f"Found {len(errors)} snippet import error(s):")
        for error in errors:
            print(f"  - {error}")
        return 1

    print(
        f"Checked imports in {checked} snippet file(s) and "
        f"{checked_images} local image reference(s): OK"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
