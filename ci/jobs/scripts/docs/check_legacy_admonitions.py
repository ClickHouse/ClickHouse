#!/usr/bin/env python3
"""Reject legacy Docusaurus admonitions in canonical documentation sources."""

import re
import sys
from pathlib import Path


LEGACY_ADMONITION_RE = re.compile(
    r"^[ \t]*:{3,}(?:note|warning|tip|info|caution|danger|important)"
    r"(?:[ \t\[].*)?$",
    re.MULTILINE,
)
LOGICAL_ADMONITION_RE = re.compile(
    r"^[ \t]*:{3,}(?:note|warning|tip|info|caution|danger|important)"
    r"(?:[ \t\[][^\r\n]*)?\r?\n",
    re.MULTILINE,
)
BLOCK_COMMENT_PATTERN = r"/\*[\s\S]*?\*/"
CPP_LINE_COMMENT_PATTERN = r"//(?:\\\r?\n|[^\r\n])*(?:\r?\n|$)"
SQL_LINE_COMMENT_PATTERN = r"--[^\r\n]*(?:\r?\n|$)"
SQL_TRIVIA_PATTERN = rf"(?:\s|{BLOCK_COMMENT_PATTERN}|{SQL_LINE_COMMENT_PATTERN})*"
CPP_TOKEN_SEPARATOR_RE = re.compile(
    rf"(?:\s|{BLOCK_COMMENT_PATTERN}|{CPP_LINE_COMMENT_PATTERN})*"
)
SQL_LITERAL_SEPARATOR_RE = re.compile(
    rf"{SQL_TRIVIA_PATTERN}"
    rf"(?:\){SQL_TRIVIA_PATTERN})*"
    r"\|\|"
    rf"{SQL_TRIVIA_PATTERN}"
    rf"(?:\({SQL_TRIVIA_PATTERN})*"
)
CPP_TOKEN_RE = re.compile(
    rf"(?P<line_comment>{CPP_LINE_COMMENT_PATTERN})"
    rf"|(?P<block_comment>{BLOCK_COMMENT_PATTERN})"
    r"|(?P<character>(?:u8|u|U|L)?'(?:\\[\s\S]|[^'\\])*')"
    r'|(?P<raw_string>(?:u8|u|U|L)?R"'
    r'(?P<raw_delimiter>[^ ()\\\t\r\n]{0,16})'
    r"\((?P<raw_body>[\s\S]*?)\)(?P=raw_delimiter)\")"
    r'|(?P<string>(?:u8|u|U|L)?"(?P<string_body>(?:\\[\s\S]|[^"\\])*)")'
)
SQL_TOKEN_RE = re.compile(
    rf"(?P<line_comment>{SQL_LINE_COMMENT_PATTERN})"
    rf"|(?P<block_comment>{BLOCK_COMMENT_PATTERN})"
    r'|(?P<identifier>"(?:""|[^"])*")'
    r"|(?P<string>'(?P<string_body>(?:\\[\s\S]|''|[^'\\])*)')"
)
SIMPLE_ESCAPES = {
    "a": "\a",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
}
SOURCE_EXTENSIONS = {".cpp", ".h", ".hpp", ".inc"}
# This compatibility test intentionally exercises the legacy renderer syntax.
SOURCE_EXCLUSIONS = {
    Path("Client/tests/gtest_terminal_markdown_renderer.cpp"),
}
DOC_EXTENSIONS = {".md", ".mdx"}
LOCALES = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}


def is_localized_doc(relative_path):
    """Localized pages are read-only output owned by the translation workflow."""
    return relative_path.parts[0] in LOCALES or (
        relative_path.parts[0] == "snippets"
        and len(relative_path.parts) > 1
        and relative_path.parts[1] in LOCALES
    )


def canonical_documentation_files(repo_root):
    source_root = repo_root / "src"
    for path in source_root.rglob("*"):
        if (
            path.suffix in SOURCE_EXTENSIONS
            and path.relative_to(source_root) not in SOURCE_EXCLUSIONS
        ):
            yield path

    docs_root = repo_root / "docs"
    for path in docs_root.rglob("*"):
        if path.suffix not in DOC_EXTENSIONS:
            continue
        relative_path = path.relative_to(docs_root)
        if relative_path.parts[0] == "_migration" or is_localized_doc(relative_path):
            continue
        yield path

    yield from (repo_root / "ci/jobs/scripts/docs/autogenerate/sql").glob("*.sql")


def decode_escaped_text(body, body_offset, doubled_quote=False):
    """Decode the escapes which can affect logical line structure and punctuation."""
    decoded = []
    source_offsets = []
    index = 0
    while index < len(body):
        if doubled_quote and body.startswith("''", index):
            decoded.append("'")
            source_offsets.append(body_offset + index)
            index += 2
            continue
        if body[index] != "\\" or index + 1 == len(body):
            decoded.append(body[index])
            source_offsets.append(body_offset + index)
            index += 1
            continue

        escape_offset = body_offset + index
        escaped = body[index + 1]
        if escaped == "\n":
            index += 2
            continue
        if escaped == "\r":
            index += 3 if index + 2 < len(body) and body[index + 2] == "\n" else 2
            continue

        if escaped in SIMPLE_ESCAPES:
            value = SIMPLE_ESCAPES[escaped]
            length = 2
        elif escaped == "x":
            end = index + 2
            while end < len(body) and body[end] in "0123456789abcdefABCDEF":
                end += 1
            if end == index + 2:
                value = "x"
                length = 2
            else:
                codepoint = int(body[index + 2 : end], 16)
                value = (
                    chr(codepoint)
                    if codepoint <= sys.maxunicode
                    else "\N{REPLACEMENT CHARACTER}"
                )
                length = end - index
        elif escaped in "01234567":
            end = index + 2
            while end < min(index + 4, len(body)) and body[end] in "01234567":
                end += 1
            value = chr(int(body[index + 1 : end], 8))
            length = end - index
        else:
            value = escaped
            length = 2

        decoded.append(value)
        source_offsets.append(escape_offset)
        index += length

    return "".join(decoded), source_offsets


def cpp_string_literals(text):
    for match in CPP_TOKEN_RE.finditer(text):
        if match.group("raw_string") is not None:
            start, end = match.span("raw_body")
            yield match.start(), match.end(), match.group("raw_body"), list(range(start, end))
            continue
        if match.group("string") is None:
            continue
        start, _ = match.span("string_body")
        value, offsets = decode_escaped_text(match.group("string_body"), start)
        yield match.start(), match.end(), value, offsets


def sql_string_literals(text):
    for match in SQL_TOKEN_RE.finditer(text):
        if match.group("string") is None:
            continue
        start, _ = match.span("string_body")
        value, offsets = decode_escaped_text(
            match.group("string_body"), start, doubled_quote=True
        )
        yield match.start(), match.end(), value, offsets


def concatenate_literals(text, literals, separator_re):
    current_value = ""
    current_offsets = []
    previous_end = None
    for start, end, value, offsets in literals:
        if previous_end is None or separator_re.fullmatch(text[previous_end:start]) is None:
            if current_offsets:
                yield current_value, current_offsets
            current_value = value
            current_offsets = offsets.copy()
        else:
            current_value += value
            current_offsets.extend(offsets)
        previous_end = end
    if current_offsets:
        yield current_value, current_offsets


def logical_documentation_strings(path, text):
    if path.suffix == ".sql":
        return concatenate_literals(text, sql_string_literals(text), SQL_LITERAL_SEPARATOR_RE)
    return concatenate_literals(text, cpp_string_literals(text), CPP_TOKEN_SEPARATOR_RE)


def find_legacy_admonitions(repo_root):
    findings = []
    for path in canonical_documentation_files(repo_root):
        text = path.read_text(encoding="utf-8")
        if path.suffix in SOURCE_EXTENSIONS or path.suffix == ".sql":
            for logical_text, source_offsets in logical_documentation_strings(path, text):
                for match in LOGICAL_ADMONITION_RE.finditer(logical_text):
                    source_offset = source_offsets[match.start()]
                    line_number = text.count("\n", 0, source_offset) + 1
                    findings.append(
                        f"{path.relative_to(repo_root)}:{line_number}:{match.group(0).strip()}"
                    )
        else:
            for match in LEGACY_ADMONITION_RE.finditer(text):
                line_number = text.count("\n", 0, match.start()) + 1
                findings.append(
                    f"{path.relative_to(repo_root)}:{line_number}:{match.group(0).strip()}"
                )
    return findings


def main():
    repo_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    findings = find_legacy_admonitions(repo_root)
    if findings:
        print("Legacy Docusaurus admonitions found in canonical documentation sources:")
        print("\n".join(findings))
        return 1
    print("OK: canonical documentation sources use Mintlify admonition components.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
