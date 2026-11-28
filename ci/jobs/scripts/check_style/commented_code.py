#!/usr/bin/env python3
"""Find commented-out code in C++, JavaScript, Python and shell sources.

Commented-out fragments of code are forbidden in the repository: code is either
live or deleted (Git remembers it). A commented-out block is not compiled, not
run, not tested and not reviewed again, so a "temporary" one quietly becomes
permanent. The only exception is a short fragment of debug logging (at most
`MAX_DEBUG_LOGGING_LINES` lines), such as `// LOG_DEBUG(log, "...");`.

A "block" is a run of consecutive full-line comments (documentation comments
`///` and `//!` are not considered). A block is reported when most of its lines
look like code rather than prose. To fix a report, delete the code; if it is an
explanation, write it as a sentence with the code in backticks, which reads as
prose: `// Returns a > b ? 1 : (a < b ? -1 : 0).` instead of `// a > b ? 1 : ...;`.

Usage: commented_code.py FILE...
"""

import ast
import re
import sys

MAX_DEBUG_LOGGING_LINES = 3

DEBUG_LOGGING = re.compile(
    r"\b(LOG_(TEST|TRACE|DEBUG|INFO|WARNING|ERROR|FATAL)|std::(cerr|cout|clog)|fmt::print|f?printf|"
    r"DUMP|print|pprint|console\.(log|debug|error|warn)|logging\.\w+|logger\.\w+|log\.\w+|echo|debug\w*)\b|^log\s*=\s*sys\.std(out|err)$"
    r"|^set -[exv]+$|^<<"
)

KEYWORDS = {
    "c": set(
        """
        alignas alignof auto bool break case catch char class const constexpr consteval constinit continue
        decltype default delete do double else enum explicit export extern false final float for friend goto if
        import inline int long mutable namespace new noexcept nullptr operator override private protected public
        register return short signed sizeof static struct switch template this throw true try typedef typename
        union unsigned using virtual void volatile while let var function await async of in typeof instanceof
        """.split()
    ),
    "py": set(
        """
        and as assert async await break class continue def del elif else except false finally for from global if
        import in is lambda none nonlocal not or pass raise return true try while with yield
        """.split()
    ),
    "sh": set(
        """
        if then else elif fi for while until do done case esac in function select time local export readonly
        declare
        """.split()
    ),
}
# Two plain words in a row are enough for C++ and JavaScript, where code almost always separates
# identifiers with punctuation; a commented-out declaration such as `String s;` is missed, which is fine.
PROSE_RUN = {"c": 2, "py": 3, "sh": 3}
QUOTED = re.compile(r""""(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|`[^`]*`""")
WORD = re.compile(r"[A-Za-z]+")
SH_PUNCTUATION = re.compile(r"[$|&<>=;`\\]")


def is_prose(text, lang):
    """A run of plain words that are not keywords: code rarely has them outside string literals."""
    if re.search(r"[A-Za-z][.,]$", text):
        return True  # Code does not end with a word followed by a period or a comma.
    if lang == "c" and re.search(r"(^|\s)(e\.g\.|i\.e\.|vs\.?|then)(\s|$)", QUOTED.sub(" ", text)):
        return True  # Pseudo-code in an explanation: `if x then y`, `A vs B`.
    threshold = PROSE_RUN[lang]
    if lang == "sh" and not SH_PUNCTUATION.search(text):
        threshold = 2
    run = 0
    for token in QUOTED.sub(" \0 ", text).split(" "):
        if not token:
            continue
        word = token.rstrip(".,:;!?")
        if WORD.fullmatch(word) and word.lower() not in KEYWORDS[lang]:
            run += 1
            if run >= threshold:
                return True
        else:
            run = 0
    return False


C_KEYWORD_START = re.compile(
    r"^(if|else|for|while|do|switch|case|return|break|continue|throw|try|catch|auto|const|constexpr|"
    r"static|using|typedef|template|namespace|struct|class|enum|delete|new|goto|"
    r"#\s*(include|if|ifdef|ifndef|endif|else|elif|define|undef|pragma)|"
    r"let|var|function|await|async|export|import)\b"
)


def c_like_code_line(text):
    if not text or is_prose(text, "c"):
        return False
    if text in ("{", "}", "};", "});", ")", "]", "]);"):
        return True
    if C_KEYWORD_START.match(text) and re.search(r"[;{}()<>=]", text):
        return True
    if text.endswith(";") and re.search(r"\(.*\)|\s=\s|->|::|\+\+|--|\[|^\w+;$|\w+\s+\w+;$", text):
        return True
    if text.endswith("{") and re.search(r"\)|\b(else|do|try|namespace|struct|class|enum)\b|=", text):
        return True
    return False


SH_COMMANDS = (
    r"if|then|else|elif|fi|for|while|until|do|done|case|esac|export|local|readonly|declare|"
    r"echo|printf|cd|rm|mkdir|cp|mv|ln|cat|grep|sed|awk|find|xargs|sort|uniq|head|tail|tee|"
    r"sudo|apt|apt-get|pip3?|python3?|clickhouse(-\w+)?|curl|wget|docker|git|set|source|exit|"
    r"kill|sleep|wait|trap|chmod|chown|tar|zstd|ls|test|make|cmake|ninja|ulimit|eval|exec"
)
SH_COMMAND_START = re.compile(rf"^({SH_COMMANDS})(\s|$)")
SH_SYNTAX = re.compile(r"(\$\{?\w|\$\(|\s\|\s|&&|\|\||\s[12]?>>?\s?\S|\s--?\w|\\$|;;|\[\[|\]\])")
SH_ASSIGN = re.compile(
    r"""^(export\s+|local\s+)?[A-Za-z_][A-Za-z0-9_]*=("[^"]*"|'[^']*'|\$\([^)]*\)|[^\s"']*)\s*$"""
)
SH_KEYWORD_LINE = re.compile(r"^(fi|done|esac|then|else|do|\}|\{|;;)$")


def sh_code_line(text):
    if not text or is_prose(text, "sh"):
        return False
    if SH_KEYWORD_LINE.match(text) or SH_ASSIGN.match(text):
        return True
    if SH_COMMAND_START.match(text) and (SH_SYNTAX.search(text) or re.search(r"\s/\S|\s\./", text)):
        return True
    return False


CODE_STATEMENTS = (
    ast.Assign, ast.AugAssign, ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef,
    ast.Return, ast.Raise, ast.If, ast.For, ast.AsyncFor, ast.While, ast.With, ast.AsyncWith, ast.Try, ast.Assert,
    ast.Delete, ast.Global, ast.Nonlocal,
)


def is_code_statement(node):
    """Statements that prose does not parse into (unlike `X is Y`, `A in B`, `Note: text`)."""
    if isinstance(node, ast.AnnAssign):
        return node.value is not None
    if isinstance(node, ast.Expr):
        return isinstance(node.value, (ast.Call, ast.Await))
    return isinstance(node, CODE_STATEMENTS)


def is_label(tree):
    """`# setting = value` above the code it describes: a single assignment of a constant or a bare name."""
    return (
        len(tree.body) == 1
        and isinstance(tree.body[0], ast.Assign)
        and isinstance(tree.body[0].value, (ast.Constant, ast.Name, ast.UnaryOp))
    )


def python_line_is_code(text):
    if not text or is_prose(text, "py"):
        return False
    if re.match(r"^\w[\w.]* \(", text):
        return False  # `Name (words)` is prose: code does not put a space before the parenthesis of a call.
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return bool(re.match(r"^(if|elif|for|while|with|def|class|try|except|else|finally)\b.*:$", text))
    return not is_label(tree) and any(is_code_statement(n) for n in tree.body)


def python_block_is_code(texts):
    try:
        tree = ast.parse("\n".join(texts))
    except (SyntaxError, ValueError):
        # A fragment such as the body of an `if` does not parse alone: judge it line by line.
        return [python_line_is_code(t) for t in texts]
    if is_label(tree) or not any(is_code_statement(node) for node in tree.body):
        return [False] * len(texts)
    return [python_line_is_code(t) or (bool(t) and not is_prose(t, "py") and len(tree.body) > 1) for t in texts]


LANGS = {
    ".h": "c",
    ".hpp": "c",
    ".cpp": "c",
    ".c": "c",
    ".cc": "c",
    ".js": "c",
    ".mjs": "c",
    ".ts": "c",
    ".py": "py",
    ".sh": "sh",
}

# Pragmas and tool directives look like code but are configuration.
DIRECTIVE = re.compile(
    r"^(noqa|type:|pylint:|pyright:|mypy:|fmt:|isort:|ruff:|shellcheck|-\*-|NOLINT|clang-format|eslint|"
    r"prettier|jshint|@ts-|nosec|pragma|SPDX|[Tt]ags:|no-|!)"
)


def comment_blocks(lines, lang):
    marker = "//" if lang == "c" else "#"
    block = []
    for number, line in enumerate(lines, 1):
        stripped = line.strip()
        is_comment = stripped.startswith(marker)
        if lang == "c" and stripped.startswith(("///", "//!")):
            is_comment = False  # Documentation comments may contain examples.
        if lang in ("py", "sh") and number == 1 and stripped.startswith("#!"):
            is_comment = False
        if is_comment:
            text = stripped[len(marker):]
            block.append((number, text.strip()))
        else:
            if block:
                yield block
            block = []
    if block:
        yield block


def find_commented_code(path, lines):
    lang = LANGS.get(path[path.rfind("."):])
    if lang is None:
        return []
    findings = []
    for block in comment_blocks(lines, lang):
        texts = [t for _, t in block]
        if lang == "py":
            flags = python_block_is_code(texts)
        elif lang == "sh":
            flags = [sh_code_line(t) for t in texts]
        else:
            flags = [c_like_code_line(t) for t in texts]
        flags = [f and not DIRECTIVE.match(t) for f, t in zip(flags, texts)]
        code = [t for f, t in zip(flags, texts) if f]
        if not code:
            continue
        if len(code) * 2 < len([t for t in texts if t]):
            continue  # Mostly prose: a code example inside an explanation.
        if len(code) <= MAX_DEBUG_LOGGING_LINES and all(DEBUG_LOGGING.search(t) for t in code):
            continue
        first = next(n for (n, _), f in zip(block, flags) if f)
        findings.append((first, len(code), code[0]))
    return findings


def check_commented_code(files) -> str:
    """Report of the commented-out code in `files`, empty if there is none."""
    messages = []
    for path in files:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().split("\n")
        for line, count, sample in find_commented_code(path, lines):
            messages.append(
                f"{path}:{line}: commented-out code ({count} line(s)), delete it or write the explanation "
                f"as a sentence: {sample[:100]}"
            )
    return "\n".join(messages)


if __name__ == "__main__":
    report = check_commented_code(sys.argv[1:])
    if report:
        print(report)
    sys.exit(1 if report else 0)
