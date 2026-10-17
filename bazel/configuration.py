"""Recover template and copy inputs from the configured CMake trace.

The generated BUILD rules use the original inputs. Configured file contents
are used only to verify substitutions during regeneration, never as sources
to check into Git.
"""

from pathlib import Path
import json
import os
import re


def template_substitutions(template, configured, at_only=False, newline_style=None):
    """Extract substitutions and prove that they reproduce the reference file."""
    original_template, original_configured = template, configured
    if newline_style:
        if newline_style not in {"LF", "UNIX", "CRLF", "DOS", "WIN32"}:
            raise ValueError("Unsupported CMake newline style: " + newline_style)
        template = template.replace("\r\n", "\n")
        configured = configured.replace("\r\n", "\n")
    tokens = r"^[ \t]*#[ \t]*cmakedefine(?:01)?[^\n]*|@[A-Za-z_][A-Za-z_0-9]*@"
    if not at_only:
        tokens += r"|\$\{(?:[^{}]|\$\{[^{}]*\})+\}"
    # zlib's wrapper first converts these two directives into CMake variables.
    tokens += r"|^#ifdef (?:HAVE_UNISTD_H|NEED_PTRDIFF_T)[^\n]*"
    keys = []
    pattern = []
    offset = 0
    for match in re.finditer(tokens, template, re.MULTILINE):
        pattern.append(re.escape(template[offset:match.start()]))
        key = match[0]
        if key in keys:
            pattern.append("(?P=v" + str(keys.index(key)) + ")")
        else:
            pattern.append("(?P<v" + str(len(keys)) + ">[\\s\\S]*?)")
            keys.append(key)
        offset = match.end()
    pattern.append(re.escape(template[offset:]))
    match = re.fullmatch("".join(pattern), configured)
    if not match:
        raise ValueError("Configured output does not match its template")
    # A directive such as HAVE_ATTRIBUTE can prefix a longer directive.
    substitutions = {key: match["v" + str(keys.index(key))] for key in sorted(keys, key=lambda key: (-len(key), key))}
    if newline_style:
        if "\r\n" in original_template:
            substitutions = {"\r\n": "\n", **substitutions}
        if newline_style in {"CRLF", "DOS", "WIN32"}:
            substitutions["\n"] = "\r\n"
    actual = original_template
    for key, value in substitutions.items():
        actual = actual.replace(key, value)
    if actual != original_configured:
        raise ValueError("Template substitutions are order dependent")
    return substitutions


def read_configurations(root, build):
    """Map configured output paths to source inputs and their CMake provenance."""
    outputs = {}
    build_files = []
    for directory, dirs, files in os.walk(build):
        dirs[:] = [d for d in dirs if d not in {"CMakeFiles", ".cmake", "native", "cargo", "external"}]
        build_files.extend(Path(directory) / name for name in files)

    def output_path(value):
        path = Path(os.path.normpath(value))
        if path.is_absolute():
            return path
        matches = [p for p in build_files if p.as_posix().endswith("/" + path.as_posix())]
        if len(matches) != 1:
            raise ValueError("Ambiguous configured output: " + value)
        return matches[0]

    for line in (build / "trace.jsonl").open():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        command = record.get("cmd", "").lower()
        args = record.get("args", [])
        if not args or command not in {"configure_file", "file"}:
            continue
        provenance = str(Path(record["file"]).relative_to(root)) + ":" + str(record["line"]) if record["file"].startswith(str(root) + "/") else record["file"]
        if command == "configure_file":
            # ExternalProject bookkeeping is unrelated to compilation.
            if args[0].startswith("/usr/") or not any(args[1].endswith(suffix) for suffix in (".h", ".hh", ".hpp", ".cpp", ".cc", ".c", "__config_site", "__assertion_handler")):
                continue
            source = Path(args[0])
            if not source.is_absolute():
                source = Path(record["file"]).parent / source
            if source == build / "contrib/zlib-ng-cmake/zconf.h.cmakein":
                source = root / "contrib/zlib-ng/zconf.h.in"
            destination = output_path(args[1])
            outputs[destination] = {
                "source": source,
                "kind": "copy" if "COPYONLY" in args else "template",
                "at_only": "@ONLY" in args,
                "newline_style": args[args.index("NEWLINE_STYLE") + 1] if "NEWLINE_STYLE" in args else None,
                "provenance": provenance,
            }
        elif args[0].upper() == "COPY":
            index = args.index("DESTINATION")
            destination = Path(args[index + 1])
            for value in ";".join(args[1:index]).split(";"):
                source = Path(value)
                if source.is_dir():
                    sources = [p for p in source.rglob("*") if p.is_file() and p.suffix in {".h", ".hh"}]
                    copies = [(p, destination / p.relative_to(source)) for p in sources]
                else:
                    copies = [(source, destination / source.name)]
                for original, copied in copies:
                    outputs[copied] = {"kind": "copy", "source": original, "provenance": provenance}
        elif args[0].upper() == "RENAME" and Path(args[1]) in outputs:
            outputs[Path(args[2])] = outputs.pop(Path(args[1]))
        elif args[0].upper() == "WRITE":
            outputs[Path(args[1])] = {"kind": "write", "content": "".join(args[2:]), "provenance": provenance}

    result = {}
    for output, spec in outputs.items():
        if not output.is_relative_to(build) or not output.is_file():
            continue
        relative = output.relative_to(build).as_posix()
        if not relative.startswith("contrib/"):
            continue
        if "source" in spec:
            # Wrapper templates can be symlinks into submodules. Map the real
            # source to its fetched repository, which owns the template bytes.
            spec["source"] = spec["source"].resolve(strict=True)
        # read_text() normalizes CRLF and would conceal byte-level differences.
        expected = output.read_bytes().decode()
        if spec["kind"] == "copy":
            if spec["source"].read_bytes().decode() != expected:
                raise ValueError("Copied configuration differs from source: " + relative)
        elif spec["kind"] == "template":
            try:
                spec["substitutions"] = template_substitutions(
                    spec["source"].read_bytes().decode(), expected,
                    spec.pop("at_only"), spec.pop("newline_style"))
            except ValueError as error:
                raise ValueError(relative + ": " + str(error)) from error
        elif spec["content"] != expected:
            raise ValueError("Written configuration differs from trace: " + relative)
        result[relative] = spec
    return result
