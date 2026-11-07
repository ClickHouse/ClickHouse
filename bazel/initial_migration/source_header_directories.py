"""Keep external BUILD files' reviewed source directories when importing CMake."""

import ast

from initial_migration.compact_build import Source


ATTR = "source_header_directories"
MODULE = "@clickhouse//bazel/rules:source_header_directories.bzl"
CC_LIBRARIES = {"cc_library", "ch_cc_library"}


def read(overlays, read_only):
    reviewed = {}
    for path in sorted(overlays.glob("*.BUILD.bazel")):
        # boost_headers.bazel.tmpl declares Boost's directories, including its computed lists.
        if path in read_only or path.name == "clickhouse_boost.BUILD.bazel":
            continue
        text = path.read_text()
        if ATTR not in text:
            continue
        source = Source(text)
        for _, rule in source.rules():
            attrs = {kw.arg: kw.value for kw in rule.keywords}
            if ATTR not in attrs:
                continue
            name = ast.literal_eval(attrs["name"])
            value = attrs[ATTR]
            if not (getattr(rule.func, "id", None) in CC_LIBRARIES
                    and isinstance(value, ast.Call) and getattr(value.func, "id", None) == ATTR
                    and not value.keywords and len(value.args) == 1 and isinstance(value.args[0], ast.List)
                    and all(isinstance(x, ast.Constant) and isinstance(x.value, str) for x in value.args[0].elts)):
                raise ValueError(f"{path}:{name}: expected {ATTR}([literal paths]) on a C++ library")
            reviewed.setdefault(path, {})[name] = source.text(value)
    return reviewed


def restore(reviewed):
    for path, owners in reviewed.items():
        before = path.read_text() if path.is_file() else ""
        source = Source(before)
        restored = set()
        edits = []
        for _, rule in source.rules():
            attrs = {kw.arg: kw.value for kw in rule.keywords}
            name = getattr(attrs["name"], "value", None)
            if name not in owners or getattr(rule.func, "id", None) not in CC_LIBRARIES:
                continue
            restored.add(name)
            if ATTR in attrs:
                edits.append((*source.span(attrs[ATTR]), owners[name]))
            else:
                pos = source.span(rule.keywords[0])[0]
                edits.append((pos, pos, f"{ATTR} = {owners[name]},\n    "))
        if missing := owners.keys() - restored:
            raise ValueError(f"{path} lost {ATTR} owner(s): {', '.join(sorted(missing))}")
        loads = [stmt.value for stmt in source.module.body if isinstance(stmt, ast.Expr)
                 and isinstance(stmt.value, ast.Call) and getattr(stmt.value.func, "id", None) == "load"]
        if not any(load.args[0].value == MODULE and (any(arg.value == ATTR for arg in load.args[1:])
                   or any(kw.arg == ATTR and kw.value.value == ATTR for kw in load.keywords)) for load in loads):
            pos = source.span(loads[0])[0] if loads else 0
            edits.append((pos, pos, f'load("{MODULE}", "{ATTR}")\n'))
        after = source.rewrite(edits)
        if after != before:
            path.write_text(after)
