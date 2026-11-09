"""Declare cross-package C++ source families in the package that owns the files."""
import ast
from collections import defaultdict
from itertools import groupby
import json
import posixpath
import re

from initial_migration.first_party_packages import (
    Build, COMPILERS, CPUS, Source, _package_source_globs, attributes, call_name, native_directory,
)


def _local_sources(folder, files):
    """Use globs for numbered compiler inputs while keeping legacy files in link order."""
    exprs = []
    names = []
    def numbered_prefix(name):
        match = re.fullmatch(r"(.*?)[0-9]+[.]cpp", name)
        return match[1] if match else None

    for prefix, chunks in groupby(files, key=numbered_prefix):
        chunk = list(chunks)
        if prefix and len(chunk) >= 8:
            cut = next((i for i in range(1, len(chunk)) if chunk[i] <= chunk[i - 1]), len(chunk))
            pattern = prefix + "*.cpp"
            matches = sorted(p.relative_to(folder).as_posix() for p in folder.glob(pattern))
            tail = chunk[cut:]
            if cut >= 8 and len(tail) <= 3 and sorted(chunk) == matches and len(set(chunk)) == len(chunk):
                if names:
                    exprs.append(json.dumps(names))
                    names = []
                options = ", exclude = " + json.dumps(tail) if tail else ""
                exprs.append("glob(" + json.dumps([pattern]) + options + ")")
                names.extend(tail)
                continue
        names.extend(chunk)
    if names:
        exprs.append(json.dumps(names))
    return " + ".join(exprs)


def group_owned_sources(root, paths, inventory, packages):
    paths = {p.parent.relative_to(root).as_posix(): p for p in paths if native_directory(p.parent.relative_to(root).as_posix())}
    requested = defaultdict(set)
    lists = []
    documents = {pkg: Source(p.read_text()) for pkg, p in sorted(paths.items())}
    label = re.compile(r"(?:@clickhouse)?//([^:]+):([^:]+[.]cpp)")

    def owner(value):
        m = label.fullmatch(value) if isinstance(value, str) else None
        return m[1] if m and m[1] in documents else None

    for pkg, doc in documents.items():
        for _, rule in doc.rules():
            attr = attributes(rule)
            if call_name(rule) not in COMPILERS or "srcs" not in attr:
                continue
            for ls in ast.walk(attr["srcs"]):
                if not isinstance(ls, ast.List) or not all(isinstance(e, ast.Constant) for e in ls.elts):
                    continue
                chunks = [(k, list(v)) for k, v in groupby(ast.literal_eval(ls), owner)]
                candidates = [(k, v) for k, v in chunks if k and k != pkg and len(v) >= 3]
                if not candidates:
                    continue
                for k, v in candidates:
                    requested[k].update(s.split(":", 1)[1] for s in v)
                lists.append((pkg, ls, attr["name"].value, call_name(rule).endswith("test") or
                              isinstance(attr.get("testonly"), ast.Constant) and attr["testonly"].value, chunks))

    # A shared list shortens a broad compiler glob even when the other CMake
    # group has no useful glob of its own (for example the three DBMS table functions).
    complements = set()
    for pkg, filenames in requested.items():
        folder = paths[pkg].parent
        root_files = {p.name for p in folder.glob("*.cpp")}
        others = root_files - filenames
        if len(others) < 24:
            continue
        doc = Build(root, paths[pkg], inventory, packages)
        for _, rule in doc.source.rules():
            attr = attributes(rule)
            if call_name(rule) not in COMPILERS or "srcs" not in attr:
                continue
            for cpu in CPUS:
                names = {v.removeprefix("//" + pkg + ":").removeprefix(":") for v in doc.values(attr["srcs"], cpu)}
                included = root_files & names
                if included <= others and len(others - included) <= 4 and len(included) >= 24:
                    complements.add(pkg)
                    break
            if pkg in complements:
                break

    groups = defaultdict(dict)
    names = {pkg: {attr["name"].value for _, rule in doc.rules()
                   if isinstance((attr := attributes(rule)).get("name"), ast.Constant)}
             for pkg, doc in documents.items()}
    edits = defaultdict(list)
    for pkg, ls, target, test, chunks in lists:
        entries = []
        for k, labels in chunks:
            if not k or k == pkg or len(labels) < 3:
                entries.extend(labels)
                continue
            files = tuple(s.split(":", 1)[1] for s in labels)
            folder = paths[k].parent
            if any(any(k + "/" + f == p or (k + "/" + f).startswith(p + "/")
                       for p in packages if p.startswith(k + "/")) for f in files):
                entries.extend(labels)
                continue
            dirs = {posixpath.dirname(f) for f in files}
            use_constant = k in complements and "" in dirs
            if use_constant:
                name = target.replace("__", "_") + "_sources"
                expr = _local_sources(folder, files)
            elif "" not in dirs and len(files) >= 6:
                stem = next(iter(dirs)) if len(dirs) == 1 else re.sub(r"__compile_[0-9]+$", "", target)
                name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", stem.replace("/", "_")).lower() + "_sources"
                expr = _package_source_globs(folder, files, "*.cpp", False)
            else:
                expr = None
            if expr is None or files not in groups[k] and name in names[k]:
                entries.extend(labels)
                continue
            names[k].add(name)
            definition = groups[k].setdefault(files, dict(name=name, expr=expr, packages=set(), testonly=True, constant=use_constant))
            definition["packages"].add("//" + pkg + ":__pkg__")
            definition["testonly"] &= bool(test)
            entries.append("//" + k + ":" + definition["name"])
        if entries != ast.literal_eval(ls):
            edits[pkg].append((*documents[pkg].span(ls), json.dumps(entries)))

    for pkg, definitions in groups.items():
        doc = documents[pkg]
        declarations = []
        for definition in definitions.values():
            name, expr = definition["name"], definition["expr"]
            if definition["constant"]:
                value = "_" + name.upper()
                declarations.append(value + " = " + expr)
                expr = value
            testonly = ", testonly = True" if definition["testonly"] else ""
            visibility = json.dumps(sorted(definition["packages"]))
            declarations.append(f'filegroup(name = "{name}", srcs = {expr}{testonly}, visibility = {visibility})')
        if declarations:
            first = next(stmt for stmt in doc.module.body if not (isinstance(stmt, ast.Expr) and call_name(stmt.value) in {"load", "package"}))
            pos = doc.span(first)[0]
            edits[pkg].append((pos, pos, "\n\n".join(declarations) + "\n\n"))
    for pkg, replacements in edits.items():
        paths[pkg].write_text(documents[pkg].rewrite(replacements))
