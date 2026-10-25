"""Render a package's sorted C++ sources as a glob excluding its other compilers' files."""

import ast
import json
from pathlib import Path

from initial_migration.factor_build_selections import CPUS, cpu_choices, list_parts
from initial_migration.normalize_cpp_builds import CPP_LIBRARIES, call_name
from initial_migration.prune_inherited_headers import SourceDocument
from initial_migration.simplify_globs import _literal_strings


def _root_cpp_prefix(doc, srcs, files):
    """Stop at the first subdirectory, header or unrecognized expression."""
    parts = list_parts(srcs)
    by_cpu = {cpu: [] for cpu in CPUS}
    guarded = False
    for i, part in enumerate(parts):
        literals = _literal_strings(part)
        if literals is not None:
            end = next((j for j, name in enumerate(literals) if name not in files), len(literals))
            for ordered in by_cpu.values():
                ordered.extend(literals[:end])
            if end < len(literals):
                tail = [json.dumps(literals[end:])] + [doc.source.text(n) for n in parts[i + 1:]]
                return by_cpu, guarded, tail
            continue

        branches = cpu_choices(part)
        if branches and set(branches) != set(CPUS):
            break
        rows = {}
        for cpu in CPUS:
            node = branches.get(cpu, part)
            if call_name(node) == "glob" and len(node.args) == 1:
                patterns = _literal_strings(node.args[0])
                if not patterns or any("/" in p or not p.endswith(".cpp") for p in patterns):
                    break
                rows[cpu] = doc.glob_strings(node)
            else:
                rows[cpu] = _literal_strings(node)
            if rows[cpu] is None or not files.issuperset(rows[cpu]):
                break
        else:
            guarded |= bool(branches)
            for cpu, row in rows.items():
                by_cpu[cpu].extend(row)
            continue
        break
    else:
        i = len(parts)
    return by_cpu, guarded, [doc.source.text(n) for n in parts[i:]]


def _compact(doc, srcs, owners, files):
    by_cpu, guarded, tail = _root_cpp_prefix(doc, srcs, files)
    if not all(row and row == sorted(set(row)) for row in by_cpu.values()):
        return None

    missing = {cpu: files - set(row) for cpu, row in by_cpu.items()}
    common = set.intersection(*missing.values())
    names = [name for name, members in owners.items()
             if members and members <= common and doc.constants[name].lineno < srcs.lineno]
    if not names:
        return None
    covered = set.union(*(owners[name] for name in names))
    globs = {}
    for cpu in CPUS:
        extra = sorted(missing[cpu] - covered)
        # A long second inventory is harder to maintain than the original srcs.
        if len(extra) > 4:
            return None
        exclude = " + ".join([*names, *([json.dumps(extra)] if extra else [])])
        globs[cpu] = 'glob(["*.cpp"], exclude = ' + exclude + ')'
    replacement = globs[CPUS[0]]
    if guarded:
        replacement = "select({" + ", ".join(json.dumps(k) + ": " + v for k, v in globs.items()) + "})"
    replacement = " + ".join([replacement, *tail])
    if len(replacement) + 80 < len(doc.source.text(srcs)) and ast.dump(ast.parse(replacement, mode="eval").body) != ast.dump(srcs):
        return replacement
    return None


def compact_owned_source_globs(root, build_paths):
    """Reuse filegroup source lists to exclude files compiled by a different target."""
    for build_path in build_paths:
        path = Path(root) / build_path
        files = {p.name for p in path.parent.glob("*.cpp") if p.is_file()}
        if not files:
            continue
        doc = SourceDocument(path, files=files)
        owners = {}
        for _, rule in doc.source.rules():
            srcs = next((k.value for k in rule.keywords if k.arg == "srcs"), None)
            if call_name(rule) == "filegroup" and isinstance(srcs, ast.Name) and srcs.id in doc.constants:
                try:
                    members = doc.glob_strings(srcs)
                except ValueError:
                    continue
                if all(m.endswith(".cpp") and not m.startswith(("/", "@")) and ":" not in m
                       and "*" not in m and ".." not in Path(m).parts for m in members):
                    owners[srcs.id] = files.intersection(members)
        if not any(owners.values()):
            continue
        edits = []
        for _, rule in doc.source.rules():
            if call_name(rule) not in CPP_LIBRARIES:
                continue
            for kw in rule.keywords:
                if kw.arg == "srcs" and "#" not in doc.source.text(kw.value):
                    updated = _compact(doc, kw.value, owners, files)
                    if updated:
                        edits.append((*doc.source.span(kw.value), updated))
        if edits:
            path.write_text(doc.source.rewrite(edits))
