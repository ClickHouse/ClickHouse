#!/usr/bin/env python3
"""Publish Poco component headers and OpenSSL CMake headers in their owners.

Only BUILD.bazel and clickhouse_openssl.BUILD.bazel can be updated. The pinned
v3 OpenSSL scan and the checked-in Poco headers determine the public deps.
Existing compiled targets retain every source and compiled dependency in order.
"""

import argparse
import ast
from collections import Counter
import copy
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Expand, Source, ast_key
from initial_migration.prune_inherited_headers import Document
from initial_migration.repository_inputs import REPOSITORIES, ROOT
from initial_migration.scan_include_adjacency import includes

CONFIG_ROOT = "contrib/openssl-cmake/common/include"
CONFIG_TARGET = "@clickhouse//:openssl_cmake_headers"
NATIVE_TARGET = "@clickhouse_openssl//:openssl_native_headers"
COMPONENTS = {
    "Foundation": "foundation",
    "JSON": "json",
    "Net": "net",
    "NetSSL_OpenSSL": "net_ssl",
    "Redis": "redis",
    "Util": "util",
    "XML": "xml",
}
COMPILED = {
    "_poco_" + suffix: "poco_" + suffix + "_headers"
    for suffix in COMPONENTS.values()
} | {
    "_poco_net_ssl__compile_0": "poco_net_ssl_headers",
    "_poco_net_ssl__compile_1": "poco_net_ssl_headers",
}
OPENSSL = {"crypto", "crypto__compile_0", "crypto__compile_1", "ssl"}


def string_list(values):
    return "[" + ", ".join(json.dumps(value) for value in values) + "]"


def rules(source):
    return {
        ast.literal_eval(attrs["name"]): (call.func.id, attrs)
        for _, call in source.rules()
        for attrs in [{kw.arg: kw.value for kw in call.keywords}]
    }


def verify(before, after, allowed, added):
    old = Source(ast.unparse(Expand().visit(ast.parse(before))))
    new = Source(ast.unparse(Expand().visit(ast.parse(after))))
    original, rewritten = rules(old), rules(new)
    if set(rewritten) != set(original) | set(added):
        raise ValueError("Added or removed an unexpected target")
    for name, (kind, old_attrs) in original.items():
        new_kind, new_attrs = rewritten[name]
        ignored = allowed.get(name, set())
        if kind != new_kind:
            raise ValueError("Changed rule kind of " + name)
        if set(old_attrs) - ignored != set(new_attrs) - ignored:
            raise ValueError("Changed unrelated attributes of " + name)
        for field in set(old_attrs) - ignored:
            if ast_key(old_attrs[field]) != ast_key(new_attrs[field]):
                raise ValueError(f"Changed unrelated {name}.{field}")
    for name in set(added) - set(original):
        kind, attrs = rewritten[name]
        if kind != "cc_library" or "srcs" in attrs:
            raise ValueError("The new provider must own headers only: " + name)


class Migration:
    def __init__(self, facts_directory):
        spec = REPOSITORIES["contrib/openssl"]
        path = facts_directory / (spec["name"] + "-v3-" + spec["sha256"] + ".json.gz")
        with gzip.open(path, "rt") as stream:
            facts = json.load(stream)
        if (facts["sha256"], facts["commit"], facts["version"]) != (
                spec["sha256"], spec["commit"], 3):
            raise ValueError("Unverified OpenSSL source archive " + str(path))
        self.inventories = {"contrib/openssl": facts["inventory"]}
        self.config_headers = {p.relative_to(ROOT).as_posix()
                               for p in (ROOT / CONFIG_ROOT).rglob("*.h")}
        source_includes = {name.removeprefix("include/")
                           for name in facts["inventory"] if name.startswith("include/")}
        collisions = {name.removeprefix(CONFIG_ROOT + "/")
                      for name in self.config_headers} & source_includes
        if collisions:
            raise ValueError("OpenSSL source/config include collisions: " + repr(sorted(collisions)))
        self.stats = Counter()
        self.files = {}
        index = {}
        for component in COMPONENTS:
            root = ROOT / "base/poco" / component / "include"
            self.files[component] = sorted(root.rglob("*.h"))
            for path in self.files[component]:
                spelling = path.relative_to(root).as_posix()
                if spelling in index:
                    raise ValueError("Poco include collision: " + spelling)
                index[spelling] = component
        self.public_deps = {}
        for component, paths in self.files.items():
            deps = set()
            for path in paths:
                for line, directive, style, operand in includes(path.read_bytes(), str(path)):
                    if directive == "#include_next":
                        raise ValueError(f"Unverified include_next: {path}:{line}")
                    if style not in {"quote", "angle"}:
                        continue
                    owner = index.get(operand)
                    if owner is not None and owner != component:
                        deps.add(":poco_" + COMPONENTS[owner] + "_headers")
                    if operand.startswith("openssl/"):
                        if operand not in source_includes and CONFIG_ROOT + "/" + operand not in self.config_headers:
                            raise ValueError(f"Unresolved OpenSSL header: {path}:{line}: {operand}")
                        deps.add(":openssl_cmake_headers")
            self.public_deps["poco_" + COMPONENTS[component] + "_headers"] = sorted(deps)
        self.public_deps["openssl_cmake_headers"] = [NATIVE_TARGET]
        self.public_deps["openssl_native_headers"] = []
        # Every new dependency terminates at one of these source-only owners.
        # Existing compiled Poco deps never become dependencies of a provider.
        def visit(target, active=()):
            if target in active:
                raise ValueError("Header dependency cycle: " + " -> ".join((*active, target)))
            for dep in self.public_deps[target]:
                visit(dep.rsplit(":", 1)[-1], (*active, target))
        for target in self.public_deps:
            visit(target)

    def config_label(self, node):
        return (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value.removeprefix("@clickhouse//:") in self.config_headers)

    def local_include(self, node):
        if isinstance(node, ast.Constant) and node.value == "include":
            return True
        return (isinstance(node, ast.Call)
                and getattr(node.func, "id", "") == "repository_local_include"
                and len(node.args) == 2 and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == CONFIG_ROOT)

    def filtered(self, doc, node, reject, attr):
        if isinstance(node, ast.Name) and node.id in doc.constants:
            return self.filtered(doc, doc.constants[node.id], reject, attr)
        if isinstance(node, (ast.List, ast.Tuple)):
            kept = []
            for item in node.elts:
                if reject(item):
                    self.stats[attr] += 1
                else:
                    kept.append(copy.deepcopy(item))
            return ast.List(elts=kept, ctx=ast.Load())
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self.filtered(doc, node.left, reject, attr)
            right = self.filtered(doc, node.right, reject, attr)
            if isinstance(left, ast.List) and not left.elts:
                return right
            if isinstance(right, ast.List) and not right.elts:
                return left
            return ast.BinOp(left=left, op=ast.Add(), right=right)
        if (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "select"
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            table = node.args[0]
            return ast.Call(func=copy.deepcopy(node.func),
                            args=[ast.Dict(keys=copy.deepcopy(table.keys), values=[
                                self.filtered(doc, branch, reject, attr) for branch in table.values])],
                            keywords=copy.deepcopy(node.keywords))
        # globs and any expressions not containing literal foreign filenames
        # are retained. No glob exclusion is needed to own the new headers.
        return copy.deepcopy(node)

    def update(self, doc):
        source = doc.source
        existing = rules(source)
        edits = []
        is_root = doc.repo == "@clickhouse"
        for _, call in source.rules():
            attrs = {kw.arg: kw.value for kw in call.keywords}
            name = ast.literal_eval(attrs["name"])
            if is_root and name in COMPILED:
                dep = ":" + COMPILED[name]
                if any(dep in doc.values(attrs.get("deps"), arch) for arch in ("x86_64", "aarch64")):
                    continue
                node = attrs["deps"]
                edits.append((*source.span(node), source.text(node) + " + " + string_list([dep])))
            if not is_root and name in OPENSSL:
                if name.startswith("crypto__compile_"):
                    dep = attrs["deps"]
                    if CONFIG_TARGET not in doc.values(dep, "x86_64"):
                        edits.append((*source.span(dep), source.text(dep) + " + " + string_list([CONFIG_TARGET])))
                for kw in call.keywords:
                    if kw.arg in {"hdrs", "textual_hdrs", "additional_compiler_inputs"}:
                        reject = self.config_label
                    elif kw.arg in {"includes", "local_includes"}:
                        reject = self.local_include
                    else:
                        continue
                    start_count = self.stats[kw.arg]
                    filtered = self.filtered(doc, kw.value, reject, kw.arg)
                    if self.stats[kw.arg] == start_count:
                        continue
                    if isinstance(filtered, ast.List) and not filtered.elts:
                        start, end = source.span(kw)
                        if source.bytes[end:end + 1] == b",":
                            end += 1
                        edits.append((start, end, ""))
                    else:
                        edits.append((*source.span(kw.value), ast.unparse(ast.fix_missing_locations(filtered))))
        text = source.rewrite(edits)
        added = []
        if is_root:
            for component, suffix in COMPONENTS.items():
                name = "poco_" + suffix + "_headers"
                if name in existing:
                    continue
                root = "base/poco/" + component + "/include"
                added.append("\ncc_library(\n"
                             f"    name = {json.dumps(name)},\n"
                             f"    hdrs = glob({string_list([root + '/**/*.h'])}),\n"
                             f"    includes = {string_list([root])},\n"
                             + (f"    deps = {string_list(self.public_deps[name])},\n"
                                if self.public_deps[name] else "")
                             + ")\n")
            if "openssl_cmake_headers" not in existing:
                added.append("\ncc_library(\n"
                             '    name = "openssl_cmake_headers",\n'
                             f"    hdrs = glob({string_list([CONFIG_ROOT + '/**/*.h'])}),\n"
                             f"    includes = {string_list([CONFIG_ROOT])},\n"
                             f"    deps = {string_list([NATIVE_TARGET])},\n"
                             ")\n")
            allowed = {name: {"deps"} for name in COMPILED}
            new_names = set(self.public_deps) - {"openssl_native_headers"}
        else:
            if "openssl_native_headers" not in existing:
                added.append("""
cc_library(
    name = "openssl_native_headers",
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.H",
    ]),
    textual_hdrs = ["include/crypto/riscv_arch.def"],
    includes = ["include"],
)
""")
            allowed = {name: {"deps", "hdrs", "textual_hdrs", "additional_compiler_inputs",
                              "includes", "local_includes"} for name in OPENSSL}
            new_names = {"openssl_native_headers"}
        text += "".join(added)
        verify(source.bytes.decode(), text, allowed, new_names)
        return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--facts", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    migration = Migration(args.facts)
    paths = [ROOT / "BUILD.bazel", ROOT / "bazel/dependencies/clickhouse_openssl.BUILD.bazel"]
    updates = {}
    for path in paths:
        doc = Document(path, migration.inventories)
        text = migration.update(doc)
        if text != path.read_text():
            updates[path] = text
    for component, files in migration.files.items():
        name = "poco_" + COMPONENTS[component] + "_headers"
        print(name, len(files), migration.public_deps[name])
    print("OpenSSL CMake headers", len(migration.config_headers), migration.public_deps["openssl_cmake_headers"])
    print("Removed duplicate OpenSSL attributes:", dict(migration.stats))
    print("Compiled srcs and every unrelated expanded attribute verified in", len(paths), "BUILD files")
    for path, text in updates.items():
        print(path.relative_to(ROOT))
        if args.write:
            path.write_text(text)


if __name__ == "__main__":
    main()
