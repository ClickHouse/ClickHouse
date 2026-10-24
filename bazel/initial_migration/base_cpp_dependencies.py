#!/usr/bin/env python3
"""Publish base and Poco headers from their Bazel packages after migration."""

import ast
from collections import Counter
import json
from pathlib import Path
import posixpath
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.compact_build import Source
from initial_migration.native_public_includes import directory, list_elements
from initial_migration.prune_inherited_headers import ARCHES, CC_PROVIDERS, Document, Graph
from initial_migration.repository_inputs import ROOT
from initial_migration.scan_include_adjacency import includes as source_includes

POCO = {
    "Foundation": "foundation",
    "JSON": "json",
    "Net": "net",
    "NetSSL_OpenSSL": "net_ssl",
    "Redis": "redis",
    "Util": "util",
    "XML": "xml",
}
NET_SSL_HEADERS = "@clickhouse//base/poco/NetSSL_OpenSSL:poco_net_ssl_headers"


def strings(values):
    return json.dumps(values, indent=4)


def attrs(call):
    return {kw.arg: kw.value for kw in call.keywords}


def replace_attrs(text, changes):
    """Replace named attributes; keep all other BUILD expressions verbatim."""
    source = Source(text)
    edits = []
    for _, rule in source.rules():
        old = attrs(rule)
        changes_for_rule = changes.get(ast.literal_eval(old["name"]), {})
        if not changes_for_rule:
            continue
        remaining = dict(changes_for_rule)
        for kw in rule.keywords:
            if kw.arg not in remaining:
                continue
            value = remaining.pop(kw.arg)
            if value is None:
                start, end = source.span(kw)
                if source.bytes[end:end + 1] == b",":
                    end += 1
                edits.append((start, end, ""))
            else:
                edits.append((*source.span(kw.value), value))
        addition = "".join(f"    {key} = {value},\n" for key, value in remaining.items() if value is not None)
        if addition:
            edits.append((source.span(rule)[1] - 1, source.span(rule)[1] - 1, "\n" + addition))
    return source.rewrite(edits)


def build_files(root):
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "*.bazel", "*.bzl"], cwd=root,
    ).decode().split("\0")
    return [root / name for name in names if name and (root / name).is_file()]


def publish_base(root, paths, stats, from_generator):
    original = root / "base/base/BUILD.bazel"
    harmful = root / "base/harmful/BUILD.bazel"
    dest = root / "base/BUILD.bazel"
    if not original.exists():
        return
    if dest.exists() and not from_generator:
        raise ValueError("Both base and base/base packages exist before migration")

    source = Source(original.read_text())
    common = next(call for _, call in source.rules()
                  if ast.literal_eval(attrs(call)["name"]) == "common")
    data = attrs(common)
    srcs = ast.literal_eval(data["srcs"])
    moved = [name if name.startswith(("//", "@")) else "base/" + name.removeprefix(":") for name in srcs]
    deps = ast.literal_eval(data["deps"])
    bare_poco = {"//base/poco/" + component + ":headers" for component in POCO}
    removable = bare_poco | {
        "//base/glibc-compatibility:headers", "//base/glibc-rseq:headers",
        "//base/readpassphrase:headers", "//base/widechar_width:headers",
        "//contrib/openssl-cmake:headers", "//:headers",
    }
    deps = [dep for dep in deps if dep not in removable]
    deps += [":base_headers", "//base/poco/Util:poco_util_headers"]
    common_text = replace_attrs(source.text(common), {
        "common": {"srcs": strings(moved), "deps": strings(deps), "local_includes": None}
    })
    harmful_source = Source(harmful.read_text())
    harmful_call = next(call for _, call in harmful_source.rules()
                        if ast.literal_eval(attrs(call)["name"]) == "harmful")
    harmful_text = replace_attrs(harmful_source.text(harmful_call), {
        "harmful": {
            "srcs": strings(["harmful/harmful.c"]), "local_includes": None,
            "deps": strings(["@llvm-project//compiler-rt:headers", ":base_headers"]),
        }
    })
    exports = []
    for stmt in source.module.body:
        if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                and getattr(stmt.value.func, "id", "") == "exports_files"):
            exports += ["base/" + value for value in ast.literal_eval(stmt.value.args[0])]
    # License and PCG belonged to the root package before //base was created.
    exports += ["poco/LICENSE"]
    text = (
        'load("@clickhouse//bazel/rules:ch_cc_library.bzl", "ch_cc_library")\n'
        'load("@rules_cc//cc:cc_library.bzl", "cc_library")\n\n'
        'package(default_visibility = ["//visibility:public"])\n\n'
        "exports_files(" + strings(exports) + ")\n\n"
        + common_text + "\n\n" + harmful_text + """
cc_library(
    name = "base_headers",
    hdrs = glob(["base/**/*.h"]),
    includes = ["."],
)

cc_library(
    name = "headers",
    hdrs = glob(["pcg-random/**/*.hpp"]),
)
"""
    )
    dest.write_text(text)
    original.unlink()
    harmful.unlink()
    stats["base targets moved"] += 2
    stats["base local includes removed"] += 17
    root_build = root / "BUILD.bazel"
    source = Source(root_build.read_text())
    edits = []
    for stmt in source.module.body:
        if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                and getattr(stmt.value.func, "id", "") == "exports_files"):
            for node in ast.walk(stmt.value):
                if isinstance(node, ast.List) and any(
                        isinstance(el, ast.Constant) and el.value == "base/poco/LICENSE" for el in node.elts):
                    kept = [ast.literal_eval(el) for el in node.elts
                            if ast.literal_eval(el) != "base/poco/LICENSE"]
                    edits.append((*source.span(node), strings(kept)))
                    break
    text = source.rewrite(edits)
    text = replace_attrs(text, {"headers": {
        "hdrs": 'glob(["**/*.h"])',
        "deps": strings(["//base:headers"]),
    }})
    root_build.write_text(text)

    # Only label spellings change. Do not rename file identities in CMake's
    # link data, codegen outputs, or binary comparison maps.
    for path in paths:
        if not path.exists():
            continue
        before = path.read_text()
        after = before.replace("//base/base:common", "//base:common").replace(
            "//base/base:headers", "//base:base_headers").replace(
            "//base/harmful:harmful", "//base:harmful").replace(
            "//base/base:time.h", "//base:base/time.h").replace(
            "//:base/poco/LICENSE", "//base:poco/LICENSE")
        if after != before:
            path.write_text(after)
            stats["files relabelled"] += 1


def publish_poco(root, paths, stats):
    # A fresh CMake capture does not contain the Poco header targets introduced
    # during migration. Recreate those targets from Poco's public includes.
    spelling_owner = {}
    public_files = {}
    for part in POCO:
        base = root / "base/poco" / part / "include"
        public_files[part] = sorted(base.rglob("*.h"))
        for file in public_files[part]:
            spelling = file.relative_to(base).as_posix()
            if spelling in spelling_owner:
                raise ValueError("Poco public include collision: " + spelling)
            spelling_owner[spelling] = part
    for part, suffix in POCO.items():
        path = root / "base/poco" / part / "BUILD.bazel"
        text = path.read_text()
        names = {ast.literal_eval(attrs(rule)["name"]) for _, rule in Source(text).rules()}
        name = "poco_" + suffix + "_headers"
        if name not in names:
            deps = set()
            for file in public_files[part]:
                for _, _, _, operand in source_includes(file.read_bytes(), str(file)):
                    owner = spelling_owner.get(operand)
                    if owner and owner != part:
                        deps.add("//base/poco/" + owner + ":poco_" + POCO[owner] + "_headers")
                    if operand.startswith("openssl/"):
                        deps.add("//contrib/openssl-cmake:openssl_cmake_headers")
            text += (
                "\ncc_library(\n"
                "    name = " + json.dumps(name) + ",\n"
                '    hdrs = glob(["include/**/*.h"]),\n'
                '    includes = ["include"],\n'
                "    deps = " + strings(sorted(deps)) + ",\n)\n"
            )
            changes = {}
            for _, rule in Source(text).rules():
                data = attrs(rule)
                compiled_name = ast.literal_eval(data["name"])
                if compiled_name in {"_poco_" + suffix,
                                     "_poco_" + suffix + "__compile_0",
                                     "_poco_" + suffix + "__compile_1"}:
                    doc = Source(text)
                    existing = doc.text(data["deps"]) if "deps" in data else "[]"
                    changes[compiled_name] = {"deps": existing + " + " + strings([":" + name])}
            path.write_text(replace_attrs(text, changes))
            stats["Poco public providers declared"] += 1
    for path, name, declaration in (
        (root / "contrib/openssl-cmake/BUILD.bazel", "openssl_cmake_headers", """
cc_library(
    name = "openssl_cmake_headers",
    hdrs = glob(["common/include/**/*.h"]),
    includes = ["common/include"],
    deps = ["@clickhouse_openssl//:openssl_native_headers"],
)
"""),
        (root / "bazel/dependencies/clickhouse_openssl.BUILD.bazel", "openssl_native_headers", """
cc_library(
    name = "openssl_native_headers",
    hdrs = glob(["include/**/*.h", "include/**/*.H"]),
    textual_hdrs = ["include/crypto/riscv_arch.def"],
    includes = ["include"],
)
"""),
    ):
        text = path.read_text()
        if name not in {ast.literal_eval(attrs(rule)["name"]) for _, rule in Source(text).rules()}:
            path.write_text(text + declaration)
            stats["OpenSSL public providers declared"] += 1
    kafka = root / "bazel/dependencies/clickhouse_librdkafka.BUILD.bazel"
    source = Source(kafka.read_text())
    constants = [stmt for stmt in source.module.body if isinstance(stmt, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == "_GENERATED_HEADER_INPUTS_0" for t in stmt.targets)]
    if constants:
        captured = ast.literal_eval(constants[0].value)
        if not all(v.startswith(tuple("@clickhouse//base/poco/" + part + ":include/" for part in POCO)) for v in captured):
            raise ValueError("librdkafka captured non-Poco headers")
        changes = {}
        for _, rule in source.rules():
            data = attrs(rule)
            name = ast.literal_eval(data["name"])
            if name not in {"_rdkafka__compile_0", "_rdkafka__compile_1"}:
                continue
            if not any(isinstance(node, ast.Name) and node.id == "_GENERATED_HEADER_INPUTS_0"
                       for node in ast.walk(data["hdrs"])):
                raise ValueError(name + " lost Poco header inputs before provider migration")
            # Only this known list of public headers is inherited from NetSSL.
            class StripPoco(ast.NodeTransformer):
                def visit_BinOp(self, node):
                    if isinstance(node.op, ast.Add) and isinstance(node.right, ast.Name) and node.right.id == "_GENERATED_HEADER_INPUTS_0":
                        return self.visit(node.left)
                    return self.generic_visit(node)
            hdrs = ast.unparse(ast.fix_missing_locations(StripPoco().visit(ast.parse(source.text(data["hdrs"]), mode="eval").body)))
            deps = ast.literal_eval(data["deps"])
            changes[name] = {"hdrs": hdrs, "deps": strings(deps + [NET_SSL_HEADERS])}
        text = replace_attrs(source.bytes.decode(), changes)
        source = Source(text)
        edits = []
        for stmt in source.module.body:
            if not isinstance(stmt, ast.Assign):
                continue
            names = [n.id for n in stmt.targets if isinstance(n, ast.Name)]
            if "_GENERATED_HEADER_INPUTS_0" in names:
                edits.append((*source.span(stmt), ""))
            if "_RDKAFKA_LOCAL_INCLUDES" in names:
                old = stmt.value
                kept = [source.text(node) for node in old.elts if not (
                    isinstance(node, ast.Call) and getattr(node.func, "id", "") == "repository_local_include"
                    and node.args and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value.startswith("base/poco/"))]
                stats["librdkafka Poco local includes"] += len(old.elts) - len(kept)
                edits.append((*source.span(old), "[" + ",\n".join(kept) + "]"))
        kafka.write_text(source.rewrite(edits))
        stats["librdkafka public header entries replaced"] += len(captured) * len(changes)

    for part, suffix in POCO.items():
        path = root / "base/poco" / part / "BUILD.bazel"
        source = Source(path.read_text())
        name = "poco_" + suffix + "_headers"
        provider = next(call for _, call in source.rules()
                        if ast.literal_eval(attrs(call)["name"]) == name)
        data = attrs(provider)
        if isinstance(data["hdrs"], ast.List):
            current = set(ast.literal_eval(data["hdrs"]))
            expected = {p.relative_to(path.parent).as_posix() for p in (path.parent / "include").rglob("*.h")}
            if current != expected:
                raise ValueError(name + ": public header glob changed membership")
            stats["Poco public headers converted to glob"] += len(current)
        changes = {name: {"hdrs": 'glob(["include/**/*.h"])', "includes": '["include"]'}}
        if part == "Net":
            changes["_poco_net"] = {"local_includes": None}
        after = replace_attrs(source.bytes.decode(), changes)
        if after != source.bytes.decode():
            path.write_text(after)

    # Remove exports only when no other BUILD or runtime rule references them.
    contents = {p: p.read_text() for p in paths if p.exists()}
    for part in ("Net", "Foundation", "NetSSL_OpenSSL"):
        path = root / "base/poco" / part / "BUILD.bazel"
        source = Source(path.read_text())
        edits = []
        for stmt in source.module.body:
            if (not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call)
                    or getattr(stmt.value.func, "id", "") != "exports_files"):
                continue
            files = ast.literal_eval(stmt.value.args[0])
            kept = [name for name in files if not name.startswith("include/") or any(
                "//base/poco/" + part + ":" + name in content for p, content in contents.items() if p != path)]
            stats["Poco exports removed"] += len(files) - len(kept)
            if not kept:
                edits.append((*source.span(stmt), ""))
            elif len(kept) != len(files):
                edits.append((*source.span(stmt.value.args[0]), strings(kept)))
        if edits:
            path.write_text(source.rewrite(edits))


def inherited_netssl_includes(root, paths, stats):
    """Remove only include roots provided by NetSSL's reachable public deps."""
    docs = []
    for path in paths:
        if not path.is_file() or (path.name != "BUILD.bazel" and not path.name.endswith(".BUILD.bazel")):
            continue
        package = path.parent.relative_to(root).as_posix().removeprefix(".") if path.name == "BUILD.bazel" else ""
        docs.append(Document(path, package=package))
    graph = Graph(docs)
    target = root / "base/poco/NetSSL_OpenSSL/BUILD.bazel"
    doc = next(d for d in docs if d.path == target)
    includes = None
    for cpu in ARCHES:
        per_target = []
        for name in ("_poco_net_ssl__compile_0", "_poco_net_ssl__compile_1"):
            pending = list(graph.dependencies(doc.reference(":" + name), cpu))
            found, seen = set(), set()
            while pending:
                ref = pending.pop()
                if ref in seen or ref not in graph.rules:
                    continue
                seen.add(ref)
                owner, kind, fields = graph.rules[ref]
                if kind not in CC_PROVIDERS | {"alias"}:
                    continue
                for node in list_elements(fields.get("includes"), owner, cpu):
                    value = directory(node, owner)
                    if value is not None and not value.startswith("$(GENDIR)/"):
                        found.add(posixpath.normpath(posixpath.join(owner.package or owner.owner or "", value)))
                pending.extend(graph.dependencies(ref, cpu))
            per_target.append(found)
        current = set.intersection(*per_target)
        includes = includes & current if includes is not None else current
    source = doc.source
    edits = []
    for stmt in source.module.body:
        if not isinstance(stmt, ast.Assign) or not any(
                isinstance(t, ast.Name) and t.id == "_GENERATED_LOCAL_INCLUDES_0" for t in stmt.targets):
            continue
        for node in ast.walk(stmt.value):
            if not isinstance(node, ast.List):
                continue
            kept = []
            for item in node.elts:
                value = directory(item, doc)
                path = posixpath.normpath(posixpath.join(doc.package, value)) if value is not None else None
                if path is not None and path in includes:
                    stats["NetSSL inherited includes removed"] += 1
                else:
                    kept.append(source.text(item))
            if len(kept) != len(node.elts):
                edits.append((*source.span(node), "[" + ",\n".join(kept) + "]"))
    if edits:
        target.write_text(source.rewrite(edits))


def run(root, from_generator=False):
    stats = Counter()
    paths = build_files(root)
    publish_base(root, paths, stats, from_generator)
    compatibility = root / "base/glibc-compatibility/BUILD.bazel"
    before = compatibility.read_text()
    source = Source(before)
    compile_0 = next(rule for _, rule in source.rules()
                     if ast.literal_eval(attrs(rule)["name"]) == "glibc-compatibility__compile_0")
    data = attrs(compile_0)
    if "hdrs" in data:
        if ast.literal_eval(data["hdrs"]) != ["spawn.h"]:
            raise ValueError("glibc-compatibility__compile_0 public headers changed")
        compatibility.write_text(replace_attrs(before, {
            "glibc-compatibility__compile_0": {
                "srcs": source.text(data["srcs"]) + ' + ["spawn.h"]',
                "hdrs": None,
            }
        }))
        stats["glibc-compatibility spawn.h moved to srcs"] += 1
    base = root / "base/BUILD.bazel"
    if base.exists() and base not in paths:
        paths.append(base)
    city = root / "contrib/cityhash102/BUILD.bazel"
    before = city.read_text()
    after = replace_attrs(before, {"headers": {"includes": '["include"]'}})
    if after != before:
        city.write_text(after)
    publish_poco(root, paths, stats)
    inherited_netssl_includes(root, paths, stats)
    return dict(stats)


if __name__ == "__main__":
    print(json.dumps(run(ROOT), indent=2))
