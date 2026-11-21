"""Declare the OpenLDAP headers used by ClickHouse on liblber and libldap."""

import ast
import json

from initial_migration.compact_build import Source


CONFIG = ("lber_types", "ldap_features", "ldap_config", "portable")
BER_HEADERS = [":lber_types", ":ldap_features", "include/lber.h", "include/ldap_cdefs.h"]
LDAP_HEADERS = ["include/ldap.h", "include/openldap.h"]
PRIVATE = [":ldap_config", ":portable"] + [
    "include/ac/" + name + ".h" for name in
    ("assert", "ctype", "errno", "fdset", "localize", "socket", "stdarg", "stdlib", "string", "time", "unistd")
] + [
    "include/" + name + ".h" for name in
    ("lber_pvt", "ldap_defaults", "ldap_log", "ldap_queue", "lutil", "lutil_lockf")
] + ["libraries/liblber/lber-int.h"]
LDAP_ONLY = ["include/ac/bytes.h", "include/ac/dirent.h", "include/ac/param.h", "include/ac/signal.h"] + [
    "include/" + name + ".h" for name in
    ("ldap_avl", "ldap_int_thread", "ldap_pvt", "ldap_pvt_thread", "ldap_pvt_uc", "ldap_rq", "ldap_schema", "ldap_utf8", "ldif")
] + ["libraries/liblunicode/ucdata/ucdata.h"]


def native_library(target):
    group, = target["compileGroups"]
    assert group["language"] == "C"
    srcs = []
    for index in group["sourceIndexes"]:
        source = target["sources"][index]
        path = source["path"]
        if source.get("isGenerated"):
            srcs.append("@clickhouse//bazel:gen/contrib/openldap-cmake/" + path.rsplit("/", 1)[-1])
        else:
            assert path.startswith("contrib/openldap/") and path.endswith(".c"), path
            srcs.append(path.removeprefix("contrib/openldap/"))
    return srcs, [d["define"] for d in group["defines"]]


def overlay(targets):
    lines = ['load("@bazel_skylib//rules:copy_file.bzl", "copy_file")',
             'load("@clickhouse//bazel/rules:ch_cc_library.bzl", "ch_cc_library")',
             'exports_files(["LICENSE", "build/mkversion"])']
    for name in CONFIG:
        out = "include/" + name + ".h"
        by_cpu = {"@clickhouse//contrib/openldap-cmake:linux_" + arch:
                  "@clickhouse//contrib/openldap-cmake:linux_" + arch + "/" + out for arch in ("x86_64", "aarch64")}
        lines.append(f'copy_file(name = "{name}", src = select({json.dumps(by_cpu)}), out = "{out}", allow_symlink = True)')
    lines.extend([
        '# liblber and libldap both include these implementation headers.',
        '_OPENLDAP_PRIVATE_HEADERS = ' + json.dumps(PRIVATE),
    ])
    for name in ("_lber", "_ldap"):
        srcs, defines = native_library(targets["x86_64"][name])
        if native_library(targets["aarch64"][name]) != (srcs, defines):
            raise ValueError(f"OpenLDAP {name} sources or defines differ by architecture")
        sources = json.dumps(srcs + (LDAP_ONLY if name == "_ldap" else [])) + " + _OPENLDAP_PRIVATE_HEADERS"
        if name == "_ldap":
            sources += ' + glob(["libraries/libldap/**/*.h"])'
        attrs = [
            f'name = "{name}"', 'srcs = ' + sources,
            'hdrs = ' + json.dumps(BER_HEADERS if name == "_lber" else LDAP_HEADERS), 'copts = ["-w", "-std=gnu11"]',
            'language = "C"', 'local_defines = ' + json.dumps(defines),
            'local_includes = ["libraries/lib' + name[1:] + '"]', 'visibility = ["//visibility:public"]',
        ]
        if name == "_lber":
            attrs.append('includes = ["include"]')
        else:
            attrs.extend(['deps = [":_lber"]', 'implementation_deps = ["@clickhouse_openssl//:crypto", "@clickhouse_openssl//:ssl"]'])
        lines.append("ch_cc_library(" + ", ".join(attrs) + ")")
    return "\n\n".join(lines) + "\n"


def declare_openldap(root, targets):
    (root / "bazel/dependencies/clickhouse_openldap.BUILD.bazel").write_text(overlay(targets))
    path = root / "src/BUILD.bazel"
    source = Source(path.read_text())
    api = "@clickhouse_openldap//:_api"
    ldap = "@clickhouse_openldap//:_ldap"
    lber = "@clickhouse_openldap//:_lber"
    arm = "@platforms//cpu:aarch64"
    # Convert header labels from the initial CMake import to the owner of ldap.h.
    scope = {st.targets[0].id: st.value for st in source.module.body
             if isinstance(st, ast.Assign) and isinstance(st.targets[0], ast.Name)}
    def declarations(node):
        yield node
        if isinstance(node, ast.Name) and node.id in scope:
            yield from declarations(scope[node.id])
        for child in ast.iter_child_nodes(node):
            yield from declarations(child)
    edits = {}
    for _, call in source.rules():
        fields = {kw.arg: kw.value for kw in call.keywords}
        if fields["name"].value == "dbms__compile_0":
            for kw in call.keywords:
                if kw.arg in ("implementation_deps", None):
                    for n in declarations(kw.value):
                        if isinstance(n, ast.Constant) and n.value == api:
                            edits[source.span(n)] = json.dumps(ldap)
        if fields["name"].value == "dbms__compile_4":
            # The ARM group has neither C++ sources nor public headers.
            for kw in (k for k in call.keywords if k.arg in ("deps", "implementation_deps")):
                for branches in (n for n in ast.walk(kw.value) if isinstance(n, ast.Dict)):
                    for key, branch in zip(branches.keys, branches.values):
                        if getattr(key, "value", None) == arm and isinstance(branch, ast.List):
                            kept = [n for n in branch.elts if getattr(n, "value", None) not in {api, lber}]
                            if len(kept) != len(branch.elts):
                                edits[source.span(branch)] = ast.unparse(ast.List(elts=kept, ctx=ast.Load()))
    text = source.rewrite([(start, end, val) for (start, end), val in edits.items()])
    if api in text:
        raise ValueError("Unexpected reference to the removed OpenLDAP _api target")
    path.write_text(text)
