"""Share ordered Clang options emitted for native C++ tools and LLVM runtime objects."""

import ast
from itertools import groupby

from initial_migration.compact_build import Source, ast_key
from initial_migration.normalize_cpp_builds import call_name, drop_unused_loads

MODULE = "bazel/rules/native_compile_options.bzl"
LABEL = "@clickhouse//bazel/rules:native_compile_options.bzl"


def option_atoms(node):
    """Flatten list addition; keep every select and prefix-map call intact."""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return option_atoms(node.left) + option_atoms(node.right)
    return node.elts if isinstance(node, ast.List) else [node]


def render(atoms, spellings=None):
    terms = []
    for literal, group in groupby(atoms, lambda n: isinstance(n, ast.Constant)):
        values = [spellings[n] if spellings and n in spellings else ast.unparse(n) for n in group]
        if literal:
            terms.append("[" + values[0] + "]" if len(values) == 1 else "[\n" + ",\n".join(values) + ",\n]")
        else:
            terms.extend(values)
    return " + ".join(terms)


def shared_native_compile_options(root, paths):
    definitions = Source((root / MODULE).read_text()).module.body
    def assignments(statements):
        return {n.targets[0].id: n.value for n in statements
                if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)}

    constants = assignments(definitions)
    functions = {n.name: n.body[-1].value for n in definitions if isinstance(n, ast.FunctionDef)}

    def expand(node, standard, bindings=constants):
        # The imported CMake commands are Release commands. Match the opt arm
        # but emit the configurable expression for Bazel.
        if node is constants["OPT_DRIVER_FLAGS"]:
            return expand(constants["OPT_FLAGS"], standard)
        if isinstance(node, ast.IfExp) and isinstance(node.test, ast.Name) and node.test.id == "standard":
            return expand(node.body if standard else node.orelse, standard, bindings)
        if call_name(node) == "native_release_copts":
            return expand(functions["native_release_copts"], standard)
        if isinstance(node, ast.Name):
            if node.id == "standard":
                return [ast.Constant(value=standard)]
            if node.id in bindings:
                return expand(bindings[node.id], standard, bindings)
            if bindings is not constants:
                raise ValueError(f"Unknown name in imported Clang option list: {node.id}")
        if isinstance(node, ast.List) or (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add)):
            return [v for term in option_atoms(node) for v in expand(term, standard, bindings)]
        return [node]

    # Resolve only the imported option list and the constants it references in its own module.
    for filename, symbol in (("ch_cc_library.bzl", "CLICKHOUSE_CODE_FLAGS"),
                             ("repository_paths.bzl", "CLICKHOUSE_WARNINGS")):
        module = Source((root / "bazel/rules" / filename).read_text()).module
        bindings = assignments(module.body)
        constants[symbol] = ast.List(elts=expand(bindings[symbol], None, bindings), ctx=ast.Load())

    patterns = []
    for symbol, standards in (("native_cxx_tool_copts", ("-std=c++17", "-std=c++23")),
                              ("NATIVE_CODEGEN_COPTS", (None,)),
                              ("NATIVE_TOOL_LINKOPTS", (None,)),
                              ("DBMS_TEST_DRIVER_COPTS", (None,)),
                              ("native_release_copts", ("-std=c++23", None)),
                              ("OPT_DRIVER_FLAGS", (None,))):
        for standard in standards:
            new = symbol if symbol not in functions else "{}({})".format(symbol, repr(standard) if standard else "")
            old = expand((functions | constants)[symbol], standard)
            patterns.append((symbol, list(map(ast_key, old)), ast.parse(new, mode="eval").body))

    aws_module = "bazel/dependencies/aws_crt_defines.bzl"
    aws_flags = assignments(Source((root / aws_module).read_text()).module.body)["AWS_CRT_COPTS"]
    aws_prefix = list(map(ast_key, aws_flags.elts))
    for path in sorted(paths):
        source = Source(path.read_text())
        edits, imports = [], {}
        build_constants = assignments(source.module.body)
        for _, rule in source.rules():
            kind = call_name(rule)
            if kind == "ch_cc_library" and path.name.startswith(("clickhouse_aws_c_", "clickhouse_aws_checksums.")):
                attrs = {kw.arg: kw.value for kw in rule.keywords}
                if getattr(attrs.get("language"), "value", None) == "C" and "copts" in attrs:
                    for opts in ast.walk(attrs["copts"]):
                        if isinstance(opts, ast.List) and list(map(ast_key, opts.elts[:len(aws_prefix)])) == aws_prefix:
                            suffix = opts.elts[len(aws_prefix):]
                            edits.append((*source.span(opts), "AWS_CRT_COPTS" + (" + " + render(suffix) if suffix else "")))
                            imports.setdefault("@clickhouse//bazel/dependencies:aws_crt_defines.bzl", set()).add("AWS_CRT_COPTS")
            is_runtime = kind in {"llvm_runtime_objects", "llvm_outline_atomics"}
            if not is_runtime and kind not in {"cc_binary", "cc_test"}:
                continue
            attributes = ("copts",) if is_runtime else ("copts", "linkopts")
            for attr in rule.keywords:
                if attr.arg not in attributes:
                    continue
                value = attr.value
                options = value
                if kind == "llvm_outline_atomics" and isinstance(value, ast.Name):
                    # Every action of this macro is arm64-only. Its captured
                    # x86_64 arm is empty and cannot select compilation flags.
                    value = options = build_constants[value.id]
                    if call_name(value) == "select":
                        branches = value.args[0]
                        options = dict(zip([key.value for key in branches.keys], branches.values))["@platforms//cpu:aarch64"]
                if not isinstance(options, (ast.List, ast.BinOp)):
                    continue
                atoms = option_atoms(options)
                spellings = {atom: source.text(atom) for atom in atoms}
                original = list(map(ast_key, atoms))
                for name, match, replacement in patterns:
                    if kind == "cc_test":
                        start = 0 if name == "DBMS_TEST_DRIVER_COPTS" and original[:len(match)] == match else None
                    elif attr.arg == "linkopts":
                        start = 0 if name == "NATIVE_TOOL_LINKOPTS" and original[:len(match)] == match else None
                    elif is_runtime and name in {"NATIVE_CODEGEN_COPTS", "native_release_copts", "OPT_DRIVER_FLAGS"}:
                        start = next((i for i in range(len(atoms) - len(match) + 1)
                                      if [ast_key(a) for a in atoms[i:i + len(match)]] == match), None)
                    else:
                        start = 0 if name == "native_cxx_tool_copts" and original == match else None
                    if start is not None:
                        atoms[start:start + len(match)] = [replacement]
                        imports.setdefault(LABEL, set()).add(name)
                if is_runtime:
                    # libunwind repeats -O3 immediately after the same level in native_release_copts.
                    atoms = [atom for i, atom in enumerate(atoms)
                             if not (i and call_name(atoms[i - 1]) == "native_release_copts"
                                     and isinstance(atom, ast.Constant) and atom.value == "-O3")]
                if [ast_key(a) for a in atoms] != original:
                    edits.append((*source.span(value), render(atoms, spellings)))
        if edits:
            load = "".join("load(" + ", ".join(map(repr, [label, *sorted(names)])) + ")\n"
                           for label, names in sorted(imports.items()))
            path.write_text(drop_unused_loads(Source(load + source.rewrite(edits))))
