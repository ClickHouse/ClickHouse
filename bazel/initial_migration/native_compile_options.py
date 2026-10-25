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


def render(atoms):
    terms = []
    for literal, group in groupby(atoms, lambda n: isinstance(n, ast.Constant)):
        values = [ast.unparse(n) for n in group]
        if literal:
            terms.append("[\n" + ",\n".join(values) + ",\n]")
        else:
            terms.extend(values)
    return " + ".join(terms)


def shared_native_compile_options(root, paths):
    definitions = Source((root / MODULE).read_text()).module.body
    codegen = next(n.value for n in definitions if isinstance(n, ast.Assign) and n.targets[0].id == "NATIVE_CODEGEN_COPTS")
    functions = {n.name: n.body[-1].value for n in definitions if isinstance(n, ast.FunctionDef)}

    def expand(node, standard):
        # Read the two return expressions from native_compile_options.bzl so a
        # changed CMake option cannot match a stale second copy in the importer.
        if isinstance(node, ast.IfExp) and isinstance(node.test, ast.Name) and node.test.id == "standard":
            return expand(node.body if standard else node.orelse, standard)
        if call_name(node) == "native_release_copts":
            return expand(functions["native_release_copts"], standard)
        if isinstance(node, ast.Name):
            if node.id == "standard":
                return [ast.Constant(value=standard)]
            if node.id == "NATIVE_CODEGEN_COPTS":
                return option_atoms(codegen)
        if isinstance(node, (ast.BinOp, ast.List)):
            return [v for term in option_atoms(node) for v in expand(term, standard)]
        return [node]

    patterns = []
    for symbol, standards in (("native_cxx_tool_copts", ("-std=c++17", "-std=c++23")),
                              ("NATIVE_CODEGEN_COPTS", (None,)),
                              ("native_release_copts", ("-std=c++23", None))):
        for standard in standards:
            new = symbol if symbol not in functions else "{}({})".format(symbol, repr(standard) if standard else "")
            old = expand(functions.get(symbol, codegen), standard)
            patterns.append((symbol, list(map(ast_key, old)), ast.parse(new, mode="eval").body))

    for path in sorted(paths):
        source = Source(path.read_text())
        edits, imports = [], set()
        for _, rule in source.rules():
            kind = call_name(rule)
            if kind not in {"cc_binary", "ch_cc_binary", "llvm_runtime_objects"}:
                continue
            for attr in rule.keywords:
                if attr.arg != "copts" or not isinstance(attr.value, ast.BinOp):
                    continue
                atoms = option_atoms(attr.value)
                original = list(map(ast_key, atoms))
                for name, match, replacement in patterns:
                    if kind == "llvm_runtime_objects" and name != "native_cxx_tool_copts":
                        start = next((i for i in range(len(atoms) - len(match) + 1)
                                      if [ast_key(a) for a in atoms[i:i + len(match)]] == match), None)
                    else:
                        start = 0 if name == "native_cxx_tool_copts" and original == match else None
                    if start is not None:
                        atoms[start:start + len(match)] = [replacement]
                        imports.add(name)
                if [ast_key(a) for a in atoms] != original:
                    edits.append((*source.span(attr.value), render(atoms)))
        if edits:
            load = "load(" + ", ".join(repr(v) for v in (LABEL, *sorted(imports))) + ")\n"
            path.write_text(drop_unused_loads(Source(load + source.rewrite(edits))))
