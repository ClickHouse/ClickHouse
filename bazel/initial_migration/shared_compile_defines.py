"""Use the owning Starlark definitions for identical native local_defines."""

import ast
import copy

from initial_migration.compact_build import Source
from initial_migration.factor_build_selections import StrictConfigured, call_name, no_match
from initial_migration.normalize_cpp_builds import compile_defaults, drop_unused_loads, remove_keyword

# Load modules in their Starlark dependency order. Each exported value is an
# ordered list for each supported CPU; a missing select default is significant.
MODULES = {
    "bazel/rules/ch_cc_library.bzl": ("CXX_RUNTIME_DEFINES",),
    "src/common_io_flags.bzl": (
        "CLICKHOUSE_COMMON_IO_CONSUMER_DEFINES", "CLICKHOUSE_COMMON_IO_LOCAL_DEFINES",
        "DBMS_CONSUMER_DEFINES", "DBMS_LOCAL_DEFINES", "DBMS_USEARCH_LOCAL_DEFINES",
    ),
    "programs/program_defines.bzl": ("CLICKHOUSE_PROGRAM_DEFINES", "CLICKHOUSE_PROGRAM_COMPRESSION_DEFINES"),
    "bazel/dependencies/aws_crt_defines.bzl": ("AWS_CRT_DEFINES",),
}
CPUS = ("x86_64", "aarch64", "unsupported")
FAMILY_MODULE = "@clickhouse//src:common_io_flags.bzl"
FAMILIES = ("dbms_defines", "common_io_defines")


class Defines(StrictConfigured):
    def visit_Subscript(self, node):
        node = self.generic_visit(node)
        if call_name(node.value) == "dict" and isinstance(node.slice, ast.Constant):
            return next((kw.value for kw in node.value.keywords if kw.arg == node.slice.value), node)
        return node

    def visit_Call(self, node):
        node = super().visit_Call(node)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Lambda) and not node.keywords and len(node.args) <= 1:
            # configured() binds the helper's module constants before it is loaded in a BUILD file.
            scoped = copy.copy(self)
            scoped.constants = {"extra": node.args[0] if node.args else node.func.args.defaults[0]}
            return scoped.visit(copy.deepcopy(node.func.body))
        if (call_name(node) == "sorted" and len(node.args) == 1 and not node.keywords
                and isinstance(node.args[0], ast.List)
                and all(isinstance(v, ast.Constant) and isinstance(v.value, str) for v in node.args[0].elts)):
            return ast.List(elts=sorted(node.args[0].elts, key=lambda v: v.value), ctx=ast.Load())
        return node


def loaded_names(call):
    return {v.value: v.value for v in call.args[1:]} | {v.arg: v.value.value for v in call.keywords}


def has_compile_sources(node):
    """ch_cc_library tests srcs before Bazel resolves select()."""
    if isinstance(node, ast.List):
        return bool(node.elts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return has_compile_sources(node.left) or has_compile_sources(node.right)
    if call_name(node) == "glob":
        # Bazel rejects an empty glob unless allow_empty is enabled.
        return not any(kw.arg == "allow_empty" and not (
            isinstance(kw.value, ast.Constant) and kw.value.value is False) for kw in node.keywords)
    return call_name(node) == "select"


def omit_compile_defaults(source):
    """Omit only exact, unconditional defaults of ch_cc_library."""
    lists, runtime, edits, removed_names = {}, set(), [], set()
    for stmt in source.module.body:
        if (isinstance(stmt, ast.Assign) and isinstance(stmt.targets[0], ast.Name)
                and isinstance(stmt.value, ast.List)
                and all(isinstance(v, ast.Constant) and isinstance(v.value, str) for v in stmt.value.elts)):
            lists[stmt.targets[0].id] = stmt
        if (isinstance(stmt, ast.Expr) and call_name(stmt.value) == "load" and
                stmt.value.args[0].value.removeprefix("@clickhouse") == "//bazel/rules:ch_cc_library.bzl"):
            runtime.update(k for k, v in loaded_names(stmt.value).items() if v == "CXX_RUNTIME_DEFINES")
    for _, rule in source.rules():
        defaults = compile_defaults(rule)
        if not defaults:
            continue
        # ch_grpc_cc_library always passes its generated grpc sources to ch_cc_library.
        if call_name(rule) == "ch_cc_library" and not has_compile_sources(next(kw.value for kw in rule.keywords if kw.arg == "srcs")):
            continue
        for kw in rule.keywords:
            if kw.arg not in defaults:
                continue
            name = kw.value.id if isinstance(kw.value, ast.Name) else None
            if kw.arg == "local_defines":
                matches = name in runtime
            else:
                flags = lists[name].value if name in lists else kw.value
                matches = (isinstance(flags, ast.List) and all(isinstance(v, ast.Constant) for v in flags.elts)
                           and [v.value for v in flags.elts] == defaults["copts"])
            if matches:
                edits.append(remove_keyword(source, kw))
                if name in lists:
                    removed_names.add(name)
    if not edits:
        return source.bytes.decode()
    updated = Source(source.rewrite(edits))
    used = {n.id for n in ast.walk(updated.module) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    updated = Source(updated.rewrite([(*updated.span(stmt), "") for stmt in updated.module.body
                                     if isinstance(stmt, ast.Assign) and isinstance(stmt.targets[0], ast.Name)
                                     and stmt.targets[0].id in removed_names - used]))
    return drop_unused_loads(updated)


def configured(source, modules):
    visitors = [Defines(cpu, collapse_failed=False) for cpu in CPUS]
    for statement in source.module.body:
        if isinstance(statement, ast.Expr) and call_name(statement.value) == "load":
            call = statement.value
            imported = modules.get(call.args[0].value)
            if imported:
                for visitor, exports in zip(visitors, imported):
                    visitor.constants.update({local: exports[remote] for local, remote in loaded_names(call).items() if remote in exports})
        elif isinstance(statement, ast.FunctionDef) and statement.name in FAMILIES:
            if ([p.arg for p in statement.args.args] != ["extra"] or len(statement.args.defaults) != 1 or
                    ast.unparse(statement.args.defaults[0]) != "[]" or not isinstance(statement.body[-1], ast.Return)):
                raise ValueError(f"unexpected signature of {statement.name}")
            for visitor in visitors:
                body = visitor.visit(copy.deepcopy(statement.body[-1].value))
                visitor.constants[statement.name] = ast.Lambda(args=statement.args, body=body)
        elif (isinstance(statement, ast.Assign) and len(statement.targets) == 1
              and isinstance(statement.targets[0], ast.Name)):
            for visitor in visitors:
                visitor.constants[statement.targets[0].id] = visitor.visit(copy.deepcopy(statement.value))
    return visitors


def signature(visitors, node):
    ordered = []
    for visitor in visitors:
        value = visitor.visit(copy.deepcopy(node))
        if no_match(value):
            ordered.append(None)
        elif isinstance(value, ast.List) and all(
                isinstance(s, ast.Constant) and isinstance(s.value, str) for s in value.elts):
            ordered.append(tuple(s.value for s in value.elts))
        else:
            return None
    return tuple(ordered)


def replacement(value, names, families):
    if value in names:
        label, symbol = names[value]
        return label, symbol, symbol
    if value and value[2] is None and all(value[:2]):
        for symbol, base in families.items():
            extra = [flag for flag in value[0] if flag not in base[0]]
            if all(tuple(sorted(row + tuple(extra))) == actual for row, actual in zip(base[:2], value[:2])):
                return FAMILY_MODULE, symbol, symbol + "(" + (repr(extra) if extra else "") + ")"
    return None


def share_local_defines(root, paths):
    modules, names, families = {}, {}, {}
    for path, exports in MODULES.items():
        package, _, filename = path.rpartition("/")
        label = "@clickhouse//" + package + ":" + filename
        visitors = configured(Source((root / path).read_text()), modules)
        modules[label] = [v.constants for v in visitors]
        if label == FAMILY_MODULE:
            families = {s: signature(visitors, ast.parse(s + "()", mode="eval").body) for s in FAMILIES}
        for symbol in exports:
            key = signature(visitors, ast.Name(id=symbol, ctx=ast.Load()))
            if key is None or key in names:
                raise ValueError(f"{label}: expected distinct ordered definitions for {symbol}")
            names[key] = (label, symbol)

    for path in sorted(paths):
        source = Source(path.read_text())
        visitors = configured(source, modules)
        edits, imports, aliases = [], {}, set()
        package_families = families if path.relative_to(root).parts[0] in {"base", "programs", "src"} else {}
        defined = {n.targets[0].id for n in source.module.body
                   if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)}
        bindings = {}
        for statement in source.module.body:
            if isinstance(statement, ast.Expr) and call_name(statement.value) == "load":
                call = statement.value
                for local, remote in loaded_names(call).items():
                    bindings[local] = (call.args[0].value, remote)
        for _, rule in source.rules():
            if call_name(rule) not in {"ch_cc_library", "poco_cc_library"}:
                continue
            for attr in rule.keywords:
                if attr.arg != "local_defines":
                    continue
                owner = replacement(signature(visitors, attr.value), names, package_families)
                if not owner:
                    continue
                label, symbol, expr = owner
                attrs = {kw.arg: kw.value for kw in rule.keywords}
                if (symbol == "DBMS_LOCAL_DEFINES" and path == root / "src/BUILD.bazel"
                        and attrs["name"].value == "dbms__compile_0"):
                    deps = signature(visitors, attrs["implementation_deps"])
                    if deps is None or any(row is None or row.count("@clickhouse_usearch//:usearch") != 1 for row in deps[:2]):
                        raise ValueError("dbms__compile_0 must receive the USearch definitions from its dependency")
                    symbol = "DBMS_USEARCH_LOCAL_DEFINES"
                    expr = symbol
                if symbol in defined or bindings.get(symbol, (label, symbol)) != (label, symbol):
                    continue
                if ast.dump(attr.value) == ast.dump(ast.parse(expr, mode="eval").body):
                    continue
                # Edit the shared definition once. Keep uses of a package's attribute dictionary.
                if symbol in FAMILIES:
                    if isinstance(attr.value, ast.Subscript):
                        continue
                    if isinstance(attr.value, ast.Name) and attr.value.id.startswith("_") and not attr.value.id.startswith("_GENERATED_"):
                        aliases.add(attr.value.id)
                        continue
                if isinstance(attr.value, ast.Name):
                    if attr.value.id == symbol:
                        continue
                    if attr.value.id.startswith("_"):
                        aliases.add(attr.value.id)
                edits.append((*source.span(attr.value), expr))
                imports.setdefault(label, set()).add(symbol)
        # An attribute dictionary may still reference a factored list after its rules have been rewritten.
        for statement in source.module.body:
            if isinstance(statement, ast.Assign) and statement.targets[0].id in aliases:
                owner = replacement(signature(visitors, statement.value), names, package_families)
                if owner:
                    label, symbol, expr = owner
                    if ast.dump(statement.value) == ast.dump(ast.parse(expr, mode="eval").body):
                        continue
                    edits.append((*source.span(statement.value), expr))
                    imports.setdefault(label, set()).add(symbol)
        if not edits:
            continue
        for statement in source.module.body:
            if isinstance(statement, ast.Expr) and call_name(statement.value) == "load":
                call = statement.value
                needed = imports.pop(call.args[0].value, set()) - {v.value for v in call.args[1:]}
                if needed:
                    call.args.extend(ast.Constant(value=s) for s in sorted(needed))
                    edits.append((*source.span(call), ast.unparse(call)))
        loads = "".join("load(" + ", ".join(repr(s) for s in (label, *sorted(symbols))) + ")\n"
                        for label, symbols in sorted(imports.items()))
        updated = Source(loads + source.rewrite(edits))
        used = {n.id for n in ast.walk(updated.module) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
        path.write_text(updated.rewrite([(*updated.span(n), "") for n in updated.module.body
                                         if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
                                         and n.targets[0].id in aliases - used]))
