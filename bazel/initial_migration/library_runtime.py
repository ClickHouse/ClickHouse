"""Remove runtime attributes supplied by ch_cc_library after graph pruning."""

import ast
from collections import Counter, defaultdict
from copy import deepcopy

from initial_migration.compact_build import Expand, Source, ast_key, remove_unused_constants
from initial_migration.normalize_cpp_builds import call_name, drop_unused_loads

CPUS = ("x86_64", "aarch64")
HEADERS = (
    "@llvm-project//compiler-rt:headers",
    "@llvm-project//libc:libc_headers",
    "@llvm-project//libunwind:headers",
)
RUNTIME = tuple("@clickhouse_llvm_project//:" + name for name in ("cxx", "cxxabi", "unwind"))
CXX_INPUTS = tuple("@clickhouse_llvm_project//:" + name for name in (
    "runtime_source_inputs", "runtime_generated_headers",
))
LLVM_INCLUDES = ("/libunwind/include", "/libc", "/compiler-rt/include")


class Symbols(Expand):
    """Expand generated names without folding a select that has no default."""

    def visit_Call(self, node):
        return self.generic_visit(node)

    def visit_BinOp(self, node):
        return self.generic_visit(node)


def fields(call):
    return {kw.arg: kw.value for kw in call.keywords}


def literal(node, default=None):
    return node.value if isinstance(node, ast.Constant) else default


def selected(node, cpu):
    """Preserve unknown expressions as single elements when checking a prefix."""
    if isinstance(node, ast.List):
        return node.elts
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return selected(node.left, cpu) + selected(node.right, cpu)
    if call_name(node) == "select":
        choices = {literal(k): v for k, v in zip(node.args[0].keys, node.args[0].values)}
        if not set(choices) <= {"@platforms//cpu:" + arch for arch in CPUS} | {"//conditions:default"}:
            return [node]
        branch = choices.get("@platforms//cpu:" + cpu, choices.get("//conditions:default"))
        if branch is None:
            raise ValueError("No matching CPU in select")
        return selected(branch, cpu)
    return [node]


def llvm_directory(node):
    """Resolve only literal concatenation and llvm_toolchain_path()."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if call_name(node) == "llvm_toolchain_path" and not node.args and not node.keywords:
        return "<llvm-toolchain>"
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = llvm_directory(node.left), llvm_directory(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def strip_items(node, labels=(), directories=(), symbols=None, requests=None):
    """Edit list elements only, keeping select conditions and every other call."""
    node = deepcopy(node)
    if isinstance(node, ast.Name) and symbols is not None and node.id in symbols:
        value = symbols[node.id]
        changed = strip_items(value, labels, directories)
        if ast_key(value) != ast_key(changed):
            requests[node.id].append(changed)
    elif isinstance(node, ast.List):
        node.elts = [item for item in node.elts
                     if literal(item) not in labels and llvm_directory(item) not in directories]
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        node.left = strip_items(node.left, labels, directories, symbols, requests)
        node.right = strip_items(node.right, labels, directories, symbols, requests)
        if isinstance(node.left, ast.List) and not node.left.elts:
            return node.right
        if isinstance(node.right, ast.List) and not node.right.elts:
            return node.left
    elif call_name(node) == "select":
        node.args[0].values = [strip_items(value, labels, directories, symbols, requests)
                               for value in node.args[0].values]
    return node


def normalize(text, package):
    source = Source(text)
    symbols = {statement.targets[0].id: statement.value for statement in source.module.body
               if isinstance(statement, ast.Assign) and len(statement.targets) == 1
               and isinstance(statement.targets[0], ast.Name)
               and statement.targets[0].id.startswith("_GENERATED_")}
    uses = Counter(node.id for node in ast.walk(source.module)
                   if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load))
    requests = defaultdict(list)
    attributes = []
    expanded = Symbols().visit(ast.parse(text))
    expanded_rules = {
        literal(fields(statement.value)["name"]): statement.value
        for statement in expanded.body
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
        and "name" in fields(statement.value)
    }
    edits = []
    empty = ast.List(elts=[], ctx=ast.Load())
    for _, rule in source.rules():
        if call_name(rule) != "ch_cc_library":
            continue
        original = fields(rule)
        attrs = fields(expanded_rules[literal(original["name"])])
        arches = ast.literal_eval(attrs["arches"]) if "arches" in attrs else CPUS
        language = literal(attrs.get("language"), "CXX")
        own_cxx = language == "CXX" and literal(attrs.get("clickhouse_code"), False)
        deps = attrs.get("deps", empty)
        configured = [[literal(item) for item in selected(deps, cpu)] for cpu in arches]
        headers = all(tuple(values[:len(HEADERS)]) == HEADERS for values in configured)
        runtime = "link_runtime" not in original and all(
            all(values.count(label) == 1 for label in RUNTIME) for values in configured)
        # Some incompatible targets still declare headers on the other CPU.
        # Do not replace unconditional declarations with an arches-limited
        # macro default.
        for cpu in CPUS:
            if cpu not in arches:
                try:
                    inactive = [literal(item) for item in selected(deps, cpu)]
                except ValueError:
                    continue
                headers = headers and not any(label in inactive for label in HEADERS)
                runtime = runtime and not any(label in inactive for label in RUNTIME)
        include_values = ()
        if own_cxx:
            depth = len(package.split("/")) if package else 0
            include_values = tuple("../" * depth + "<llvm-toolchain>" + suffix for suffix in LLVM_INCLUDES)
            for cpu in arches:
                items = selected(attrs.get("local_includes", empty), cpu)
                # A file may already have been normalized. It must not contain
                # a partial set or an LLVM directory before a later directory.
                found = [llvm_directory(item) for item in items if llvm_directory(item) in include_values]
                if found and tuple(llvm_directory(item) for item in items[-3:]) != include_values:
                    raise ValueError(f"{package}:{literal(original['name'])} {cpu}: unexpected LLVM includes")
        labels = (HEADERS if headers else ()) + (RUNTIME if runtime else ())
        filters = {
            "deps": (labels, ()),
            "local_includes": ((), include_values),
        }
        if language in ("C", "ASM"):
            # C and assembly sources do not need the captured libc++ source and generated-header inputs.
            filters["additional_compiler_inputs"] = (CXX_INPUTS, ())
        for field, (remove_labels, remove_directories) in filters.items():
            replacement = strip_items(attrs.get(field, empty), remove_labels, remove_directories)
            if field not in original or ast_key(attrs[field]) == ast_key(replacement):
                continue
            if isinstance(replacement, ast.List) and not replacement.elts:
                kw = next(kw for kw in rule.keywords if kw.arg == field)
                start, end = source.span(kw)
                while source.bytes[end:end + 1] in (b" ", b"\t"):
                    end += 1
                if source.bytes[end:end + 1] == b",":
                    end += 1
                edits.append((start, end, ""))
            else:
                previous = {name: len(items) for name, items in requests.items()}
                named = strip_items(original[field], remove_labels, remove_directories, symbols, requests)
                requested = {name for name, items in requests.items() if len(items) != previous.get(name, 0)}
                attributes.append((original[field], named, replacement, requested))
        if runtime:
            _, end = source.span(rule)
            edits.append((end - 1, end - 1, "link_runtime = True,\n"))
    shared = {name for name, values in requests.items() if len(values) == uses[name]
              and len({ast_key(value) for value in values}) == 1}
    for name in shared:
        edits.append((*source.span(symbols[name]), ast.unparse(ast.fix_missing_locations(requests[name][0]))))
    for original, named, expanded_value, requested in attributes:
        replacement = named if requested <= shared else expanded_value
        if ast_key(original) != ast_key(replacement):
            edits.append((*source.span(original), ast.unparse(ast.fix_missing_locations(replacement))))
    result = source.rewrite(edits)
    while True:
        updated = remove_unused_constants(Source(result))
        updated = drop_unused_loads(Source(updated))
        if updated == result:
            return result
        result = updated
