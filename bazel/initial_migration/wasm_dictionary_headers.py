"""Declare the C++ headers used by ASTCreateWasmFunctionQuery and ExternalDictionariesLoader."""
import ast
import json
from initial_migration.generate_build import append_header_names, exclude_common_headers
from initial_migration.prune_inherited_headers import ARCHES, Document


WASM = "//src/Functions:wasm_udf_api"
LOADER = "//src:external_dictionaries_loader"
DICTIONARY = "//src/Dictionaries:dictionary_api"
DICTIONARY_HEADERS = ["DictionaryStructure.h", "IDictionary.h", "IDictionarySource.h"]


def apply(root):
    def document(package, files=()):
        doc = Document(root / package / "BUILD.bazel", package=package, files=files)
        rules = {kw.value.value: rule for _, rule in doc.source.rules()
                 for kw in rule.keywords if kw.arg == "name"}
        return doc, rules

    def field(rule, name):
        return next(kw.value for kw in rule.keywords if kw.arg == name)

    def provider(label, source):
        # Before the migration splits these libraries, the package's public headers own the same file.
        package, _, name = label[2:].partition(":")
        doc, rules = document(package, [source])
        candidates = []
        for target, rule in rules.items():
            hdrs = next((kw.value for kw in rule.keywords if kw.arg == "hdrs"), None)
            if all(doc.reference(source) in doc.labels(hdrs, cpu) for cpu in ARCHES):
                candidates.append(target)
        preferred = name or package.rsplit("/", 1)[-1]
        if preferred in candidates:
            return label
        if "headers" in candidates:
            return f"//{package}:headers"
        if len(candidates) == 1:
            return f"//{package}:{candidates[0]}"
        raise ValueError(f"cannot choose a public owner for {package}/{source}: {candidates}")

    def release(doc, node, headers):
        # CMake can omit a header or list it in some groups; a checked BUILD may reuse one list in all groups.
        owned = {doc.reference(h) for h in headers}
        if not any(owned & doc.labels(node, arch) for arch in ARCHES):
            return []
        if isinstance(node, ast.Name):
            return release(doc, doc.constants[node.id], headers)
        if isinstance(node, ast.List):
            keep = [p for p in ast.literal_eval(node) if doc.reference(p) not in owned]
            return [(*doc.source.span(node), json.dumps(keep))]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return release(doc, node.left, headers) + release(doc, node.right, headers)
        if isinstance(node, ast.Call) and node.func.id == "select":
            return [edit for branch in node.args[0].values for edit in release(doc, branch, headers)]
        if isinstance(node, ast.Call) and node.func.id == "glob":
            excluded = next((kw.value for kw in node.keywords if kw.arg == "exclude"), None)
            if excluded is not None:
                return [append_header_names(doc, excluded, headers)]
            end = doc.source.span(node)[1] - 1
            return [(end, end, ", exclude = " + json.dumps(headers))]
        raise ValueError(f"cannot remove {headers} from {doc.source.text(node)}")

    def colocated(label, package):
        return label.removeprefix("//" + package) if label.startswith("//" + package + ":") else label

    contracts = [
        (DICTIONARY, DICTIONARY_HEADERS, [
            ("//base:common", "base/EnumReflection.h"), ("//base/poco/Util:_poco_util", "include/Poco/Util/AbstractConfiguration.h"),
            ("//src:column_types", "Columns/ColumnConst.h"), ("//src:core_types", "Core/Field.h"),
            ("//src:headers", "DataTypes/IDataType.h"), ("//src:io_string_buffers", "IO/ReadBufferFromString.h"),
            ("//src/Common:memory_tracking_api", "VectorWithMemoryTracking.h"), ("//src/Common", "Arena.h"),
        ]),
        (WASM, ["UserDefined/UserDefinedWebAssembly.h"], [
            ("//src:headers", "Interpreters/WebAssembly/WasmEngine.h"), ("//src:with_context", "Interpreters/Context_fwd.h"),
            ("//src/Parsers:parser_api", "IAST_fwd.h"), ("//src/Common:shared_mutex", "SharedMutex.h"),
            ("//src/Common:memory_tracking_api", "VectorWithMemoryTracking.h"), ("//src/Common", "StopToken.h"),
        ]),
        (LOADER, ["Interpreters/ExternalDictionariesLoader.h"], [
            ("//src:headers", "Interpreters/ExternalLoader.h"), ("//src:with_context", "Interpreters/Context_fwd.h"),
            ("//src/Common:parser_support_api", "quoteString.h"), (DICTIONARY, "IDictionary.h"),
        ]),
    ]
    for label, headers, providers in contracts:
        package, name = label[2:].split(":")
        doc, rules = document(package, headers)
        if name in rules:
            continue
        deps = list(dict.fromkeys(colocated(provider(dep, header), package) for dep, header in providers))
        edits = []
        if label == DICTIONARY:
            # The three compiler groups and :headers reuse the same public header list.
            owners = ["headers", *(f"clickhouse_dictionaries__compile_{i}" for i in range(3))]
            for target in owners:
                hdrs = next((kw.value for kw in rules[target].keywords if kw.arg == "hdrs"), None)
                edits.extend(release(doc, hdrs, headers))
        else:
            edits.append(exclude_common_headers(doc, rules["headers"], headers))
        doc.path.write_text(doc.source.rewrite(sorted(set(edits))) +
                            f'\ncc_library(\n    name = "{name}",\n    hdrs = {json.dumps(headers)},\n    deps = {json.dumps(deps)},\n)\n')

    for package, target, attr, added in [
        ("src/Functions", "headers", "deps", [WASM, LOADER]),
        ("src/Parsers", "clickhouse_parsers", "deps", [WASM]),
        *(("src/Dictionaries", target, "deps", [DICTIONARY])
          for target in ("headers", *(f"clickhouse_dictionaries__compile_{i}" for i in range(3)))),
        *((package, target, "implementation_deps", [LOADER]) for package, target in (
            ("src", "dbms__compile_0"), ("src/Functions", "clickhouse_functions_obj__compile_0"),
            ("src/Storages/System", "clickhouse_storages_system__compile_1"),
            ("src/TableFunctions", "clickhouse_table_functions"), ("programs/server", "clickhouse-server-lib"),
        )),
    ]:
        doc, rules = document(package)
        node = next((kw.value for kw in rules[target].keywords if kw.arg == attr), None)
        items = [colocated(label, package) for label in added
                 if not all(doc.reference(label) in doc.labels(node, arch) for arch in ARCHES)]
        if not items:
            continue
        anchor = next((n for n in ast.walk(node) if getattr(n, "value", None) == "//src/Common/ZooKeeper:headers"), None) if node is not None else None
        if node is None:
            end = doc.source.span(field(rules[target], "name"))[1]
            edit = (end, end, f", {attr} = " + json.dumps(items))
        elif (package, target) == ("src/Functions", "headers") and anchor is not None:
            # Keep the existing compiler search order and unit-test linker input order.
            start = doc.source.span(anchor)[0]
            edit = (start, start, ", ".join(map(json.dumps, items)) + ", ")
        else:
            edit = append_header_names(doc, node, items)
        doc.path.write_text(doc.source.rewrite([edit]))
