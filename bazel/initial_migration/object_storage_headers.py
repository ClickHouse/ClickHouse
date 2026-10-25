"""Assign ObjectStorage configuration, TableFunctions and S3 I/O headers to their public C++ owners."""
import ast
import json
from initial_migration.generate_build import header_glob_exclusion
from initial_migration.prune_inherited_headers import ARCHES, Document

_CONFIG = [
    "Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h",
    "Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h",
    "Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h",
    "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h",
    "Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h",
    "Storages/ObjectStorage/S3/Configuration.h",
]
_TABLE = [
    "ITableFunctionCluster.h", "TableFunctionFileCluster.h",
    "TableFunctionObjectStorage.h", "TableFunctionURLCluster.h",
]
_IO = ["IO/ReadBufferFromS3.h", "IO/S3AuthSettings.h", "IO/S3Common.h", "IO/S3Settings.h"]


def apply(root):
    def load(package):
        doc = Document(root / package / "BUILD.bazel", package=package)
        rules = {next(k.value.value for k in rule.keywords if k.arg == "name"): rule
                 for _, rule in doc.source.rules() if any(k.arg == "name" for k in rule.keywords)}
        return doc, rules

    for package, name, headers, deps in [
        ("src", "object_storage_configuration", _CONFIG, [
            "//base:common", "//base/poco/JSON:_poco_json", "//bazel:clickhouse_config_headers",
            ":clickhouse_common_io__compile_0", ":core_types", ":headers", ":io_string_buffers",
            ":with_context", "//src/AggregateFunctions:headers", "//src/Common",
            "//src/Common:parser_support_api", "//src/Parsers:parser_api", "@clickhouse_fmtlib//:_fmt",
        ]),
        ("src/TableFunctions", "object_storage", _TABLE, [
            ":headers", "//bazel:clickhouse_config_headers", "//src:headers",
            "//src:object_storage_configuration", "//src/Parsers:headers",
        ]),
    ]:
        doc, rules = load(package)
        if name in rules:
            continue
        constant = "_" + name.upper() + "_HEADERS"
        _, excluded = header_glob_exclusion(rules["headers"])
        end = doc.source.span(excluded)[1]
        package_call = next(s.value for s in doc.source.module.body
                            if isinstance(s, ast.Expr) and isinstance(s.value, ast.Call)
                            and getattr(s.value.func, "id", None) == "package")
        pos = doc.source.span(package_call)[1]
        edits = [(pos, pos, f"\n\n{constant} = {json.dumps(headers)}"), (end, end, " + " + constant)]
        if package == "src":
            current = doc.constants["_IO_COMPILED_API_HEADERS"].right
            values = ast.literal_eval(current)
            if any(p in values for p in _IO):
                raise ValueError("unexpected ownership of the S3 I/O headers")
            edits.append((*doc.source.span(current), json.dumps(sorted(values + _IO))))
        doc.path.write_text(doc.source.rewrite(edits) +
                            f'\ncc_library(\n    name = "{name}",\n    hdrs = {constant},\n    deps = {json.dumps(deps)},\n)\n')

    aggregate, rules = load("src/AggregateFunctions")
    fields = {kw.arg: kw for kw in rules["clickhouse_aggregate_functions__compile_0"].keywords}
    if "hdrs" in fields:
        public = next(kw.value for kw in rules["headers"].keywords if kw.arg == "hdrs")
        if ast.dump(fields["hdrs"].value) != ast.dump(public):
            raise ValueError("the aggregate compilation library and its public header library have different headers")
        start, end = aggregate.source.span(fields["hdrs"])
        if aggregate.source.bytes[end:end + 1] == b",":
            end += 1
        aggregate.path.write_text(aggregate.source.rewrite([(start, end, "")]))

    for package, target, attr, labels in [
        ("src/AggregateFunctions", "clickhouse_aggregate_functions__compile_0", "deps", [":headers"]),
        ("src", "dbms__compile_0", "implementation_deps", [":object_storage_configuration", "//src/TableFunctions:object_storage"]),
        ("src/Storages/System", "clickhouse_storages_system__compile_1", "implementation_deps", ["//src:object_storage_configuration"]),
        ("src/TableFunctions", "clickhouse_table_functions", "implementation_deps", ["//src:object_storage_configuration", ":object_storage"]),
        ("src", "unit_tests_dbms", "deps", [":object_storage_configuration"]),
    ]:
        doc, rules = load(package)
        node = next(kw.value for kw in rules[target].keywords if kw.arg == attr)
        present = [doc.reference(label) in doc.labels(node, arch) for arch in ARCHES for label in labels]
        if all(present):
            continue
        if any(present):
            raise ValueError(f"partial dependency list on //{package}:{target}")
        start = doc.source.span(node)[0]
        doc.path.write_text(doc.source.rewrite([(start, start, json.dumps(labels) + " + ")]))
