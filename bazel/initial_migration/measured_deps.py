"""Assign the first-party dependencies verified by the compiler and linker reports."""

import ast
import json

from initial_migration.first_party_packages import Build, CPUS, canonical_dependency

TZDATA = '@clickhouse_cctz//:tzdata'
# Key: (package, rule). Values: (unused dependencies, private dependencies, public dependencies).
DEPENDENCIES = {
    ('src/Access/Common', 'clickhouse_common_access'): ([TZDATA], [], []),
    ('src/Common/mysqlxx', 'mysqlxx'): ([TZDATA], [], []),
    ('src/Common/benchmarks', 'benchmark_stack_trace_objects'): ([TZDATA], [], []),
    ('src/Common/benchmarks', 'integer_hash_tables_and_hashes_objects'): (['@clickhouse_libbcrypt//:_bcrypt'], [], []),
    ('src/Columns/benchmarks', 'benchmark_compute_hash_into_objects'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src/Common/ZooKeeper', 'clickhouse_common_zookeeper_base'): (['@clickhouse_replxx//:_replxx'], [], []),
    ('src/Common/benchmarks', 'orc_string_dictionary_objects'): ([TZDATA], [], []),
    ('src/Common/benchmarks', 'profile_events_objects'): ([TZDATA], [], []),
    ('src/Common/benchmarks', 'wrap_in_nullable_objects'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src/Functions/TimeSeries', 'clickhouse_functions_time_series'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src/Storages/benchmarks', 'benchmark_statistics_objects'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src/TableFunctions', 'clickhouse_table_functions'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src', 'clickhouse_common_io__compile_0'): ([], [TZDATA], []),
    ('src/Functions', 'clickhouse_functions_obj__compile_0'): ([], [TZDATA], []),
    ('src/Storages/System', 'clickhouse_storages_system__compile_1'): (
        [], ['//bazel:clickhouse_config_headers', '//base:common', '//src:headers', '//src/Common', TZDATA], []),
    ('programs/format', 'clickhouse-format-lib'): ([], [TZDATA], []),
    ('src', 'unit_tests_dbms'): ([], [], [TZDATA]),
}


def apply(root):
    """Set the declared visibility of each dependency without changing select conditions."""
    for package in sorted({package for package, _ in DEPENDENCIES}):
        path = root / package / 'BUILD.bazel'
        build = Build(root, path, (), ())
        source = build.source
        edits = []
        found = set()
        canonical = lambda label: canonical_dependency(label, package, '@clickhouse')
        for _, rule in source.rules():
            attrs = {k.arg: k.value for k in rule.keywords}
            name = getattr(attrs.get('name'), 'value', None)
            spec = DEPENDENCIES.get((package, name))
            if spec is None:
                continue
            found.add(name)
            unused, private, public = spec
            for field, discard, add in (
                    ('deps', unused + private, public),
                    ('implementation_deps', unused + public, private)):
                original = attrs.get(field)
                if original is None and not add:
                    continue
                node = build.expand(original) if original is not None else ast.List(elts=[], ctx=ast.Load())
                # factored compile attributes use dict(...)[field].
                class Field(ast.NodeTransformer):
                    def visit_Subscript(self, sub):
                        sub = self.generic_visit(sub)
                        if isinstance(sub.value, ast.Call) and isinstance(sub.value.func, ast.Name) and sub.value.func.id == 'dict':
                            return next(kw.value for kw in sub.value.keywords if kw.arg == sub.slice.value)
                        raise ValueError(f'{package}:{name}: unsupported dependency expression {ast.unparse(sub)}')
                node = Field().visit(node)
                values = [{canonical(v) for v in build.values(node, cpu)} for cpu in CPUS]
                omit = {canonical(v) for v in discard}
                missing = [v for v in add if not all(canonical(v) in existing for existing in values)]
                if not missing and not any(omit & existing for existing in values):
                    continue
                drop = omit | {canonical(v) for v in missing}
                for item in ast.walk(node):
                    if isinstance(item, ast.List):
                        item.elts = [v for v in item.elts if not (
                            isinstance(v, ast.Constant) and isinstance(v.value, str) and canonical(v.value) in drop)]
                result = (ast.unparse(node) if any(drop & existing for existing in values)
                          else source.text(original) if original is not None else '[]')
                if missing:
                    result = json.dumps(missing) + ' + ' + result if original is not None else json.dumps(missing)
                if original is not None:
                    edits.append((*source.span(original), result))
                else:
                    end = source.span(rule)[1] - 1
                    edits.append((end, end, f'\n    {field} = {result},\n'))
        if found != {n for pkg, n in DEPENDENCIES if pkg == package}:
            raise ValueError(f'{package}: missing measured dependency target')
        if edits:
            path.write_text(source.rewrite(edits))
