"""Assign the dependencies verified by the compiler and linker reports."""

import ast
from collections import Counter
import json

from initial_migration.first_party_packages import Build, CPUS, attributes, call_name, canonical_dependency
from initial_migration.library_runtime import HEADERS as LLVM_RUNTIME_HEADERS
from initial_migration.prune_inherited_headers import inherited_source_headers
from initial_migration.web_resources import constant_defs, constant_dict_field, eval_node

TZDATA = '@clickhouse_cctz//:tzdata'
FUNCTION_REGISTRATION = '//src/Functions:clickhouse_functions_obj__compile_0'
UNIT_TEST_FUNCTION_DEPS = [
    FUNCTION_REGISTRATION, '//src/Access/Common:headers', '//src/Common/ZooKeeper:headers',
    '//src/Functions:headers', '//src/Parsers:headers',
]

# Key: (package or root repository, rule). Values: (unused dependencies, private dependencies, public dependencies).
DEPENDENCIES = {
    ('@clickhouse_azure', '_azure_sdk'): (['@clickhouse//contrib/libxml2-cmake:libxml2_config_headers'], [], []),
    ('@clickhouse_flatbuffers', 'flatc'): (list(LLVM_RUNTIME_HEADERS), [], []),
    ('programs', 'clickhouse__compile_0'): (['//base:harmful'], [], []),
    ('src/Access/Common', 'clickhouse_common_access'): ([TZDATA], [], []),
    ('src/Common/mysqlxx', 'mysqlxx'): ([TZDATA], ['//src/Common'], []),
    ('src/Common/benchmarks', 'benchmark_stack_trace_objects'): ([TZDATA], [], []),
    ('src/Common/benchmarks', 'integer_hash_tables_and_hashes_objects'): (['@clickhouse_libbcrypt//:_bcrypt'], [], []),
    ('src/Columns/benchmarks', 'benchmark_compute_hash_into_objects'): (['@clickhouse_icu//:_icudata'], ['//src:headers'], []),
    ('src/Columns/benchmarks', 'column_insert_many_from_objects'): (['@clickhouse_icu//:_icudata'], ['//src:headers'], []),
    ('src/Common/ZooKeeper', 'clickhouse_common_zookeeper_base'): (['@clickhouse_replxx//:_replxx'], [], []),
    ('src/Common/benchmarks', 'orc_string_dictionary_objects'): ([TZDATA, '@clickhouse_jemalloc//:_jemalloc'], [], []),
    ('src/Common/benchmarks', 'profile_events_objects'): ([TZDATA], [], []),
    ('src/Common/benchmarks', 'benchmark_radix_sort_objects'): (
        ['@clickhouse_icu//:_icudata'], ['//src:headers', '//src/Functions:headers'], []),
    ('src/Common/benchmarks', 'wrap_in_nullable_objects'): (
        ['@clickhouse_icu//:_icudata'], ['//src:headers', '//src/Functions:headers'], []),
    ('src/Functions/GatherUtils', 'clickhouse_functions_gatherutils'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src/Loggers', 'loggers'): (['@clickhouse_icu//:_icudata'], ['//src/Common'], []),
    ('src/Functions/JSONPath', 'clickhouse_functions_jsonpath'): ([], ['//src/Parsers:headers'], ['//src/Parsers:parser_api']),
    ('src/Functions/TimeSeries', 'clickhouse_functions_time_series'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src/Storages/benchmarks', 'benchmark_statistics_objects'): (['@clickhouse_icu//:_icudata'], ['//src:headers'], []),
    ('src/TableFunctions', 'clickhouse_table_functions'): (['@clickhouse_icu//:_icudata'], [], []),
    ('src', 'clickhouse_common_io__compile_0'): ([], [
        TZDATA, '//base:pcg_random', '//src/Common:current_metrics', '//src/Common:string_utils',
        '@clickhouse_boost//:boost_headers_no_openssl', '@clickhouse_fmtlib//:_fmt', '@clickhouse_openssl//:crypto',
    ], []),
    ('src', 'dbms__compile_0'): ([
        '@clickhouse_brotli//:_brotli', '@clickhouse_bzip2//:_bzip2', '@clickhouse_xz//:_liblzma',
        '@clickhouse_grpc//:address_sorting', '@clickhouse_mongo_c_driver//:_libbson',
    ], [
        '//bazel:arrow_flatbuffer_headers', '//contrib/cityhash102:_cityhash',
        '//contrib/librdkafka-cmake:rdhttp_poco_transport', '//src/Access/Common:headers',
        '@clickhouse_abseil_cpp//:absl_function_ref', '@clickhouse_azure//:_azure_sdk',
        '@clickhouse_flatbuffers//:_flatbuffers', '@clickhouse_google_protobuf//:_libprotobuf',
        '@clickhouse_libdivide//:libdivide', '@clickhouse_libuv//:_uv', '@clickhouse_mongo_cxx_driver//:_mongocxx',
        '@clickhouse_openssl//:crypto', '@clickhouse_openssl//:ssl', '@clickhouse_sparsehash_c11//:sparsehash_c11',
        '@clickhouse_sqlite_amalgamation//:_sqlite', '@clickhouse_zlib_ng//:_zlib', '@clickhouse_zstd//:_zstd',
    ], ['//src/Parsers:parser_api', ':column_types', ':core_defines', '//src/Compression:codec_api',
        '@clickhouse_libhdfs3//:_hdfs3', '//src/Common:string_utils']),
    ('src/Functions', 'clickhouse_functions_obj__compile_0'): ([], [TZDATA], []),
    ('src/Functions/array', 'clickhouse_functions_array__compile_0'): ([], ['//src:dbms__compile_0'], []),
    ('src/Storages/System', 'headers'): (['//bazel:clickhouse_config_headers'], [], []),
    ('src/Storages/System', 'clickhouse_storages_system__compile_1'): (
        [], ['//base:common', '//src:headers', '//src/Common', TZDATA, ':headers'], []),
    ('programs/format', 'clickhouse-format-lib'): ([], [TZDATA], []),
    ('src', 'unit_tests_dbms'): (['@clickhouse_icu//:_icudata', '@clickhouse_libcpuid//:_cpuid'], [], [TZDATA]),
}

# Drop direct dependencies unused in both native DWYU reports or already supplied
# by retained public dependencies before and after them in the declared order.
_EMBEDDED_UNUSED = {
    '@clickhouse_aws//:_aws__compile_0', '@clickhouse_azure//:_azure_sdk', '@clickhouse_openssl//:crypto',
    '//base:pcg_random', '@clickhouse_brotli//:_brotli', '@clickhouse_bzip2//:_bzip2',
    '@clickhouse_cctz//:_cctz', '@clickhouse_double_conversion//:_double-conversion',
    '@clickhouse_libdeflate//:_libdeflate', '@clickhouse_simdutf//:_simdutf', '@clickhouse_snappy//:_snappy',
    '@clickhouse_xz//:_liblzma', '@clickhouse_zlib_ng//:_zlib', '@clickhouse_zmij//:_zmij', '@lz4//:lz4',
}
EMBEDDED_UNUSED = {
    'clickhouse_common_io_embedded': _EMBEDDED_UNUSED | {
        '@clickhouse_libssh//:_ssh', '@clickhouse_silk//:_silk_fibers',
        '//bazel:clickhouse_config_headers', '@clickhouse_abseil_cpp//:absl_base_config',
        '@clickhouse_boost//:boost_headers_no_openssl', '@clickhouse_crc32c//:_crc32c',
        '@clickhouse_fast_float//:fast_float', '@clickhouse_heks//:heks', '@clickhouse_jemalloc//:_jemalloc',
        '@llvm-project//libunwind:headers',
    },
    'dbms_embedded': _EMBEDDED_UNUSED | {
        '//base/glibc-compatibility', '@clickhouse_abseil_cpp//:absl_inlined_vector_internal',
        '@clickhouse_abseil_cpp//:absl_raw_hash_set', '@clickhouse_abseil_cpp//:absl_strings',
        '@clickhouse_abseil_cpp//:absl_synchronization', '@clickhouse_arrow//:_arrow__compile_0',
        '@clickhouse_aws_c_mqtt//:aws_c_mqtt', '@clickhouse_croaring//:_roaring',
        '@clickhouse_google_protobuf//:_libprotobuf', '@clickhouse_hive_metastore//:_hivemetastore',
        '@clickhouse_librdkafka//:_rdkafka__compile_0', '@clickhouse_llvm_project//:LLVMSupport',
        '@clickhouse_mongo_cxx_driver//:_bsoncxx', '@clickhouse_mongo_cxx_driver//:_mongocxx',
        '@clickhouse_openssl//:ssl', '@clickhouse_thrift//:_thrift', '@clickhouse_zstd//:_zstd',
        '@lz4//:lz4_frame',
        '//base/glibc-rseq:glibc-rseq', '@clickhouse_abseil_cpp//:absl_log_severity',
        '@clickhouse_capnproto//:_kj', '@clickhouse_datasketches_cpp//:datasketches_cpp',
        '@clickhouse_flatbuffers//:_flatbuffers', '@clickhouse_grpc//:address_sorting',
        '@clickhouse_lemmagen_c//:_lemmagen', '@clickhouse_libbcrypt//:_bcrypt',
        '@clickhouse_libdivide//:libdivide', '@clickhouse_libfiu//:_libfiu',
        '@clickhouse_libstemmer_c//:_stemmer', '@clickhouse_liburing//:_liburing', '@clickhouse_libuv//:_uv',
        '@clickhouse_llvm_project//:llvm_headers', '@clickhouse_mecab//:_mecab',
        '@clickhouse_mongo_c_driver//:_libbson', '@clickhouse_msgpack_c//:msgpack_c',
        '@clickhouse_openldap//:_api', '@clickhouse_openldap//:_lber', '@clickhouse_sha3iuf//:_sha3iuf',
        '@clickhouse_simdjson//:_simdjson', '@clickhouse_simsimd//:_simsimd',
        '@clickhouse_sparsehash_c11//:sparsehash_c11', '@clickhouse_sqlite_amalgamation//:_sqlite',
        '@clickhouse_ulid_c//:_ulid', '@clickhouse_usearch//:usearch',
        '@clickhouse_vectorscan//:_vectorscan__compile_0', '@clickhouse_wordnet_blast//:_wnb',
        '@clickhouse_xxhash//:xxHash',
    },
}


def embedded_object_edits(build, rule, name, attrs, canonical):
    """Keep the declared link order when making the six headerless objects' dependencies private."""
    source = build.source
    defs = constant_defs(source.module)
    def values(node, cpu):
        return ast.literal_eval(eval_node(node, defs, cpu))
    if 'hdrs' in attrs or 'textual_hdrs' in attrs:
        raise ValueError(f'{name}: now publishes headers; review its public dependencies')
    def retained(cpu):
        unused = {canonical(v) for v in EMBEDDED_UNUSED[name]}
        if name == 'dbms_embedded' and cpu == 'x86_64':
            unused.add(canonical('@clickhouse_simdcomp//:_simdcomp'))
        labels = {}
        for label in values(attrs.get('deps'), cpu) + values(attrs.get('implementation_deps'), cpu):
            key = canonical(label)
            if key not in unused:
                labels.setdefault(key, label)
        return list(labels.values())
    deps = retained(CPUS[0])
    if deps != retained(CPUS[1]):
        raise ValueError(f'{name}: X86 and ARM dependencies differ')
    edits = []
    if 'deps' in attrs or values(attrs.get('implementation_deps'), CPUS[0]) != deps:
        parts = [json.dumps(deps)]
        group = values(defs['_PROTO_LIBRARIES'].value, CPUS[0]) if '_PROTO_LIBRARIES' in defs else []
        if group and group[0] in deps:
            i = deps.index(group[0])
            if deps[i:i + len(group)] == group:
                parts = ([json.dumps(deps[:i])] if i else []) + ['_PROTO_LIBRARIES']
                if deps[i + len(group):]:
                    parts.append(json.dumps(deps[i + len(group):]))
        edits.append((*source.span(attrs['implementation_deps']), ' + '.join(parts)))
        for k in rule.keywords:
            if k.arg == 'deps':
                edits.append((source.starts[k.lineno - 1], source.starts[k.end_lineno], ''))
    inputs = values(attrs['srcs'], CPUS[0])
    if inputs != values(attrs['srcs'], CPUS[1]):
        raise ValueError(f'{name}: X86 and ARM source files differ')
    drop = inherited_source_headers(build, name, inputs, deps, (':headers', ':column_types'))
    if drop:
        edits.append((*source.span(attrs['srcs']), json.dumps([v for v in inputs if v not in drop])))
        for item in ast.walk(attrs['srcs']):
            if isinstance(item, ast.Name) and item.id in defs and sum(
                    isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id == item.id
                    for n in ast.walk(source.module)) == 1:
                declaration = defs[item.id]
                edits.append((source.starts[declaration.lineno - 1], source.starts[declaration.end_lineno], ''))
    return edits


def editable_dependency_lists(build, node, drop, canonical):
    """Remove labels at their declaration if every changed list has one consumer."""
    uses = Counter(n.id for n in ast.walk(build.source.module) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load))
    subscripts = [n for n in ast.walk(build.source.module) if isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name)]
    edits = []

    def visit(n, writable):
        if isinstance(n, ast.Name) and n.id in build.constants:
            return visit(build.constants[n.id], writable and uses[n.id] == 1)
        if isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name):
            value = constant_dict_field(n, build.constants)
            if value is None:
                return False
            refs = [s for s in subscripts if s.value.id == n.value.id]
            single = (len(refs) == uses[n.value.id] and
                      all(isinstance(s.slice, ast.Constant) for s in refs) and
                      sum(s.slice.value == n.slice.value for s in refs) == 1)
            return visit(value, writable and single)
        if isinstance(n, ast.List) and all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in n.elts):
            keep = [e.value for e in n.elts if canonical(e.value) not in drop]
            if len(keep) != len(n.elts):
                if not writable:
                    return False
                edits.append((*build.source.span(n), json.dumps(keep)))
            return True
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Add):
            return visit(n.left, writable) and visit(n.right, writable)
        if isinstance(n, ast.Call) and getattr(n.func, 'id', '') == 'select' and isinstance(n.args[0], ast.Dict):
            return all(visit(branch, writable) for branch in n.args[0].values)
        return False

    return edits if visit(node, True) else None


def apply(root):
    """Set the declared visibility of each dependency without changing select conditions."""
    # Older CMake imports declare cc_library wrappers at these labels instead of aliases.
    aliases = {}
    for pkg in ('contrib/libxml2-cmake', 'contrib/lz4-cmake'):
        path = root / pkg / 'BUILD.bazel'
        if path.is_file():
            for _, rule in Build(root, path, (), ()).source.rules():
                attrs = attributes(rule)
                if call_name(rule) == 'alias' and isinstance(attrs['actual'], ast.Constant):
                    aliases[canonical_dependency(attrs['name'].value, pkg, '@clickhouse')] = attrs['actual'].value
    for package in sorted({package for package, _ in DEPENDENCIES}):
        repository = package if package.startswith('@') else '@clickhouse'
        local_package = '' if package.startswith('@') else package
        path = (root / 'bazel/dependencies' / (repository[1:] + '.BUILD.bazel')
                if package.startswith('@') else root / package / 'BUILD.bazel')
        build = Build(root, path, (), ())
        source = build.source
        edits = []
        found = set()
        embedded = set()
        canonical = lambda label: canonical_dependency(label, local_package, repository)
        for _, rule in source.rules():
            attrs = {k.arg: k.value for k in rule.keywords}
            name = getattr(attrs.get('name'), 'value', None)
            if package == 'src' and name in EMBEDDED_UNUSED:
                edits.extend(embedded_object_edits(build, rule, name, attrs, canonical))
                embedded.add(name)
                continue
            spec = DEPENDENCIES.get((package, name))
            if spec is None:
                continue
            found.add(name)
            unused, private, public = spec
            if package == 'src' and name in ('dbms__compile_0', 'clickhouse_common_io__compile_0'):
                # The CMake import predates some header owners. Only move existing edges.
                defs = constant_defs(source.module)
                present = [{canonical(v) for field in ('deps', 'implementation_deps')
                            for v in ast.literal_eval(eval_node(attrs.get(field), defs, cpu))} for cpu in CPUS]
                for label in private + public:
                    if (canonical(label) in present[0]) != (canonical(label) in present[1]):
                        raise ValueError(f'{name}: measured dependency differs by CPU: {label}')
                private = [v for v in private if v == TZDATA or canonical(v) in present[0]]
                public = [v for v in public if canonical(v) in present[0]]
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
                        field = constant_dict_field(sub, {})
                        if field is not None:
                            return field
                        raise ValueError(f'{package}:{name}: unsupported dependency expression {ast.unparse(sub)}')
                node = Field().visit(node)
                values = [{canonical(v) for v in build.values(node, cpu)} for cpu in CPUS]
                omit = {canonical(v) for v in discard}
                if (package, name) == ('@clickhouse_azure', '_azure_sdk'):
                    omit = {alias for alias in omit if all(aliases.get(alias) in existing for existing in values)}
                missing = [v for v in add if not all(canonical(v) in existing for existing in values)]
                if (package, name, field) == ('src', 'unit_tests_dbms', 'deps'):
                    # These owners are already reachable through clickhouse_functions_obj.
                    # Keep their LinkerInputs in the same order when declaring them directly.
                    branches = {}
                    for cpu in CPUS:
                        before = build.values(node, cpu)
                        drop = omit | {canonical(v) for v in missing + UNIT_TEST_FUNCTION_DEPS}
                        if cpu == 'x86_64':
                            drop.add(canonical('@clickhouse_fastops//:_fastops'))
                        ordered = missing + [v for v in before if canonical(v) not in drop]
                        lz4_frame = aliases.get(canonical('//contrib/lz4-cmake:_lz4'))
                        if lz4_frame in ordered:
                            # Preserve the first LZ4 dependency for the ordered direct module maps.
                            ordered = list(dict.fromkeys(
                                lz4_frame if dep == '//contrib/lz4-cmake:_lz4' else dep for dep in ordered))
                        i = ordered.index('//src/Functions:clickhouse_functions_obj')
                        ordered[i:i] = UNIT_TEST_FUNCTION_DEPS
                        branches['@platforms//cpu:' + cpu] = ordered
                    if any(branches['@platforms//cpu:' + cpu] != build.values(node, cpu) for cpu in CPUS):
                        edits.append((*source.span(original), 'select(' + json.dumps(branches) + ')'))
                    continue
                if not missing and not any(omit & existing for existing in values):
                    continue
                drop = omit | {canonical(v) for v in missing}
                if (package, name, field) == ('programs', 'clickhouse__compile_0', 'deps'):
                    # The final CMake link rule owns the x86-only harmful archive.
                    remaining = [[v for v in build.values(node, cpu) if canonical(v) not in drop] for cpu in CPUS]
                    if remaining[0] == remaining[1]:
                        edits.append((*source.span(original), json.dumps(missing + remaining[0])))
                        continue
                if original is not None:
                    direct = editable_dependency_lists(build, original, drop, canonical)
                    if direct is not None:
                        edits.extend(direct)
                        if missing:
                            start = source.span(original)[0]
                            edits.append((start, start, json.dumps(missing) + ' + '))
                        continue
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
        if embedded and embedded != EMBEDDED_UNUSED.keys():
            raise ValueError(f'{package}: incomplete native #embed object rules')
        if found != {n for pkg, n in DEPENDENCIES if pkg == package}:
            raise ValueError(f'{package}: missing measured dependency target')
        if edits:
            path.write_text(source.rewrite(edits))
