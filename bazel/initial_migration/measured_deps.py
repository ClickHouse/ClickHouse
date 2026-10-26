"""Assign the dependencies verified by the compiler and linker reports."""

import ast
from collections import Counter
import json

from initial_migration.first_party_packages import Build, CPUS, attributes, call_name, canonical_dependency
from initial_migration.factor_build_selections import list_parts
from initial_migration.library_runtime import HEADERS as LLVM_RUNTIME_HEADERS
from initial_migration.prune_inherited_headers import inherited_source_headers
from initial_migration.web_resources import constant_defs, constant_dict_field, eval_node

GLIBC_COMPATIBILITY = '//base/glibc-compatibility'
UNWIND = '@clickhouse_llvm_project//:unwind'
TZDATA = '@clickhouse_cctz//:tzdata'
SPARSEHASH = '@clickhouse_sparsehash_c11//:sparsehash_c11'
MINIZIP = '@clickhouse_minizip_ng//:_minizip'
ARRAY_LENGTH = '//src/Functions/array:headers'
FUNCTION_REGISTRATION = '//src/Functions:clickhouse_functions_obj__compile_0'
UNIT_TEST_FUNCTION_DEPS = [
    FUNCTION_REGISTRATION, '//src/Access/Common:headers', '//src/Common/ZooKeeper:headers',
    '//src/Functions:headers', '//src/Parsers:headers',
]

# Key: (package or root repository, rule). Values: (unused dependencies, private dependencies, public dependencies).
DEPENDENCIES = {
    ('@clickhouse_arrow', '_arrow__compile_0'): ([], ['@clickhouse_boost//:boost_headers_no_openssl'], []),
    ('@clickhouse_aws', '_aws'): ([
        '@clickhouse_aws_c_auth//:aws_c_auth',
        '@clickhouse_aws_c_cal//:aws_c_cal',
        '@clickhouse_aws_c_event_stream//:aws_c_event_stream',
        '@clickhouse_aws_c_common//:aws_c_common',
        '@clickhouse_aws_c_io//:aws_c_io',
        '@clickhouse_aws_c_http//:aws_c_http',
    ], [], []),
    ('@clickhouse_azure', '_azure_sdk'): (
        ['@clickhouse//contrib/libxml2-cmake:libxml2_config_headers'], ['@clickhouse_libxml2//:_libxml2'], []),
    ('@clickhouse_flatbuffers', 'flatc'): (list(LLVM_RUNTIME_HEADERS), [], []),
    ('@clickhouse_grpc', 'grpc_cpp_plugin'): ([
        '@clickhouse_abseil_cpp//:absl_examine_stack',
        '@clickhouse_abseil_cpp//:absl_flags_private_handle_accessor',
        '@clickhouse_abseil_cpp//:absl_flags_program_name',
        '@clickhouse_abseil_cpp//:absl_graphcycles_internal',
        '@clickhouse_abseil_cpp//:absl_log_internal_fnmatch',
        '@clickhouse_abseil_cpp//:absl_log_internal_format',
        '@clickhouse_abseil_cpp//:absl_log_internal_globals',
        '@clickhouse_abseil_cpp//:absl_malloc_internal',
        '@clickhouse_abseil_cpp//:absl_stacktrace',
        '@clickhouse_abseil_cpp//:absl_symbolize',
    ], [], []),
    ('@clickhouse_grpc', 'grpc_plugin_support'): (['@clickhouse_google_protobuf//:_libprotoc'], [], []),
    ('@clickhouse_isa_l', '_isal__compile_0'): (list(LLVM_RUNTIME_HEADERS), [], []),
    ('@clickhouse_libhdfs3', '_hdfs3__compile_0'): ([], ['@clickhouse_google_protobuf//:_libprotobuf'], []),
    ('@clickhouse_zstd', '_zstd'): ([], [':_zstd__compile_1'], []),
    ('src', 'headers'): ([
        '@clickhouse_double_conversion//:_double-conversion',
        '@clickhouse_mongo_cxx_driver//:_bsoncxx', '@clickhouse_mongo_cxx_driver//:_mongocxx',
    ], [], []),
    ('src', 'clickhouse_source_headers'): ([
        '//base:common', '//src/Common', '//src/Parsers:headers',
        '@clickhouse_mongo_cxx_driver//:_bsoncxx', '@clickhouse_mongo_cxx_driver//:_mongocxx', ARRAY_LENGTH,
    ], [], []),
    ('src', 'clickhouse_common_io'): ([MINIZIP], [], []),
    ('src/AggregateFunctions', 'clickhouse_aggregate_functions'): ([MINIZIP], [], []),
    ('src/Dictionaries', 'clickhouse_dictionaries'): ([MINIZIP], [], []),
    ('src/Storages/System', 'clickhouse_storages_system'): ([MINIZIP], [], []),
    ('src', 'dbms__compile_4'): ([ARRAY_LENGTH], [], []),
    ('src/Functions', 'clickhouse_functions_obj__compile_2'): ([ARRAY_LENGTH], [], []),
    ('base/readpassphrase', 'readpassphrase'): ([UNWIND], [], []),
    ('base/poco/Foundation', '_poco_foundation'): ([], [UNWIND], []),
    ('base/widechar_width', 'widechar_width'): ([UNWIND], [], []),
    ('contrib/bech32', '_bech32'): ([GLIBC_COMPATIBILITY], [], []),
    ('contrib/cityhash102', '_cityhash'): ([GLIBC_COMPATIBILITY], [], []),
    ('contrib/consistent-hashing', '_consistent_hashing'): ([GLIBC_COMPATIBILITY], [], []),
    ('contrib/libfarmhash', '_farmhash'): ([GLIBC_COMPATIBILITY, UNWIND], [], []),
    ('contrib/libmetrohash', '_metrohash'): ([GLIBC_COMPATIBILITY, UNWIND], [], []),
    ('contrib/murmurhash', '_murmurhash'): ([GLIBC_COMPATIBILITY, UNWIND], [], []),
    ('programs', 'clickhouse__compile_0'): (['//base:harmful'], [], []),
    ('programs/keeper', 'clickhouse-keeper-lib'): ([], ['//src/Dictionaries:clickhouse_dictionaries'], []),
    ('src/Daemon', 'headers'): (['//base/poco/XML:_poco_xml'], [], []),
    ('src/Dictionaries', 'headers'): (
        [SPARSEHASH, '//src/Functions:cast_api', '//src:core_defines', '//src/Common:shared_mutex'], [], []),
    ('src/Parsers', 'headers'): (['//base/poco/JSON:_poco_json'], [], []),
    ('src/TableFunctions', 'headers'): (['//base:common'], [], []),
    ('src/Access/Common', 'clickhouse_common_access'): ([TZDATA], [], []),
    ('src/AggregateFunctions', 'clickhouse_aggregate_functions__compile_1'): ([
        '//base:pcg_random', '//bazel:clickhouse_config_headers', '@clickhouse_boost//:boost_headers_no_openssl',
        '@clickhouse_croaring//:_roaring__compile_0', '@clickhouse_datasketches_cpp//:datasketches_cpp',
    ], ['//src:headers'], []),
    ('src/Functions/URL', 'clickhouse_functions_url'): ([
        MINIZIP,
        '@clickhouse_mongo_c_driver//:_libbson',
        '@clickhouse_arrow//:_orc',
    ], [], []),
    ('src/Functions/URL', 'clickhouse_functions_url__compile_1'): ([
        '@clickhouse_llvm_project//:LLVMBitReader', '@clickhouse_llvm_project//:LLVMBitstreamReader',
        '@clickhouse_llvm_project//:LLVMSupport', GLIBC_COMPATIBILITY,
    ], [], []),
    **{('src/Common', name): (['//src:headers'], [], [])
       for name in ('clickhouse_malloc', 'clickhouse_new_delete')},
    ('src/Common', 'Common'): (['@clickhouse_mongo_cxx_driver//:_mongocxx'], [], []),
    ('src/Common/mysqlxx', 'mysqlxx'): ([TZDATA], ['//src/Common'], []),
    ('src/Common/benchmarks', 'benchmark_stack_trace_objects'): ([TZDATA, '@clickhouse_boost//:_boost_context', '//contrib/lz4-cmake:_lz4'], [], []),
    ('src/Common/benchmarks', 'integer_hash_tables_and_hashes_objects'): (['@clickhouse_libbcrypt//:_bcrypt', '@clickhouse_boost//:_boost_context'], [], []),
    ('src/Columns/benchmarks', 'benchmark_compute_hash_into_objects'): (['@clickhouse_icu//:_icudata'], ['//src:headers'], []),
    ('src/Columns/benchmarks', 'column_insert_many_from_objects'): (['@clickhouse_icu//:_icudata'], ['//src:headers'], []),
    ('src/Common/ZooKeeper', 'clickhouse_common_zookeeper_base'): (['@clickhouse_replxx//:_replxx'], [], []),
    ('src/Common/benchmarks', 'orc_string_dictionary_objects'): ([TZDATA, '@clickhouse_jemalloc//:_jemalloc', '@clickhouse_boost//:_boost_context', '//contrib/lz4-cmake:_lz4'], [], []),
    ('src/Common/benchmarks', 'profile_events_objects'): ([TZDATA, '@clickhouse_boost//:_boost_context', '//contrib/lz4-cmake:_lz4'], [], []),
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
    ('src', 'benchmark_runtime_filter_objects'): ([], [
        '@clickhouse_google_benchmark//:google_benchmark', '//bazel:clickhouse_config_headers',
        '@clickhouse_fmtlib//:_fmt', '//base:common', ':dbms', ':dbms__compile_0', ':dbms__compile_1', '//src/Common',
        '//src/Functions:headers', '//src/Common:parser_support_api', '//src:headers', '//src:column_types',
    ], []),
    ('src', 'clickhouse_common_io__compile_0'): ([], [
        TZDATA, '//base:pcg_random', '//src/Common:string_utils',
        '@clickhouse_boost//:boost_headers_no_openssl', '@clickhouse_fmtlib//:_fmt', '@clickhouse_openssl//:crypto',
    ], []),
    ('src', 'dbms__compile_0'): ([
        '//base/widechar_width',
        '@clickhouse_brotli//:_brotli', '@clickhouse_bzip2//:_bzip2', '@clickhouse_xz//:_liblzma',
        '@clickhouse_grpc//:address_sorting', '@clickhouse_mongo_c_driver//:_libbson',
    ], [
        '//bazel:arrow_flatbuffer_headers',
        '//contrib/librdkafka-cmake:rdhttp_poco_transport',
        '@clickhouse_abseil_cpp//:absl_function_ref', '@clickhouse_azure//:_azure_sdk',
        '@clickhouse_flatbuffers//:_flatbuffers',
        '@clickhouse_libdivide//:libdivide', '@clickhouse_libuv//:_uv',
        '@clickhouse_openssl//:crypto', '@clickhouse_openssl//:ssl', '@clickhouse_sparsehash_c11//:sparsehash_c11',
        '@clickhouse_zlib_ng//:_zlib', '@clickhouse_zstd//:_zstd',
        '@lz4//:lz4',
    ], ['//src/Parsers:parser_api', ':column_types', ':core_defines', '//src/Compression:codec_api',
        '@clickhouse_libhdfs3//:_hdfs3', '//src/Common:string_utils']),
    ('src/Functions', 'clickhouse_functions_obj__compile_0'): ([], [TZDATA], []),
    ('src/Functions/array', 'headers'): ([
        '//src:headers', '//base:common', '//src/Common:memory_tracking_api',
        '//src/Common:assert_cast', '//src:with_context', '//src/Common', '//src:column_types',
    ], [], []),
    ('src/Functions/array', 'clickhouse_functions_array__compile_0'): ([], ['//src:dbms__compile_0'], []),
    ('src/Storages/System', 'headers'): (['//bazel:clickhouse_config_headers'], [], []),
    ('src/Storages/System', 'clickhouse_storages_system__compile_1'): (
        [], ['//base:common', '//src:headers', '//src/Common', TZDATA, ':headers'], []),
    ('programs/format', 'clickhouse-format-lib'): ([], [TZDATA], []),
    ('src', 'unit_tests_dbms'): (
        [MINIZIP, '@clickhouse_icu//:_icudata', '@clickhouse_libcpuid//:_cpuid',
         *LLVM_RUNTIME_HEADERS, '@llvm-project//third-party/siphash'], [], [TZDATA]),
}

# Drop direct dependencies unused in both native DWYU reports or already supplied
# by retained public dependencies before and after them in the declared order.
EMBEDDED_OBJECTS = {'clickhouse_common_io_embedded', 'dbms_embedded'}
DBMS_EMBEDDED_UNUSED = {
    '//base/widechar_width',
    '@clickhouse_aws//:_aws__compile_0', '@clickhouse_azure//:_azure_sdk', '@clickhouse_openssl//:crypto',
    '//base:pcg_random', '@clickhouse_brotli//:_brotli', '@clickhouse_bzip2//:_bzip2',
    '@clickhouse_cctz//:_cctz', '@clickhouse_double_conversion//:_double-conversion',
    '@clickhouse_libdeflate//:_libdeflate', '@clickhouse_simdutf//:_simdutf', '@clickhouse_snappy//:_snappy',
    '@clickhouse_xz//:_liblzma', '@clickhouse_zlib_ng//:_zlib', '@clickhouse_zmij//:_zmij', '@lz4//:lz4',
    ARRAY_LENGTH,
    '//base/glibc-compatibility', '//src/Common:cow',
    '@clickhouse_abseil_cpp//:absl_function_ref', '@clickhouse_abseil_cpp//:absl_inlined_vector_internal',
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
}


def embedded_object_edits(build, rule, name, attrs, canonical):
    """Set implementation_deps for the targets that compile ClickHouse's embedded resources."""
    source = build.source
    defs = constant_defs(source.module)
    def values(node, cpu):
        return ast.literal_eval(eval_node(node, defs, cpu))
    if 'hdrs' in attrs or 'textual_hdrs' in attrs:
        raise ValueError(f'{name}: now publishes headers; review its public dependencies')
    def retained(cpu):
        if name == 'clickhouse_common_io_embedded':
            # FrequencyHolder.cpp includes Common/FrequencyHolder.h and its ZstdInflatingReadBuffer needs zstd.h.
            return ['//src/Common', '@clickhouse_zstd//:_zstd']
        unused = {canonical(v) for v in DBMS_EMBEDDED_UNUSED}
        if cpu == 'x86_64':
            unused.add(canonical('@clickhouse_simdcomp//:_simdcomp'))
        labels = {}
        for label in values(attrs.get('deps'), cpu) + values(attrs.get('implementation_deps'), cpu):
            key = canonical(label)
            if key not in unused:
                labels.setdefault(key, label)
        # Keeper's request handlers include config.h; the embedded client's descriptors use Boost.Iostreams.
        headers = ['//bazel:clickhouse_config_headers', '@clickhouse_boost//:boost_headers_no_openssl']
        for header in headers:
            labels.pop(canonical(header), None)
        deps = list(labels.values())
        i = deps.index('//src/Common/ZooKeeper:clickhouse_common_zookeeper_base') + 1
        deps[i:i] = headers
        return deps
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
    sources = [eval_node(attrs['srcs'], defs, cpu) for cpu in CPUS]
    if ast.dump(sources[0]) != ast.dump(sources[1]):
        raise ValueError(f'{name}: X86 and ARM source files differ')
    # Prune inherited literal headers; keep private source globs and their exclusions unchanged.
    parts = list_parts(sources[0])
    if any(not isinstance(part, ast.List) and call_name(part) != 'glob' for part in parts):
        raise ValueError(f'{name}: unexpected embedded source expression')
    inputs = [label for part in parts if isinstance(part, ast.List) for label in ast.literal_eval(part)]
    drop = inherited_source_headers(build, name, inputs, deps, (':headers', ':column_types'))
    if drop:
        for part in parts:
            if isinstance(part, ast.List):
                part.elts = [item for item in part.elts if item.value not in drop]
        kept = [ast.unparse(part) for part in parts if not isinstance(part, ast.List) or part.elts]
        edits.append((*source.span(attrs['srcs']), ' + '.join(kept) or '[]'))
        for item in ast.walk(attrs['srcs']):
            if isinstance(item, ast.Name) and item.id in defs and sum(
                    isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id == item.id
                    for n in ast.walk(source.module)) == 1:
                declaration = defs[item.id]
                edits.append((source.starts[declaration.lineno - 1], source.starts[declaration.end_lineno], ''))
    return edits


# HashedDictionary's sparsehash types and Core/Defines.h are private to the
# three compilers. Only groups 0 and 1 compile CacheDictionary.h's SharedMutex.
for group in range(3):
    key = ('src/Dictionaries', f'clickhouse_dictionaries__compile_{group}')
    private = DEPENDENCIES.setdefault(key, ([], [], []))[1]
    private.extend([SPARSEHASH, *(['//src/Common:shared_mutex'] if group < 2 else []), '//src:core_defines'])


def editable_dependency_lists(build, node, drop, canonical, replacements=None, *, insert_after=None):
    """Keep list constants shared when every reader drops the same dependencies."""
    uses = Counter(n.id for n in ast.walk(build.source.module) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load))
    subscripts = [n for n in ast.walk(build.source.module) if isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name)]
    edits = []

    def all_readers_drop(name):
        if not drop or replacements or insert_after:
            return False
        readers = 0
        for _, rule in build.source.rules():
            attrs = attributes(rule)
            unused = DEPENDENCIES.get((build.package, attrs['name'].value), ([], [], []))[0]
            if drop <= {canonical(label) for label in unused}:
                readers += sum(ref.id == name for field in ('deps', 'implementation_deps') if field in attrs
                               for ref in ast.walk(attrs[field]) if isinstance(ref, ast.Name))
        return readers == uses[name]

    def visit(n, writable):
        if isinstance(n, ast.Name) and n.id in build.constants:
            return visit(build.constants[n.id], writable and (uses[n.id] == 1 or all_readers_drop(n.id)))
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
            labels = [e.value for e in n.elts]
            keep = [(replacements or {}).get(canonical(v), v) for v in labels if canonical(v) not in drop]
            if insert_after and insert_after[0] in keep:
                i = keep.index(insert_after[0]) + 1
                keep[i:i] = insert_after[1]
            if keep != labels:
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


def usearch_implementation_dep(root):
    """USearch supplies dbms's SimSIMD link input at its original position."""
    path = root / 'src/BUILD.bazel'
    build = Build(root, path, (), ())
    rule = next(rule for _, rule in build.source.rules() if attributes(rule)['name'].value == 'dbms__compile_0')
    deps = attributes(rule)['implementation_deps']
    usearch = '@clickhouse_usearch//:usearch'
    simsimd = '@clickhouse_simsimd//:_simsimd'
    defs = constant_defs(build.source.module)
    branches = [ast.literal_eval(eval_node(deps, defs, cpu)) for cpu in CPUS]
    if all(row.count(usearch) == 1 and simsimd not in row for row in branches):
        return
    if not all(row.count(simsimd) == row.count(usearch) == 1 for row in branches):
        raise ValueError('dbms__compile_0: expected one USearch and one SimSIMD dependency')
    edits = editable_dependency_lists(build, deps, {usearch}, lambda label: label, {simsimd: usearch})
    if not edits:
        raise ValueError('dbms__compile_0: cannot replace its private SimSIMD dependency')
    path.write_text(build.source.rewrite(edits))


def private_poco_expat_headers(root):
    """Keep XML/src's shared Expat headers in the C and C++ libraries that include them."""
    path = root / 'base/poco/XML/BUILD.bazel'
    if not path.is_file():
        return
    source = Build(root, path, (), ()).source
    rules = {attributes(rule)['name'].value: rule for _, rule in source.rules()}
    xml, expat = (rules[n] for n in ('_poco_xml', '_poco_xml_expat'))
    attrs = attributes(expat)
    shared = '_EXPAT_INTERNAL_HEADERS'
    if any(isinstance(s, ast.Assign) and s.targets[0].id == shared for s in source.module.body):
        return
    hdrs = attrs['hdrs']
    foundation = '//base/poco/Foundation:_poco_foundation'
    if not (isinstance(hdrs, ast.BinOp) and isinstance(hdrs.op, ast.Add)
            and call_name(hdrs.left) == 'glob' and ast.literal_eval(hdrs.left.args[0]) == ['include/Poco/XML/expat*.h']
            and all(h.startswith('src/') and h.endswith('.h') for h in ast.literal_eval(hdrs.right))
            and ast.literal_eval(attrs['deps']) == [foundation]):
        raise ValueError('Unexpected Poco XML Expat header or dependency declarations')
    first_var = next(s for s in source.module.body if isinstance(s, ast.Assign))
    start = source.span(first_var)[0]
    edits = [(start, start, f'{shared} = {source.text(hdrs.right)}\n\n'),
             (*source.span(hdrs), source.text(hdrs.left))]
    for rule in (xml, expat):
        end = source.span(attributes(rule)['srcs'])[1]
        edits.append((end, end, ' + ' + shared))
    dep = next(kw for kw in expat.keywords if kw.arg == 'deps')
    edits.append((*source.span(dep), f'implementation_deps = ["{foundation}"]'))
    path.write_text(source.rewrite(edits))


def apply(root):
    """Set the declared visibility of each dependency without changing select conditions."""
    private_poco_expat_headers(root)
    # Older CMake imports declare cc_library wrappers at these labels instead of aliases.
    aliases = {}
    for pkg in ('contrib/libxml2-cmake', 'contrib/lz4-cmake'):
        path = root / pkg / 'BUILD.bazel'
        if path.is_file():
            source = Build(root, path, (), ()).source
            edits = []
            for statement, rule in source.rules():
                attrs = attributes(rule)
                name = getattr(attrs.get('name'), 'value', None)
                if pkg == 'contrib/libxml2-cmake' and name == 'libxml2_config_headers':
                    # _libxml2 publishes xmlversion.h and owns its private config.h.
                    kind = call_name(rule)
                    if not ((kind == 'alias' and attrs['actual'].value == '@clickhouse_libxml2//:_libxml2') or
                            (kind == 'cc_library' and set(attrs) == {'name', 'hdrs', 'includes'})):
                        raise ValueError('Unexpected libxml2 configuration-header target')
                    edits.append((*source.span(statement), ''))
                elif call_name(rule) == 'alias' and isinstance(attrs['actual'], ast.Constant):
                    aliases[canonical_dependency(name, pkg, '@clickhouse')] = attrs['actual'].value
            if edits:
                path.write_text(source.rewrite(edits).rstrip() + '\n')
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
            if package == 'src' and name in EMBEDDED_OBJECTS:
                edits.extend(embedded_object_edits(build, rule, name, attrs, canonical))
                embedded.add(name)
                continue
            if (package, name) in {('src/Functions', 'clickhouse_functions_obj'),
                                   ('src/Functions/array', 'clickhouse_functions_array')}:
                # Keep Minizip's LinkerInput at its original position in both Functions libraries.
                link = ':minizip_link' if package == 'src/Functions' else '//src/Functions:minizip_link'
                replacements = {MINIZIP: link}
                if package == 'src/Functions' and not any(
                        attributes(other)['name'].value == 'minizip_link' for _, other in source.rules()):
                    start = source.span(rule)[0]
                    edits.append((start, start,
                                  'cc_library(name = "minizip_link", implementation_deps = [' +
                                  json.dumps(MINIZIP) + '], visibility = ["//src/Functions/array:__pkg__"])\n\n'))
                direct = editable_dependency_lists(build, attrs['deps'], set(), canonical, replacements)
                if direct is None:
                    raise ValueError(f'{name}: cannot replace the Minizip dependency in place')
                edits.extend(direct)
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
                if (package, name, field) == ('src', 'benchmark_runtime_filter_objects', 'deps'):
                    # CMake links dbms to the optional executable. The seven benchmark
                    # objects have no public headers and consume the private dependencies above.
                    edits.append((*source.span(original), '[]'))
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
                insert_after = None
                if (package, name, field) == ('src', 'clickhouse_common_io__compile_0', 'implementation_deps'):
                    # Keep widechar_width, Minizip and clickhouse_common_config after FrequencyHolder.o in unit_tests_dbms.
                    anchor = ':clickhouse_common_io_embedded'
                    following = ['//base/widechar_width', MINIZIP, '//src/Common/Config:clickhouse_common_config']
                    if not all({canonical(v) for v in [anchor, *following]} <= row for row in values):
                        raise ValueError(f'{name}: expected embedded, Minizip and configuration dependencies')
                    omit.update(canonical(v) for v in following)
                    insert_after = (anchor, following)
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
                        # gtest_interval_tree.cpp links IntervalTree.cpp's explicit instantiations.
                        interval_tree = '//src/Common:interval_tree'
                        ordered = [interval_tree if dep == ':clickhouse_common_io__compile_2' else dep
                                   for dep in ordered]
                        if (interval_tree not in ordered and
                                '//src/Common:tests/gtest_interval_tree.cpp' in build.values(attrs.get('srcs'), cpu)):
                            # 9cd6b1b placed IntervalTree between the Poco objects and ThreadFuzzer.
                            following = ':clickhouse_common_io__compile_3'
                            predecessors = (
                                '//base/poco/NetSSL_OpenSSL:_poco_net_ssl__compile_1',
                                '//base/poco/NetSSL_OpenSSL:secure_socket',
                                '//base/poco/Foundation:thread_pool',
                                '@clickhouse_heks//:heks',
                            )
                            i = (ordered.index(following) if following in ordered else
                                 max((i + 1 for i, dep in enumerate(ordered) if dep in predecessors),
                                     default=len(ordered)))
                            ordered.insert(i, interval_tree)
                        # Storage links its gRPC objects before its REST objects in unit_tests_dbms.
                        storage = '@clickhouse_google_cloud_cpp//:google_cloud_cpp_storage'
                        rest = '@clickhouse_google_cloud_cpp//:google_cloud_cpp_rest_internal'
                        if storage in ordered and rest in ordered:
                            ordered.remove(rest)
                            ordered.insert(ordered.index(storage) + 1, rest)
                        i = ordered.index('//src/Functions:clickhouse_functions_obj')
                        ordered[i:i] = UNIT_TEST_FUNCTION_DEPS
                        branches['@platforms//cpu:' + cpu] = ordered
                    if any(branches['@platforms//cpu:' + cpu] != build.values(node, cpu) for cpu in CPUS):
                        # Keep factored lists when only unused labels were removed.
                        direct = editable_dependency_lists(build, original, omit, canonical)
                        if direct is not None and all(branches['@platforms//cpu:' + cpu] ==
                                [v for v in build.values(node, cpu) if canonical(v) not in omit] for cpu in CPUS):
                            edits.extend(direct)
                        else:
                            edits.append((*source.span(original), 'select(' + json.dumps(branches) + ')'))
                    continue
                if not missing and not any(omit & existing for existing in values):
                    continue
                if (package, name, field) == ('base/poco/Foundation', '_poco_foundation', 'implementation_deps'):
                    # Keep libunwind immediately after PCRE in the ARM unit_tests_dbms link.
                    pcre, = [v for v in original.elts if getattr(v, 'value', None) == ':_poco_foundation_pcre']
                    end = source.span(pcre)[1]
                    edits.append((end, end, ', ' + json.dumps(UNWIND)))
                    continue
                drop = omit | {canonical(v) for v in missing}
                if (package, name, field) == ('programs', 'clickhouse__compile_0', 'deps'):
                    # The final CMake link rule owns the x86-only harmful archive.
                    remaining = [[v for v in build.values(node, cpu) if canonical(v) not in drop] for cpu in CPUS]
                    if remaining[0] == remaining[1]:
                        edits.append((*source.span(original), json.dumps(missing + remaining[0])))
                        continue
                if original is not None:
                    direct = editable_dependency_lists(build, original, drop, canonical, insert_after=insert_after)
                    if direct is None and insert_after:
                        raise ValueError(f'{name}: cannot reorder implementation_deps')
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
        if embedded and embedded != EMBEDDED_OBJECTS:
            raise ValueError(f'{package}: incomplete native #embed object rules')
        if found != {n for pkg, n in DEPENDENCIES if pkg == package}:
            raise ValueError(f'{package}: missing measured dependency target')
        if edits:
            path.write_text(source.rewrite(dict.fromkeys(edits)))

    usearch_implementation_dep(root)
