#!/usr/bin/env python3
"""Place native ClickHouse targets in the source directories that own them.

The CMake capture is repository-wide. This is its final BUILD rendering pass,
after external repositories and generated files have been assigned their own
repositories. This pass never edits CMake source files or the CMake link order.
"""
import argparse
import ast
from collections import Counter, defaultdict
from copy import deepcopy
from functools import cache
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, compact
from initial_migration.normalize_cpp_builds import normalize, prove
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT
from initial_migration.simplify_globs import GlobFiles, simplify_globs

CPUS = ('x86_64', 'aarch64')
COMPILERS = {'ch_cc_library', 'ch_cc_test', 'cc_library', 'cc_test', 'cc_binary'}
TUS = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm', '.ASM')
TEXTUAL = ('.inc', '.inl', '.ipp', '.tpp', '.icc', '.def', '.defs', '.th', '.td',
           '.tbl', '.tbi', '.ixx', '.cuh', '.msh', '.txx')
LABEL_FIELDS = {
    'srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps',
    'implementation_deps', 'actual', 'tools', 'data', 'template', 'src', 'refs',
    'static_library', 'shared_library', 'interface_library', 'binary',
}
LOCATION = re.compile(r'\$\((locations?|execpaths?|rootpaths?)\s+([^\s)]+)\)')
MAIN_LABEL = re.compile(r'^(?P<repo>@clickhouse)?//(?P<pkg>[^:]*):(?P<name>[^\s]*)$')
DEPENDENCY_LABEL = re.compile(r'^(?P<repo>@[^/]+)?//(?P<pkg>[^:]*)(?::(?P<name>[^\s]*))?$')


def quoted(value):
    return ast.Constant(value=value)


def expr(value):
    return ast.parse(value, mode='eval').body


def list_expr(values):
    return ast.List(elts=[quoted(value) for value in values], ctx=ast.Load())


def render(node):
    return ast.unparse(ast.fix_missing_locations(node))


def call_name(node):
    return getattr(node.func, 'id', '') if isinstance(node, ast.Call) else ''


def attributes(node):
    return {kw.arg: kw.value for kw in node.keywords}


def logical_name(name):
    return re.sub(r'__(?:compile|runtime_objects)_\d+$', '', name)


def physical(package, value):
    return posixpath.join(package, value) if package else value


def relative_label(package, name, current='', repo=''):
    if package == current and not repo:
        return ':' + name
    if package and posixpath.basename(package) == name:
        return repo + '//' + package
    return repo + '//' + package + ':' + name


def canonical_dependency(value, package, repo):
    match = DEPENDENCY_LABEL.fullmatch(value)
    if match:
        owner = match['repo'] or repo
        pkg = match['pkg']
        name = match['name'] if match['name'] is not None else posixpath.basename(pkg)
        return owner + '//' + pkg + ':' + name
    return repo + '//' + package + ':' + value.removeprefix(':')


# Public declarations used before a library can link clickhouse_parsers.
PARSER_INTERFACE_HEADERS = (
    "CommonParsers.h", "IAST.h", "IASTFormatState.h", "IASTHash.h", "IAST_fwd.h",
    "IParser.h", "IParserBase.h", "IdentifierQuotingStyle.h", "InlineVector.h",
    "Lexer.h", "LiteralEscapingStyle.h", "SyncReplicaMode.h", "TokenIterator.h", "parseQuery.h",
)


def declare_parser_interface(rows):
    targets = {ast.literal_eval(attributes(row.value)["name"]): row.value for row in rows
               if isinstance(row, ast.Expr) and "name" in attributes(row.value)}
    if "parser_api" in targets:
        return

    # CMake lists the public headers among clickhouse_parsers' sources.
    # NativePackages also publishes a glob in :headers. Both targets retain
    # the public headers through :parser_api.
    compiled = targets["clickhouse_parsers"]
    hdrs = attributes(compiled)["hdrs"]
    found = set()
    for items in ast.walk(hdrs):
        if isinstance(items, ast.List):
            public = [item for item in items.elts if isinstance(item, ast.Constant)
                      and item.value in PARSER_INTERFACE_HEADERS]
            found.update(item.value for item in public)
            items.elts = [item for item in items.elts if item not in public]
    if found != set(PARSER_INTERFACE_HEADERS):
        raise ValueError("clickhouse_parsers is missing public headers: " + str(sorted(set(PARSER_INTERFACE_HEADERS) - found)))
    headers = targets["headers"]
    glob = attributes(headers)["hdrs"]
    if call_name(glob) != "glob":
        raise ValueError("expected //src/Parsers:headers.hdrs to be a glob")
    excluded = attributes(glob).get("exclude")
    if excluded is None:
        glob.keywords.append(ast.keyword(arg="exclude", value=list_expr(PARSER_INTERFACE_HEADERS)))
    else:
        glob.keywords = [kw for kw in glob.keywords if kw.arg != "exclude"]
        glob.keywords.append(ast.keyword(arg="exclude", value=list_expr([*ast.literal_eval(excluded), *PARSER_INTERFACE_HEADERS])))
    for target in (compiled, headers):
        deps = next((kw for kw in target.keywords if kw.arg == "deps"), None)
        if deps:
            deps.value = ast.BinOp(left=deps.value, op=ast.Add(), right=list_expr([":parser_api"]))
        else:
            target.keywords.append(ast.keyword(arg="deps", value=list_expr([":parser_api"])))

    # These are the owners at the native package step. publish_base() moves
    # //base/base:headers to //base:base_headers later in generate_build.py.
    rows.append(expr_stmt(
        'ch_cc_library(name="parser_api", hdrs=%s, clickhouse_code=True, strip_include_prefix="/src", '
        'deps=["//base/base:headers", "//contrib/cityhash102:headers", "//src:headers", '
        '"//src/Common:headers", "@clickhouse_boost//:boost_headers_no_openssl"])' % json.dumps(PARSER_INTERFACE_HEADERS)
    ))


def declare_compiled_headers(rows, files, compiler, headers, field="hdrs", *, when_compiling=None):
    """Assign headers to their compiling target. Use srcs for implementation and test headers."""
    globs = GlobFiles(files)

    def files_in(node):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return files_in(node.left) + files_in(node.right)
        if call_name(node) == "glob":
            excluded = attributes(node).get("exclude")
            return list(globs.result(files_in(node.args[0]), files_in(excluded) if excluded else []))
        return ast.literal_eval(node)

    selected = set(files_in(headers))

    class RemoveHeaders(ast.NodeTransformer):
        def visit_List(self, node):
            node.elts = [self.visit(item) for item in node.elts
                         if not isinstance(item, ast.Constant) or not isinstance(item.value, str) or item.value.removeprefix(":") not in selected]
            return node

        def visit_BinOp(self, node):
            self.generic_visit(node)
            if isinstance(node.op, ast.Add):
                for empty, other in ((node.left, node.right), (node.right, node.left)):
                    if isinstance(empty, ast.List) and not empty.elts:
                        return other
            return node

        def visit_Call(self, node):
            if call_name(node) != "glob":
                return self.generic_visit(node)
            excluded = attributes(node).get("exclude")
            included = set(files_in(node))
            removed = included & selected
            if removed:
                if removed == included:
                    return list_expr([])
                if excluded is None:
                    node.keywords.append(ast.keyword(arg="exclude", value=list_expr(sorted(removed))))
                else:
                    values = [*files_in(excluded), *sorted(removed)]
                    next(kw for kw in node.keywords if kw.arg == "exclude").value = list_expr(values)
            return node

    compilers = {ast.literal_eval(attributes(row.value)["name"]): row.value for row in rows
                 if isinstance(row, ast.Expr) and call_name(row.value) in COMPILERS}
    for rule in compilers.values():
        for kw in rule.keywords:
            if kw.arg in {"srcs", "hdrs", "textual_hdrs", "private_hdrs"}:
                kw.value = RemoveHeaders().visit(kw.value)
    names = [compiler] if isinstance(compiler, str) else compiler
    if when_compiling:
        from initial_migration.web_resources import eval_node
        srcs = attributes(compilers[compiler])["srcs"]
        owners = {cpu: compiler if when_compiling in {
            src.removeprefix(":") for src in files_in(eval_node(srcs, {}, cpu))
        } else "headers" for cpu in CPUS}
        names = sorted(set(owners.values()))
    for name in names:
        owner = compilers[name]
        value = deepcopy(headers)
        if when_compiling and len(names) > 1:
            value = expr("select(" + json.dumps({
                "@platforms//cpu:" + cpu: sorted(selected) if target == name else []
                for cpu, target in owners.items()
            }) + ")")
        attribute = next((kw for kw in owner.keywords if kw.arg == field), None)
        if attribute is None:
            owner.keywords.append(ast.keyword(arg=field, value=value))
        elif isinstance(attribute.value, ast.List) and isinstance(value, ast.List):
            attribute.value.elts.extend(value.elts)
        else:
            attribute.value = ast.BinOp(left=attribute.value, op=ast.Add(), right=value)


# Public headers read outside dbms__compile_0 and clickhouse_common_io__compile_0.
# A whole directory or filename family below is public on that compiling target.
DBMS_PUBLIC_HEADERS = {
    "Access": ("DiskAccessStorage.h", "UsersConfigParser.h", "ZooKeeperReplicator.h"),
    "Analyzer": (
        "ColumnTransformers.h", "ConstantNode.h", "FunctionNode.h", "Identifier.h", "JoinNode.h",
        "QueryNode.h", "TableFunctionNode.h", "TableNode.h", "Utils.h",
    ),
    "Backups": (
        "BackupEntryWithChecksumCalculation.h", "BackupFactory.h", "BackupFileInfo.h", "IBackupEntry.h",
    ),
    "BridgeHelper": ("IBridgeHelper.h", "XDBCBridgeHelper.h"),
    "Client": (
        "ClientApplicationBase.h", "ClientBaseHelpers.h", "ConnectionString.h", "PortsProbe.h",
        "ReplxxLineReader.h", "scaleInteractiveDelayByFanout.h",
    ),
    "Columns": ("ColumnBLOB.h", "ColumnNothing.h", "ReverseIndex.h"),
    "Coordination": (
        "FourLetterCommand.h", "KeeperAsynchronousMetrics.h", "KeeperMemNodesStorage.h",
        "LoggerWrapper.h", "ReadBufferFromNuraftBuffer.h", "ZooKeeperDataReader.h",
    ),
    "Coordination/Storage": ("BlockCache.h", "Memtable.h", "NodeRefCache.h", "Sorted*.h"),
    "Core": ("PostgreSQLProtocol.h", "SortCursor.h"),
    "Core/PostgreSQL": ("Connection.h", "ConnectionHolder.h", "PoolWithFailover.h", "Utils.h"),
    "Core/YTsaurus": ("YTsaurusClient.h",),
    "DataTypes": ("DataTypeSet.h", "FieldToDataType.h", "getMostSubtype.h"),
    "DataTypes/Serializations": ("SerializationObjectHelpers.h", "SerializationObjectSharedData.h"),
    "Databases": ("DatabaseOverlay.h", "DatabaseURL.h"),
    "Databases/DataLake": ("HTTPBasedCatalogUtils.h", "HiveCatalog.h"),
    "Databases/MySQL": ("DatabaseMySQL.h",),
    "Disks": (
        "DiskEncryptedTransaction.h", "IDiskTransaction.h", "MetadataStorageWithPathWrapper.h",
        "SingleDiskVolume.h",
    ),
    "Disks/DiskObjectStorage/MetadataStorages": (
        "DiskObjectStorageMetadata.h", "InMemoryRemovalQueue.h", "MetadataOperationsHolder.h",
        "MetadataStorageTransactionState.h", "NormalizedPath.h",
    ),
    "Disks/DiskObjectStorage/MetadataStorages/Local": ("*.h",),
    "Disks/DiskObjectStorage/MetadataStorages/Memory": ("*.h",),
    "Disks/DiskObjectStorage/MetadataStorages/PlainRewritable": ("PlainRewritable*.h",),
    "Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata": ("*.h",),
    "Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions": ("*.h",),
    "Disks/DiskObjectStorage/ObjectStorages/Web": ("WebObjectStorage.h",),
    "Disks/IO": ("CachedOnDiskWriteBufferFromFile.h",),
    "Formats": ("FormatParserSharedResources.h", "FormatSchemaInfo.h", "ProtobufSchemas.h"),
    "Interpreters": (
        "AdaptiveAggregationImpl.h", "ArrayJoinAction.h", "BloomFilterHash.h",
        "CancellationChecker.h", "ContextTimeSeriesTagsCollector.h", "DNSCacheUpdater.h",
        "EmbeddedDictionaries.h", "FunctionSecretArgumentsFinder.h", "InterpreterExplainQuery.h",
        "InterpreterSetQuery.h", "JoinExpressionActions.h", "JoinOperator.h", "MaterializedCTE.h",
        "NormalizeSelectWithUnionQueryVisitor.h", "SecretArgumentsRegistry.h",
        "SelectIntersectExceptQueryVisitor.h", "ServerAsynchronousMetrics.h", "SortedBlocksWriter.h",
        "TableJoin.h", "convertColumnToType.h", "loadMetadata.h",
        "parseColumnsListForTableFunction.h",
    ),
    "Interpreters/ClusterProxy": ("executeQuery.h",),
    "Interpreters/HashJoin": ("HashJoin.h",),
    "Interpreters/WebAssembly": ("WasmTimeRuntime.h",),
    "Planner": ("PlannerContext.h", "PlannerCorrelatedSubqueries.h", "Utils.h"),
    "Processors": ("IAccumulatingTransform.h", "ResizeProcessor.h"),
    "Processors/Executors": ("PushingPipelineExecutor.h",),
    "Processors/Executors/Runtime/V1": ("ExecutionThreadContext.h", "ExecutorTasks.h", "PollingQueue.h"),
    "Processors/Formats": ("IRowInputFormat.h", "RowInputFormatWith*.h"),
    "Processors/Formats/Impl": (
        "ArrowGeoTypes.h", "CHColumnToArrowColumn.h", "JSONEachRowRowInputFormat.h",
    ),
    "Processors/Formats/Impl/Parquet": (
        "Decoding.h", "Prefetcher.h", "ReadCommon.h", "Reader.h", "ThriftUtil.h", "Write.h",
    ),
    "Processors/Merges": ("DistinctSortedTransform.h", "IMergingTransform.h"),
    "Processors/Merges/Algorithms": (
        "DistinctSortedAlgorithm.h", "MergingSortedAlgorithm.h", "RowFilterInfo.h",
    ),
    "Processors/QueryPlan": (
        "ExpressionStep.h", "JoinEstimation.h", "JoinStep.h",
        "QueryPlanStepRegistry.h", "SortingStep.h",
    ),
    "Processors/QueryPlan/Optimizations": ("RelationStatistics.h", "Utils.h"),
    "Processors/QueryPlan/Optimizations/Cascades": (
        "Cost.h", "Group*.h", "ImplementationStrategy.h", "OptimizerContext.h", "Properties.h",
        "Statistics.h", "Task.h",
    ),
    "Processors/Sources": (
        "MongoDBSource.h", "MySQLSource.h", "ObfuscateSource.h", "PostgreSQLSource.h",
        "RemoteSource.h", "YTsaurusSource.h",
    ),
    "Processors/Transforms": (
        "AddingDefaultsTransform.h", "AggregatingTransform.h", "BlockNestedLoopJoin*.h",
        "ChunkRowRange.h", "ColumnGathererTransform.h", "Distinct*.h", "JoiningTransform.h",
        "SortingTransform.h", "getSourceFromASTInsertQuery.h",
    ),
    "QueryPipeline": ("DistributedPlanExecutor.h",),
    "Server": (
        "GRPCServer.h", "HTTPHandlerFactory.h", "HTTPHandlerRequestFilter.h",
        "HTTPRequestHandlerFactoryMain.h", "KeeperHTTPHandlerFactory.h",
        "KeeperHTTPStorageHandler.h", "KeeperTCPHandler*.h", "MySQLHandlerFactory.h",
        "PostgreSQLHandlerFactory.h", "PrometheusRequestHandlerFactory.h", "ProtocolServerAdapter.h",
        "StartupWarnings.h", "TCPHandler*.h", "TLSHandler*.h", "createServer.h",
        "socketBindListen.h", "stopServers.h", "waitServersToFinish.h",
    ),
    "Server/ACME": ("API.h", "Client.h"),
    "Server/ArrowFlight": ("ArrowFlightServer.h", "commandSelector.h"),
    "Server/DistributedQuery": ("*.h",),
    "Server/HTTP": ("HTTPRequestHandlerFactory.h", "HTTPServer.h", "HTTPServerConnectionFactory.h"),
    "Server/IcebergRESTCatalog": ("IcebergRESTCatalogHandlerFactory.h", "IcebergRESTCatalogWarehouse.h"),
    "Server/StatelessWorker": ("StatelessWorkerEndpoint.h",),
    "Storages": (
        "CompressionCodecSelector.h", "ExecutableSettings.h", "GenerateRandomSettings.h",
        "HivePartitioningUtils.h", "RedisCommon.h", "StorageArrowFlight.h", "StorageDictionary.h",
        "StorageExecutable.h", "StorageFilesystem.h", "StorageFuzzJSON.h", "StorageFuzzQuery.h",
        "StorageInput.h", "StorageJoin.h", "StorageLoop.h", "StorageMerge.h", "StorageMergeTree*.h",
        "StorageMongoDB.h", "StorageMySQL.h", "StorageNull.h", "StorageObfuscate.h",
        "StoragePostgreSQL.h", "StoragePrometheusQuery.h", "StorageRedis.h", "StorageSQLite.h",
        "StorageSet.h", "StorageTimeSeries*.h", "StorageValues.h", "StorageXDBC.h",
    ),
    "Storages/ArrowFlight": ("ArrowFlightConnection.h",),
    "Storages/BigQuery": (
        "BigQueryClient.h", "BigQueryConfiguration.h", "BigQuerySchema.h", "StorageBigQuery.h",
    ),
    "Storages/Distributed": ("DistributedSettings.h", "parseRemoteFunctionArguments.h"),
    "Storages/FileLog": ("DirectoryWatcherBase.h", "FileLogDirectoryWatcher.h"),
    "Storages/Hive": ("HiveCommon.h", "HiveFile.h", "HiveSettings.h", "StorageHive.h"),
    "Storages/MergeTree": (
        "*PlainMergeTreeTask.h", "IPostingListCodec.h", "MergeMutateSelectedEntry.h",
        "MergeTreeCleanupThread.h", "MergeTreeDeduplicationLog.h", "MergeTreeIndexConditionText.h",
        "MergeTreeIndexJSONSubcolumnHelper.h", "MergeTreeIndexText.h", "MergeTreeMutationEntry.h",
        "MergedPartOffsets.h", "PostingListBlockCodec.h", "PostingListSegment.h",
        "StorageFromMergeTreeProjection.h", "TextIndexBlockedPositionsCodec.h",
    ),
    "Storages/MergeTree/PatchParts": ("PatchPartsLock.h",),
    "Storages/MergeTree/Streaming/Subscription": ("*.h",),
    "Storages/MergeTree/UniqueKey": (
        "DeleteBitmapCache.h", "DeleteBitmapFileOps.h", "DeleteBitmapStore.h", "UniqueKeyProbe.h",
    ),
    "Storages/MySQL": ("MySQLHelpers.h",),
    "Storages/ObjectStorage": ("StorageObjectStorageCluster.h",),
    "Storages/ObjectStorage/Azure": ("AzureSecretArguments.h",),
    "Storages/ObjectStorage/HDFS": ("HDFSCommon.h", "HDFSErrorWrapper.h", "ReadBufferFromHDFS.h"),
    "Storages/ObjectStorage/S3": ("S3SecretArguments.h",),
    "Storages/ObjectStorage/Web": ("Configuration.h",),
    "Storages/PostgreSQL": ("PostgreSQLSettings.h",),
    "Storages/TimeSeries": ("PrometheusQueryEvaluationSettings.h",),
    "Storages/TimeSeries/PrometheusQueryToSQL": ("Converter.h",),
    "Storages/YTsaurus": ("StorageYTsaurus.h",),
    "WindowFunctions": ("helpers.h",),
}

COMMON_IO_PUBLIC_HEADERS = {
    "IO": (
        "*BufferFromEncryptedFile.h", "Ask.h", "AsynchronousReadBufferFromFile.h",
        "AsynchronousReadBufferFromFileDescriptor.h", "BoundedReadBuffer.h",
        "BufferWithOwnMemoryImpl.h", "CascadeWriteBuffer.h", "Expect404ResponseScope.h",
        "LZMA*Buffer.h", "Libdeflate*.h",
        "LimitSeekableReadBuffer.h", "LocalSourceReader.h", "MMappedFile.h", "MMappedFileCache.h", "MySQL*.h",
        "PackedFilesWriter.h", "PageCacheProvider.h", "PeekableWriteBuffer.h",
        "PipelineReadBuffer.h", "ReadBufferFromFileDecorator.h", "ReadBufferFromFileView.h",
        "ReadPipeline.h", "SilkFiberStreamSocketImpl.h", "SilkSecureFiberStreamSocketImpl.h",
        "SocketPeerClosed.h", "StdIStreamFromMemory.h", "SynchronousReader.h", "TimeoutSetter.h",
        "WriteBufferFromArena.h", "WriteBufferFromFileDecorator.h", "WriteBufferFromS3.h",
        "Zlib*Buffer.h", "ZstdDeflatingWriteBuffer.h", "parseHTTPDate.h", "preadNoWait.h", "writeValidUTF8.h",
    ),
}


def declare_src_compiled_headers(rows, files):
    for compiler, directories in (
        ("dbms__compile_0", DBMS_PUBLIC_HEADERS),
        ("clickhouse_common_io__compile_0", COMMON_IO_PUBLIC_HEADERS),
    ):
        paths = [directory + "/" + name for directory, names in directories.items() for name in names]
        patterns = [p for p in paths if "*" in p]
        literals = list_expr([p for p in paths if "*" not in p])
        headers = ast.BinOp(left=expr("glob(" + json.dumps(patterns) + ")"), op=ast.Add(), right=literals)
        declare_compiled_headers(rows, files, compiler, headers)
    # dbms group 0 publishes IStorage's full interface where it compiles StorageGenerateRandom.
    declare_compiled_headers(rows, files, "dbms__compile_0", list_expr(["Storages/StorageGenerateRandom.h"]),
                             when_compiling="Storages/StorageGenerateRandom.cpp")


def declare_dictionary_private_headers(rows, files):
    # These headers are read only by the listed CMake compile groups.
    for groups, names in (
        ((0,), ("DirectDictionary.h", "FlatDictionary.h")),
        ((1,), (
            "CacheDictionaryStorage.h", "NaiveBayesTrainer.h", "CassandraDictionarySource.h",
            "CassandraHelpers.h", "CassandraSource.h", "ExecutableDictionarySource.h",
            "ExecutablePoolDictionarySource.h", "FileDictionarySource.h", "HTTPDictionarySource.h",
            "IPAddressDictionary.h", "MongoDBDictionarySource.h", "MySQLDictionarySource.h",
            "NullDictionarySource.h", "PolygonDictionary.h", "PolygonDictionaryImplementations.h",
            "PolygonDictionaryUtils.h", "PostgreSQLDictionarySource.h", "RedisDictionarySource.h",
            "RedisSource.h", "RegExpTreeDictionary.h", "XDBCDictionarySource.h",
            "YAMLRegExpTreeDictionarySource.h", "YTsaurusDictionarySource.h", "readInvalidateQuery.h",
        )),
        ((2,), ("HashedArrayDictionary.h",)),
        ((1, 2), ("RangeHashedDictionary.h",)),
        ((0, 1), ("CacheDictionary.h", "CacheDictionaryUpdateQueue.h")),
        ((0, 1, 2), (
            "ClickHouseDictionarySource.h", "DictionaryPipelineExecutor.h",
            "DictionarySourceHelpers.h", "HashedDictionary.h", "HashedDictionaryCollectionTraits.h",
            "HashedDictionaryCollectionType.h", "HashedDictionaryParallelLoader.h",
            "InvalidateQueryResponse.h",
        )),
    ):
        compilers = [f"clickhouse_dictionaries__compile_{index}" for index in groups]
        declare_compiled_headers(rows, files, compilers, list_expr(names), field="srcs")


# Only these dbms compile groups read these headers from other Bazel packages.
DBMS_PRIVATE_PACKAGE_HEADERS = {
    ("src/Common", (0,)): (
        "BSONCXXHelper.h", "CounterInFile.h", "Increment.h", "LockGuardWithStopWatch.h",
        "NamedCollections/NamedCollectionConfiguration.h", "PrimeGenerator.h",
        "Scheduler/Workload/createWorkloadEntityStorage.h", "SystemAllocatedMemoryHolder.h",
        "saturatedWaitDuration.h",
    ),
    ("src/Functions", (0,)): (
        "FunctionTopKFilter.h", "TypeMismatchStrictness.h", "castNestedResult.h", "exists.h",
        "extractKeyValuePairs.h", "generateSnowflakeID.h", "productQuantization.h", "vectorQuantization.h",
        "UserDefined/UserDefinedExecutableFunctionDriver*.h", "UserDefined/UserDefinedSQLObject*.h",
        "UserDefined/UserDefinedSQLFunctionVisitor.h", "UserDefined/UserDefinedWebAssemblyScriptAbi.h",
        "UserDefined/UserDefinedWebAssemblyTypeHelpers.h", "UserDefined/createUserDefinedSQLObjectsStorage.h",
    ),
    ("src/Functions", (1,)): ("FunctionIfBase.h",),
    ("src/Functions", (0, 1)): (
        "FunctionDynamicAdaptor.h", "FunctionVariantAdaptor.h", "if.h", "logical.h", "multiIf.h",
    ),
    ("src/Functions/array", (0,)): ("arrayResize.h", "emptyArrayToSingle.h"),
    ("src/TableFunctions", (0,)): ("TableFunctionView.h",),
}


def declare_dbms_package_headers(rows, inventory):
    private = defaultdict(set)
    labels = defaultdict(list)
    for (package, groups), patterns in DBMS_PRIVATE_PACKAGE_HEADERS.items():
        files = inventory(package)
        globs = GlobFiles(files)
        missing = [pattern for pattern in patterns if not globs.matching(pattern)]
        if missing:
            raise ValueError(f"{package}: missing private dbms headers: {missing}")
        names = globs.result(patterns, ())
        private[package].update(names)
        declare_compiled_headers(rows[package], files, (), list_expr(names))
        labels[groups].extend(relative_label(package, name, "src") for name in names)
    for groups, paths in labels.items():
        compilers = [f"dbms__compile_{index}" for index in groups]
        declare_compiled_headers(rows["src"], inventory("src"), compilers, list_expr(paths), field="srcs")
    return {package: sorted(names) for package, names in private.items()}


# Only programs/server/Server.cpp reads these factories.
SERVER_PRIVATE_HEADERS = (
    "Server/ProxyV1HandlerFactory.h", "Server/SSH/SSHPtyHandlerFactory.h",
    "Server/TCPProtocolStackFactory.h", "Server/TCPProtocolStackHandler.h",
)


def declare_server_private_headers(rows, inventory):
    declare_compiled_headers(rows["src"], inventory("src"), (), list_expr(SERVER_PRIVATE_HEADERS))
    labels = [relative_label("src", name, "programs/server") for name in SERVER_PRIVATE_HEADERS]
    declare_compiled_headers(rows["programs/server"], inventory("programs/server"),
                             "clickhouse-server-lib", list_expr(labels), field="srcs")


def declare_dbms_private_headers(rows, files):
    # CMake dbms group 0 also compiles the handlers whose Bazel sources embed web assets.
    server_headers = ast.BinOp(left=expr('glob(["Server/ClientEmbedded/*.h"])'), op=ast.Add(), right=list_expr([
        "Server/HTTPHandler.h", "Server/HTTPResponseHeaderWriter.h", "Server/IndexRequestHandler.h",
        "Server/KeeperDashboardRequestHandler.h", "Server/KeeperJemallocHandler.h",
        "Server/StaticRequestHandler.h", "Server/WebTerminalRequestHandler.h", "Server/WebUIRequestHandler.h",
    ]))
    declare_compiled_headers(rows, files, ("dbms__compile_0", "dbms_embedded"), server_headers, field="srcs")
    private_globs = expr(
        'glob(["Backups/BackupCoordinationReplicated*.h", "Interpreters/HashJoin/*.h"], exclude=['
        '"Interpreters/HashJoin/HashJoin.h", "Interpreters/HashJoin/ScatteredBlock.h"])'
    )
    dbms_headers = ast.BinOp(left=private_globs, op=ast.Add(), right=list_expr([
        "Access/resolveSetting.h",
        "Analyzer/InDepthQueryTreeVisitor.h",
        "Analyzer/RecursiveCTE.h",
        "Analyzer/Resolve/IdentifierLookup.h",
        "Analyzer/UnionNode.h",
        "Backups/BackupConcurrencyCheck.h",
        "Backups/BackupCoordinationFileInfos.h",
        "Backups/BackupCoordinationKeeperMapTables.h",
        "Interpreters/AddDefaultDatabaseVisitor.h",
        "Interpreters/getHeaderForProcessingStage.h",
        "Interpreters/joinDispatch.h",
        "Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h",
        "Processors/Merges/Algorithms/FixedSizeDequeWithGaps.h",
        "Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h",
        "Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h",
        "Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h",
        "Processors/QueryPlan/Optimizations/joinOrder.h",
        "Storages/MergeTree/UniqueKey/BlockAllocation.h",
        "Storages/NATS/NATS_fwd.h",
        "Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema.h",
        "Storages/StorageTableFunction.h",
        "Storages/StorageTableProxy.h",
    ]))
    declare_compiled_headers(rows, files, "dbms__compile_0", dbms_headers, field="srcs")
    declare_compiled_headers(rows, files, ("dbms__compile_0", "dbms__compile_2"), list_expr([
        "Columns/ColumnCompressed.h", "Interpreters/AggregationUtils.h",
        "Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h", "Processors/QueryPlan/QueryPlanFormat.h",
        "Processors/Transforms/WindowTransform.h", "WindowFunctions/IWindowFunction.h",
    ]), field="srcs")
    declare_compiled_headers(rows, files, "unit_tests_dbms", expr('glob(["**/tests/*.h"])'), field="srcs")


def group_runtime_filter_benchmark_sources(root):
    """Compile the six shared Functions sources with the benchmark's CMake flags."""
    files = [
        "ComparisonParams.cpp", "equals.cpp", "isDistinctFrom.cpp", "isNotDistinctFrom.cpp",
        "notEquals.cpp", "registerFunctions.cpp",
    ]
    name = "runtime_filter_benchmark_sources"
    label = "//src/Functions:" + name
    old_labels = ["//src/Functions:" + file for file in files]
    own_source = "Processors/benchmarks/benchmark_runtime_filter.cpp"
    package = root / "src/Functions/BUILD.bazel"
    src_build = root / "src/BUILD.bazel"
    functions, benchmark = Source(package.read_text()), Source(src_build.read_text())
    targets = {ast.literal_eval(attributes(rule)["name"]): rule for _, rule in functions.rules()
               if "name" in attributes(rule)}
    srcs = next(attributes(rule)["srcs"] for _, rule in benchmark.rules()
                if attributes(rule).get("name") and attributes(rule)["name"].value == "benchmark_runtime_filter_objects")
    expected = files + ["registerFunctions.h"]
    if name in targets:
        group = attributes(targets[name])
        if (call_name(targets[name]) != "filegroup" or ast.literal_eval(group["srcs"]) != expected
                or ast.literal_eval(group["visibility"]) != ["//src:__pkg__"]
                or ast.literal_eval(srcs) != [own_source, label]):
            raise ValueError("runtime_filter_benchmark_sources differs from the CMake benchmark sources")
        return old_labels
    if ast.literal_eval(srcs) != [own_source, *old_labels]:
        raise ValueError("benchmark_runtime_filter_objects changed its CMake sources or their order")
    exports = next(stmt for stmt in functions.module.body
                   if isinstance(stmt, ast.Expr) and call_name(stmt.value) == "exports_files")
    end = functions.span(exports)[1]
    declaration = ('\n\nfilegroup(name=' + json.dumps(name) + ', srcs=' + json.dumps(expected) +
                   ', visibility=["//src:__pkg__"])')
    package.write_text(functions.rewrite([(end, end, declaration)]))
    src_build.write_text(benchmark.rewrite([(*benchmark.span(srcs), json.dumps([own_source, label]))]))
    return old_labels


def git_inventory(root):
    """Native CMake inputs, regardless of generated BUILD package boundaries."""
    listing = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=root,
    ).decode().split('\0')
    return tuple(sorted(
        name for name in dict.fromkeys(listing)
        if name and PurePosixPath(name).name not in {'BUILD', 'BUILD.bazel'}
        and (root / name).is_file()
        and not any(name == owner or name.startswith(owner + '/') for owner in REPOSITORIES)
    ))


def native_directory(directory):
    return directory in {'src', 'base', 'programs', 'contrib'} or (
        directory.startswith(('src/', 'base/', 'programs/')) or
        directory.startswith('contrib/') and directory.split('/')[1] not in {
            value.split('/')[1] for value in REPOSITORIES
        }
    )


class Build:
    def __init__(self, root, path, inventory, packages, text=None):
        self.root = root
        self.path = path
        self.package = path.parent.relative_to(root).as_posix().removeprefix('.')
        self.text = path.read_text() if text is None else text
        self.source = Source(self.text)
        self.constants = {stmt.targets[0].id: stmt.value for stmt in self.source.module.body
                          if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                          and isinstance(stmt.targets[0], ast.Name)}
        self.inventory = tuple(
            posixpath.relpath(name, self.package or '.')
            for name in inventory if physical(self.package, '') == '' or
            name.startswith(self.package + '/')
            if not any(other and other != self.package and other.startswith(self.package + '/' if self.package else '')
                       and name.startswith(other + '/') for other in packages)
        )
        self.globs = GlobFiles(self.inventory)
        self.loads = [stmt for stmt in self.source.module.body
                      if isinstance(stmt, ast.Expr) and call_name(stmt.value) == 'load']

    def expand(self, node, stack=()):
        class Expand(ast.NodeTransformer):
            def visit_Name(visitor, name):
                if name.id in self.constants:
                    if name.id in stack:
                        raise ValueError('Cyclic constant: ' + name.id)
                    return self.expand(self.constants[name.id], (*stack, name.id))
                return name
        return Expand().visit(deepcopy(node))

    def glob_values(self, node):
        opts = attributes(node)
        if not node.args or set(opts) - {'exclude', 'allow_empty', 'exclude_directories'}:
            raise ValueError('Unsupported glob: ' + render(node))
        patterns = ast.literal_eval(node.args[0])
        excludes = ast.literal_eval(opts['exclude']) if 'exclude' in opts else []
        return tuple(physical(self.package, item) for item in self.globs.result(patterns, excludes))

    def values(self, node, cpu):
        if node is None:
            return []
        if isinstance(node, ast.Name) and node.id in self.constants:
            return self.values(self.constants[node.id], cpu)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [node.value]
        if isinstance(node, (ast.List, ast.Tuple)):
            return [item for elt in node.elts for item in self.values(elt, cpu)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.values(node.left, cpu) + self.values(node.right, cpu)
        if call_name(node) == 'glob':
            return ['//' + self.package + ':' + posixpath.relpath(file, self.package or '.')
                    for file in self.glob_values(node)]
        if call_name(node) == 'select' and isinstance(node.args[0], ast.Dict):
            table = {ast.literal_eval(k): v for k, v in zip(node.args[0].keys, node.args[0].values)}
            if set(table) <= {'@platforms//cpu:' + arch for arch in CPUS} | {'//conditions:default'}:
                return self.values(table.get('@platforms//cpu:' + cpu, table.get('//conditions:default')), cpu)
            # Metadata choices can select alternate generated files. Preserve
            # all branches separately in validation, never infer a file order.
            return [value for key, branch in table.items() for value in self.values(branch, cpu)]
        return []


def deduplicate_dependencies(build, stats):
    """Remove repeated C++ deps after aliases become their actual labels."""
    external = build.path.name.endswith('.BUILD.bazel') and build.path.parent.name == 'dependencies'
    repo = '@' + build.path.name.removesuffix('.BUILD.bazel') if external else '@clickhouse'
    package = '' if external else build.package
    edits = []
    expected = {}
    rules = list(build.source.rules())
    for index, (_, rule) in enumerate(rules):
        for attr in rule.keywords:
            if attr.arg not in {'deps', 'implementation_deps'}:
                continue
            expanded = build.expand(attr.value)
            for node in ast.walk(expanded):
                if not isinstance(node, ast.Call):
                    continue
                if (call_name(node) != 'select' or not node.args or
                        not isinstance(node.args[0], ast.Dict)):
                    raise ValueError(f'{build.path}: cannot evaluate {attr.arg}: {render(node)}')
                conditions = {ast.literal_eval(key) for key in node.args[0].keys}
                if conditions - {'@platforms//cpu:' + cpu for cpu in CPUS} - {'//conditions:default'}:
                    raise ValueError(f'{build.path}: unsupported {attr.arg} conditions: {conditions}')
            outputs = {}
            changed = False
            for cpu in CPUS:
                original = build.values(expanded, cpu)
                seen = set()
                kept = []
                for value in original:
                    label = canonical_dependency(value, package, repo)
                    if label in seen:
                        stats['duplicate_dependencies'] += 1
                        changed = True
                    else:
                        seen.add(label)
                        kept.append(value)
                outputs[cpu] = kept
                expected[index, attr.arg, cpu] = [canonical_dependency(v, package, repo) for v in kept]
                stats['verified_dependency_lists'] += 1
            if changed:
                if outputs[CPUS[0]] == outputs[CPUS[1]]:
                    result = list_expr(outputs[CPUS[0]])
                else:
                    result = ast.Call(
                        func=ast.Name(id='select', ctx=ast.Load()),
                        args=[ast.Dict(keys=[quoted('@platforms//cpu:' + cpu) for cpu in CPUS],
                                       values=[list_expr(outputs[cpu]) for cpu in CPUS])],
                        keywords=[],
                    )
                edits.append((*build.source.span(attr.value), render(result)))
                stats['deduplicated_dependency_attributes'] += 1
    if not edits:
        return build.text
    text = build.source.rewrite(edits)
    rewritten = Build(build.root, build.path, (), (), text)
    after_rules = list(rewritten.source.rules())
    for index, ((_, before), (_, after)) in enumerate(zip(rules, after_rules, strict=True)):
        old_attrs, new_attrs = attributes(before), attributes(after)
        if (call_name(before) != call_name(after) or old_attrs.keys() != new_attrs.keys()):
            raise ValueError(f'{build.path}: dependency pruning changed rule declarations')
        for attr in old_attrs:
            if attr not in {'deps', 'implementation_deps'}:
                if ast.dump(old_attrs[attr]) != ast.dump(new_attrs[attr]):
                    raise ValueError(f'{build.path}: dependency pruning changed {attr}')
            else:
                for cpu in CPUS:
                    actual = [canonical_dependency(v, package, repo)
                              for v in rewritten.values(new_attrs[attr], cpu)]
                    if actual != expected[index, attr, cpu] or len(actual) != len(set(actual)):
                        raise ValueError(f'{build.path}: {attr} changed dependency order for {cpu}')
    return text


class Migration:
    def __init__(self, root, stage='all', from_generator=False):
        self.root = root
        self.stage = stage
        self.from_generator = from_generator
        self.files = git_inventory(root)
        self.file_set = set(self.files)
        self.existing = sorted({
            path.parent.relative_to(root).as_posix().removeprefix('.')
            for name in subprocess.check_output(
                ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
                 '--', 'BUILD', 'BUILD.bazel', '**/BUILD', '**/BUILD.bazel'], cwd=root,
            ).decode().split('\0')
            if name and (path := root / name).is_file()
        })
        main_packages = [p for p in self.existing if not p or native_directory(p)]
        # Regeneration rendered a new flat CMake graph into root BUILD; the old
        # generated packages must not hide native files from that capture.
        if from_generator:
            main_packages = ['']
        self.builds = [Build(root, root / pkg / 'BUILD.bazel', self.files,
                             [p for p in self.existing if not native_directory(p)] if from_generator else self.existing)
                       for pkg in main_packages]
        self.root_build = next(build for build in self.builds if not build.package)
        self.packages = set(self.existing)
        self.target_packages = {}
        self.aliases = {}
        self.exports = defaultdict(set)
        self.dbms_private_exports = {}
        self.header_packages = set()
        self.rows = defaultdict(list)
        self.loads = defaultdict(dict)
        self.stats = Counter()
        self.compiles = {}
        self.textual_names = set()
        self.needed_rules = {}
        self._identify()

    def selected(self, package):
        if self.stage == 'base':
            return package.startswith(('base/', 'contrib/')) or package == 'base'
        return native_directory(package)

    @cache
    def nearest(self, filename):
        name = physical('', filename)
        directory = posixpath.dirname(name)
        while directory:
            if (self.root / directory / 'CMakeLists.txt').is_file():
                return directory
            directory = posixpath.dirname(directory)
        return ''

    def source_filename(self, value, old_package):
        if value.startswith(':'):
            value = '//' + old_package + value
        if not value.startswith(('@', '//')):
            value = '//' + old_package + ':' + value
        m = MAIN_LABEL.match(value)
        if not m:
            return None
        name = physical(m['pkg'], m['name'])
        if name.startswith('bazel/gen/'):
            # Copies for #embed preserve first-party source filenames.
            m = re.match(r'bazel/gen/(?:x86_64|aarch64)/embedded/(.*)', name)
            return m[1] if m else None
        return name if name in self.file_set else None

    def _identify(self):
        candidates = defaultdict(set)
        for build in self.builds:
            for _, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                kind = call_name(call)
                if not build.package and kind == 'alias':
                    actual = attrs.get('actual')
                    if isinstance(actual, ast.Constant) and isinstance(actual.value, str) and actual.value.startswith('@'):
                        self.aliases[name] = actual.value
                        continue
                if kind not in COMPILERS:
                    continue
                key = (build.package, logical_name(name))
                for cpu in CPUS:
                    for value in build.values(attrs.get('srcs'), cpu):
                        src = self.source_filename(value, build.package)
                        if src and src.endswith(TUS):
                            candidates[key].add(src)
                if build.package:
                    self.target_packages[build.package, name] = build.package
        # Determine owners for a logical library, not one flag variant at a
        # time. Generated-only variants stay beside their ordinary sources.
        owners = {}
        for key, values in candidates.items():
            source_dirs = [posixpath.dirname(value) for value in values]
            common = posixpath.commonpath(source_dirs)
            owner = self.nearest(physical(common, '_source_.cc'))
            owners[key] = owner if self.selected(owner) else key[0]
        for build in self.builds:
            for _, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                if not build.package and name in self.aliases:
                    continue
                package = owners.get((build.package, logical_name(name)), build.package)
                if self.stage == 'all' and not build.package:
                    # These targets compile only generated/foreign files or
                    # deliberately share sources with other CMake directories.
                    # Name their actual CMake declaration rather than guessing
                    # from an empty or repository-wide source LCA.
                    declarations = {
                        '_fuzzer': 'contrib/libfuzzer-cmake',
                        '_fuzzer_no_main': 'contrib/libfuzzer-cmake',
                        '_lz4': 'contrib/lz4-cmake',
                        '_prometheus_protobufs': 'contrib/prometheus-protobufs-cmake',
                        'clickhouse_grpc_protos': 'src/Server/grpc_protos',
                        'clickhouse_common_io': 'src',
                        'clickhouse_common_io_embedded': 'src',
                        'dbms_embedded': 'src',
                        'common': 'base/base',
                        'contrib': 'contrib',
                        'unit_tests_dbms': 'src',
                        # Replace the transitional root input library with a
                        # header-only aggregate alongside the src libraries.
                        'headers': 'src',
                    }
                    package = declarations.get(logical_name(name), package)
                # Published Poco include interfaces have no compiled files.
                if name.startswith('poco_') and name.endswith('_headers'):
                    part = {
                        'foundation': 'Foundation', 'net': 'Net', 'net_ssl': 'NetSSL_OpenSSL',
                        'json': 'JSON', 'xml': 'XML', 'redis': 'Redis', 'util': 'Util',
                    }.get(name[len('poco_'):-len('_headers')])
                    if part:
                        package = 'base/poco/' + part
                if name in {'arrow_cmake_headers', 'librdkafka_cmake_headers', 'openssl_cmake_headers'}:
                    package = 'contrib/' + name.split('_')[0] + '-cmake'
                if name == 'clickhouse' or name == 'unit_tests_dbms' and call_name(call) == 'alias':
                    package = ''
                if not self.selected(package):
                    package = build.package
                self.target_packages[build.package, name] = package
                self.packages.add(package)
        # Existing globs for source wrappers belong in each wrapper directory,
        # even if the directory contains only installed/configuration headers.
        for build in self.builds:
            for node in ast.walk(build.source.module):
                if call_name(node) != 'glob':
                    continue
                for file in build.glob_values(node):
                    parts = file.split('/')
                    if len(parts) < 3:
                        continue
                    if parts[0] == 'contrib' and '/'.join(parts[:2]) not in REPOSITORIES:
                        package = '/'.join(parts[:2])
                        if self.selected(package):
                            self.packages.add(package)
        self.packages = {p for p in self.packages if p is not None}
        # These declarations were hidden in declare_inputs() at the root.
        # Their files already have natural owners; Rust consumes DefaultInfo.
        self.aliases.update({
            'krb5_gen_map': '@clickhouse_krb5//:gen_map',
            'krb5_gen': '@clickhouse_krb5//:gen',
            'cmake_cargo_config': '//contrib/corrosion-cmake:config.toml.in',
        })
        self.target_packages['', 'openssl_configured_headers'] = 'contrib/openssl-cmake'
        self.packages.update({'contrib/corrosion-cmake', 'contrib/openssl-cmake'})

    @cache
    def file_package(self, filename):
        packages = (p for p in self.packages if p and filename.startswith(p + '/'))
        return max(packages, key=len, default='')

    @cache
    def package_inventory(self, package):
        return tuple(
            posixpath.relpath(file, package or '.')
            for file in self.files if self.file_package(file) == package
        )

    @cache
    def mapped(self, value, current, old_package='', cross_repository=False, source_file=True):
        """Resolve main labels, record exact cross-package file exports."""
        if value.startswith(':'):
            value = '//' + old_package + value
        elif not value.startswith(('@', '//')):
            if not source_file:
                return value
            value = '//' + old_package + ':' + value
        m = MAIN_LABEL.fullmatch(value)
        if not m:
            return value
        repo = m['repo'] or ('@clickhouse' if cross_repository else '')
        pkg, name = m['pkg'], m['name']
        if not pkg and name in self.aliases:
            self.stats['removed_alias_references'] += 1
            actual = self.aliases[name]
            if actual.startswith('//'):
                return self.mapped(actual, current, '', cross_repository)
            return actual
        target = self.target_packages.get((pkg, name))
        if target is not None:
            if not pkg and name == 'headers' and target == 'src':
                name = 'clickhouse_source_headers'
            return relative_label(target, name, current, repo)
        file = physical(pkg, name)
        if file in self.file_set:
            owner = self.file_package(file)
            relative = file.removeprefix(owner + '/') if owner else file
            if owner != current or repo:
                self.exports[owner].add(relative)
                return relative_label(owner, relative, current, repo)
            return relative
        return relative_label(pkg, name, current, repo)

    def location(self, value, current, old_package='', cross_repository=False):
        return LOCATION.sub(
            lambda m: '$(' + m[1] + ' ' + self.mapped(m[2], current, old_package, cross_repository) + ')',
            value,
        )

    def file_values(self, build, node, current, field, headers):
        """Rewrite one original glob after declaring new package boundaries."""
        values = build.glob_values(node)
        selected = []
        same_package_glob = build.package == current and field in {'srcs', 'hdrs', 'textual_hdrs'}
        for file in values:
            owner = self.file_package(file)
            suffix = PurePosixPath(file).suffix
            if field in {'hdrs', 'textual_hdrs'} and file.endswith((*CC_HEADER_SUFFIXES, *TEXTUAL)):
                if owner != current:
                    self.header_packages.add(owner)
                    headers.add(owner)
                    continue
            if field == 'additional_compiler_inputs':
                # Never feed unrelated source/metadata files to C++ actions.
                # Keep named includes and #embed inputs, regardless of suffix.
                if (suffix not in (*CC_HEADER_SUFFIXES, *TEXTUAL) and
                        PurePosixPath(file).name not in self.textual_names):
                    self.stats['dropped_metadata_or_unincluded_tus'] += 1
                    continue
                if owner != current and file.endswith((*CC_HEADER_SUFFIXES, *TEXTUAL)):
                    self.header_packages.add(owner)
                    headers.add(owner)
                    continue
            if not (same_package_glob and owner == current):
                old_name = file.removeprefix(build.package + '/') if build.package else file
                selected.append(self.mapped('//' + build.package + ':' + old_name, current, build.package))
        if same_package_glob:
            # The glob automatically stops at new child packages. Preserve
            # the original file order in srcs and the original exclusions.
            if field == 'srcs' and selected:
                return list_expr([self.mapped('//' + build.package + ':' +
                                               (file.removeprefix(build.package + '/') if build.package else file),
                                               current, build.package) for file in values])
            if selected:
                return ast.BinOp(left=node, op=ast.Add(), right=list_expr(selected))
            return node
        # Local globs remain true globs when the old expression names only
        # files owned by the destination; transform literal patterns by prefix.
        if (values and len(selected) == len(values) and
                all(self.file_package(file) == current for file in values)):
            prefix = posixpath.relpath(build.package or '.', current or '.')
            opts = attributes(node)
            patterns = ast.literal_eval(node.args[0])
            excluded = ast.literal_eval(opts['exclude']) if 'exclude' in opts else []
            combined = [posixpath.normpath(posixpath.join(prefix, p)) for p in (*patterns, *excluded)]
            if all(not p.startswith('../') and p != '..' for p in combined):
                args = list_expr(combined[:len(patterns)])
                kwargs = [ast.keyword(arg='exclude', value=list_expr(combined[len(patterns):]))] if excluded else []
                return ast.Call(func=ast.Name(id='glob', ctx=ast.Load()), args=[args], keywords=kwargs)
        return list_expr(selected)

    def transform(self, node, build, current, field, headers):
        node = build.expand(node)

        class Visitor(ast.NodeTransformer):
            def visit_Call(visitor, call):
                if call_name(call) == 'glob':
                    return self.file_values(build, call, current, field, headers)
                if call_name(call) == 'select':
                    out = call
                    # Select keys are not source labels; only process values.
                    out.args[0].values = [visitor.visit(branch) for branch in call.args[0].values]
                    return out
                return visitor.generic_visit(call)

            def visit_Constant(visitor, item):
                if not isinstance(item.value, str):
                    return item
                value = item.value
                if field in LABEL_FIELDS:
                    value = self.mapped(value, current, build.package)
                else:
                    value = self.location(value, current, build.package)
                return quoted(value)
        out = Visitor().visit(node)
        if field in {'local_includes', 'includes'} and current != build.package:
            # Keep the same normalized directories and their order. The
            # rules_cc helper accepts .. within the execution root.
            prefix = posixpath.relpath(build.package or '.', current or '.') + '/'

            class Includes(ast.NodeTransformer):
                def visit_List(visitor, items):
                    return ast.List(elts=[
                        ast.BinOp(left=quoted(prefix), op=ast.Add(), right=elt)
                        for elt in items.elts
                    ], ctx=ast.Load())
            out = Includes().visit(out)
        return out

    def prepare(self):
        from initial_migration.repository_inputs import textual_source_names
        self.textual_names = textual_source_names(None)
        if any(call_name(stmt.value) == 'declare_inputs' for stmt in self.root_build.source.module.body
               if isinstance(stmt, ast.Expr)):
            self.rows['contrib/openssl-cmake'].append(expr_stmt(
                'filegroup(name="openssl_configured_headers",srcs=glob(["common/include/**"]))'))
        for build in self.builds:
            for load in build.loads:
                key = render(load)
                for pkg in self.packages:
                    if not pkg or native_directory(pkg):
                        self.loads[pkg][key] = load
            for stmt, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                if not build.package and name in self.aliases:
                    continue
                current = self.target_packages[build.package, name]
                headers = set()
                kw = []
                for item in call.keywords:
                    if item.arg == 'name':
                        value = deepcopy(item.value)
                        if not build.package and name == 'headers' and current == 'src':
                            value = quoted('clickhouse_source_headers')
                    else:
                        value = self.transform(item.value, build, current, item.arg, headers)
                    kw.append(ast.keyword(arg=item.arg, value=value))
                if headers:
                    deps = next((item for item in kw if item.arg == 'deps'), None)
                    refs = list_expr([relative_label(owner, 'headers', current) for owner in sorted(headers)])
                    if deps:
                        deps.value = ast.BinOp(left=deps.value, op=ast.Add(), right=refs)
                    else:
                        kw.append(ast.keyword(arg='deps', value=refs))
                rebuilt = ast.Expr(value=ast.Call(func=deepcopy(call.func), args=deepcopy(call.args), keywords=kw))
                self.rows[current].append(rebuilt)
                if not build.package and name == 'unit_tests_dbms' and current == 'src':
                    self.rows[''].append(expr_stmt(
                        'alias(name="unit_tests_dbms",actual="//src:unit_tests_dbms")'))
                if current != build.package:
                    self.stats['moved_targets'] += 1
        for package in self.header_packages:
            if any(isinstance(row, ast.Expr) and ast.literal_eval(attributes(row.value)['name']) == 'headers'
                   for row in self.rows[package]):
                continue
            self.rows[package].append(expr_stmt(
                'cc_library(name="headers", hdrs=glob(%s), textual_hdrs=glob(%s))' % (
                    json.dumps(['**/*' + suffix for suffix in CC_HEADER_SUFFIXES if suffix not in TEXTUAL]),
                    json.dumps(['**/*' + suffix for suffix in TEXTUAL]),
                )))
            self.loads[package]['headers'] = expr_stmt('load("@rules_cc//cc:cc_library.bzl", "cc_library")')
        if self.from_generator and 'src/Parsers' in self.rows:
            declare_parser_interface(self.rows['src/Parsers'])
            for package, compiler, header in (
                ('src/Functions', 'clickhouse_functions_obj__compile_0', 'registerFunctions.h'),
                ('src/Storages/System', 'clickhouse_storages_system__compile_1', 'attachSystemTables.h'),
            ):
                declare_compiled_headers(self.rows[package], self.package_inventory(package), compiler, list_expr([header]))
            declare_src_compiled_headers(self.rows["src"], self.package_inventory("src"))
            declare_dbms_private_headers(self.rows["src"], self.package_inventory("src"))
            self.dbms_private_exports = declare_dbms_package_headers(self.rows, self.package_inventory)
            for package, names in self.dbms_private_exports.items():
                self.exports[package].update(names)
        if self.from_generator and "programs/server" in self.rows:
            declare_server_private_headers(self.rows, self.package_inventory)
            self.exports["src"].update(SERVER_PRIVATE_HEADERS)
        if self.from_generator:
            for package, compiler, headers in (
                ("src/Compression", "clickhouse_compression",
                 list_expr(["PFor/bitpack.h", "PFor/block.h", "PFor/bulk.h", "PFor/vertical.h"])),
                ("src/Functions/JSONPath", "clickhouse_functions_jsonpath", expr('glob(["Generator/VisitorJSONPath*.h"])')),
            ):
                if package in self.rows:
                    declare_compiled_headers(self.rows[package], self.package_inventory(package), compiler, headers, field="srcs")
        if self.from_generator and "src/Dictionaries" in self.rows:
            declare_dictionary_private_headers(self.rows["src/Dictionaries"], self.package_inventory("src/Dictionaries"))
        data = self.rewrite_consumers()
        builds = self.render_builds()
        data.update(builds)
        self.verify(data)
        for path, content in builds.items():
            normalized = normalize(content, native=True)
            prove(content, normalized, path)
            data[path] = normalized
        return data

    def normalize_source(self, value, package):
        if value.startswith(':'):
            return physical(package, value[1:])
        if value.startswith(('//', '@clickhouse//')):
            value = canonical_dependency(value, '', '')
        match = MAIN_LABEL.fullmatch(value)
        if match:
            return physical(match['pkg'], match['name'])
        return value if value.startswith('@') else physical(package, value)

    def verify(self, rendered):
        """Verify TU order and compile/link options on both target CPUs."""
        new_builds = {
            package: Build(self.root, self.root / package / 'BUILD.bazel',
                           self.files, self.packages,
                           rendered[self.root / package / 'BUILD.bazel'])
            for package in self.rows
        }
        rules = {}
        for package, build in new_builds.items():
            for node in ast.walk(build.source.module):
                if call_name(node) != 'glob':
                    continue
                opts = attributes(node)
                patterns = ast.literal_eval(node.args[0])
                excluded = ast.literal_eval(opts['exclude']) if 'exclude' in opts else []
                for pattern in patterns:
                    if not build.globs.matching(pattern):
                        raise ValueError(f'{build.path}:{node.lineno}: empty glob pattern {pattern}')
                    self.stats['verified_glob_patterns'] += 1
                if not build.globs.result(patterns, excluded):
                    raise ValueError(f'{build.path}:{node.lineno}: empty glob result')
            for _, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                key = package, name
                if key in rules:
                    raise ValueError('Duplicate native declaration: ' + str(key))
                rules[key] = build, call, attrs
        for original in self.builds:
            for _, call in original.source.rules():
                before = attributes(call)
                name = ast.literal_eval(before['name'])
                if not original.package and name in self.aliases:
                    continue
                package = self.target_packages[original.package, name]
                mapped_name = 'clickhouse_source_headers' if (
                    not original.package and name == 'headers' and package == 'src') else name
                rebuilt, new_call, after = rules[package, mapped_name]
                if call_name(call) != call_name(new_call):
                    raise ValueError('Native rule changed kind: ' + name)
                for cpu in CPUS:
                    for field in ('srcs',):
                        old_srcs = [self.normalize_source(value, original.package)
                                    for value in original.values(before.get(field), cpu)]
                        new_srcs = [self.normalize_source(value, rebuilt.package)
                                    for value in rebuilt.values(after.get(field), cpu)]
                        old_tus = [v for v in old_srcs if v.endswith(TUS)]
                        new_tus = [v for v in new_srcs if v.endswith(TUS)]
                        if old_tus != new_tus:
                            raise ValueError(f'{name} {cpu} changed ordered translation units: ' +
                                             repr((old_tus[:10], new_tus[:10])))
                for field in before.keys() - LABEL_FIELDS - {
                        'name', 'local_includes', 'includes',
                        'copts', 'linkopts', 'cmd', 'env'}:
                    if ast.dump(original.expand(before[field])) != ast.dump(rebuilt.expand(after[field])):
                        raise ValueError(f'{name} changed attribute {field}')
                for field in {'copts', 'linkopts', 'env'} & before.keys():
                    old_flags = original.expand(before[field])
                    # A location macro may name a relocated target. It must
                    # be the sole possible change within a compiler flag.
                    for literal in ast.walk(old_flags):
                        if isinstance(literal, ast.Constant) and isinstance(literal.value, str):
                            literal.value = self.location(literal.value, package, original.package)
                    if ast.dump(old_flags) != ast.dump(rebuilt.expand(after[field])):
                        raise ValueError(f'{name} changed compile or link options in {field}')
                self.stats['verified_native_rules'] += 1
        for arch in ('', '_aarch64'):
            path = self.root / ('bazel/rules/cmake_link_data' + arch + '.bzl')
            if path not in rendered:
                continue
            old = next(ast.literal_eval(stmt.value) for stmt in ast.parse(path.read_text()).body
                       if isinstance(stmt, ast.Assign) and stmt.targets[0].id == 'CMAKE_LINK')
            new = next(ast.literal_eval(stmt.value) for stmt in ast.parse(rendered[path]).body
                       if isinstance(stmt, ast.Assign) and stmt.targets[0].id == 'CMAKE_LINK')
            expected = dict(old, refs=[self.mapped(ref, None, '', source_file=False) for ref in old['refs']])
            if new != expected:
                raise ValueError(f'{path}: altered a CMake object count, archive or link sequence')
            self.stats['verified_link_plans'] += 1
        for path, text in rendered.items():
            origin = path.parent.relative_to(self.root).as_posix()
            if origin == '.':
                origin = ''
            foreign = path.parent == self.root / 'bazel/dependencies'
            source = Source(text)
            for node in ast.walk(source.module):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                for value in [node.value, *[m[2] for m in LOCATION.finditer(node.value)]]:
                    match = MAIN_LABEL.fullmatch(
                        canonical_dependency(value, '', '')
                        if value.startswith(('//', '@clickhouse//')) else value)
                    if not match or foreign and not match['repo']:
                        continue
                    # Also reject labels for unknown native packages: a
                    # missing generated rule is not a source filename.
                    build_file = self.root / match['pkg'] / 'BUILD.bazel'
                    if (native_directory(match['pkg']) or not match['pkg']) and not (
                            build_file in rendered or build_file.is_file()):
                        raise ValueError(f'{path}:{node.lineno}: {value} refers to a missing package')
                    file = physical(match['pkg'], match['name'])
                    if file not in self.file_set:
                        continue
                    owner = self.file_package(file)
                    build_file = self.root / owner / 'BUILD.bazel'
                    if owner != match['pkg'] or not (
                            build_file in rendered or build_file.is_file()):
                        raise ValueError(f'{path}:{node.lineno}: {value} has no source package {owner}')
                    # Only native BUILD files are replaced here. Existing
                    # bazel/ and Rust BUILD files manage their own exports.
                    if native_directory(owner) and (origin != owner or foreign):
                        rel = file.removeprefix(owner + '/')
                        if build_file in rendered and rel not in self.exports[owner]:
                            raise ValueError(f'{path}:{node.lineno}: {value} is not exported by {owner}')
                    self.stats['verified_source_labels'] += 1

    def rewrite_consumers(self):
        """Rewrite concrete labels in BUILD, module, rule, and runtime data."""
        output = {}
        native_builds = {build.path for build in self.builds}
        filenames = subprocess.check_output(
            ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
             '--', '*.bzl', '*.BUILD.bazel', '*.MODULE.bazel', 'BUILD', 'BUILD.bazel',
             '**/BUILD', '**/BUILD.bazel', 'MODULE.bazel'], cwd=self.root,
        ).decode().split('\0')
        for filename in dict.fromkeys(filenames):
            path = self.root / filename
            if not filename or not path.is_file() or path in native_builds or not (
                    filename.endswith(('.bzl', '.BUILD.bazel', '.MODULE.bazel')) or
                    path.name in {'BUILD', 'BUILD.bazel', 'MODULE.bazel'}):
                continue
            if filename == 'bazel/initial_migration/root_inputs.bzl':
                continue
            try:
                source = Source(path.read_text())
            except SyntaxError:
                continue
            edits = []
            cross_repo = filename.startswith('bazel/dependencies/') and filename.endswith('.BUILD.bazel')
            for node in ast.walk(source.module):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                value = node.value
                if MAIN_LABEL.fullmatch(value) and (not cross_repo or value.startswith('@clickhouse//')):
                    value = self.mapped(value, None, '', cross_repo, source_file=False)
                else:
                    value = LOCATION.sub(
                        lambda m: (m[0] if not MAIN_LABEL.fullmatch(m[2]) or
                                   cross_repo and not m[2].startswith('@clickhouse//') else
                                   '$(' + m[1] + ' ' + self.mapped(m[2], None, '', cross_repo) + ')'),
                        value)
                if value != node.value:
                    edits.append((*source.span(node), json.dumps(value)))
            result = source.rewrite(edits)
            if cross_repo:
                result = deduplicate_dependencies(Build(self.root, path, (), (), result), self.stats)
            if result != path.read_text():
                ast.parse(result, filename=filename)
                output[path] = result
        return output

    def render_builds(self):
        rendered = {}
        packages = set(self.rows) | set(self.exports)
        for package in sorted(packages):
            if package and not native_directory(package):
                continue
            loads = dict(self.loads[package])
            # Source generation stays in //bazel; this temporary root macro
            # will be removed when its dynamic source exports are expanded.
            lines = [render(stmt) for stmt in loads.values()
                     if not any(isinstance(n, ast.Constant) and n.value == 'declare_inputs' for n in ast.walk(stmt))]
            names = {call_name(row.value) for row in self.rows[package] if isinstance(row, ast.Expr)}
            if 'cc_library' in names and not any('"cc_library"' in text or "'cc_library'" in text for text in lines):
                lines.append('load("@rules_cc//cc:cc_library.bzl", "cc_library")')
            lines = list(dict.fromkeys(lines))
            lines.append('package(default_visibility = ["//visibility:public"])')
            private = self.dbms_private_exports.get(package, ())
            if private:
                lines.append('exports_files(' + json.dumps(private) + ', visibility=["//src:__pkg__"])')
            exported = self.exports[package] - set(private)
            if package == "src" and (server := sorted(exported.intersection(SERVER_PRIVATE_HEADERS))):
                lines.append('exports_files(' + json.dumps(server) + ', visibility=["//programs/server:__pkg__"])')
                exported.difference_update(server)
            if exported:
                lines.append('exports_files(' + json.dumps(sorted(exported)) + ')')
            lines += [render(row) for row in self.rows[package]]
            text = simplify_globs(
                '\n\n'.join(lines) + '\n', self.package_inventory(package), self.stats,
            )
            text = deduplicate_dependencies(
                Build(self.root, self.root / package / 'BUILD.bazel', (), (), text), self.stats,
            )
            # compact() preserves statement indentation. ast.unparse() places
            # whole rules on one line, so it must be formatted first.
            text = subprocess.run(
                ['buildifier', '-config=' + str(self.root / '.buildifier.json'),
                 '-type=build', '-mode=fix', '-lint=fix', '-warnings=load'], input=text, capture_output=True,
                text=True, check=True, cwd=self.root,
            ).stdout
            text = compact(text)
            ast.parse(text)
            rendered[self.root / package / 'BUILD.bazel'] = text
        return rendered


def expr_stmt(value):
    return ast.Expr(value=expr(value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--stage', choices=('base', 'all'), default='all')
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--deduplicate-deps', action='store_true',
                        help='Only remove repeated native and external dependency labels.')
    args = parser.parse_args()
    if args.deduplicate_deps:
        files = subprocess.check_output(
            ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
             '--', 'BUILD.bazel', '**/BUILD.bazel', '*.BUILD.bazel'], cwd=args.root,
        ).decode().split('\0')
        stats = Counter()
        for filename in dict.fromkeys(files):
            if not filename:
                continue
            path = args.root / filename
            package = path.parent.relative_to(args.root).as_posix().removeprefix('.')
            external = package == 'bazel/dependencies' and path.name.endswith('.BUILD.bazel')
            if path.is_file() and (external or not package or native_directory(package)):
                build = Build(args.root, path, (), ())
                result = deduplicate_dependencies(build, stats)
                if result != build.text:
                    stats['rewritten_builds'] += 1
                    if args.write:
                        path.write_text(result)
                stats['verified_builds'] += 1
        print(json.dumps(stats, indent=2))
        return
    migration = Migration(args.root, args.stage)
    result = migration.prepare()
    print(json.dumps(dict(migration.stats, files=len(result)), indent=2))
    if args.write:
        for path, value in result.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value)


if __name__ == '__main__':
    main()
