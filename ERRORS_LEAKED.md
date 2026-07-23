# Handled exceptions leaked to system error tables

This audit follows [PR #111141](https://github.com/ClickHouse/ClickHouse/pull/111141).
`DB::Exception` records itself in `system.errors` when it is constructed. `system.error_log` is
populated from the same counters. Consequently, an exception used only for probing, parser
backtracking, a non-throwing API, or a fallback which succeeds is currently visible as an error.

The fix pattern is a narrow `Exception::SuppressErrorCodesScope` around the speculative operation.
If a suppressed `DB::Exception` is later propagated, it must first call `recordToSystemErrors`.
Scopes maintain a thread-local nesting depth, and `recordToSystemErrors` records only after every
surrounding scope has unwound. The scope must not cover the fallback itself or a replacement exception
describing terminal failure.

## Classification

| Status | Meaning |
|---|---|
| Fixed | The handled exception is suppressed, with propagated exceptions explicitly recorded. |
| Partially fixed | The distinguishable handled branches are fixed; broader ambiguous branches remain unchanged. |
| Deferred | No safe error-code boundary was found, so existing telemetry is preserved. |
| Open | The surrounding operation succeeds or returns an expected negative result. Suppress it. |
| Policy decision | The process continues, but the caught exception may represent useful operational telemetry. |
| Keep telemetry | The operation really failed, is only translated/reported/rethrown, or cleanup failed. |
| Not server-visible | The catch is in a standalone process which has no server `system.errors` table. |
| Not a leak | The catch does not consume a `DB::Exception` which records in the system tables. |

## Audit scope

Production catches under `src/`, `base/`, `programs/`, and `utils/` were reviewed. Tests, fuzzers,
benchmarks, examples, third-party-only exceptions, cleanup-and-rethrow paths, protocol handlers which
return the actual query error, and asynchronous failures which remain failures were excluded from the
confirmed list. Base-class catches such as `catch (Poco::Exception &)` were included because
`DB::Exception` derives from `Poco::Exception`. The audit covered approximately 2,700 production catch
sites after those exclusions.

## Existing fix

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| VALUES-001 | `ValuesBlockInputFormat::{tryReadValue,tryParseExpressionUsingTemplate,parseExpression}` | Streaming and expression-template probes fall back to SQL expression parsing. | Narrow scopes; explicitly record decimal overflow and every other propagated exception. | Fixed by #111141 |

## Formats, data types, parsers, I/O, and compression

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| FMT-001 | `DataTypeFactory::getImpl<true>` in `src/DataTypes/DataTypeFactory.cpp` | Invalid enum, tuple, or registered type returns `nullptr`. | Scope each creator probe. Preserve the non-throwing contract. | Fixed |
| FMT-002 | `DataTypeMap::getDynamicSubcolumnData` in `src/DataTypes/DataTypeMap.cpp` | An invalid dynamic `Map` key subcolumn returns `nullptr`. | Scope only the non-throwing branch; leave the `ILLEGAL_COLUMN` path recorded. | Fixed |
| FMT-003 | `tryDeserializeText` helpers in `ISerialization.cpp`, `SimpleTextSerialization.h`, `SerializationCustomSimpleText.cpp`, `SerializationFixedString.cpp`, and `SerializationString.cpp` | Parse exceptions become `false` and partially inserted data is rolled back. | Scope only try-mode; record non-parse exceptions before propagation. | Fixed |
| FMT-004 | Non-throwing deserialization in `SerializationArray.cpp`, `SerializationMap.cpp`, and `SerializationTuple.cpp` | Parse exceptions become `false` after nested-column rollback. | Scope only non-throwing template instantiations; record non-parse exceptions. | Fixed |
| FMT-005 | `FormatSchemaInfo::storeSchemaOnDisk` | A writer losing a concurrent schema-cache race accepts the winner's file. | Scope the write/rename attempt; record and rethrow if no final file exists. | Fixed |
| FMT-006 | `FormatFactory::wrapReadBufferIfNeeded` | File-size probe failure falls back to a single-threaded reader. | Scope `getFileSizeFromReadBuffer`. | Fixed |
| FMT-007 | `readSchemaFromFormatImpl` in `src/Formats/ReadSchemaUtils.cpp` | Schema inference retries another file or format and can succeed. | Scope each attempt. Final `CANNOT_DETECT_FORMAT` remains recorded. | Fixed |
| FMT-008 | `ProtobufSerializer::buildMessageSerializer` | `PROTOBUF_FIELD_NOT_REPEATED` selects the mixed nested-layout fallback. | Scope the layout probe; record every non-fallback rethrow. | Fixed |
| FMT-009 | `tryGetLiteralBool` in `src/Parsers/makeASTForLogicalFunction.cpp` | Failed literal conversion leaves the expression unsimplified. | Scope the conversion probe. | Fixed |
| FMT-010 | `ParserSetQuery::parseNameValuePairWithParameterOrDefault` | Boolean-setting shorthand recognition can return `false`. | Scope `Settings::castValueUtil`; preserve unexpected failures. | Fixed |
| FMT-011 | `ParserStringLiteral::parseImpl` | Malformed quoted text is parser backtracking, later replaced by final syntax diagnostics. | Scope only quoted-string parsing; preserve non-parse failures. | Fixed |
| FMT-012 | `parseAccessFlags` in `parseAccessRightsElements.cpp` | Unknown access words reject one grammar alternative. | Suppress `UNKNOWN_ACCESS_TYPE`; preserve unexpected failures. | Fixed |
| FMT-013 | `ToTimeSpan::convertImpl` in `KQLCastingFunctions.cpp` | Invalid KQL `timespan` conversion emits `NULL`. | Scope expected conversion failures; preserve resource/internal errors. | Fixed |
| FMT-014 | Fractional `DateTime64` and `Time64` parsing in `src/IO/ReadHelpers.h` | Failure immediately before `.` continues as a fractional value. | Scope the whole-part parser; record no-dot and unexpected rethrows. | Fixed |
| FMT-015 | `SeekableReadBuffer::tryGetPosition` | Position failure becomes `std::nullopt`. | Scope `getPosition`; define expected capability codes narrowly. | Fixed |
| FMT-016 | `ReadWriteBufferFromHTTP::{nextImpl,getFileInfo}` | Configured GET `404` becomes EOF; unsupported HEAD falls back to GET. | Scope each request; record statuses which still propagate. | Fixed |
| FMT-017 | `S3::{getRunningAvailabilityZone,tryGetRunningAvailabilityZone}` | Metadata providers are probed in sequence; optional wrapper returns empty. | Scope each provider and the optional wrapper; direct all-fail API records its final error. | Fixed |
| FMT-018 | `CopyS3FileHelper::performMultipartUploadCopy` | `ACCESS_DENIED` on server-side copy falls back to read/write copy. | Scope multipart copy; record every other rethrow. | Fixed |
| FMT-019 | `CompressionCodecFactory::{fillCodecDescriptions,getCodecDocumentations}` | No-SSL builds skip unavailable encryption codecs. | Scope codec creation; suppress only `OPENSSL_ERROR`. | Fixed |
| FMT-P01 | `ReadWriteBufferFromHTTP::{tryGetFileSize,tryGetLastModificationTime}` | Metadata failures become `nullopt` after retries. | Decide whether exhausted network/server failures remain telemetry. | Policy decision |
| FMT-P02 | `CompressionCodecEncrypted::Configuration::tryLoad` | Failed key configuration reload returns `false`. | Decide before this currently unused production path gains callers. | Policy decision |
| FMT-P03 | `tryParseKQLQuery` | Parse failure returns `nullptr`, but its only production caller throws a replacement exception. | Treat as translation unless a successful caller is added. | Keep telemetry |
| FMT-P04 | `LongConnection::drainTail` in `ReaderExecutor.cpp` | Query can finish, but a failed drain makes the connection non-reusable. | Retain unless failed connection cleanup is declared non-error telemetry. | Policy decision |
| FMT-P05 | Zstd/zlib write-buffer cleanup catches | Data was written, but final codec cleanup failed. | Abnormal cleanup failures remain useful telemetry. | Keep telemetry |

## Query analysis, planning, execution, and row formats

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| EXEC-001 | `ConstantNode::toASTImpl` | Failed natural literal typing emits an explicit cast. | Scope `FieldToDataType`. | Fixed |
| EXEC-002 | `convertFieldToTypeCheckEnum` in `Analyzer/SetUtils.cpp` | Allowed unknown enum values become `nullopt`. | Suppress `UNKNOWN_ELEMENT_OF_ENUM`; record other rethrows. | Fixed |
| EXEC-003 | `InverseDictionaryLookupPass::isRewriteSemanticallySafe` | Failed constant folding skips an optional rewrite. | Scope folding. | Deferred |
| EXEC-004 | `ConvertOrLikeChainVisitor::enterImpl` | Invalid `LIKE` optimization leaves original branches. | Scope pattern conversion; record unexpected rethrows. | Fixed |
| EXEC-005 | `IdentifierResolver::tryResolveTableIdentifier` | Stale refresh UUID retries by public table name. | Suppress expected missing/mismatch codes; record others. | Fixed |
| EXEC-006 | `QueryAnalyzer::resolveTableFunction` | Unresolved remote table-function arguments are deferred to remote analysis. | Suppress `UNKNOWN_IDENTIFIER`; record other rethrows. | Fixed |
| EXEC-007 | Constant `if` and `multiIf` dead-branch resolution in `resolveFunction.cpp` | Invalid unreachable branches are discarded. | Scope only dead-branch resolution. | Deferred |
| EXEC-008 | Executable-UDF rewrite probe in `resolveFunction.cpp` | `BAD_ARGUMENTS` keeps the expression as an identifier. | Suppress only `BAD_ARGUMENTS`. | Fixed |
| EXEC-009 | `PlannerJoinTree` view pushdown probe | Failed view analysis skips `ORDER BY`/`LIMIT` pushdown. | Scope the sample-block probe. | Deferred |
| EXEC-010 | `ActionsDAG::tryFoldFunctionToConstant` | Best-effort fold failure leaves runtime execution. | Scope only best-effort mode; strict mode records. | Deferred |
| EXEC-011 | Default-input filter probes in `ActionsDAG` and query-plan optimization utilities | Evaluation failure conservatively returns false or unknown. | Scope isolated evaluation. | Partially fixed |
| EXEC-012 | `tryConvertFieldToType` | Conversion failure returns an empty `Field`. | Scope the conversion call. | Deferred |
| EXEC-013 | `parseColumnsListForTableFunction` | Invalid structure returns `false` plus diagnostic text. | Scope parsing/validation. | Deferred |
| EXEC-014 | `getRegexpJITMatcher` | JIT compilation failure falls back to RE2. | Scope compilation/cache insertion. | Deferred |
| EXEC-015 | `PreparedSets` clone probe | `NOT_IMPLEMENTED` selects the destructive set-build path. | Suppress only `NOT_IMPLEMENTED`. | Fixed |
| EXEC-016 | `MutationsNonDeterministicHelpers` | Failed constant evaluation leaves the expression unchanged. | Scope evaluation. | Deferred |
| EXEC-017 | `Cluster::Address::{getResolvedAddress,Address}` | DNS failure marks non-local; missing port retries with configured port. | Scope each probe. | Fixed |
| EXEC-018 | DDL host/address probes in `DDLTask.cpp` | Unknown remote hosts are skipped while a local host can be found. | Scope each probe; record a saved exception before terminal rethrow. | Fixed |
| EXEC-019 | `distributedIndexAnalysis.cpp` | Failed remote analysis falls back to initiator analysis. | Scope attempts; record delayed cancellation/terminal rethrow. | Deferred |
| EXEC-020 | `InterpreterSystemQuery::doRestartReplica` | Intermediate restart attempts can fail before later success. | Scope attempts; record non-retryable or exhausted failure. | Deferred |
| EXEC-021 | `InterpreterCreateQuery` with `allow_materialized_view_with_bad_select` | Configured compatibility mode accepts bad view analysis. | Scope only enabled compatibility path; preserve `ACCESS_DENIED`. | Deferred |
| EXEC-022 | `InterpreterCreateQuery` `IF NOT EXISTS` metadata and rename races | Existing/concurrently created object becomes a successful no-op. | Scope only no-op probes; record other failures. | Fixed |
| EXEC-023 | `InterpreterInsertQuery` pruning-DAG construction | Unsupported pruning falls back to unpruned insert. | Scope isolated filter analysis. | Deferred |
| EXEC-024 | `Context::tryCheckClientConnectionToMyKeeperCluster` | Health failure returns `false`. | Scope the complete probe. | Deferred |
| EXEC-025 | `TableNameHints::isHintNameVisible` | Failed optional lookup hides one hint. | Scope lookup. | Deferred |
| EXEC-026 | `InterpreterCreateFunctionQuery` old workdir read | Unreadable sidecar is repaired by recreation. | Scope sidecar read. | Fixed |
| EXEC-027 | `VersionMetadata::checkConsistency` | Disappearing part directory is an accepted race. | Suppress expected `CANNOT_OPEN_FILE`; record others. | Fixed |
| EXEC-028 | `VersionMetadataOnDisk::removeTmpMetadataFile` | Failed diagnostic read does not block cleanup. | Scope diagnostic read only. | Deferred |
| EXEC-029 | `ServerAsynchronousMetrics` replica-delay collection | `TABLE_IS_READ_ONLY` is an accepted transition race. | Suppress only that code. | Fixed |
| EXEC-030 | `FileSegment::renameToIncludeSizeInNameUnlocked` | Failed suffix rename keeps a valid legacy cache filename. | Scope rename/cache cleanup. | Fixed |
| EXEC-031 | `IRowInputFormat::read` | Configured parse errors skip malformed rows. | Scope each `readRow`; record branches which exceed limits or rethrow. | Fixed |
| EXEC-032 | `IRowInputFormat` and `RowInputFormatWithDiagnosticInfo` diagnostic reparsing | Secondary failures only enrich or omit diagnostics. | Scope diagnostic work so only the primary error is counted. | Fixed |
| EXEC-033 | Diagnostic delimiter/suffix probes in CSV, TSV, JSONCompact, and Template row formats | Throwing assertions are used only to produce diagnostics. | Scope secondary probes. | Fixed |
| EXEC-034 | `CSVRowInputFormat::readFieldOrDefault` | Bad field becomes default when configured. | Scope deserialization only in tolerant mode. | Fixed |
| EXEC-035 | Template format prefix/suffix probes | Parse failure means suffix not found. | Scope accepted parse codes; record others. | Fixed |
| EXEC-036 | `NativeORCBlockInputFormat::convertFieldToORCLiteral` | Failed literal conversion disables predicate pushdown. | Scope conversion. | Fixed |
| EXEC-037 | Arrow and Parquet schema conversion with skip-unsupported settings | Unsupported columns are omitted. | Scope only configured tolerant mode; record other codes. | Fixed |
| EXEC-038 | Arrow IPC reachable-buffer calculation | Failed pre-walk reads the whole body. | Scope pre-walk. | Fixed |
| EXEC-039 | `AvroConfluentSchemaRegistry::runWithRetry` | Intermediate transport failures can precede success. | Scope attempts; record final/non-retryable exception. | Fixed |
| EXEC-040 | Optional query-plan hash/preallocation calculation | Unserializable plans skip a preallocation hint. | Scope key calculation. | Fixed |
| EXEC-041 | Distributed join optimization probes | Incompatible keys keep a local join. | Scope common-type/distribution probes. | Fixed |
| EXEC-042 | `convertAnyJoinToSemiOrAntiJoin` filter evaluation | Unknown evaluation preserves the original join. | Scope partial evaluation. | Fixed |
| EXEC-043 | `ReadFromRemote::addLazyPipe` | Failed remote connections can fall back to stale local replica. | Scope acquisition; record saved exception before terminal rethrow. | Fixed |
| EXEC-P01 | `StreamingFormatExecutor` error callbacks | Dead-letter/stream callbacks can consume malformed messages. | Decide whether callback-handled messages are success; record callback rethrows. | Policy decision |
| EXEC-P02 | `IRowInputFormat` connection handling | Network errors can become end-of-input. | Decide whether expected stream disconnects remain telemetry. | Policy decision |
| EXEC-P03 | `InterpreterCheckQuery` | Exceptions become failed result rows. | The check operation reports failure as data; decide table semantics. | Policy decision |
| EXEC-P04 | `materialized_views_ignore_errors` | Main insert succeeds while a materialized-view insert genuinely fails. | Keep unless query-view logging is intended to replace system telemetry. | Policy decision |
| EXEC-P05 | External function driver/reload catches | DDL can succeed while a driver or reload remains stale. | Operationally material degraded state; decide explicitly. | Policy decision |
| EXEC-P06 | Dictionary/external-loader fallback catches | Old objects or unordered reload can remain usable after a real load failure. | Decide whether degraded loader health remains telemetry. | Policy decision |
| EXEC-P07 | `TransactionLog` unknown status | Finalization can continue asynchronously after Keeper failure. | Keeper failure is operationally real. | Policy decision |
| EXEC-P08 | Optional metrics, instrumentation, profiling, and shell-source probes | Primary query/server work continues without auxiliary telemetry. | Decide per subsystem; avoid one blanket policy. | Policy decision |
| EXEC-P09 | `PreparedSets` future rethrow | Exception is constructed on another thread and later rethrown. | Scope must be at construction; waiter catch is too late. | Policy decision |

## Storages, databases, disks, and backups

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| STOR-001 | `StorageSystemTables` optional row/byte statistics | Statistics failures produce zero/empty columns. | Scope each optional statistic probe. | Fixed |
| STOR-002 | `StorageSystemFunctions` resolver/UDF introspection | Unresolvable or disappearing functions are omitted. | Scope each lookup. | Fixed |
| STOR-003 | `StorageSystemStackTrace` `/proc` reads | Disappearing threads are skipped. | Scope TOCTOU probes. | Fixed |
| STOR-004 | `StorageSystemRemoteDataPaths` | Removed paths or incomplete metadata are skipped. | Scope per-path probe. | Fixed |
| STOR-005 | `StorageSystemDetachedParts`, `StorageSystemKeeperSnapshots`, `StorageSystemKeeperChangelogs`, `StorageSystemDDLWorkerQueue`, `StorageSystemIcebergHistory`, `StorageSystemIcebergFiles`, `StorageSystemZooKeeperInfo`, and `StorageSystemDictionaries` | Per-object failures become result defaults, error columns, or omitted stale rows. | Scope each isolated row probe; preserve terminal table-read failures. | Partially fixed |
| STOR-006 | `StatusRequestsPool` consumers in `StorageSystemReplicas` and `StorageSystemDatabaseReplicas` | `ABORTED` status requests are omitted. | Suppress `ABORTED`; record other future exceptions before propagation. | Fixed |
| STOR-007 | `WhatIfIndexEstimator`, `WhatIfStatisticalEstimator`, `ConditionTemplate`, `KeyCondition`, and statistics selectivity estimators | Failed optional estimates disable optimization. | Scope each estimate. | Fixed |
| STOR-008 | `MergeTreeDataSelectExecutor` and `MergeTreeCommittingBlock` probes | Failed optional analysis/finalization path uses conservative behavior. | Scope only accepted fallback branch. | Fixed |
| STOR-009 | `DataPartStorageOnDiskPacked` and `IMergeTreeDataPart` optional statistics/codec/projection/minmax metadata | Missing or old metadata uses legacy/default behavior. | Scope each optional read; record corruption/retryable propagation where applicable. | Partially fixed |
| STOR-010 | `checkDataPart` result-producing catches | Validation errors are returned in check results. | Suppress only exceptions represented in successful result rows. | Fixed |
| STOR-011 | MergeTree temporary-lock retries | Intermediate `PART_IS_TEMPORARILY_LOCKED` or `ABORTED` can later succeed. | Scope attempts; record timeout/non-retryable terminal error. | Fixed |
| STOR-012 | `StorageReplicatedMergeTree` detached/fetched-part races | Concurrent moves/creates select another valid path. | Scope expected races; record every terminal failure. | Fixed |
| STOR-013 | `StorageReplicatedMergeTree` local clone/fetch and queue compatibility fallbacks | Incompatible local data falls back to remote fetch or legacy parsing. | Scope probes; preserve final fetch/parse failure. | Partially fixed |
| STOR-014 | `StorageReplicatedMergeTree` replica refresh, status, fetch retries, `CHECK TABLE`, and backup metadata fallback | Optional status fields, retries, or fallback metadata can succeed. | Scope per attempt/probe; record terminal and retryable propagation. | Partially fixed |
| STOR-015 | `getStructureOfRemoteTable` | Failed shards are tried until another provides structure. | Scope attempts; final `NO_REMOTE_SHARD_AVAILABLE` remains recorded. | Fixed |
| STOR-016 | `StorageMerge` optional database metadata | `UNKNOWN_DATABASE` omits one source. | Suppress expected absence only. | Fixed |
| STOR-017 | `StorageMemory` zero-size missing backup entry | Missing empty payload is accepted. | Scope the optional entry read. | Fixed |
| STOR-018 | `StorageView` parallel-replica probe | Failed optimization uses ordinary execution. | Scope probe. | Deferred |
| STOR-019 | `StorageInMemoryMetadata` optional compatibility read | Failure uses existing/default metadata. | Scope accepted compatibility fallback. | Fixed |
| STOR-020 | `StorageDistributed` restore target analysis | Persisted columns are used when target analysis fails. | Scope analysis; preserve final invalid metadata failure. | Deferred |
| STOR-021 | `StorageFile` mmap and query-condition-cache probes | Reading falls back without mmap/cache. | Scope optional optimization. | Fixed |
| STOR-022 | `StorageURL` failover and engine dispatch probes | Another URL/engine can succeed. | Scope attempts; record saved exception when all fail. | Fixed |
| STOR-023 | `checkAndGetLiteralArgument` | Failed literal conversion returns an expected negative result. | Scope conversion. | Fixed |
| STOR-024 | `StorageObjectStorage` and source optional pruning/metadata probes | Failure uses conservative object reading. | Scope optional analysis. | Fixed |
| STOR-025 | `KeeperHandlingConsumer` | `ZNODEEXISTS` means another consumer owns the lock. | Suppress expected lock conflict. | Fixed |
| STOR-026 | `MaterializedPostgreSQLConsumer` | Expected structure mismatch or bad value uses configured fallback/default. | Scope tolerant mode; preserve strict failure. | Fixed |
| STOR-027 | `DistributedAsyncInsertBatch` | Split retries/per-file aggregation can recover. | Scope attempts; record terminal file failure. | Fixed |
| STOR-028 | Paimon latest-snapshot hint | Failed `LATEST` hint falls back to snapshot listing. | Scope hint probe. | Deferred |
| STOR-029 | Delta Lake total rows/bytes and token refresh | Optional totals become null; stale token retries. | Scope probes/attempts; record exhausted refresh. | Partially fixed |
| STOR-030 | Iceberg v1/v2 schema parsing and `IF NOT EXISTS` races | Alternate schema parser or concurrent object wins. | Scope attempts; preserve final aggregate/strict error. | Fixed |
| STOR-031 | Delta Lake engine-predicate pushdown | Failed optional predicate conversion uses ClickHouse filtering. | Scope only configured fallback; record configured propagation. | Fixed |
| STOR-032 | `DatabaseOverlay` delegated operations | Missing/unsupported child database falls through to another layer. | Scope each delegated probe. | Deferred |
| STOR-033 | `DatabaseHDFS` and `DatabaseS3` metadata probes | Missing remote metadata uses fallback/empty result. | Missing-file probes are suppressed; other storage errors remain telemetry. | Partially fixed |
| STOR-034 | `DDLDependencyVisitor` | Unresolvable dependency is omitted from best-effort collection. | Scope optional resolution. | Fixed |
| STOR-035 | PostgreSQL/MySQL database metadata probes | Per-table introspection failures skip stale objects. | Scope isolated probes. | Partially fixed |
| STOR-036 | `DatabaseReplicated` status, cluster, replica, expression, restore, and ALTER probes | Errors become nullable status, fallback parsing, idempotent restore, or conservative classification. | Scope per probe; preserve replacement/terminal exceptions. | Fixed |
| STOR-037 | Data Lake catalog metadata/listing/auth-refresh/conflict paths | Missing entries, expired auth, or conflict-style updates can recover. | Scope individual attempts; record terminal/non-conflict errors. | Partially fixed |
| STOR-038 | `StoragePolicy` and read-only disk detection | Unsupported/broken optional disk is skipped or marked read-only. | Scope expected capability probes. | Fixed |
| STOR-039 | File-read mmap and `O_DIRECT` initialization | Ordinary buffered I/O fallback succeeds. | Scope optional initialization. | Fixed |
| STOR-040 | Cached-on-disk read/write buffers | Cache miss/corruption/write failure can bypass cache and use source storage. | Scope cache-only operations; preserve source/terminal failure. | Fixed |
| STOR-041 | Web/S3 object-storage metadata and read failover | Alternate request/path can succeed. | Scope attempts; record saved terminal exception. | Fixed |
| STOR-042 | `DiskObjectStorageMetadata` optional metadata read | Missing metadata selects recovery/default path. | Scope optional read. | Deferred |
| STOR-043 | `BackupIO_Default` optional native operation | Unsupported optimization uses generic backup I/O. | Scope capability probe. | Deferred |
| STOR-044 | `BackupEntriesCollector` metadata retry | Inconsistent metadata can stabilize; disappearing optional database is skipped. | Scope attempts; record timeout/non-retryable terminal error. | Partially fixed |
| STOR-P01 | Degraded startup/attach for replicated, backup, Data Lake, and MySQL databases | Server/table attaches while a configured subsystem is broken. | Decide whether degraded availability should remain operational telemetry. | Policy decision |
| STOR-P02 | `DatabaseAtomic` auxiliary symlink cleanup | Metadata operation succeeds while convenience symlink update fails. | Decide whether filesystem degradation remains telemetry. | Policy decision |
| STOR-P03 | Object-storage removal-log corruption | Recovery continues despite malformed deletion metadata. | Keep corruption visible unless proven to be an expected torn-tail artifact. | Policy decision |
| STOR-P04 | Paimon at-most-once gap skipping | Missing sequence is intentionally skipped. | Data-loss semantics make this operationally material. | Policy decision |
| STOR-P05 | Iceberg write/conflict retry | A later commit can succeed after conflict. | Decide global retry policy; always record terminal conflict. | Policy decision |
| STOR-P06 | ObjectStorageQueue post-processing retries | Intermediate delete/move/tag attempts can recover. | Suppress only if successful retry is not telemetry; terminal failure must record. | Policy decision |
| STOR-P07 | Lost-part replacement and post-drop cleanup | Main operation continues after real local state loss/cleanup failure. | Retain unless dedicated telemetry fully replaces system errors. | Policy decision |
| STOR-P08 | Ignored detach failures and information-schema degraded attach | Server continues with incomplete cleanup or schema. | Operationally material; decide explicitly. | Policy decision |

## Common, core, access, coordination, and Keeper

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| COMMON-001 | `ExecutionStatus::tryDeserializeText` | Malformed status returns `false` without changing the object. | Scope temporary deserialization. | Fixed |
| COMMON-002 | `CounterInFile::fixIfBroken` | Empty/truncated counter file is repaired. | Scope parse; suppress expected EOF codes and record others. | Fixed |
| COMMON-003 | `NamedCollectionsMetadataStorage` refresh | Concurrently removed znode is skipped. | Suppress `ZNONODE`; record other Keeper exceptions. | Fixed |
| COMMON-004 | `QueryFuzzer` aggregate probes | Rejected random aggregate/type keeps old type or uses `count`. | Scope speculative factory calls. | Fixed |
| COMMON-005 | `MemoryWorker` cgroup and `/proc` probes | Missing/malformed optional files use host/jemalloc data or skip one update. | Scope each probe; preserve unexpected internal/resource failures. | Fixed |
| COMMON-006 | `AsynchronousMetrics` optional OS metric probes | One unavailable metric is omitted while the update succeeds. | Scope each independent probe; preserve unexpected errors. | Fixed |
| COMMON-007 | `OpenTelemetryTraceContext::Span::addAttribute` supplier | Throwing optional supplier returns `false`. | Scope supplier invocation. | Fixed |
| COMMON-008 | macOS dSYM lookup in `SymbolIndex` | Malformed optional symbols are ignored. | Scope dSYM parsing. | Fixed |
| COMMON-009 | `HTTPConnectionPool::atConnectionDestroy` | Pool-limit race discards reuse while request remains successful. | Suppress only `HTTP_CONNECTION_LIMIT_REACHED`. | Fixed |
| COMMON-010 | `CPULeaseAllocation::release` | Workload-drop race makes scheduler node disappear. | Suppress `INVALID_SCHEDULER_NODE`; record other rethrows. | Fixed |
| COMMON-011 | `ServerUUID` recovery | Malformed nonessential UUID is replaced. | Scope read/parse; replacement write remains recorded. | Fixed |
| COMMON-012 | `SettingsConstraints` non-throw/clamp mode | Bad cast/comparison skips or clamps a setting. | Scope only non-throw mode. | Fixed |
| COMMON-013 | `SettingsAuthResponseParser` | Successful HTTP auth ignores malformed optional settings body. | Scope expected settings parsing; preserve unexpected errors. | Fixed |
| COMMON-014 | `IAccessStorage` dependency cleanup | Read-only storage references cannot be updated but drop succeeds. | Suppress `ACCESS_STORAGE_READONLY`; retain other cleanup failures. | Fixed |
| COMMON-015 | `DiskAccessStorage` list-file rebuild | Corrupt derived index is rebuilt from entity files. | Scope list read; rebuild remains outside scope. | Fixed |
| COMMON-016 | `DiskAccessStorage` old-file deletion after replacement | New entity remains committed and restart marker repairs stale file. | Scope accepted stale-file cleanup failure. | Fixed |
| COMMON-017 | `GSSAcceptor` token processing | Expected Kerberos protocol error becomes failed auth state. | Suppress expected `KERBEROS_ERROR`; outer public auth failure records once. | Deferred |
| COMMON-018 | Replicated access user-error retry | Concurrent znode races can succeed on retry. | Scope attempts; record final/non-user error. | Fixed |
| COMMON-019 | `Changelog` torn final record | Keeper starts from valid prefix. | Suppress only expected tail truncation/EOF; retain corruption/version errors. | Fixed |
| COMMON-020 | `Changelog` move capability probe | `NOT_IMPLEMENTED` uses cross-disk move. | Scope native move; preserve other failures. | Fixed |
| COMMON-021 | Keeper snapshot-size probes | Snapshot works without cached/logged size. | Scope `getFileSize`. | Deferred |
| COMMON-022 | Keeper commit-thread profiler setup | Keeper commits continue without optional profiling. | Scope known profiler setup failures. | Deferred |
| COMMON-P01 | ZooKeeper, named-collection, access, YTsaurus, HTTP-auth, and Keeper retries | Transient backend attempt can later succeed. | Decide repository-wide retry telemetry policy; terminal failure always records. | Policy decision |
| COMMON-P02 | No-throw ZooKeeper APIs | Failure is returned as an error code rather than an exception. | The operation still failed; retain unless call-site semantics prove an expected probe. | Policy decision |
| COMMON-P03 | DNS fallback and allowed-host checks | Stale data or a negative check is used after DNS failure. | DNS health is operationally useful; decide narrowly. | Policy decision |
| COMMON-P04 | Ignored malformed workload/access/auth configuration | Server continues with invalid entity omitted. | Administrator error should normally remain visible. | Policy decision |
| COMMON-P05 | Keeper state/snapshot recovery | Older state can load after corruption/current-state failure. | Keep corruption visible; suppress only proven interrupted-write artifacts. | Policy decision |
| COMMON-P06 | Coverage instrumentation | Coverage work is optional, but failed writes are real. | Not normal production behavior; handle separately if needed. | Policy decision |

## Server and in-process client code

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| SRV-001 | `MySQLHandlerFactory` RSA key loading | Invalid configured key falls back to ephemeral RSA key. | Scope configured-key read; generated replacement remains outside scope. | Fixed |
| SRV-002 | `PlacementInfo::initialize` | Failed placement discovery becomes empty placement. | Scope discovery only. | Fixed |
| SRV-003 | `TCPHandler::receiveHello` certificate auth | `AUTHENTICATION_FAILED` retries password auth. | Scope certificate attempt; record all other rethrows. | Fixed |
| SRV-004 | PostgreSQL `COPY`, `PREPARE`, `EXECUTE`, and `DEALLOCATE` probes | Nonmatching SQL falls through to another handler. | Scope each grammar probe. | Fixed |
| SRV-005 | Arrow Flight prepared-statement schema inference | Failed null-parameter inference returns statement without schema. | Scope inference only; actual execution remains recorded. | Fixed |
| SRV-006 | `Connection::connect` multi-address loop | Later resolved address can connect. | Scope per address; record final failure. | Fixed |
| SRV-007 | `Connection::ping` | Stale/broken connection returns `false` for reconnect/failover. | Scope ping. | Fixed |
| SRV-008 | `ConnectionEstablisher::run` | Stale pooled connection reconnects or another replica is selected. | Scope attempts; record unexpected/final exception. | Fixed |
| SRV-009 | `ClientBaseHelpers::highlight` | Parser failure falls back to lexer/partial highlighting. | Scope optional parser passes. | Fixed |
| SRV-010 | `ClientBase::initTTYBuffer` | Missing `/dev/tty` disables progress rendering. | Suppress only `CANNOT_OPEN_FILE`. | Fixed |
| SRV-011 | `ClientBase::processOrdinaryQuery` | `DEADLOCK_AVOIDED` retries before rows are received. | Scope attempts; record exhausted/non-deadlock failure. | Fixed |
| SRV-012 | Interactive history-file creation | Concurrent `EEXIST` is accepted. | Suppress only `EEXIST`; retain other filesystem errors. | Fixed |
| SRV-013 | `Suggest::load` | Deadlock retries or session-limit fallback can later load suggestions. | Scope attempts; record terminal/unexpected failure. | Fixed |
| SRV-014 | Keystroke interceptor start/stop | Optional terminal interception is disabled. | Scope optional UI operation. | Fixed |
| SRV-015 | `executeQueryForSingleString` AI schema lookup | Failure returns empty schema and AI work may continue. | Scope complete query/packet handling. | Fixed |
| SRV-016 | `programs/server/Server.cpp::sanityChecks` | Missing optional procfs/sysfs/mdraid diagnostics do not block startup. | One scope per probe. | Fixed |
| SRV-P01 | MySQL session/default-database setup | Handler emits error but also continues handshake. | Clarify protocol contract before suppression. | Policy decision |
| SRV-P02 | Arrow Flight per-setting errors | RPC succeeds with structured setting errors. | Suppress expected validation only; retain internal errors. | Policy decision |
| SRV-P03 | SSH authentication method negotiation | One method can fail before another succeeds. | Authentication failures are also security telemetry. | Policy decision |
| SRV-P04 | Multi-address listener `listen_try` | Another bind can succeed, usually after a Poco-only error. | Do not blanket-scope; suppress only demonstrated expected `DB::Exception`. | Policy decision |
| SRV-P05 | OOM score, `mlock`, and cgroup observer setup | Server continues without requested OS tuning/observation. | Requested configuration failure is operationally useful. | Policy decision |
| SRV-P06 | Startup scripts, discovery, certificate reload, and ACME | Server continues/retries after a configured subsystem fails. | Retain telemetry. | Keep telemetry |
| SRV-P07 | Terminal editor/search/help and AI initialization | Shell remains alive, but requested feature visibly failed. | Usually retain; suppress only true optional capability probes. | Policy decision |

Standalone `clickhouse-client`, disks/install/keeper utility catches can be made internally consistent,
but they cannot populate a running server's `system.errors` or `system.error_log`. They are classified as
`Not server-visible` and are outside this cleanup.

## Functions, table functions, and bridges

| ID | Location | Handled outcome | Required treatment | Status |
|---|---|---|---|---|
| FUNC-001 | `FunctionCaseWithExpression::executeImpl` | `NOT_IMPLEMENTED` in `transform` falls back to `multiIf`. | Scope build; record any other rethrow. | Fixed |
| FUNC-002 | `FunctionBaseAI::executeImpl` and `FunctionAiEmbed::executeImpl` | Attempts retry; non-throw mode returns empty/default result. | A scope may span fiber-aware provider I/O; defer until suppression state is execution-context-local. | Deferred |
| FUNC-003 | `formatQueryOrNull` and `formatQuerySingleLineOrNull` | Malformed SQL becomes `NULL`. | Scope parse only in null mode. | Fixed |
| FUNC-004 | `highlightQuery` | Syntax failure returns partial highlighting. | Scope parser; suppress syntax errors and preserve others. | Fixed |
| FUNC-005 | `parseReadableSizeOrZero` and `parseReadableSizeOrNull` | Invalid input becomes zero or `NULL`. | Scope parse only in tolerant modes. | Fixed |
| FUNC-006 | `file(path, default)` | Missing/inaccessible file returns default or `NULL`. | Scope file access only when fallback argument exists. | Fixed |
| FUNC-007 | Non-throwing generic conversion in `FunctionsConversion.h` | Deserialization failure inserts default/`NULL`. | Scope only `throw_on_error == false`. | Fixed |
| FUNC-008 | `FunctionCast::createWrapperIfCanConvert` | Unsupported Variant/Dynamic alternative is skipped. | Scope wrapper construction/execution probe. | Fixed |
| FUNC-009 | `FunctionDynamicAdaptor` tolerant build/execute | Type mismatch produces `NULL` rows. | Scope tolerant mode; record unexpected errors. | Fixed |
| FUNC-010 | `FunctionVariantAdaptor` tolerant build/execute and return-type inference | Incompatible alternatives are skipped or become `NULL`. | Scope each tolerant probe; strict final aggregate error records. | Fixed |
| FUNC-011 | `TableFunctionViewIfPermitted::isPermitted` | `ACCESS_DENIED` selects the `ELSE` table function. | Scope sample-block probe; record other rethrows. | Fixed |
| FUNC-012 | CatBoost, external-dictionary, and XDBC bridge handshakes/reinitialization | Failed availability probe starts/restarts bridge and can later succeed. | Scope each HTTP probe; final protocol/availability error remains recorded. | Fixed |
| FUNC-P01 | Executable UDF stdin `EPIPE` | Driver may have valid output despite not consuming all input. | Prove a successful contract before suppressing only `EPIPE`. | Policy decision |
| FUNC-P02 | External-loader invalidation/dependency/update-time failures | Old object may remain usable but refresh health is degraded. | Keep unless loaded-but-stale is defined as successful fallback. | Policy decision |

## Reviewed catches which should remain recorded

The following broad categories were reviewed and are intentionally not suppression targets:

- Cleanup followed by unconditional rethrow, exception annotation, or exception translation where the
  query or operation still fails.
- Request handlers that catch only to send the actual SQL/protocol error to the client.
- Failed background merges, mutations, replication, certificate reloads, dictionary refreshes, uploads,
  and writes where retrying or keeping the process alive does not make the failed attempt successful.
- Destructor/finalizer failures involving data flush, compression finalization, deletion, or durable
  metadata cleanup.
- Corruption, checksum, logical, data-loss, and security/authentication failures unless a row above names
  a narrower expected branch.
- Plain Poco, standard-library, SDK, or third-party exceptions which never construct `DB::Exception` and
  therefore cannot affect the system error tables.

## Verification requirements

Each fix needs evidence for both sides of the contract:

1. The handled path succeeds or returns its documented negative/default value without changing the
   relevant local error count or attributing an entry in `system.error_log` to the query.
2. A control path where the same or an unexpected exception propagates records it exactly once.

Query-level tests should use unique query IDs and the assertion pattern from
`04613_values_expression_fallback_no_system_errors.sh`. Unit tests can compare
`ErrorCodes::getErrorCodeByName` counters as in `gtest_exception.cpp`.

## Current verification

The current implementation status is 122 fixed groups (including `VALUES-001`), 10 partially fixed
groups, 25 deferred groups, 34 policy decisions, and 3 groups which intentionally keep telemetry.

Verification completed for this pass:

- `unit_tests_dbms` and `clickhouse` build successfully without warnings.
- The focused `Exception` and `ExecutionStatus` unit tests passed 1,200,000 executions over 200,000
  repetitions in 56 seconds.
- `04626_handled_fallbacks_no_system_errors` passed 20/20 repetitions in 58 seconds.
- `04613_values_expression_fallback_no_system_errors` passed 20/20 repetitions in 53 seconds.
- Six existing tolerant conversion/parser/function tests passed 600/600 repetitions in 61 seconds.
- The two existing Protobuf nested-layout fallback tests passed 140/140 repetitions in 54 seconds.
- `check_cpp.sh` completed successfully and `various_checks.sh` produced no findings.
