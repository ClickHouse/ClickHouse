#pragma once

/// Universal executable for various clickhouse applications
#if ENABLE_CLICKHOUSE_SERVER || !defined(ENABLE_CLICKHOUSE_SERVER)
int mainEntryClickHouseServer(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_CLIENT || !defined(ENABLE_CLICKHOUSE_CLIENT)
int mainEntryClickHouseClient(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_LOCAL || !defined(ENABLE_CLICKHOUSE_LOCAL)
int mainEntryClickHouseLocal(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_BENCHMARK || !defined(ENABLE_CLICKHOUSE_BENCHMARK)
int mainEntryClickHouseBenchmark(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_PERFORMANCE_TEST || !defined(ENABLE_CLICKHOUSE_PERFORMANCE_TEST)
int mainEntryClickHousePerformanceTest(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG || !defined(ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG)
int mainEntryClickHouseExtractFromConfig(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_COMPRESSOR || !defined(ENABLE_CLICKHOUSE_COMPRESSOR)
int mainEntryClickHouseCompressor(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_FORMAT || !defined(ENABLE_CLICKHOUSE_FORMAT)
int mainEntryClickHouseFormat(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_COPIER || !defined(ENABLE_CLICKHOUSE_COPIER)
int mainEntryClickHouseClusterCopier(int argc, char ** argv);
#endif
#if ENABLE_CLICKHOUSE_OBFUSCATOR || !defined(ENABLE_CLICKHOUSE_OBFUSCATOR)
int mainEntryClickHouseObfuscator(int argc, char ** argv);
#endif


#if USE_EMBEDDED_COMPILER
int mainEntryClickHouseClang(int argc, char ** argv);
int mainEntryClickHouseLLD(int argc, char ** argv);
#endif