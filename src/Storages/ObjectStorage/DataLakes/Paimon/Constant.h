#pragma once

namespace Paimon
{
/// for schema
constexpr const char * PAIMON_SCHEMA_PREFIX = "schema-";
constexpr const char * PAIMON_SCHEMA_DIR = "schema";
/// for snapshot
constexpr const char * PAIMON_SNAPSHOT_DIR = "snapshot";
constexpr const char * PAIMON_SNAPSHOT_PRIFIX = "snapshot-";
constexpr const char * PAIMON_SNAPSHOT_EARLIEST_HINT = "EARLIEST";
constexpr const char * PAIMON_SNAPSHOT_LATEST_HINT = "LATEST";
/// for options
constexpr const char * PAIMON_SCAN_MODE = "scan.mode";
constexpr const char * PAIMON_DEFAULT_PARTITION_NAME = "partition.default-name";
/// for manifest list
constexpr const char * PAIMON_MANIFEST_DIR = "manifest";
constexpr const char * COLUMN_PAIMON_MANIFEST_LIST_FILE_NAME = "_FILE_NAME";
constexpr const char * COLUMN_PAIMON_MANIFEST_LIST_FILE_SIZE = "_FILE_SIZE";
constexpr const char * COLUMN_PAIMON_MANIFEST_LIST_NUM_ADDED_FILES = "_NUM_ADDED_FILES";
constexpr const char * COLUMN_PAIMON_MANIFEST_LIST_NUM_DELETED_FILES = "_NUM_DELETED_FILES";
constexpr const char * COLUMN_PAIMON_MANIFEST_LIST_PARTITION_STATS = "_PARTITION_STATS";
constexpr const char * COLUMN_PAIMON_MANIFEST_SCHEMA_ID = "_SCHEMA_ID";

/// for simple stats
constexpr const char * COLUMN_SIMPLE_STATS_MAX_VALUES = "_MAX_VALUES";
constexpr const char * COLUMN_SIMPLE_STATS_MIN_VALUES = "_MIN_VALUES";
constexpr const char * COLUMN_SIMPLE_STATS_NULL_COUNTS = "_NULL_COUNTS";

/// for data manifest
constexpr const char * COLUMN_PAIMON_MANIFEST_KIND = "_KIND";
constexpr const char * COLUMN_PAIMON_MANIFEST_PARTITION = "_PARTITION";
constexpr const char * COLUMN_PAIMON_MANIFEST_BUCKET = "_BUCKET";
constexpr const char * COLUMN_PAIMON_MANIFEST_TOTAL_BUCKETS = "_TOTAL_BUCKETS";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE = "_FILE";

/// for data file meta
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_FILE_NAME = "_FILE_NAME";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_FILE_SIZE = "_FILE_SIZE";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_ROW_COUNT = "_ROW_COUNT";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_MIN_KEY = "_MIN_KEY";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_MAX_KEY = "_MAX_KEY";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_KEY_STATS = "_KEY_STATS";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_VALUE_STATS = "_VALUE_STATS";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_MIN_SEQUENCE_NUMBER = "_MIN_SEQUENCE_NUMBER";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_MAX_SEQUENCE_NUMBER = "_MAX_SEQUENCE_NUMBER";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_SCHEMA_ID = "_SCHEMA_ID";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_LEVEL = "_LEVEL";
constexpr const char * COLUMN_PAIMON_MANIFEST_EXTRA_FILES = "_EXTRA_FILES";
constexpr const char * COLUMN_PAIMON_MANIFEST_CREATION_TIME = "_CREATION_TIME";
constexpr const char * COLUMN_PAIMON_MANIFEST_DELETE_ROW_COUNT = "_DELETE_ROW_COUNT";
constexpr const char * COLUMN_PAIMON_MANIFEST_EMBEDDED_FILE_INDEX = "_EMBEDDED_FILE_INDEX";
constexpr const char * COLUMN_PAIMON_MANIFEST_FILE_SOURCE = "_FILE_SOURCE";
constexpr const char * COLUMN_PAIMON_MANIFEST_VALUE_STATS_COLS = "_VALUE_STATS_COLS";
constexpr const char * COLUMN_PAIMON_MANIFEST_EXTERNAL_PATH = "_EXTERNAL_PATH";

}
