
#include "SqlMode.h"
#include "MySQLBaseParser.h"
  // import { MySQLBaseParser } from './MySQLBaseParser'
  // import { SqlMode } from './common'


// Generated from MySQLParser.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"




class  MySQLParser : public MySQLBaseParser {
public:
  enum {
    ACCOUNT_SYMBOL = 1, ASCII_SYMBOL = 2, ALWAYS_SYMBOL = 3, BACKUP_SYMBOL = 4, 
    BEGIN_SYMBOL = 5, BYTE_SYMBOL = 6, CACHE_SYMBOL = 7, CHARSET_SYMBOL = 8, 
    CHECKSUM_SYMBOL = 9, CLOSE_SYMBOL = 10, COMMENT_SYMBOL = 11, COMMIT_SYMBOL = 12, 
    CONTAINS_SYMBOL = 13, DEALLOCATE_SYMBOL = 14, DO_SYMBOL = 15, END_SYMBOL = 16, 
    EXECUTE_SYMBOL = 17, FLUSH_SYMBOL = 18, FOLLOWS_SYMBOL = 19, FORMAT_SYMBOL = 20, 
    GROUP_REPLICATION_SYMBOL = 21, HANDLER_SYMBOL = 22, HELP_SYMBOL = 23, 
    HOST_SYMBOL = 24, INSTALL_SYMBOL = 25, LANGUAGE_SYMBOL = 26, NO_SYMBOL = 27, 
    OPEN_SYMBOL = 28, OPTIONS_SYMBOL = 29, OWNER_SYMBOL = 30, PARSER_SYMBOL = 31, 
    PARTITION_SYMBOL = 32, PORT_SYMBOL = 33, PRECEDES_SYMBOL = 34, PREPARE_SYMBOL = 35, 
    REMOVE_SYMBOL = 36, REPAIR_SYMBOL = 37, RESET_SYMBOL = 38, RESTORE_SYMBOL = 39, 
    ROLLBACK_SYMBOL = 40, SAVEPOINT_SYMBOL = 41, SECURITY_SYMBOL = 42, SERVER_SYMBOL = 43, 
    SIGNED_SYMBOL = 44, SLAVE_SYMBOL = 45, SOCKET_SYMBOL = 46, SONAME_SYMBOL = 47, 
    START_SYMBOL = 48, STOP_SYMBOL = 49, TRUNCATE_SYMBOL = 50, UNICODE_SYMBOL = 51, 
    UNINSTALL_SYMBOL = 52, UPGRADE_SYMBOL = 53, WRAPPER_SYMBOL = 54, XA_SYMBOL = 55, 
    SHUTDOWN_SYMBOL = 56, ACTION_SYMBOL = 57, ADDDATE_SYMBOL = 58, AFTER_SYMBOL = 59, 
    AGAINST_SYMBOL = 60, AGGREGATE_SYMBOL = 61, ALGORITHM_SYMBOL = 62, ANALYZE_SYMBOL = 63, 
    ANY_SYMBOL = 64, AT_SYMBOL = 65, AUTHORS_SYMBOL = 66, AUTO_INCREMENT_SYMBOL = 67, 
    AUTOEXTEND_SIZE_SYMBOL = 68, AVG_ROW_LENGTH_SYMBOL = 69, AVG_SYMBOL = 70, 
    BINLOG_SYMBOL = 71, BIT_SYMBOL = 72, BLOCK_SYMBOL = 73, BOOL_SYMBOL = 74, 
    BOOLEAN_SYMBOL = 75, BTREE_SYMBOL = 76, CASCADED_SYMBOL = 77, CATALOG_NAME_SYMBOL = 78, 
    CHAIN_SYMBOL = 79, CHANGED_SYMBOL = 80, CHANNEL_SYMBOL = 81, CIPHER_SYMBOL = 82, 
    CLIENT_SYMBOL = 83, CLASS_ORIGIN_SYMBOL = 84, COALESCE_SYMBOL = 85, 
    CODE_SYMBOL = 86, COLLATION_SYMBOL = 87, COLUMN_NAME_SYMBOL = 88, COLUMN_FORMAT_SYMBOL = 89, 
    COLUMNS_SYMBOL = 90, COMMITTED_SYMBOL = 91, COMPACT_SYMBOL = 92, COMPLETION_SYMBOL = 93, 
    COMPRESSED_SYMBOL = 94, COMPRESSION_SYMBOL = 95, ENCRYPTION_SYMBOL = 96, 
    CONCURRENT_SYMBOL = 97, CONNECTION_SYMBOL = 98, CONSISTENT_SYMBOL = 99, 
    CONSTRAINT_CATALOG_SYMBOL = 100, CONSTRAINT_SCHEMA_SYMBOL = 101, CONSTRAINT_NAME_SYMBOL = 102, 
    CONTEXT_SYMBOL = 103, CONTRIBUTORS_SYMBOL = 104, CPU_SYMBOL = 105, CUBE_SYMBOL = 106, 
    CURRENT_SYMBOL = 107, CURSOR_NAME_SYMBOL = 108, DATA_SYMBOL = 109, DATAFILE_SYMBOL = 110, 
    DATETIME_SYMBOL = 111, DATE_SYMBOL = 112, DAY_SYMBOL = 113, DEFAULT_AUTH_SYMBOL = 114, 
    DEFINER_SYMBOL = 115, DELAY_KEY_WRITE_SYMBOL = 116, DES_KEY_FILE_SYMBOL = 117, 
    DIAGNOSTICS_SYMBOL = 118, DIRECTORY_SYMBOL = 119, DISABLE_SYMBOL = 120, 
    DISCARD_SYMBOL = 121, DISK_SYMBOL = 122, DUMPFILE_SYMBOL = 123, DUPLICATE_SYMBOL = 124, 
    DYNAMIC_SYMBOL = 125, ENDS_SYMBOL = 126, ENUM_SYMBOL = 127, ENGINE_SYMBOL = 128, 
    ENGINES_SYMBOL = 129, ERROR_SYMBOL = 130, ERRORS_SYMBOL = 131, ESCAPE_SYMBOL = 132, 
    EVENT_SYMBOL = 133, EVENTS_SYMBOL = 134, EVERY_SYMBOL = 135, EXPANSION_SYMBOL = 136, 
    EXPORT_SYMBOL = 137, EXTENDED_SYMBOL = 138, EXTENT_SIZE_SYMBOL = 139, 
    FAULTS_SYMBOL = 140, FAST_SYMBOL = 141, FOUND_SYMBOL = 142, ENABLE_SYMBOL = 143, 
    FULL_SYMBOL = 144, FILE_SYMBOL = 145, FILE_BLOCK_SIZE_SYMBOL = 146, 
    FILTER_SYMBOL = 147, FIRST_SYMBOL = 148, FIXED_SYMBOL = 149, GENERAL_SYMBOL = 150, 
    GEOMETRY_SYMBOL = 151, GEOMETRYCOLLECTION_SYMBOL = 152, GET_FORMAT_SYMBOL = 153, 
    GRANTS_SYMBOL = 154, GLOBAL_SYMBOL = 155, HASH_SYMBOL = 156, HOSTS_SYMBOL = 157, 
    HOUR_SYMBOL = 158, IDENTIFIED_SYMBOL = 159, IGNORE_SERVER_IDS_SYMBOL = 160, 
    INVOKER_SYMBOL = 161, IMPORT_SYMBOL = 162, INDEXES_SYMBOL = 163, INITIAL_SIZE_SYMBOL = 164, 
    INSTANCE_SYMBOL = 165, INNODB_SYMBOL = 166, IO_SYMBOL = 167, IPC_SYMBOL = 168, 
    ISOLATION_SYMBOL = 169, ISSUER_SYMBOL = 170, INSERT_METHOD_SYMBOL = 171, 
    JSON_SYMBOL = 172, KEY_BLOCK_SIZE_SYMBOL = 173, LAST_SYMBOL = 174, LEAVES_SYMBOL = 175, 
    LESS_SYMBOL = 176, LEVEL_SYMBOL = 177, LINESTRING_SYMBOL = 178, LIST_SYMBOL = 179, 
    LOCAL_SYMBOL = 180, LOCKS_SYMBOL = 181, LOGFILE_SYMBOL = 182, LOGS_SYMBOL = 183, 
    MAX_ROWS_SYMBOL = 184, MASTER_SYMBOL = 185, MASTER_HEARTBEAT_PERIOD_SYMBOL = 186, 
    MASTER_HOST_SYMBOL = 187, MASTER_PORT_SYMBOL = 188, MASTER_LOG_FILE_SYMBOL = 189, 
    MASTER_LOG_POS_SYMBOL = 190, MASTER_USER_SYMBOL = 191, MASTER_PASSWORD_SYMBOL = 192, 
    MASTER_SERVER_ID_SYMBOL = 193, MASTER_CONNECT_RETRY_SYMBOL = 194, MASTER_RETRY_COUNT_SYMBOL = 195, 
    MASTER_DELAY_SYMBOL = 196, MASTER_SSL_SYMBOL = 197, MASTER_SSL_CA_SYMBOL = 198, 
    MASTER_SSL_CAPATH_SYMBOL = 199, MASTER_TLS_VERSION_SYMBOL = 200, MASTER_SSL_CERT_SYMBOL = 201, 
    MASTER_SSL_CIPHER_SYMBOL = 202, MASTER_SSL_CRL_SYMBOL = 203, MASTER_SSL_CRLPATH_SYMBOL = 204, 
    MASTER_SSL_KEY_SYMBOL = 205, MASTER_AUTO_POSITION_SYMBOL = 206, MAX_CONNECTIONS_PER_HOUR_SYMBOL = 207, 
    MAX_QUERIES_PER_HOUR_SYMBOL = 208, MAX_STATEMENT_TIME_SYMBOL = 209, 
    MAX_SIZE_SYMBOL = 210, MAX_UPDATES_PER_HOUR_SYMBOL = 211, MAX_USER_CONNECTIONS_SYMBOL = 212, 
    MEDIUM_SYMBOL = 213, MEMORY_SYMBOL = 214, MERGE_SYMBOL = 215, MESSAGE_TEXT_SYMBOL = 216, 
    MICROSECOND_SYMBOL = 217, MIGRATE_SYMBOL = 218, MINUTE_SYMBOL = 219, 
    MIN_ROWS_SYMBOL = 220, MODIFY_SYMBOL = 221, MODE_SYMBOL = 222, MONTH_SYMBOL = 223, 
    MULTILINESTRING_SYMBOL = 224, MULTIPOINT_SYMBOL = 225, MULTIPOLYGON_SYMBOL = 226, 
    MUTEX_SYMBOL = 227, MYSQL_ERRNO_SYMBOL = 228, NAME_SYMBOL = 229, NAMES_SYMBOL = 230, 
    NATIONAL_SYMBOL = 231, NCHAR_SYMBOL = 232, NDBCLUSTER_SYMBOL = 233, 
    NEVER_SYMBOL = 234, NEXT_SYMBOL = 235, NEW_SYMBOL = 236, NO_WAIT_SYMBOL = 237, 
    NODEGROUP_SYMBOL = 238, NONE_SYMBOL = 239, NUMBER_SYMBOL = 240, NVARCHAR_SYMBOL = 241, 
    OFFSET_SYMBOL = 242, OLD_PASSWORD_SYMBOL = 243, ONE_SHOT_SYMBOL = 244, 
    ONE_SYMBOL = 245, PACK_KEYS_SYMBOL = 246, PAGE_SYMBOL = 247, PARTIAL_SYMBOL = 248, 
    PARTITIONING_SYMBOL = 249, PARTITIONS_SYMBOL = 250, PASSWORD_SYMBOL = 251, 
    PHASE_SYMBOL = 252, PLUGIN_DIR_SYMBOL = 253, PLUGIN_SYMBOL = 254, PLUGINS_SYMBOL = 255, 
    POINT_SYMBOL = 256, POLYGON_SYMBOL = 257, PRESERVE_SYMBOL = 258, PREV_SYMBOL = 259, 
    PRIVILEGES_SYMBOL = 260, PROCESS_SYMBOL = 261, PROCESSLIST_SYMBOL = 262, 
    PROFILE_SYMBOL = 263, PROFILES_SYMBOL = 264, PROXY_SYMBOL = 265, QUARTER_SYMBOL = 266, 
    QUERY_SYMBOL = 267, QUICK_SYMBOL = 268, READ_ONLY_SYMBOL = 269, REBUILD_SYMBOL = 270, 
    RECOVER_SYMBOL = 271, REDO_BUFFER_SIZE_SYMBOL = 272, REDOFILE_SYMBOL = 273, 
    REDUNDANT_SYMBOL = 274, RELAY_SYMBOL = 275, RELAYLOG_SYMBOL = 276, RELAY_LOG_FILE_SYMBOL = 277, 
    RELAY_LOG_POS_SYMBOL = 278, RELAY_THREAD_SYMBOL = 279, RELOAD_SYMBOL = 280, 
    REORGANIZE_SYMBOL = 281, REPEATABLE_SYMBOL = 282, REPLICATION_SYMBOL = 283, 
    REPLICATE_DO_DB_SYMBOL = 284, REPLICATE_IGNORE_DB_SYMBOL = 285, REPLICATE_DO_TABLE_SYMBOL = 286, 
    REPLICATE_IGNORE_TABLE_SYMBOL = 287, REPLICATE_WILD_DO_TABLE_SYMBOL = 288, 
    REPLICATE_WILD_IGNORE_TABLE_SYMBOL = 289, REPLICATE_REWRITE_DB_SYMBOL = 290, 
    RESUME_SYMBOL = 291, RETURNED_SQLSTATE_SYMBOL = 292, RETURNS_SYMBOL = 293, 
    REVERSE_SYMBOL = 294, ROLLUP_SYMBOL = 295, ROTATE_SYMBOL = 296, ROUTINE_SYMBOL = 297, 
    ROWS_SYMBOL = 298, ROW_COUNT_SYMBOL = 299, ROW_FORMAT_SYMBOL = 300, 
    ROW_SYMBOL = 301, RTREE_SYMBOL = 302, SCHEDULE_SYMBOL = 303, SCHEMA_NAME_SYMBOL = 304, 
    SECOND_SYMBOL = 305, SERIAL_SYMBOL = 306, SERIALIZABLE_SYMBOL = 307, 
    SESSION_SYMBOL = 308, SIMPLE_SYMBOL = 309, SHARE_SYMBOL = 310, SLOW_SYMBOL = 311, 
    SNAPSHOT_SYMBOL = 312, SOUNDS_SYMBOL = 313, SOURCE_SYMBOL = 314, SQL_AFTER_GTIDS_SYMBOL = 315, 
    SQL_AFTER_MTS_GAPS_SYMBOL = 316, SQL_BEFORE_GTIDS_SYMBOL = 317, SQL_CACHE_SYMBOL = 318, 
    SQL_BUFFER_RESULT_SYMBOL = 319, SQL_NO_CACHE_SYMBOL = 320, SQL_THREAD_SYMBOL = 321, 
    STACKED_SYMBOL = 322, STARTS_SYMBOL = 323, STATS_AUTO_RECALC_SYMBOL = 324, 
    STATS_PERSISTENT_SYMBOL = 325, STATS_SAMPLE_PAGES_SYMBOL = 326, STATUS_SYMBOL = 327, 
    STORAGE_SYMBOL = 328, STRING_SYMBOL = 329, SUBCLASS_ORIGIN_SYMBOL = 330, 
    SUBDATE_SYMBOL = 331, SUBJECT_SYMBOL = 332, SUBPARTITION_SYMBOL = 333, 
    SUBPARTITIONS_SYMBOL = 334, SUPER_SYMBOL = 335, SUSPEND_SYMBOL = 336, 
    SWAPS_SYMBOL = 337, SWITCHES_SYMBOL = 338, TABLE_NAME_SYMBOL = 339, 
    TABLES_SYMBOL = 340, TABLE_CHECKSUM_SYMBOL = 341, TABLESPACE_SYMBOL = 342, 
    TEMPORARY_SYMBOL = 343, TEMPTABLE_SYMBOL = 344, TEXT_SYMBOL = 345, THAN_SYMBOL = 346, 
    TRANSACTION_SYMBOL = 347, TRIGGERS_SYMBOL = 348, TIMESTAMP_SYMBOL = 349, 
    TIMESTAMP_ADD_SYMBOL = 350, TIMESTAMP_DIFF_SYMBOL = 351, TIME_SYMBOL = 352, 
    TYPES_SYMBOL = 353, TYPE_SYMBOL = 354, UDF_RETURNS_SYMBOL = 355, FUNCTION_SYMBOL = 356, 
    UNCOMMITTED_SYMBOL = 357, UNDEFINED_SYMBOL = 358, UNDO_BUFFER_SIZE_SYMBOL = 359, 
    UNDOFILE_SYMBOL = 360, UNKNOWN_SYMBOL = 361, UNTIL_SYMBOL = 362, USER_RESOURCES_SYMBOL = 363, 
    USER_SYMBOL = 364, USE_FRM_SYMBOL = 365, VARIABLES_SYMBOL = 366, VIEW_SYMBOL = 367, 
    VALUE_SYMBOL = 368, WARNINGS_SYMBOL = 369, WAIT_SYMBOL = 370, WEEK_SYMBOL = 371, 
    WORK_SYMBOL = 372, WEIGHT_STRING_SYMBOL = 373, X509_SYMBOL = 374, XID_SYMBOL = 375, 
    XML_SYMBOL = 376, YEAR_SYMBOL = 377, NOT2_SYMBOL = 378, CONCAT_PIPES_SYMBOL = 379, 
    INT_NUMBER = 380, LONG_NUMBER = 381, ULONGLONG_NUMBER = 382, EQUAL_OPERATOR = 383, 
    ASSIGN_OPERATOR = 384, NULL_SAFE_EQUAL_OPERATOR = 385, GREATER_OR_EQUAL_OPERATOR = 386, 
    GREATER_THAN_OPERATOR = 387, LESS_OR_EQUAL_OPERATOR = 388, LESS_THAN_OPERATOR = 389, 
    NOT_EQUAL_OPERATOR = 390, PLUS_OPERATOR = 391, MINUS_OPERATOR = 392, 
    MULT_OPERATOR = 393, DIV_OPERATOR = 394, MOD_OPERATOR = 395, LOGICAL_NOT_OPERATOR = 396, 
    BITWISE_NOT_OPERATOR = 397, SHIFT_LEFT_OPERATOR = 398, SHIFT_RIGHT_OPERATOR = 399, 
    LOGICAL_AND_OPERATOR = 400, BITWISE_AND_OPERATOR = 401, BITWISE_XOR_OPERATOR = 402, 
    LOGICAL_OR_OPERATOR = 403, BITWISE_OR_OPERATOR = 404, DOT_SYMBOL = 405, 
    COMMA_SYMBOL = 406, SEMICOLON_SYMBOL = 407, COLON_SYMBOL = 408, OPEN_PAR_SYMBOL = 409, 
    CLOSE_PAR_SYMBOL = 410, OPEN_CURLY_SYMBOL = 411, CLOSE_CURLY_SYMBOL = 412, 
    UNDERLINE_SYMBOL = 413, JSON_SEPARATOR_SYMBOL = 414, JSON_UNQUOTED_SEPARATOR_SYMBOL = 415, 
    AT_SIGN_SYMBOL = 416, AT_TEXT_SUFFIX = 417, AT_AT_SIGN_SYMBOL = 418, 
    NULL2_SYMBOL = 419, PARAM_MARKER = 420, HEX_NUMBER = 421, BIN_NUMBER = 422, 
    DECIMAL_NUMBER = 423, FLOAT_NUMBER = 424, ACCESSIBLE_SYMBOL = 425, ADD_SYMBOL = 426, 
    ALL_SYMBOL = 427, ALTER_SYMBOL = 428, ANALYSE_SYMBOL = 429, AND_SYMBOL = 430, 
    AS_SYMBOL = 431, ASC_SYMBOL = 432, ASENSITIVE_SYMBOL = 433, BEFORE_SYMBOL = 434, 
    BETWEEN_SYMBOL = 435, BIGINT_SYMBOL = 436, BINARY_SYMBOL = 437, BIN_NUM_SYMBOL = 438, 
    BIT_AND_SYMBOL = 439, BIT_OR_SYMBOL = 440, BIT_XOR_SYMBOL = 441, BLOB_SYMBOL = 442, 
    BOTH_SYMBOL = 443, BY_SYMBOL = 444, CALL_SYMBOL = 445, CASCADE_SYMBOL = 446, 
    CASE_SYMBOL = 447, CAST_SYMBOL = 448, CHANGE_SYMBOL = 449, CHAR_SYMBOL = 450, 
    CHECK_SYMBOL = 451, COLLATE_SYMBOL = 452, COLUMN_SYMBOL = 453, CONDITION_SYMBOL = 454, 
    CONSTRAINT_SYMBOL = 455, CONTINUE_SYMBOL = 456, CONVERT_SYMBOL = 457, 
    COUNT_SYMBOL = 458, CREATE_SYMBOL = 459, CROSS_SYMBOL = 460, CURDATE_SYMBOL = 461, 
    CURRENT_DATE_SYMBOL = 462, CURRENT_TIME_SYMBOL = 463, CURRENT_USER_SYMBOL = 464, 
    CURSOR_SYMBOL = 465, CURTIME_SYMBOL = 466, DATABASE_SYMBOL = 467, DATABASES_SYMBOL = 468, 
    DATE_ADD_SYMBOL = 469, DATE_SUB_SYMBOL = 470, DAY_HOUR_SYMBOL = 471, 
    DAY_MICROSECOND_SYMBOL = 472, DAY_MINUTE_SYMBOL = 473, DAY_SECOND_SYMBOL = 474, 
    DECIMAL_NUM_SYMBOL = 475, DECIMAL_SYMBOL = 476, DECLARE_SYMBOL = 477, 
    DEFAULT_SYMBOL = 478, DELAYED_SYMBOL = 479, DELETE_SYMBOL = 480, DESC_SYMBOL = 481, 
    DESCRIBE_SYMBOL = 482, DETERMINISTIC_SYMBOL = 483, DISTINCT_SYMBOL = 484, 
    DIV_SYMBOL = 485, DOUBLE_SYMBOL = 486, DROP_SYMBOL = 487, DUAL_SYMBOL = 488, 
    EACH_SYMBOL = 489, ELSE_SYMBOL = 490, ELSEIF_SYMBOL = 491, ENCLOSED_SYMBOL = 492, 
    END_OF_INPUT_SYMBOL = 493, ESCAPED_SYMBOL = 494, EXCHANGE_SYMBOL = 495, 
    EXISTS_SYMBOL = 496, EXIT_SYMBOL = 497, EXPIRE_SYMBOL = 498, EXPLAIN_SYMBOL = 499, 
    EXTRACT_SYMBOL = 500, FALSE_SYMBOL = 501, FETCH_SYMBOL = 502, FLOAT_SYMBOL = 503, 
    FORCE_SYMBOL = 504, FOREIGN_SYMBOL = 505, FOR_SYMBOL = 506, FROM_SYMBOL = 507, 
    FULLTEXT_SYMBOL = 508, GET_SYMBOL = 509, GENERATED_SYMBOL = 510, GRANT_SYMBOL = 511, 
    GROUP_SYMBOL = 512, GROUP_CONCAT_SYMBOL = 513, HAVING_SYMBOL = 514, 
    HIGH_PRIORITY_SYMBOL = 515, HOUR_MICROSECOND_SYMBOL = 516, HOUR_MINUTE_SYMBOL = 517, 
    HOUR_SECOND_SYMBOL = 518, IF_SYMBOL = 519, IGNORE_SYMBOL = 520, INDEX_SYMBOL = 521, 
    INFILE_SYMBOL = 522, INNER_SYMBOL = 523, INOUT_SYMBOL = 524, INSENSITIVE_SYMBOL = 525, 
    INSERT_SYMBOL = 526, INTERVAL_SYMBOL = 527, INTO_SYMBOL = 528, INT_SYMBOL = 529, 
    IN_SYMBOL = 530, IO_AFTER_GTIDS_SYMBOL = 531, IO_BEFORE_GTIDS_SYMBOL = 532, 
    IS_SYMBOL = 533, ITERATE_SYMBOL = 534, JOIN_SYMBOL = 535, KEYS_SYMBOL = 536, 
    KEY_SYMBOL = 537, KILL_SYMBOL = 538, LEADING_SYMBOL = 539, LEAVE_SYMBOL = 540, 
    LEFT_SYMBOL = 541, LIKE_SYMBOL = 542, LIMIT_SYMBOL = 543, LINEAR_SYMBOL = 544, 
    LINES_SYMBOL = 545, LOAD_SYMBOL = 546, LOCATOR_SYMBOL = 547, LOCK_SYMBOL = 548, 
    LONGBLOB_SYMBOL = 549, LONGTEXT_SYMBOL = 550, LONG_NUM_SYMBOL = 551, 
    LONG_SYMBOL = 552, LOOP_SYMBOL = 553, LOW_PRIORITY_SYMBOL = 554, MASTER_BIND_SYMBOL = 555, 
    MASTER_SSL_VERIFY_SERVER_CERT_SYMBOL = 556, MATCH_SYMBOL = 557, MAX_SYMBOL = 558, 
    MAXVALUE_SYMBOL = 559, MEDIUMBLOB_SYMBOL = 560, MEDIUMINT_SYMBOL = 561, 
    MEDIUMTEXT_SYMBOL = 562, MID_SYMBOL = 563, MINUTE_MICROSECOND_SYMBOL = 564, 
    MINUTE_SECOND_SYMBOL = 565, MIN_SYMBOL = 566, MODIFIES_SYMBOL = 567, 
    MOD_SYMBOL = 568, NATURAL_SYMBOL = 569, NCHAR_STRING_SYMBOL = 570, NEG_SYMBOL = 571, 
    NONBLOCKING_SYMBOL = 572, NOT_SYMBOL = 573, NOW_SYMBOL = 574, NO_WRITE_TO_BINLOG_SYMBOL = 575, 
    NULL_SYMBOL = 576, NUMERIC_SYMBOL = 577, OFFLINE_SYMBOL = 578, ON_SYMBOL = 579, 
    ONLINE_SYMBOL = 580, ONLY_SYMBOL = 581, OPTIMIZE_SYMBOL = 582, OPTIMIZER_COSTS_SYMBOL = 583, 
    OPTION_SYMBOL = 584, OPTIONALLY_SYMBOL = 585, ORDER_SYMBOL = 586, OR_SYMBOL = 587, 
    OUTER_SYMBOL = 588, OUTFILE_SYMBOL = 589, OUT_SYMBOL = 590, POSITION_SYMBOL = 591, 
    PRECISION_SYMBOL = 592, PRIMARY_SYMBOL = 593, PROCEDURE_SYMBOL = 594, 
    PURGE_SYMBOL = 595, RANGE_SYMBOL = 596, READS_SYMBOL = 597, READ_SYMBOL = 598, 
    READ_WRITE_SYMBOL = 599, REAL_SYMBOL = 600, REFERENCES_SYMBOL = 601, 
    REGEXP_SYMBOL = 602, RELEASE_SYMBOL = 603, RENAME_SYMBOL = 604, REPEAT_SYMBOL = 605, 
    REPLACE_SYMBOL = 606, REQUIRE_SYMBOL = 607, RESIGNAL_SYMBOL = 608, RESTRICT_SYMBOL = 609, 
    RETURN_SYMBOL = 610, REVOKE_SYMBOL = 611, RIGHT_SYMBOL = 612, SECOND_MICROSECOND_SYMBOL = 613, 
    SELECT_SYMBOL = 614, SENSITIVE_SYMBOL = 615, SEPARATOR_SYMBOL = 616, 
    SERVER_OPTIONS_SYMBOL = 617, SESSION_USER_SYMBOL = 618, SET_SYMBOL = 619, 
    SET_VAR_SYMBOL = 620, SHOW_SYMBOL = 621, SIGNAL_SYMBOL = 622, SMALLINT_SYMBOL = 623, 
    SPATIAL_SYMBOL = 624, SPECIFIC_SYMBOL = 625, SQLEXCEPTION_SYMBOL = 626, 
    SQLSTATE_SYMBOL = 627, SQLWARNING_SYMBOL = 628, SQL_BIG_RESULT_SYMBOL = 629, 
    SQL_CALC_FOUND_ROWS_SYMBOL = 630, SQL_SMALL_RESULT_SYMBOL = 631, SQL_SYMBOL = 632, 
    SSL_SYMBOL = 633, STARTING_SYMBOL = 634, STDDEV_SAMP_SYMBOL = 635, STDDEV_SYMBOL = 636, 
    STDDEV_POP_SYMBOL = 637, STD_SYMBOL = 638, STORED_SYMBOL = 639, STRAIGHT_JOIN_SYMBOL = 640, 
    SUBSTR_SYMBOL = 641, SUBSTRING_SYMBOL = 642, SUM_SYMBOL = 643, SYSDATE_SYMBOL = 644, 
    SYSTEM_USER_SYMBOL = 645, TABLE_REF_PRIORITY_SYMBOL = 646, TABLE_SYMBOL = 647, 
    TERMINATED_SYMBOL = 648, THEN_SYMBOL = 649, TINYBLOB_SYMBOL = 650, TINYINT_SYMBOL = 651, 
    TINYTEXT_SYMBOL = 652, TO_SYMBOL = 653, TRAILING_SYMBOL = 654, TRIGGER_SYMBOL = 655, 
    TRIM_SYMBOL = 656, TRUE_SYMBOL = 657, UNDO_SYMBOL = 658, UNION_SYMBOL = 659, 
    UNIQUE_SYMBOL = 660, UNLOCK_SYMBOL = 661, UNSIGNED_SYMBOL = 662, UPDATE_SYMBOL = 663, 
    USAGE_SYMBOL = 664, USE_SYMBOL = 665, USING_SYMBOL = 666, UTC_DATE_SYMBOL = 667, 
    UTC_TIMESTAMP_SYMBOL = 668, UTC_TIME_SYMBOL = 669, VALIDATION_SYMBOL = 670, 
    VALUES_SYMBOL = 671, VARBINARY_SYMBOL = 672, VARCHAR_SYMBOL = 673, VARIANCE_SYMBOL = 674, 
    VARYING_SYMBOL = 675, VAR_POP_SYMBOL = 676, VAR_SAMP_SYMBOL = 677, VIRTUAL_SYMBOL = 678, 
    WHEN_SYMBOL = 679, WHERE_SYMBOL = 680, WHILE_SYMBOL = 681, WITH_SYMBOL = 682, 
    WITHOUT_SYMBOL = 683, WRITE_SYMBOL = 684, XOR_SYMBOL = 685, YEAR_MONTH_SYMBOL = 686, 
    ZEROFILL_SYMBOL = 687, PERSIST_SYMBOL = 688, ROLE_SYMBOL = 689, ADMIN_SYMBOL = 690, 
    INVISIBLE_SYMBOL = 691, VISIBLE_SYMBOL = 692, EXCEPT_SYMBOL = 693, COMPONENT_SYMBOL = 694, 
    RECURSIVE_SYMBOL = 695, JSON_OBJECTAGG_SYMBOL = 696, JSON_ARRAYAGG_SYMBOL = 697, 
    OF_SYMBOL = 698, SKIP_SYMBOL = 699, LOCKED_SYMBOL = 700, NOWAIT_SYMBOL = 701, 
    GROUPING_SYMBOL = 702, PERSIST_ONLY_SYMBOL = 703, HISTOGRAM_SYMBOL = 704, 
    BUCKETS_SYMBOL = 705, REMOTE_SYMBOL = 706, CLONE_SYMBOL = 707, CUME_DIST_SYMBOL = 708, 
    DENSE_RANK_SYMBOL = 709, EXCLUDE_SYMBOL = 710, FIRST_VALUE_SYMBOL = 711, 
    FOLLOWING_SYMBOL = 712, GROUPS_SYMBOL = 713, LAG_SYMBOL = 714, LAST_VALUE_SYMBOL = 715, 
    LEAD_SYMBOL = 716, NTH_VALUE_SYMBOL = 717, NTILE_SYMBOL = 718, NULLS_SYMBOL = 719, 
    OTHERS_SYMBOL = 720, OVER_SYMBOL = 721, PERCENT_RANK_SYMBOL = 722, PRECEDING_SYMBOL = 723, 
    RANK_SYMBOL = 724, RESPECT_SYMBOL = 725, ROW_NUMBER_SYMBOL = 726, TIES_SYMBOL = 727, 
    UNBOUNDED_SYMBOL = 728, WINDOW_SYMBOL = 729, EMPTY_SYMBOL = 730, JSON_TABLE_SYMBOL = 731, 
    NESTED_SYMBOL = 732, ORDINALITY_SYMBOL = 733, PATH_SYMBOL = 734, HISTORY_SYMBOL = 735, 
    REUSE_SYMBOL = 736, SRID_SYMBOL = 737, THREAD_PRIORITY_SYMBOL = 738, 
    RESOURCE_SYMBOL = 739, SYSTEM_SYMBOL = 740, VCPU_SYMBOL = 741, MASTER_PUBLIC_KEY_PATH_SYMBOL = 742, 
    GET_MASTER_PUBLIC_KEY_SYMBOL = 743, RESTART_SYMBOL = 744, DEFINITION_SYMBOL = 745, 
    DESCRIPTION_SYMBOL = 746, ORGANIZATION_SYMBOL = 747, REFERENCE_SYMBOL = 748, 
    OPTIONAL_SYMBOL = 749, SECONDARY_SYMBOL = 750, SECONDARY_ENGINE_SYMBOL = 751, 
    SECONDARY_LOAD_SYMBOL = 752, SECONDARY_UNLOAD_SYMBOL = 753, ACTIVE_SYMBOL = 754, 
    INACTIVE_SYMBOL = 755, LATERAL_SYMBOL = 756, RETAIN_SYMBOL = 757, OLD_SYMBOL = 758, 
    NETWORK_NAMESPACE_SYMBOL = 759, ENFORCED_SYMBOL = 760, ARRAY_SYMBOL = 761, 
    OJ_SYMBOL = 762, MEMBER_SYMBOL = 763, RANDOM_SYMBOL = 764, MASTER_COMPRESSION_ALGORITHM_SYMBOL = 765, 
    MASTER_ZSTD_COMPRESSION_LEVEL_SYMBOL = 766, PRIVILEGE_CHECKS_USER_SYMBOL = 767, 
    MASTER_TLS_CIPHERSUITES_SYMBOL = 768, WHITESPACE = 769, INVALID_INPUT = 770, 
    UNDERSCORE_CHARSET = 771, IDENTIFIER = 772, NCHAR_TEXT = 773, BACK_TICK_QUOTED_ID = 774, 
    DOUBLE_QUOTED_TEXT = 775, SINGLE_QUOTED_TEXT = 776, VERSION_COMMENT_START = 777, 
    MYSQL_COMMENT_START = 778, VERSION_COMMENT_END = 779, BLOCK_COMMENT = 780, 
    POUND_COMMENT = 781, DASHDASH_COMMENT = 782, NOT_EQUAL2_OPERATOR = 783
  };

  enum {
    RuleQuery = 0, RuleSimpleStatement = 1, RuleAlterStatement = 2, RuleAlterDatabase = 3, 
    RuleAlterEvent = 4, RuleAlterLogfileGroup = 5, RuleAlterLogfileGroupOptions = 6, 
    RuleAlterLogfileGroupOption = 7, RuleAlterServer = 8, RuleAlterTable = 9, 
    RuleAlterTableActions = 10, RuleAlterCommandList = 11, RuleAlterCommandsModifierList = 12, 
    RuleStandaloneAlterCommands = 13, RuleAlterPartition = 14, RuleAlterList = 15, 
    RuleAlterCommandsModifier = 16, RuleAlterListItem = 17, RulePlace = 18, 
    RuleRestrict = 19, RuleAlterOrderList = 20, RuleAlterAlgorithmOption = 21, 
    RuleAlterLockOption = 22, RuleIndexLockAndAlgorithm = 23, RuleWithValidation = 24, 
    RuleRemovePartitioning = 25, RuleAllOrPartitionNameList = 26, RuleReorgPartitionRule = 27, 
    RuleAlterTablespace = 28, RuleAlterUndoTablespace = 29, RuleUndoTableSpaceOptions = 30, 
    RuleUndoTableSpaceOption = 31, RuleAlterTablespaceOptions = 32, RuleAlterTablespaceOption = 33, 
    RuleChangeTablespaceOption = 34, RuleAlterView = 35, RuleViewTail = 36, 
    RuleViewSelect = 37, RuleViewCheckOption = 38, RuleCreateStatement = 39, 
    RuleCreateDatabase = 40, RuleCreateDatabaseOption = 41, RuleCreateTable = 42, 
    RuleTableElementList = 43, RuleTableElement = 44, RuleDuplicateAsQueryExpression = 45, 
    RuleQueryExpressionOrParens = 46, RuleCreateRoutine = 47, RuleCreateProcedure = 48, 
    RuleCreateFunction = 49, RuleCreateUdf = 50, RuleRoutineCreateOption = 51, 
    RuleRoutineAlterOptions = 52, RuleRoutineOption = 53, RuleCreateIndex = 54, 
    RuleIndexNameAndType = 55, RuleCreateIndexTarget = 56, RuleCreateLogfileGroup = 57, 
    RuleLogfileGroupOptions = 58, RuleLogfileGroupOption = 59, RuleCreateServer = 60, 
    RuleServerOptions = 61, RuleServerOption = 62, RuleCreateTablespace = 63, 
    RuleCreateUndoTablespace = 64, RuleTsDataFileName = 65, RuleTsDataFile = 66, 
    RuleTablespaceOptions = 67, RuleTablespaceOption = 68, RuleTsOptionInitialSize = 69, 
    RuleTsOptionUndoRedoBufferSize = 70, RuleTsOptionAutoextendSize = 71, 
    RuleTsOptionMaxSize = 72, RuleTsOptionExtentSize = 73, RuleTsOptionNodegroup = 74, 
    RuleTsOptionEngine = 75, RuleTsOptionWait = 76, RuleTsOptionComment = 77, 
    RuleTsOptionFileblockSize = 78, RuleTsOptionEncryption = 79, RuleCreateView = 80, 
    RuleViewReplaceOrAlgorithm = 81, RuleViewAlgorithm = 82, RuleViewSuid = 83, 
    RuleCreateTrigger = 84, RuleTriggerFollowsPrecedesClause = 85, RuleCreateEvent = 86, 
    RuleCreateRole = 87, RuleCreateSpatialReference = 88, RuleSrsAttribute = 89, 
    RuleDropStatement = 90, RuleDropDatabase = 91, RuleDropEvent = 92, RuleDropFunction = 93, 
    RuleDropProcedure = 94, RuleDropIndex = 95, RuleDropLogfileGroup = 96, 
    RuleDropLogfileGroupOption = 97, RuleDropServer = 98, RuleDropTable = 99, 
    RuleDropTableSpace = 100, RuleDropTrigger = 101, RuleDropView = 102, 
    RuleDropRole = 103, RuleDropSpatialReference = 104, RuleDropUndoTablespace = 105, 
    RuleRenameTableStatement = 106, RuleRenamePair = 107, RuleTruncateTableStatement = 108, 
    RuleImportStatement = 109, RuleCallStatement = 110, RuleDeleteStatement = 111, 
    RulePartitionDelete = 112, RuleDeleteStatementOption = 113, RuleDoStatement = 114, 
    RuleHandlerStatement = 115, RuleHandlerReadOrScan = 116, RuleInsertStatement = 117, 
    RuleInsertLockOption = 118, RuleInsertFromConstructor = 119, RuleFields = 120, 
    RuleInsertValues = 121, RuleInsertQueryExpression = 122, RuleValueList = 123, 
    RuleValues = 124, RuleValuesReference = 125, RuleInsertUpdateList = 126, 
    RuleLoadStatement = 127, RuleDataOrXml = 128, RuleXmlRowsIdentifiedBy = 129, 
    RuleLoadDataFileTail = 130, RuleLoadDataFileTargetList = 131, RuleFieldOrVariableList = 132, 
    RuleReplaceStatement = 133, RuleSelectStatement = 134, RuleSelectStatementWithInto = 135, 
    RuleQueryExpression = 136, RuleQueryExpressionBody = 137, RuleQueryExpressionParens = 138, 
    RuleQuerySpecification = 139, RuleSubquery = 140, RuleQuerySpecOption = 141, 
    RuleLimitClause = 142, RuleSimpleLimitClause = 143, RuleLimitOptions = 144, 
    RuleLimitOption = 145, RuleIntoClause = 146, RuleProcedureAnalyseClause = 147, 
    RuleHavingClause = 148, RuleWindowClause = 149, RuleWindowDefinition = 150, 
    RuleWindowSpec = 151, RuleWindowSpecDetails = 152, RuleWindowFrameClause = 153, 
    RuleWindowFrameUnits = 154, RuleWindowFrameExtent = 155, RuleWindowFrameStart = 156, 
    RuleWindowFrameBetween = 157, RuleWindowFrameBound = 158, RuleWindowFrameExclusion = 159, 
    RuleWithClause = 160, RuleCommonTableExpression = 161, RuleGroupByClause = 162, 
    RuleOlapOption = 163, RuleOrderClause = 164, RuleDirection = 165, RuleFromClause = 166, 
    RuleTableReferenceList = 167, RuleSelectOption = 168, RuleLockingClause = 169, 
    RuleLockStrengh = 170, RuleLockedRowAction = 171, RuleSelectItemList = 172, 
    RuleSelectItem = 173, RuleSelectAlias = 174, RuleWhereClause = 175, 
    RuleTableReference = 176, RuleEscapedTableReference = 177, RuleJoinedTable = 178, 
    RuleNaturalJoinType = 179, RuleInnerJoinType = 180, RuleOuterJoinType = 181, 
    RuleTableFactor = 182, RuleSingleTable = 183, RuleSingleTableParens = 184, 
    RuleDerivedTable = 185, RuleTableReferenceListParens = 186, RuleTableFunction = 187, 
    RuleColumnsClause = 188, RuleJtColumn = 189, RuleOnEmptyOrError = 190, 
    RuleOnEmpty = 191, RuleOnError = 192, RuleJtOnResponse = 193, RuleUnionOption = 194, 
    RuleTableAlias = 195, RuleIndexHintList = 196, RuleIndexHint = 197, 
    RuleIndexHintType = 198, RuleKeyOrIndex = 199, RuleConstraintKeyType = 200, 
    RuleIndexHintClause = 201, RuleIndexList = 202, RuleIndexListElement = 203, 
    RuleUpdateStatement = 204, RuleTransactionOrLockingStatement = 205, 
    RuleTransactionStatement = 206, RuleBeginWork = 207, RuleTransactionCharacteristic = 208, 
    RuleSavepointStatement = 209, RuleLockStatement = 210, RuleLockItem = 211, 
    RuleLockOption = 212, RuleXaStatement = 213, RuleXaConvert = 214, RuleXid = 215, 
    RuleReplicationStatement = 216, RuleResetOption = 217, RuleMasterResetOptions = 218, 
    RuleReplicationLoad = 219, RuleChangeMaster = 220, RuleChangeMasterOptions = 221, 
    RuleMasterOption = 222, RulePrivilegeCheckDef = 223, RuleMasterTlsCiphersuitesDef = 224, 
    RuleMasterFileDef = 225, RuleServerIdList = 226, RuleChangeReplication = 227, 
    RuleFilterDefinition = 228, RuleFilterDbList = 229, RuleFilterTableList = 230, 
    RuleFilterStringList = 231, RuleFilterWildDbTableString = 232, RuleFilterDbPairList = 233, 
    RuleSlave = 234, RuleSlaveUntilOptions = 235, RuleSlaveConnectionOptions = 236, 
    RuleSlaveThreadOptions = 237, RuleSlaveThreadOption = 238, RuleGroupReplication = 239, 
    RulePreparedStatement = 240, RuleExecuteStatement = 241, RuleExecuteVarList = 242, 
    RuleCloneStatement = 243, RuleDataDirSSL = 244, RuleSsl = 245, RuleAccountManagementStatement = 246, 
    RuleAlterUser = 247, RuleAlterUserTail = 248, RuleUserFunction = 249, 
    RuleCreateUser = 250, RuleCreateUserTail = 251, RuleDefaultRoleClause = 252, 
    RuleRequireClause = 253, RuleConnectOptions = 254, RuleAccountLockPasswordExpireOptions = 255, 
    RuleDropUser = 256, RuleGrant = 257, RuleGrantTargetList = 258, RuleGrantOptions = 259, 
    RuleExceptRoleList = 260, RuleWithRoles = 261, RuleGrantAs = 262, RuleVersionedRequireClause = 263, 
    RuleRenameUser = 264, RuleRevoke = 265, RuleOnTypeTo = 266, RuleAclType = 267, 
    RuleRoleOrPrivilegesList = 268, RuleRoleOrPrivilege = 269, RuleGrantIdentifier = 270, 
    RuleRequireList = 271, RuleRequireListElement = 272, RuleGrantOption = 273, 
    RuleSetRole = 274, RuleRoleList = 275, RuleRole = 276, RuleTableAdministrationStatement = 277, 
    RuleHistogram = 278, RuleCheckOption = 279, RuleRepairType = 280, RuleInstallUninstallStatment = 281, 
    RuleSetStatement = 282, RuleStartOptionValueList = 283, RuleTransactionCharacteristics = 284, 
    RuleTransactionAccessMode = 285, RuleIsolationLevel = 286, RuleOptionValueListContinued = 287, 
    RuleOptionValueNoOptionType = 288, RuleOptionValue = 289, RuleSetSystemVariable = 290, 
    RuleStartOptionValueListFollowingOptionType = 291, RuleOptionValueFollowingOptionType = 292, 
    RuleSetExprOrDefault = 293, RuleShowStatement = 294, RuleShowCommandType = 295, 
    RuleNonBlocking = 296, RuleFromOrIn = 297, RuleInDb = 298, RuleProfileType = 299, 
    RuleOtherAdministrativeStatement = 300, RuleKeyCacheListOrParts = 301, 
    RuleKeyCacheList = 302, RuleAssignToKeycache = 303, RuleAssignToKeycachePartition = 304, 
    RuleCacheKeyList = 305, RuleKeyUsageElement = 306, RuleKeyUsageList = 307, 
    RuleFlushOption = 308, RuleLogType = 309, RuleFlushTables = 310, RuleFlushTablesOptions = 311, 
    RulePreloadTail = 312, RulePreloadList = 313, RulePreloadKeys = 314, 
    RuleAdminPartition = 315, RuleResourceGroupManagement = 316, RuleCreateResourceGroup = 317, 
    RuleResourceGroupVcpuList = 318, RuleVcpuNumOrRange = 319, RuleResourceGroupPriority = 320, 
    RuleResourceGroupEnableDisable = 321, RuleAlterResourceGroup = 322, 
    RuleSetResourceGroup = 323, RuleThreadIdList = 324, RuleDropResourceGroup = 325, 
    RuleUtilityStatement = 326, RuleDescribeCommand = 327, RuleExplainCommand = 328, 
    RuleExplainableStatement = 329, RuleHelpCommand = 330, RuleUseCommand = 331, 
    RuleRestartServer = 332, RuleExpr = 333, RuleBoolPri = 334, RuleCompOp = 335, 
    RulePredicate = 336, RulePredicateOperations = 337, RuleBitExpr = 338, 
    RuleSimpleExpr = 339, RuleArrayCast = 340, RuleJsonOperator = 341, RuleSumExpr = 342, 
    RuleGroupingOperation = 343, RuleWindowFunctionCall = 344, RuleWindowingClause = 345, 
    RuleLeadLagInfo = 346, RuleNullTreatment = 347, RuleJsonFunction = 348, 
    RuleInSumExpr = 349, RuleIdentListArg = 350, RuleIdentList = 351, RuleFulltextOptions = 352, 
    RuleRuntimeFunctionCall = 353, RuleGeometryFunction = 354, RuleTimeFunctionParameters = 355, 
    RuleFractionalPrecision = 356, RuleWeightStringLevels = 357, RuleWeightStringLevelListItem = 358, 
    RuleDateTimeTtype = 359, RuleTrimFunction = 360, RuleSubstringFunction = 361, 
    RuleFunctionCall = 362, RuleUdfExprList = 363, RuleUdfExpr = 364, RuleVariable = 365, 
    RuleUserVariable = 366, RuleSystemVariable = 367, RuleInternalVariableName = 368, 
    RuleWhenExpression = 369, RuleThenExpression = 370, RuleElseExpression = 371, 
    RuleCastType = 372, RuleExprList = 373, RuleCharset = 374, RuleNotRule = 375, 
    RuleNot2Rule = 376, RuleInterval = 377, RuleIntervalTimeStamp = 378, 
    RuleExprListWithParentheses = 379, RuleExprWithParentheses = 380, RuleSimpleExprWithParentheses = 381, 
    RuleOrderList = 382, RuleOrderExpression = 383, RuleGroupList = 384, 
    RuleGroupingExpression = 385, RuleChannel = 386, RuleCompoundStatement = 387, 
    RuleReturnStatement = 388, RuleIfStatement = 389, RuleIfBody = 390, 
    RuleThenStatement = 391, RuleCompoundStatementList = 392, RuleCaseStatement = 393, 
    RuleElseStatement = 394, RuleLabeledBlock = 395, RuleUnlabeledBlock = 396, 
    RuleLabel = 397, RuleBeginEndBlock = 398, RuleLabeledControl = 399, 
    RuleUnlabeledControl = 400, RuleLoopBlock = 401, RuleWhileDoBlock = 402, 
    RuleRepeatUntilBlock = 403, RuleSpDeclarations = 404, RuleSpDeclaration = 405, 
    RuleVariableDeclaration = 406, RuleConditionDeclaration = 407, RuleSpCondition = 408, 
    RuleSqlstate = 409, RuleHandlerDeclaration = 410, RuleHandlerCondition = 411, 
    RuleCursorDeclaration = 412, RuleIterateStatement = 413, RuleLeaveStatement = 414, 
    RuleGetDiagnostics = 415, RuleSignalAllowedExpr = 416, RuleStatementInformationItem = 417, 
    RuleConditionInformationItem = 418, RuleSignalInformationItemName = 419, 
    RuleSignalStatement = 420, RuleResignalStatement = 421, RuleSignalInformationItem = 422, 
    RuleCursorOpen = 423, RuleCursorClose = 424, RuleCursorFetch = 425, 
    RuleSchedule = 426, RuleColumnDefinition = 427, RuleCheckOrReferences = 428, 
    RuleCheckConstraint = 429, RuleConstraintEnforcement = 430, RuleTableConstraintDef = 431, 
    RuleConstraintName = 432, RuleFieldDefinition = 433, RuleColumnAttribute = 434, 
    RuleColumnFormat = 435, RuleStorageMedia = 436, RuleGcolAttribute = 437, 
    RuleReferences = 438, RuleDeleteOption = 439, RuleKeyList = 440, RuleKeyPart = 441, 
    RuleKeyListWithExpression = 442, RuleKeyPartOrExpression = 443, RuleKeyListVariants = 444, 
    RuleIndexType = 445, RuleIndexOption = 446, RuleCommonIndexOption = 447, 
    RuleVisibility = 448, RuleIndexTypeClause = 449, RuleFulltextIndexOption = 450, 
    RuleSpatialIndexOption = 451, RuleDataTypeDefinition = 452, RuleDataType = 453, 
    RuleNchar = 454, RuleRealType = 455, RuleFieldLength = 456, RuleFieldOptions = 457, 
    RuleCharsetWithOptBinary = 458, RuleAscii = 459, RuleUnicode = 460, 
    RuleWsNumCodepoints = 461, RuleTypeDatetimePrecision = 462, RuleCharsetName = 463, 
    RuleCollationName = 464, RuleCreateTableOptions = 465, RuleCreateTableOptionsSpaceSeparated = 466, 
    RuleCreateTableOption = 467, RuleTernaryOption = 468, RuleDefaultCollation = 469, 
    RuleDefaultEncryption = 470, RuleDefaultCharset = 471, RulePartitionClause = 472, 
    RulePartitionTypeDef = 473, RuleSubPartitions = 474, RulePartitionKeyAlgorithm = 475, 
    RulePartitionDefinitions = 476, RulePartitionDefinition = 477, RulePartitionValuesIn = 478, 
    RulePartitionOption = 479, RuleSubpartitionDefinition = 480, RulePartitionValueItemListParen = 481, 
    RulePartitionValueItem = 482, RuleDefinerClause = 483, RuleIfExists = 484, 
    RuleIfNotExists = 485, RuleProcedureParameter = 486, RuleFunctionParameter = 487, 
    RuleCollate = 488, RuleTypeWithOptCollate = 489, RuleSchemaIdentifierPair = 490, 
    RuleViewRefList = 491, RuleUpdateList = 492, RuleUpdateElement = 493, 
    RuleCharsetClause = 494, RuleFieldsClause = 495, RuleFieldTerm = 496, 
    RuleLinesClause = 497, RuleLineTerm = 498, RuleUserList = 499, RuleCreateUserList = 500, 
    RuleAlterUserList = 501, RuleCreateUserEntry = 502, RuleAlterUserEntry = 503, 
    RuleRetainCurrentPassword = 504, RuleDiscardOldPassword = 505, RuleReplacePassword = 506, 
    RuleUserIdentifierOrText = 507, RuleUser = 508, RuleLikeClause = 509, 
    RuleLikeOrWhere = 510, RuleOnlineOption = 511, RuleNoWriteToBinLog = 512, 
    RuleUsePartition = 513, RuleFieldIdentifier = 514, RuleColumnName = 515, 
    RuleColumnInternalRef = 516, RuleColumnInternalRefList = 517, RuleColumnRef = 518, 
    RuleInsertIdentifier = 519, RuleIndexName = 520, RuleIndexRef = 521, 
    RuleTableWild = 522, RuleSchemaName = 523, RuleSchemaRef = 524, RuleProcedureName = 525, 
    RuleProcedureRef = 526, RuleFunctionName = 527, RuleFunctionRef = 528, 
    RuleTriggerName = 529, RuleTriggerRef = 530, RuleViewName = 531, RuleViewRef = 532, 
    RuleTablespaceName = 533, RuleTablespaceRef = 534, RuleLogfileGroupName = 535, 
    RuleLogfileGroupRef = 536, RuleEventName = 537, RuleEventRef = 538, 
    RuleUdfName = 539, RuleServerName = 540, RuleServerRef = 541, RuleEngineRef = 542, 
    RuleTableName = 543, RuleFilterTableRef = 544, RuleTableRefWithWildcard = 545, 
    RuleTableRef = 546, RuleTableRefList = 547, RuleTableAliasRefList = 548, 
    RuleParameterName = 549, RuleLabelIdentifier = 550, RuleLabelRef = 551, 
    RuleRoleIdentifier = 552, RuleRoleRef = 553, RulePluginRef = 554, RuleComponentRef = 555, 
    RuleResourceGroupRef = 556, RuleWindowName = 557, RulePureIdentifier = 558, 
    RuleIdentifier = 559, RuleIdentifierList = 560, RuleIdentifierListWithParentheses = 561, 
    RuleQualifiedIdentifier = 562, RuleSimpleIdentifier = 563, RuleDotIdentifier = 564, 
    RuleUlong_number = 565, RuleReal_ulong_number = 566, RuleUlonglong_number = 567, 
    RuleReal_ulonglong_number = 568, RuleLiteral = 569, RuleSignedLiteral = 570, 
    RuleStringList = 571, RuleTextStringLiteral = 572, RuleTextString = 573, 
    RuleTextStringHash = 574, RuleTextLiteral = 575, RuleTextStringNoLinebreak = 576, 
    RuleTextStringLiteralList = 577, RuleNumLiteral = 578, RuleBoolLiteral = 579, 
    RuleNullLiteral = 580, RuleTemporalLiteral = 581, RuleFloatOptions = 582, 
    RuleStandardFloatOptions = 583, RulePrecision = 584, RuleTextOrIdentifier = 585, 
    RuleLValueIdentifier = 586, RuleRoleIdentifierOrText = 587, RuleSizeNumber = 588, 
    RuleParentheses = 589, RuleEqual = 590, RuleOptionType = 591, RuleVarIdentType = 592, 
    RuleSetVarIdentType = 593, RuleIdentifierKeyword = 594, RuleIdentifierKeywordsAmbiguous1RolesAndLabels = 595, 
    RuleIdentifierKeywordsAmbiguous2Labels = 596, RuleLabelKeyword = 597, 
    RuleIdentifierKeywordsAmbiguous3Roles = 598, RuleIdentifierKeywordsUnambiguous = 599, 
    RuleRoleKeyword = 600, RuleLValueKeyword = 601, RuleIdentifierKeywordsAmbiguous4SystemVariables = 602, 
    RuleRoleOrIdentifierKeyword = 603, RuleRoleOrLabelKeyword = 604
  };

  MySQLParser(antlr4::TokenStream *input);
  ~MySQLParser() override;

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; }
  virtual const std::vector<std::string>& getTokenNames() const override { return _tokenNames; } // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;


  class QueryContext;
  class SimpleStatementContext;
  class AlterStatementContext;
  class AlterDatabaseContext;
  class AlterEventContext;
  class AlterLogfileGroupContext;
  class AlterLogfileGroupOptionsContext;
  class AlterLogfileGroupOptionContext;
  class AlterServerContext;
  class AlterTableContext;
  class AlterTableActionsContext;
  class AlterCommandListContext;
  class AlterCommandsModifierListContext;
  class StandaloneAlterCommandsContext;
  class AlterPartitionContext;
  class AlterListContext;
  class AlterCommandsModifierContext;
  class AlterListItemContext;
  class PlaceContext;
  class RestrictContext;
  class AlterOrderListContext;
  class AlterAlgorithmOptionContext;
  class AlterLockOptionContext;
  class IndexLockAndAlgorithmContext;
  class WithValidationContext;
  class RemovePartitioningContext;
  class AllOrPartitionNameListContext;
  class ReorgPartitionRuleContext;
  class AlterTablespaceContext;
  class AlterUndoTablespaceContext;
  class UndoTableSpaceOptionsContext;
  class UndoTableSpaceOptionContext;
  class AlterTablespaceOptionsContext;
  class AlterTablespaceOptionContext;
  class ChangeTablespaceOptionContext;
  class AlterViewContext;
  class ViewTailContext;
  class ViewSelectContext;
  class ViewCheckOptionContext;
  class CreateStatementContext;
  class CreateDatabaseContext;
  class CreateDatabaseOptionContext;
  class CreateTableContext;
  class TableElementListContext;
  class TableElementContext;
  class DuplicateAsQueryExpressionContext;
  class QueryExpressionOrParensContext;
  class CreateRoutineContext;
  class CreateProcedureContext;
  class CreateFunctionContext;
  class CreateUdfContext;
  class RoutineCreateOptionContext;
  class RoutineAlterOptionsContext;
  class RoutineOptionContext;
  class CreateIndexContext;
  class IndexNameAndTypeContext;
  class CreateIndexTargetContext;
  class CreateLogfileGroupContext;
  class LogfileGroupOptionsContext;
  class LogfileGroupOptionContext;
  class CreateServerContext;
  class ServerOptionsContext;
  class ServerOptionContext;
  class CreateTablespaceContext;
  class CreateUndoTablespaceContext;
  class TsDataFileNameContext;
  class TsDataFileContext;
  class TablespaceOptionsContext;
  class TablespaceOptionContext;
  class TsOptionInitialSizeContext;
  class TsOptionUndoRedoBufferSizeContext;
  class TsOptionAutoextendSizeContext;
  class TsOptionMaxSizeContext;
  class TsOptionExtentSizeContext;
  class TsOptionNodegroupContext;
  class TsOptionEngineContext;
  class TsOptionWaitContext;
  class TsOptionCommentContext;
  class TsOptionFileblockSizeContext;
  class TsOptionEncryptionContext;
  class CreateViewContext;
  class ViewReplaceOrAlgorithmContext;
  class ViewAlgorithmContext;
  class ViewSuidContext;
  class CreateTriggerContext;
  class TriggerFollowsPrecedesClauseContext;
  class CreateEventContext;
  class CreateRoleContext;
  class CreateSpatialReferenceContext;
  class SrsAttributeContext;
  class DropStatementContext;
  class DropDatabaseContext;
  class DropEventContext;
  class DropFunctionContext;
  class DropProcedureContext;
  class DropIndexContext;
  class DropLogfileGroupContext;
  class DropLogfileGroupOptionContext;
  class DropServerContext;
  class DropTableContext;
  class DropTableSpaceContext;
  class DropTriggerContext;
  class DropViewContext;
  class DropRoleContext;
  class DropSpatialReferenceContext;
  class DropUndoTablespaceContext;
  class RenameTableStatementContext;
  class RenamePairContext;
  class TruncateTableStatementContext;
  class ImportStatementContext;
  class CallStatementContext;
  class DeleteStatementContext;
  class PartitionDeleteContext;
  class DeleteStatementOptionContext;
  class DoStatementContext;
  class HandlerStatementContext;
  class HandlerReadOrScanContext;
  class InsertStatementContext;
  class InsertLockOptionContext;
  class InsertFromConstructorContext;
  class FieldsContext;
  class InsertValuesContext;
  class InsertQueryExpressionContext;
  class ValueListContext;
  class ValuesContext;
  class ValuesReferenceContext;
  class InsertUpdateListContext;
  class LoadStatementContext;
  class DataOrXmlContext;
  class XmlRowsIdentifiedByContext;
  class LoadDataFileTailContext;
  class LoadDataFileTargetListContext;
  class FieldOrVariableListContext;
  class ReplaceStatementContext;
  class SelectStatementContext;
  class SelectStatementWithIntoContext;
  class QueryExpressionContext;
  class QueryExpressionBodyContext;
  class QueryExpressionParensContext;
  class QuerySpecificationContext;
  class SubqueryContext;
  class QuerySpecOptionContext;
  class LimitClauseContext;
  class SimpleLimitClauseContext;
  class LimitOptionsContext;
  class LimitOptionContext;
  class IntoClauseContext;
  class ProcedureAnalyseClauseContext;
  class HavingClauseContext;
  class WindowClauseContext;
  class WindowDefinitionContext;
  class WindowSpecContext;
  class WindowSpecDetailsContext;
  class WindowFrameClauseContext;
  class WindowFrameUnitsContext;
  class WindowFrameExtentContext;
  class WindowFrameStartContext;
  class WindowFrameBetweenContext;
  class WindowFrameBoundContext;
  class WindowFrameExclusionContext;
  class WithClauseContext;
  class CommonTableExpressionContext;
  class GroupByClauseContext;
  class OlapOptionContext;
  class OrderClauseContext;
  class DirectionContext;
  class FromClauseContext;
  class TableReferenceListContext;
  class SelectOptionContext;
  class LockingClauseContext;
  class LockStrenghContext;
  class LockedRowActionContext;
  class SelectItemListContext;
  class SelectItemContext;
  class SelectAliasContext;
  class WhereClauseContext;
  class TableReferenceContext;
  class EscapedTableReferenceContext;
  class JoinedTableContext;
  class NaturalJoinTypeContext;
  class InnerJoinTypeContext;
  class OuterJoinTypeContext;
  class TableFactorContext;
  class SingleTableContext;
  class SingleTableParensContext;
  class DerivedTableContext;
  class TableReferenceListParensContext;
  class TableFunctionContext;
  class ColumnsClauseContext;
  class JtColumnContext;
  class OnEmptyOrErrorContext;
  class OnEmptyContext;
  class OnErrorContext;
  class JtOnResponseContext;
  class UnionOptionContext;
  class TableAliasContext;
  class IndexHintListContext;
  class IndexHintContext;
  class IndexHintTypeContext;
  class KeyOrIndexContext;
  class ConstraintKeyTypeContext;
  class IndexHintClauseContext;
  class IndexListContext;
  class IndexListElementContext;
  class UpdateStatementContext;
  class TransactionOrLockingStatementContext;
  class TransactionStatementContext;
  class BeginWorkContext;
  class TransactionCharacteristicContext;
  class SavepointStatementContext;
  class LockStatementContext;
  class LockItemContext;
  class LockOptionContext;
  class XaStatementContext;
  class XaConvertContext;
  class XidContext;
  class ReplicationStatementContext;
  class ResetOptionContext;
  class MasterResetOptionsContext;
  class ReplicationLoadContext;
  class ChangeMasterContext;
  class ChangeMasterOptionsContext;
  class MasterOptionContext;
  class PrivilegeCheckDefContext;
  class MasterTlsCiphersuitesDefContext;
  class MasterFileDefContext;
  class ServerIdListContext;
  class ChangeReplicationContext;
  class FilterDefinitionContext;
  class FilterDbListContext;
  class FilterTableListContext;
  class FilterStringListContext;
  class FilterWildDbTableStringContext;
  class FilterDbPairListContext;
  class SlaveContext;
  class SlaveUntilOptionsContext;
  class SlaveConnectionOptionsContext;
  class SlaveThreadOptionsContext;
  class SlaveThreadOptionContext;
  class GroupReplicationContext;
  class PreparedStatementContext;
  class ExecuteStatementContext;
  class ExecuteVarListContext;
  class CloneStatementContext;
  class DataDirSSLContext;
  class SslContext;
  class AccountManagementStatementContext;
  class AlterUserContext;
  class AlterUserTailContext;
  class UserFunctionContext;
  class CreateUserContext;
  class CreateUserTailContext;
  class DefaultRoleClauseContext;
  class RequireClauseContext;
  class ConnectOptionsContext;
  class AccountLockPasswordExpireOptionsContext;
  class DropUserContext;
  class GrantContext;
  class GrantTargetListContext;
  class GrantOptionsContext;
  class ExceptRoleListContext;
  class WithRolesContext;
  class GrantAsContext;
  class VersionedRequireClauseContext;
  class RenameUserContext;
  class RevokeContext;
  class OnTypeToContext;
  class AclTypeContext;
  class RoleOrPrivilegesListContext;
  class RoleOrPrivilegeContext;
  class GrantIdentifierContext;
  class RequireListContext;
  class RequireListElementContext;
  class GrantOptionContext;
  class SetRoleContext;
  class RoleListContext;
  class RoleContext;
  class TableAdministrationStatementContext;
  class HistogramContext;
  class CheckOptionContext;
  class RepairTypeContext;
  class InstallUninstallStatmentContext;
  class SetStatementContext;
  class StartOptionValueListContext;
  class TransactionCharacteristicsContext;
  class TransactionAccessModeContext;
  class IsolationLevelContext;
  class OptionValueListContinuedContext;
  class OptionValueNoOptionTypeContext;
  class OptionValueContext;
  class SetSystemVariableContext;
  class StartOptionValueListFollowingOptionTypeContext;
  class OptionValueFollowingOptionTypeContext;
  class SetExprOrDefaultContext;
  class ShowStatementContext;
  class ShowCommandTypeContext;
  class NonBlockingContext;
  class FromOrInContext;
  class InDbContext;
  class ProfileTypeContext;
  class OtherAdministrativeStatementContext;
  class KeyCacheListOrPartsContext;
  class KeyCacheListContext;
  class AssignToKeycacheContext;
  class AssignToKeycachePartitionContext;
  class CacheKeyListContext;
  class KeyUsageElementContext;
  class KeyUsageListContext;
  class FlushOptionContext;
  class LogTypeContext;
  class FlushTablesContext;
  class FlushTablesOptionsContext;
  class PreloadTailContext;
  class PreloadListContext;
  class PreloadKeysContext;
  class AdminPartitionContext;
  class ResourceGroupManagementContext;
  class CreateResourceGroupContext;
  class ResourceGroupVcpuListContext;
  class VcpuNumOrRangeContext;
  class ResourceGroupPriorityContext;
  class ResourceGroupEnableDisableContext;
  class AlterResourceGroupContext;
  class SetResourceGroupContext;
  class ThreadIdListContext;
  class DropResourceGroupContext;
  class UtilityStatementContext;
  class DescribeCommandContext;
  class ExplainCommandContext;
  class ExplainableStatementContext;
  class HelpCommandContext;
  class UseCommandContext;
  class RestartServerContext;
  class ExprContext;
  class BoolPriContext;
  class CompOpContext;
  class PredicateContext;
  class PredicateOperationsContext;
  class BitExprContext;
  class SimpleExprContext;
  class ArrayCastContext;
  class JsonOperatorContext;
  class SumExprContext;
  class GroupingOperationContext;
  class WindowFunctionCallContext;
  class WindowingClauseContext;
  class LeadLagInfoContext;
  class NullTreatmentContext;
  class JsonFunctionContext;
  class InSumExprContext;
  class IdentListArgContext;
  class IdentListContext;
  class FulltextOptionsContext;
  class RuntimeFunctionCallContext;
  class GeometryFunctionContext;
  class TimeFunctionParametersContext;
  class FractionalPrecisionContext;
  class WeightStringLevelsContext;
  class WeightStringLevelListItemContext;
  class DateTimeTtypeContext;
  class TrimFunctionContext;
  class SubstringFunctionContext;
  class FunctionCallContext;
  class UdfExprListContext;
  class UdfExprContext;
  class VariableContext;
  class UserVariableContext;
  class SystemVariableContext;
  class InternalVariableNameContext;
  class WhenExpressionContext;
  class ThenExpressionContext;
  class ElseExpressionContext;
  class CastTypeContext;
  class ExprListContext;
  class CharsetContext;
  class NotRuleContext;
  class Not2RuleContext;
  class IntervalContext;
  class IntervalTimeStampContext;
  class ExprListWithParenthesesContext;
  class ExprWithParenthesesContext;
  class SimpleExprWithParenthesesContext;
  class OrderListContext;
  class OrderExpressionContext;
  class GroupListContext;
  class GroupingExpressionContext;
  class ChannelContext;
  class CompoundStatementContext;
  class ReturnStatementContext;
  class IfStatementContext;
  class IfBodyContext;
  class ThenStatementContext;
  class CompoundStatementListContext;
  class CaseStatementContext;
  class ElseStatementContext;
  class LabeledBlockContext;
  class UnlabeledBlockContext;
  class LabelContext;
  class BeginEndBlockContext;
  class LabeledControlContext;
  class UnlabeledControlContext;
  class LoopBlockContext;
  class WhileDoBlockContext;
  class RepeatUntilBlockContext;
  class SpDeclarationsContext;
  class SpDeclarationContext;
  class VariableDeclarationContext;
  class ConditionDeclarationContext;
  class SpConditionContext;
  class SqlstateContext;
  class HandlerDeclarationContext;
  class HandlerConditionContext;
  class CursorDeclarationContext;
  class IterateStatementContext;
  class LeaveStatementContext;
  class GetDiagnosticsContext;
  class SignalAllowedExprContext;
  class StatementInformationItemContext;
  class ConditionInformationItemContext;
  class SignalInformationItemNameContext;
  class SignalStatementContext;
  class ResignalStatementContext;
  class SignalInformationItemContext;
  class CursorOpenContext;
  class CursorCloseContext;
  class CursorFetchContext;
  class ScheduleContext;
  class ColumnDefinitionContext;
  class CheckOrReferencesContext;
  class CheckConstraintContext;
  class ConstraintEnforcementContext;
  class TableConstraintDefContext;
  class ConstraintNameContext;
  class FieldDefinitionContext;
  class ColumnAttributeContext;
  class ColumnFormatContext;
  class StorageMediaContext;
  class GcolAttributeContext;
  class ReferencesContext;
  class DeleteOptionContext;
  class KeyListContext;
  class KeyPartContext;
  class KeyListWithExpressionContext;
  class KeyPartOrExpressionContext;
  class KeyListVariantsContext;
  class IndexTypeContext;
  class IndexOptionContext;
  class CommonIndexOptionContext;
  class VisibilityContext;
  class IndexTypeClauseContext;
  class FulltextIndexOptionContext;
  class SpatialIndexOptionContext;
  class DataTypeDefinitionContext;
  class DataTypeContext;
  class NcharContext;
  class RealTypeContext;
  class FieldLengthContext;
  class FieldOptionsContext;
  class CharsetWithOptBinaryContext;
  class AsciiContext;
  class UnicodeContext;
  class WsNumCodepointsContext;
  class TypeDatetimePrecisionContext;
  class CharsetNameContext;
  class CollationNameContext;
  class CreateTableOptionsContext;
  class CreateTableOptionsSpaceSeparatedContext;
  class CreateTableOptionContext;
  class TernaryOptionContext;
  class DefaultCollationContext;
  class DefaultEncryptionContext;
  class DefaultCharsetContext;
  class PartitionClauseContext;
  class PartitionTypeDefContext;
  class SubPartitionsContext;
  class PartitionKeyAlgorithmContext;
  class PartitionDefinitionsContext;
  class PartitionDefinitionContext;
  class PartitionValuesInContext;
  class PartitionOptionContext;
  class SubpartitionDefinitionContext;
  class PartitionValueItemListParenContext;
  class PartitionValueItemContext;
  class DefinerClauseContext;
  class IfExistsContext;
  class IfNotExistsContext;
  class ProcedureParameterContext;
  class FunctionParameterContext;
  class CollateContext;
  class TypeWithOptCollateContext;
  class SchemaIdentifierPairContext;
  class ViewRefListContext;
  class UpdateListContext;
  class UpdateElementContext;
  class CharsetClauseContext;
  class FieldsClauseContext;
  class FieldTermContext;
  class LinesClauseContext;
  class LineTermContext;
  class UserListContext;
  class CreateUserListContext;
  class AlterUserListContext;
  class CreateUserEntryContext;
  class AlterUserEntryContext;
  class RetainCurrentPasswordContext;
  class DiscardOldPasswordContext;
  class ReplacePasswordContext;
  class UserIdentifierOrTextContext;
  class UserContext;
  class LikeClauseContext;
  class LikeOrWhereContext;
  class OnlineOptionContext;
  class NoWriteToBinLogContext;
  class UsePartitionContext;
  class FieldIdentifierContext;
  class ColumnNameContext;
  class ColumnInternalRefContext;
  class ColumnInternalRefListContext;
  class ColumnRefContext;
  class InsertIdentifierContext;
  class IndexNameContext;
  class IndexRefContext;
  class TableWildContext;
  class SchemaNameContext;
  class SchemaRefContext;
  class ProcedureNameContext;
  class ProcedureRefContext;
  class FunctionNameContext;
  class FunctionRefContext;
  class TriggerNameContext;
  class TriggerRefContext;
  class ViewNameContext;
  class ViewRefContext;
  class TablespaceNameContext;
  class TablespaceRefContext;
  class LogfileGroupNameContext;
  class LogfileGroupRefContext;
  class EventNameContext;
  class EventRefContext;
  class UdfNameContext;
  class ServerNameContext;
  class ServerRefContext;
  class EngineRefContext;
  class TableNameContext;
  class FilterTableRefContext;
  class TableRefWithWildcardContext;
  class TableRefContext;
  class TableRefListContext;
  class TableAliasRefListContext;
  class ParameterNameContext;
  class LabelIdentifierContext;
  class LabelRefContext;
  class RoleIdentifierContext;
  class RoleRefContext;
  class PluginRefContext;
  class ComponentRefContext;
  class ResourceGroupRefContext;
  class WindowNameContext;
  class PureIdentifierContext;
  class IdentifierContext;
  class IdentifierListContext;
  class IdentifierListWithParenthesesContext;
  class QualifiedIdentifierContext;
  class SimpleIdentifierContext;
  class DotIdentifierContext;
  class Ulong_numberContext;
  class Real_ulong_numberContext;
  class Ulonglong_numberContext;
  class Real_ulonglong_numberContext;
  class LiteralContext;
  class SignedLiteralContext;
  class StringListContext;
  class TextStringLiteralContext;
  class TextStringContext;
  class TextStringHashContext;
  class TextLiteralContext;
  class TextStringNoLinebreakContext;
  class TextStringLiteralListContext;
  class NumLiteralContext;
  class BoolLiteralContext;
  class NullLiteralContext;
  class TemporalLiteralContext;
  class FloatOptionsContext;
  class StandardFloatOptionsContext;
  class PrecisionContext;
  class TextOrIdentifierContext;
  class LValueIdentifierContext;
  class RoleIdentifierOrTextContext;
  class SizeNumberContext;
  class ParenthesesContext;
  class EqualContext;
  class OptionTypeContext;
  class VarIdentTypeContext;
  class SetVarIdentTypeContext;
  class IdentifierKeywordContext;
  class IdentifierKeywordsAmbiguous1RolesAndLabelsContext;
  class IdentifierKeywordsAmbiguous2LabelsContext;
  class LabelKeywordContext;
  class IdentifierKeywordsAmbiguous3RolesContext;
  class IdentifierKeywordsUnambiguousContext;
  class RoleKeywordContext;
  class LValueKeywordContext;
  class IdentifierKeywordsAmbiguous4SystemVariablesContext;
  class RoleOrIdentifierKeywordContext;
  class RoleOrLabelKeywordContext; 

  class  QueryContext : public antlr4::ParserRuleContext {
  public:
    QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    SimpleStatementContext *simpleStatement();
    BeginWorkContext *beginWork();
    antlr4::tree::TerminalNode *SEMICOLON_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryContext* query();

  class  SimpleStatementContext : public antlr4::ParserRuleContext {
  public:
    SimpleStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterStatementContext *alterStatement();
    CreateStatementContext *createStatement();
    DropStatementContext *dropStatement();
    RenameTableStatementContext *renameTableStatement();
    TruncateTableStatementContext *truncateTableStatement();
    ImportStatementContext *importStatement();
    CallStatementContext *callStatement();
    DeleteStatementContext *deleteStatement();
    DoStatementContext *doStatement();
    HandlerStatementContext *handlerStatement();
    InsertStatementContext *insertStatement();
    LoadStatementContext *loadStatement();
    ReplaceStatementContext *replaceStatement();
    SelectStatementContext *selectStatement();
    UpdateStatementContext *updateStatement();
    TransactionOrLockingStatementContext *transactionOrLockingStatement();
    ReplicationStatementContext *replicationStatement();
    PreparedStatementContext *preparedStatement();
    CloneStatementContext *cloneStatement();
    AccountManagementStatementContext *accountManagementStatement();
    TableAdministrationStatementContext *tableAdministrationStatement();
    InstallUninstallStatmentContext *installUninstallStatment();
    SetStatementContext *setStatement();
    ShowStatementContext *showStatement();
    ResourceGroupManagementContext *resourceGroupManagement();
    OtherAdministrativeStatementContext *otherAdministrativeStatement();
    UtilityStatementContext *utilityStatement();
    GetDiagnosticsContext *getDiagnostics();
    SignalStatementContext *signalStatement();
    ResignalStatementContext *resignalStatement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SimpleStatementContext* simpleStatement();

  class  AlterStatementContext : public antlr4::ParserRuleContext {
  public:
    AlterStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALTER_SYMBOL();
    AlterTableContext *alterTable();
    AlterDatabaseContext *alterDatabase();
    antlr4::tree::TerminalNode *PROCEDURE_SYMBOL();
    ProcedureRefContext *procedureRef();
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    FunctionRefContext *functionRef();
    AlterViewContext *alterView();
    AlterEventContext *alterEvent();
    AlterTablespaceContext *alterTablespace();
    AlterUndoTablespaceContext *alterUndoTablespace();
    AlterLogfileGroupContext *alterLogfileGroup();
    AlterServerContext *alterServer();
    antlr4::tree::TerminalNode *INSTANCE_SYMBOL();
    antlr4::tree::TerminalNode *ROTATE_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    RoutineAlterOptionsContext *routineAlterOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterStatementContext* alterStatement();

  class  AlterDatabaseContext : public antlr4::ParserRuleContext {
  public:
    AlterDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATABASE_SYMBOL();
    SchemaRefContext *schemaRef();
    antlr4::tree::TerminalNode *UPGRADE_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    antlr4::tree::TerminalNode *NAME_SYMBOL();
    std::vector<CreateDatabaseOptionContext *> createDatabaseOption();
    CreateDatabaseOptionContext* createDatabaseOption(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterDatabaseContext* alterDatabase();

  class  AlterEventContext : public antlr4::ParserRuleContext {
  public:
    AlterEventContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    EventRefContext *eventRef();
    DefinerClauseContext *definerClause();
    std::vector<antlr4::tree::TerminalNode *> ON_SYMBOL();
    antlr4::tree::TerminalNode* ON_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *SCHEDULE_SYMBOL();
    ScheduleContext *schedule();
    antlr4::tree::TerminalNode *COMPLETION_SYMBOL();
    antlr4::tree::TerminalNode *PRESERVE_SYMBOL();
    antlr4::tree::TerminalNode *RENAME_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *ENABLE_SYMBOL();
    antlr4::tree::TerminalNode *DISABLE_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *DO_SYMBOL();
    CompoundStatementContext *compoundStatement();
    antlr4::tree::TerminalNode *NOT_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterEventContext* alterEvent();

  class  AlterLogfileGroupContext : public antlr4::ParserRuleContext {
  public:
    AlterLogfileGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOGFILE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    LogfileGroupRefContext *logfileGroupRef();
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    antlr4::tree::TerminalNode *UNDOFILE_SYMBOL();
    TextLiteralContext *textLiteral();
    AlterLogfileGroupOptionsContext *alterLogfileGroupOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterLogfileGroupContext* alterLogfileGroup();

  class  AlterLogfileGroupOptionsContext : public antlr4::ParserRuleContext {
  public:
    AlterLogfileGroupOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AlterLogfileGroupOptionContext *> alterLogfileGroupOption();
    AlterLogfileGroupOptionContext* alterLogfileGroupOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterLogfileGroupOptionsContext* alterLogfileGroupOptions();

  class  AlterLogfileGroupOptionContext : public antlr4::ParserRuleContext {
  public:
    AlterLogfileGroupOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TsOptionInitialSizeContext *tsOptionInitialSize();
    TsOptionEngineContext *tsOptionEngine();
    TsOptionWaitContext *tsOptionWait();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterLogfileGroupOptionContext* alterLogfileGroupOption();

  class  AlterServerContext : public antlr4::ParserRuleContext {
  public:
    AlterServerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SERVER_SYMBOL();
    ServerRefContext *serverRef();
    ServerOptionsContext *serverOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterServerContext* alterServer();

  class  AlterTableContext : public antlr4::ParserRuleContext {
  public:
    AlterTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    TableRefContext *tableRef();
    OnlineOptionContext *onlineOption();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();
    AlterTableActionsContext *alterTableActions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterTableContext* alterTable();

  class  AlterTableActionsContext : public antlr4::ParserRuleContext {
  public:
    AlterTableActionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterCommandListContext *alterCommandList();
    PartitionClauseContext *partitionClause();
    RemovePartitioningContext *removePartitioning();
    StandaloneAlterCommandsContext *standaloneAlterCommands();
    AlterCommandsModifierListContext *alterCommandsModifierList();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterTableActionsContext* alterTableActions();

  class  AlterCommandListContext : public antlr4::ParserRuleContext {
  public:
    AlterCommandListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterCommandsModifierListContext *alterCommandsModifierList();
    AlterListContext *alterList();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterCommandListContext* alterCommandList();

  class  AlterCommandsModifierListContext : public antlr4::ParserRuleContext {
  public:
    AlterCommandsModifierListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AlterCommandsModifierContext *> alterCommandsModifier();
    AlterCommandsModifierContext* alterCommandsModifier(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterCommandsModifierListContext* alterCommandsModifierList();

  class  StandaloneAlterCommandsContext : public antlr4::ParserRuleContext {
  public:
    StandaloneAlterCommandsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DISCARD_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *IMPORT_SYMBOL();
    AlterPartitionContext *alterPartition();
    antlr4::tree::TerminalNode *SECONDARY_LOAD_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_UNLOAD_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StandaloneAlterCommandsContext* standaloneAlterCommands();

  class  AlterPartitionContext : public antlr4::ParserRuleContext {
  public:
    AlterPartitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    PartitionDefinitionsContext *partitionDefinitions();
    antlr4::tree::TerminalNode *PARTITIONS_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    std::vector<NoWriteToBinLogContext *> noWriteToBinLog();
    NoWriteToBinLogContext* noWriteToBinLog(size_t i);
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *REBUILD_SYMBOL();
    AllOrPartitionNameListContext *allOrPartitionNameList();
    antlr4::tree::TerminalNode *OPTIMIZE_SYMBOL();
    antlr4::tree::TerminalNode *ANALYZE_SYMBOL();
    antlr4::tree::TerminalNode *CHECK_SYMBOL();
    std::vector<CheckOptionContext *> checkOption();
    CheckOptionContext* checkOption(size_t i);
    antlr4::tree::TerminalNode *REPAIR_SYMBOL();
    std::vector<RepairTypeContext *> repairType();
    RepairTypeContext* repairType(size_t i);
    antlr4::tree::TerminalNode *COALESCE_SYMBOL();
    antlr4::tree::TerminalNode *TRUNCATE_SYMBOL();
    ReorgPartitionRuleContext *reorgPartitionRule();
    antlr4::tree::TerminalNode *REORGANIZE_SYMBOL();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    antlr4::tree::TerminalNode *EXCHANGE_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    TableRefContext *tableRef();
    WithValidationContext *withValidation();
    antlr4::tree::TerminalNode *DISCARD_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *IMPORT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterPartitionContext* alterPartition();

  class  AlterListContext : public antlr4::ParserRuleContext {
  public:
    AlterListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AlterListItemContext *> alterListItem();
    AlterListItemContext* alterListItem(size_t i);
    std::vector<CreateTableOptionsSpaceSeparatedContext *> createTableOptionsSpaceSeparated();
    CreateTableOptionsSpaceSeparatedContext* createTableOptionsSpaceSeparated(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    std::vector<AlterCommandsModifierContext *> alterCommandsModifier();
    AlterCommandsModifierContext* alterCommandsModifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterListContext* alterList();

  class  AlterCommandsModifierContext : public antlr4::ParserRuleContext {
  public:
    AlterCommandsModifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterAlgorithmOptionContext *alterAlgorithmOption();
    AlterLockOptionContext *alterLockOption();
    WithValidationContext *withValidation();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterCommandsModifierContext* alterCommandsModifier();

  class  AlterListItemContext : public antlr4::ParserRuleContext {
  public:
    AlterListItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    IdentifierContext *identifier();
    FieldDefinitionContext *fieldDefinition();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    TableElementListContext *tableElementList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *COLUMN_SYMBOL();
    CheckOrReferencesContext *checkOrReferences();
    PlaceContext *place();
    TableConstraintDefContext *tableConstraintDef();
    antlr4::tree::TerminalNode *CHANGE_SYMBOL();
    ColumnInternalRefContext *columnInternalRef();
    antlr4::tree::TerminalNode *MODIFY_SYMBOL();
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    antlr4::tree::TerminalNode *FOREIGN_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();
    KeyOrIndexContext *keyOrIndex();
    IndexRefContext *indexRef();
    RestrictContext *restrict();
    antlr4::tree::TerminalNode *DISABLE_SYMBOL();
    antlr4::tree::TerminalNode *KEYS_SYMBOL();
    antlr4::tree::TerminalNode *ENABLE_SYMBOL();
    antlr4::tree::TerminalNode *ALTER_SYMBOL();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    ExprWithParenthesesContext *exprWithParentheses();
    SignedLiteralContext *signedLiteral();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    VisibilityContext *visibility();
    antlr4::tree::TerminalNode *CHECK_SYMBOL();
    ConstraintEnforcementContext *constraintEnforcement();
    antlr4::tree::TerminalNode *RENAME_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    TableNameContext *tableName();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    IndexNameContext *indexName();
    antlr4::tree::TerminalNode *CONVERT_SYMBOL();
    CharsetContext *charset();
    CharsetNameContext *charsetName();
    CollateContext *collate();
    antlr4::tree::TerminalNode *FORCE_SYMBOL();
    antlr4::tree::TerminalNode *ORDER_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    AlterOrderListContext *alterOrderList();
    antlr4::tree::TerminalNode *UPGRADE_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONING_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterListItemContext* alterListItem();

  class  PlaceContext : public antlr4::ParserRuleContext {
  public:
    PlaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AFTER_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *FIRST_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PlaceContext* place();

  class  RestrictContext : public antlr4::ParserRuleContext {
  public:
    RestrictContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RESTRICT_SYMBOL();
    antlr4::tree::TerminalNode *CASCADE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RestrictContext* restrict();

  class  AlterOrderListContext : public antlr4::ParserRuleContext {
  public:
    AlterOrderListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<DirectionContext *> direction();
    DirectionContext* direction(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterOrderListContext* alterOrderList();

  class  AlterAlgorithmOptionContext : public antlr4::ParserRuleContext {
  public:
    AlterAlgorithmOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALGORITHM_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterAlgorithmOptionContext* alterAlgorithmOption();

  class  AlterLockOptionContext : public antlr4::ParserRuleContext {
  public:
    AlterLockOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOCK_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterLockOptionContext* alterLockOption();

  class  IndexLockAndAlgorithmContext : public antlr4::ParserRuleContext {
  public:
    IndexLockAndAlgorithmContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterAlgorithmOptionContext *alterAlgorithmOption();
    AlterLockOptionContext *alterLockOption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexLockAndAlgorithmContext* indexLockAndAlgorithm();

  class  WithValidationContext : public antlr4::ParserRuleContext {
  public:
    WithValidationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VALIDATION_SYMBOL();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *WITHOUT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WithValidationContext* withValidation();

  class  RemovePartitioningContext : public antlr4::ParserRuleContext {
  public:
    RemovePartitioningContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REMOVE_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONING_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RemovePartitioningContext* removePartitioning();

  class  AllOrPartitionNameListContext : public antlr4::ParserRuleContext {
  public:
    AllOrPartitionNameListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    IdentifierListContext *identifierList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AllOrPartitionNameListContext* allOrPartitionNameList();

  class  ReorgPartitionRuleContext : public antlr4::ParserRuleContext {
  public:
    ReorgPartitionRuleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REORGANIZE_SYMBOL();
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    NoWriteToBinLogContext *noWriteToBinLog();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    PartitionDefinitionsContext *partitionDefinitions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReorgPartitionRuleContext* reorgPartitionRule();

  class  AlterTablespaceContext : public antlr4::ParserRuleContext {
  public:
    AlterTablespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    TablespaceRefContext *tablespaceRef();
    antlr4::tree::TerminalNode *DATAFILE_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *RENAME_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    IdentifierContext *identifier();
    AlterTablespaceOptionsContext *alterTablespaceOptions();
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    antlr4::tree::TerminalNode *CHANGE_SYMBOL();
    antlr4::tree::TerminalNode *NOT_SYMBOL();
    antlr4::tree::TerminalNode *ACCESSIBLE_SYMBOL();
    antlr4::tree::TerminalNode *READ_ONLY_SYMBOL();
    antlr4::tree::TerminalNode *READ_WRITE_SYMBOL();
    std::vector<ChangeTablespaceOptionContext *> changeTablespaceOption();
    ChangeTablespaceOptionContext* changeTablespaceOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterTablespaceContext* alterTablespace();

  class  AlterUndoTablespaceContext : public antlr4::ParserRuleContext {
  public:
    AlterUndoTablespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNDO_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    TablespaceRefContext *tablespaceRef();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    antlr4::tree::TerminalNode *ACTIVE_SYMBOL();
    antlr4::tree::TerminalNode *INACTIVE_SYMBOL();
    UndoTableSpaceOptionsContext *undoTableSpaceOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterUndoTablespaceContext* alterUndoTablespace();

  class  UndoTableSpaceOptionsContext : public antlr4::ParserRuleContext {
  public:
    UndoTableSpaceOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UndoTableSpaceOptionContext *> undoTableSpaceOption();
    UndoTableSpaceOptionContext* undoTableSpaceOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UndoTableSpaceOptionsContext* undoTableSpaceOptions();

  class  UndoTableSpaceOptionContext : public antlr4::ParserRuleContext {
  public:
    UndoTableSpaceOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TsOptionEngineContext *tsOptionEngine();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UndoTableSpaceOptionContext* undoTableSpaceOption();

  class  AlterTablespaceOptionsContext : public antlr4::ParserRuleContext {
  public:
    AlterTablespaceOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AlterTablespaceOptionContext *> alterTablespaceOption();
    AlterTablespaceOptionContext* alterTablespaceOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterTablespaceOptionsContext* alterTablespaceOptions();

  class  AlterTablespaceOptionContext : public antlr4::ParserRuleContext {
  public:
    AlterTablespaceOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INITIAL_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    TsOptionAutoextendSizeContext *tsOptionAutoextendSize();
    TsOptionMaxSizeContext *tsOptionMaxSize();
    TsOptionEngineContext *tsOptionEngine();
    TsOptionWaitContext *tsOptionWait();
    TsOptionEncryptionContext *tsOptionEncryption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterTablespaceOptionContext* alterTablespaceOption();

  class  ChangeTablespaceOptionContext : public antlr4::ParserRuleContext {
  public:
    ChangeTablespaceOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INITIAL_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    TsOptionAutoextendSizeContext *tsOptionAutoextendSize();
    TsOptionMaxSizeContext *tsOptionMaxSize();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ChangeTablespaceOptionContext* changeTablespaceOption();

  class  AlterViewContext : public antlr4::ParserRuleContext {
  public:
    AlterViewContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    ViewRefContext *viewRef();
    ViewTailContext *viewTail();
    ViewAlgorithmContext *viewAlgorithm();
    DefinerClauseContext *definerClause();
    ViewSuidContext *viewSuid();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterViewContext* alterView();

  class  ViewTailContext : public antlr4::ParserRuleContext {
  public:
    ViewTailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AS_SYMBOL();
    ViewSelectContext *viewSelect();
    ColumnInternalRefListContext *columnInternalRefList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewTailContext* viewTail();

  class  ViewSelectContext : public antlr4::ParserRuleContext {
  public:
    ViewSelectContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionOrParensContext *queryExpressionOrParens();
    ViewCheckOptionContext *viewCheckOption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewSelectContext* viewSelect();

  class  ViewCheckOptionContext : public antlr4::ParserRuleContext {
  public:
    ViewCheckOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *CHECK_SYMBOL();
    antlr4::tree::TerminalNode *OPTION_SYMBOL();
    antlr4::tree::TerminalNode *CASCADED_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewCheckOptionContext* viewCheckOption();

  class  CreateStatementContext : public antlr4::ParserRuleContext {
  public:
    CreateStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE_SYMBOL();
    CreateDatabaseContext *createDatabase();
    CreateTableContext *createTable();
    CreateFunctionContext *createFunction();
    CreateProcedureContext *createProcedure();
    CreateUdfContext *createUdf();
    CreateLogfileGroupContext *createLogfileGroup();
    CreateViewContext *createView();
    CreateTriggerContext *createTrigger();
    CreateIndexContext *createIndex();
    CreateServerContext *createServer();
    CreateTablespaceContext *createTablespace();
    CreateEventContext *createEvent();
    CreateRoleContext *createRole();
    CreateSpatialReferenceContext *createSpatialReference();
    CreateUndoTablespaceContext *createUndoTablespace();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateStatementContext* createStatement();

  class  CreateDatabaseContext : public antlr4::ParserRuleContext {
  public:
    CreateDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATABASE_SYMBOL();
    SchemaNameContext *schemaName();
    IfNotExistsContext *ifNotExists();
    std::vector<CreateDatabaseOptionContext *> createDatabaseOption();
    CreateDatabaseOptionContext* createDatabaseOption(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateDatabaseContext* createDatabase();

  class  CreateDatabaseOptionContext : public antlr4::ParserRuleContext {
  public:
    CreateDatabaseOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DefaultCharsetContext *defaultCharset();
    DefaultCollationContext *defaultCollation();
    DefaultEncryptionContext *defaultEncryption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateDatabaseOptionContext* createDatabaseOption();

  class  CreateTableContext : public antlr4::ParserRuleContext {
  public:
    CreateTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    TableNameContext *tableName();
    antlr4::tree::TerminalNode *LIKE_SYMBOL();
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *TEMPORARY_SYMBOL();
    IfNotExistsContext *ifNotExists();
    TableElementListContext *tableElementList();
    CreateTableOptionsContext *createTableOptions();
    PartitionClauseContext *partitionClause();
    DuplicateAsQueryExpressionContext *duplicateAsQueryExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateTableContext* createTable();

  class  TableElementListContext : public antlr4::ParserRuleContext {
  public:
    TableElementListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TableElementContext *> tableElement();
    TableElementContext* tableElement(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableElementListContext* tableElementList();

  class  TableElementContext : public antlr4::ParserRuleContext {
  public:
    TableElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnDefinitionContext *columnDefinition();
    TableConstraintDefContext *tableConstraintDef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableElementContext* tableElement();

  class  DuplicateAsQueryExpressionContext : public antlr4::ParserRuleContext {
  public:
    DuplicateAsQueryExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionOrParensContext *queryExpressionOrParens();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DuplicateAsQueryExpressionContext* duplicateAsQueryExpression();

  class  QueryExpressionOrParensContext : public antlr4::ParserRuleContext {
  public:
    QueryExpressionOrParensContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionContext *queryExpression();
    QueryExpressionParensContext *queryExpressionParens();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryExpressionOrParensContext* queryExpressionOrParens();

  class  CreateRoutineContext : public antlr4::ParserRuleContext {
  public:
    CreateRoutineContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE_SYMBOL();
    antlr4::tree::TerminalNode *EOF();
    CreateProcedureContext *createProcedure();
    CreateFunctionContext *createFunction();
    CreateUdfContext *createUdf();
    antlr4::tree::TerminalNode *SEMICOLON_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateRoutineContext* createRoutine();

  class  CreateProcedureContext : public antlr4::ParserRuleContext {
  public:
    CreateProcedureContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROCEDURE_SYMBOL();
    ProcedureNameContext *procedureName();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    CompoundStatementContext *compoundStatement();
    DefinerClauseContext *definerClause();
    std::vector<ProcedureParameterContext *> procedureParameter();
    ProcedureParameterContext* procedureParameter(size_t i);
    std::vector<RoutineCreateOptionContext *> routineCreateOption();
    RoutineCreateOptionContext* routineCreateOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateProcedureContext* createProcedure();

  class  CreateFunctionContext : public antlr4::ParserRuleContext {
  public:
    CreateFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    FunctionNameContext *functionName();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *RETURNS_SYMBOL();
    TypeWithOptCollateContext *typeWithOptCollate();
    CompoundStatementContext *compoundStatement();
    DefinerClauseContext *definerClause();
    std::vector<FunctionParameterContext *> functionParameter();
    FunctionParameterContext* functionParameter(size_t i);
    std::vector<RoutineCreateOptionContext *> routineCreateOption();
    RoutineCreateOptionContext* routineCreateOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateFunctionContext* createFunction();

  class  CreateUdfContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    CreateUdfContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    UdfNameContext *udfName();
    antlr4::tree::TerminalNode *RETURNS_SYMBOL();
    antlr4::tree::TerminalNode *SONAME_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *STRING_SYMBOL();
    antlr4::tree::TerminalNode *INT_SYMBOL();
    antlr4::tree::TerminalNode *REAL_SYMBOL();
    antlr4::tree::TerminalNode *DECIMAL_SYMBOL();
    antlr4::tree::TerminalNode *AGGREGATE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateUdfContext* createUdf();

  class  RoutineCreateOptionContext : public antlr4::ParserRuleContext {
  public:
    RoutineCreateOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoutineOptionContext *routineOption();
    antlr4::tree::TerminalNode *DETERMINISTIC_SYMBOL();
    antlr4::tree::TerminalNode *NOT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoutineCreateOptionContext* routineCreateOption();

  class  RoutineAlterOptionsContext : public antlr4::ParserRuleContext {
  public:
    RoutineAlterOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<RoutineCreateOptionContext *> routineCreateOption();
    RoutineCreateOptionContext* routineCreateOption(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoutineAlterOptionsContext* routineAlterOptions();

  class  RoutineOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    antlr4::Token *security = nullptr;
    RoutineOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    antlr4::tree::TerminalNode *SQL_SYMBOL();
    antlr4::tree::TerminalNode *LANGUAGE_SYMBOL();
    antlr4::tree::TerminalNode *NO_SYMBOL();
    antlr4::tree::TerminalNode *CONTAINS_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *READS_SYMBOL();
    antlr4::tree::TerminalNode *MODIFIES_SYMBOL();
    antlr4::tree::TerminalNode *SECURITY_SYMBOL();
    antlr4::tree::TerminalNode *DEFINER_SYMBOL();
    antlr4::tree::TerminalNode *INVOKER_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoutineOptionContext* routineOption();

  class  CreateIndexContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    CreateIndexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateIndexTargetContext *createIndexTarget();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    IndexNameContext *indexName();
    OnlineOptionContext *onlineOption();
    antlr4::tree::TerminalNode *FULLTEXT_SYMBOL();
    antlr4::tree::TerminalNode *SPATIAL_SYMBOL();
    IndexLockAndAlgorithmContext *indexLockAndAlgorithm();
    antlr4::tree::TerminalNode *UNIQUE_SYMBOL();
    std::vector<IndexOptionContext *> indexOption();
    IndexOptionContext* indexOption(size_t i);
    std::vector<FulltextIndexOptionContext *> fulltextIndexOption();
    FulltextIndexOptionContext* fulltextIndexOption(size_t i);
    std::vector<SpatialIndexOptionContext *> spatialIndexOption();
    SpatialIndexOptionContext* spatialIndexOption(size_t i);
    IndexTypeClauseContext *indexTypeClause();
    IndexNameAndTypeContext *indexNameAndType();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateIndexContext* createIndex();

  class  IndexNameAndTypeContext : public antlr4::ParserRuleContext {
  public:
    IndexNameAndTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IndexNameContext *indexName();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    IndexTypeContext *indexType();
    antlr4::tree::TerminalNode *TYPE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexNameAndTypeContext* indexNameAndType();

  class  CreateIndexTargetContext : public antlr4::ParserRuleContext {
  public:
    CreateIndexTargetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON_SYMBOL();
    TableRefContext *tableRef();
    KeyListVariantsContext *keyListVariants();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateIndexTargetContext* createIndexTarget();

  class  CreateLogfileGroupContext : public antlr4::ParserRuleContext {
  public:
    CreateLogfileGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOGFILE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    LogfileGroupNameContext *logfileGroupName();
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *UNDOFILE_SYMBOL();
    antlr4::tree::TerminalNode *REDOFILE_SYMBOL();
    LogfileGroupOptionsContext *logfileGroupOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateLogfileGroupContext* createLogfileGroup();

  class  LogfileGroupOptionsContext : public antlr4::ParserRuleContext {
  public:
    LogfileGroupOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<LogfileGroupOptionContext *> logfileGroupOption();
    LogfileGroupOptionContext* logfileGroupOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LogfileGroupOptionsContext* logfileGroupOptions();

  class  LogfileGroupOptionContext : public antlr4::ParserRuleContext {
  public:
    LogfileGroupOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TsOptionInitialSizeContext *tsOptionInitialSize();
    TsOptionUndoRedoBufferSizeContext *tsOptionUndoRedoBufferSize();
    TsOptionNodegroupContext *tsOptionNodegroup();
    TsOptionEngineContext *tsOptionEngine();
    TsOptionWaitContext *tsOptionWait();
    TsOptionCommentContext *tsOptionComment();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LogfileGroupOptionContext* logfileGroupOption();

  class  CreateServerContext : public antlr4::ParserRuleContext {
  public:
    CreateServerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SERVER_SYMBOL();
    ServerNameContext *serverName();
    antlr4::tree::TerminalNode *FOREIGN_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *WRAPPER_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    ServerOptionsContext *serverOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateServerContext* createServer();

  class  ServerOptionsContext : public antlr4::ParserRuleContext {
  public:
    ServerOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPTIONS_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<ServerOptionContext *> serverOption();
    ServerOptionContext* serverOption(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ServerOptionsContext* serverOptions();

  class  ServerOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    ServerOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *HOST_SYMBOL();
    antlr4::tree::TerminalNode *DATABASE_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *SOCKET_SYMBOL();
    antlr4::tree::TerminalNode *OWNER_SYMBOL();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *PORT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ServerOptionContext* serverOption();

  class  CreateTablespaceContext : public antlr4::ParserRuleContext {
  public:
    CreateTablespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    TablespaceNameContext *tablespaceName();
    TsDataFileNameContext *tsDataFileName();
    antlr4::tree::TerminalNode *USE_SYMBOL();
    antlr4::tree::TerminalNode *LOGFILE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    LogfileGroupRefContext *logfileGroupRef();
    TablespaceOptionsContext *tablespaceOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateTablespaceContext* createTablespace();

  class  CreateUndoTablespaceContext : public antlr4::ParserRuleContext {
  public:
    CreateUndoTablespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNDO_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    TablespaceNameContext *tablespaceName();
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    TsDataFileContext *tsDataFile();
    UndoTableSpaceOptionsContext *undoTableSpaceOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateUndoTablespaceContext* createUndoTablespace();

  class  TsDataFileNameContext : public antlr4::ParserRuleContext {
  public:
    TsDataFileNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD_SYMBOL();
    TsDataFileContext *tsDataFile();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsDataFileNameContext* tsDataFileName();

  class  TsDataFileContext : public antlr4::ParserRuleContext {
  public:
    TsDataFileContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATAFILE_SYMBOL();
    TextLiteralContext *textLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsDataFileContext* tsDataFile();

  class  TablespaceOptionsContext : public antlr4::ParserRuleContext {
  public:
    TablespaceOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TablespaceOptionContext *> tablespaceOption();
    TablespaceOptionContext* tablespaceOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TablespaceOptionsContext* tablespaceOptions();

  class  TablespaceOptionContext : public antlr4::ParserRuleContext {
  public:
    TablespaceOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TsOptionInitialSizeContext *tsOptionInitialSize();
    TsOptionAutoextendSizeContext *tsOptionAutoextendSize();
    TsOptionMaxSizeContext *tsOptionMaxSize();
    TsOptionExtentSizeContext *tsOptionExtentSize();
    TsOptionNodegroupContext *tsOptionNodegroup();
    TsOptionEngineContext *tsOptionEngine();
    TsOptionWaitContext *tsOptionWait();
    TsOptionCommentContext *tsOptionComment();
    TsOptionFileblockSizeContext *tsOptionFileblockSize();
    TsOptionEncryptionContext *tsOptionEncryption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TablespaceOptionContext* tablespaceOption();

  class  TsOptionInitialSizeContext : public antlr4::ParserRuleContext {
  public:
    TsOptionInitialSizeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INITIAL_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionInitialSizeContext* tsOptionInitialSize();

  class  TsOptionUndoRedoBufferSizeContext : public antlr4::ParserRuleContext {
  public:
    TsOptionUndoRedoBufferSizeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *UNDO_BUFFER_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *REDO_BUFFER_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionUndoRedoBufferSizeContext* tsOptionUndoRedoBufferSize();

  class  TsOptionAutoextendSizeContext : public antlr4::ParserRuleContext {
  public:
    TsOptionAutoextendSizeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AUTOEXTEND_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionAutoextendSizeContext* tsOptionAutoextendSize();

  class  TsOptionMaxSizeContext : public antlr4::ParserRuleContext {
  public:
    TsOptionMaxSizeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MAX_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionMaxSizeContext* tsOptionMaxSize();

  class  TsOptionExtentSizeContext : public antlr4::ParserRuleContext {
  public:
    TsOptionExtentSizeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXTENT_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionExtentSizeContext* tsOptionExtentSize();

  class  TsOptionNodegroupContext : public antlr4::ParserRuleContext {
  public:
    TsOptionNodegroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NODEGROUP_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionNodegroupContext* tsOptionNodegroup();

  class  TsOptionEngineContext : public antlr4::ParserRuleContext {
  public:
    TsOptionEngineContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    EngineRefContext *engineRef();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionEngineContext* tsOptionEngine();

  class  TsOptionWaitContext : public antlr4::ParserRuleContext {
  public:
    TsOptionWaitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WAIT_SYMBOL();
    antlr4::tree::TerminalNode *NO_WAIT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionWaitContext* tsOptionWait();

  class  TsOptionCommentContext : public antlr4::ParserRuleContext {
  public:
    TsOptionCommentContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionCommentContext* tsOptionComment();

  class  TsOptionFileblockSizeContext : public antlr4::ParserRuleContext {
  public:
    TsOptionFileblockSizeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FILE_BLOCK_SIZE_SYMBOL();
    SizeNumberContext *sizeNumber();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionFileblockSizeContext* tsOptionFileblockSize();

  class  TsOptionEncryptionContext : public antlr4::ParserRuleContext {
  public:
    TsOptionEncryptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ENCRYPTION_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TsOptionEncryptionContext* tsOptionEncryption();

  class  CreateViewContext : public antlr4::ParserRuleContext {
  public:
    CreateViewContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    ViewNameContext *viewName();
    ViewTailContext *viewTail();
    ViewReplaceOrAlgorithmContext *viewReplaceOrAlgorithm();
    DefinerClauseContext *definerClause();
    ViewSuidContext *viewSuid();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateViewContext* createView();

  class  ViewReplaceOrAlgorithmContext : public antlr4::ParserRuleContext {
  public:
    ViewReplaceOrAlgorithmContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OR_SYMBOL();
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    ViewAlgorithmContext *viewAlgorithm();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewReplaceOrAlgorithmContext* viewReplaceOrAlgorithm();

  class  ViewAlgorithmContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *algorithm = nullptr;
    ViewAlgorithmContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALGORITHM_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *UNDEFINED_SYMBOL();
    antlr4::tree::TerminalNode *MERGE_SYMBOL();
    antlr4::tree::TerminalNode *TEMPTABLE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewAlgorithmContext* viewAlgorithm();

  class  ViewSuidContext : public antlr4::ParserRuleContext {
  public:
    ViewSuidContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SQL_SYMBOL();
    antlr4::tree::TerminalNode *SECURITY_SYMBOL();
    antlr4::tree::TerminalNode *DEFINER_SYMBOL();
    antlr4::tree::TerminalNode *INVOKER_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewSuidContext* viewSuid();

  class  CreateTriggerContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *timing = nullptr;
    antlr4::Token *event = nullptr;
    CreateTriggerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRIGGER_SYMBOL();
    TriggerNameContext *triggerName();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *EACH_SYMBOL();
    antlr4::tree::TerminalNode *ROW_SYMBOL();
    CompoundStatementContext *compoundStatement();
    antlr4::tree::TerminalNode *BEFORE_SYMBOL();
    antlr4::tree::TerminalNode *AFTER_SYMBOL();
    antlr4::tree::TerminalNode *INSERT_SYMBOL();
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    antlr4::tree::TerminalNode *DELETE_SYMBOL();
    DefinerClauseContext *definerClause();
    TriggerFollowsPrecedesClauseContext *triggerFollowsPrecedesClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateTriggerContext* createTrigger();

  class  TriggerFollowsPrecedesClauseContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *ordering = nullptr;
    TriggerFollowsPrecedesClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *FOLLOWS_SYMBOL();
    antlr4::tree::TerminalNode *PRECEDES_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TriggerFollowsPrecedesClauseContext* triggerFollowsPrecedesClause();

  class  CreateEventContext : public antlr4::ParserRuleContext {
  public:
    CreateEventContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    EventNameContext *eventName();
    std::vector<antlr4::tree::TerminalNode *> ON_SYMBOL();
    antlr4::tree::TerminalNode* ON_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *SCHEDULE_SYMBOL();
    ScheduleContext *schedule();
    antlr4::tree::TerminalNode *DO_SYMBOL();
    CompoundStatementContext *compoundStatement();
    DefinerClauseContext *definerClause();
    IfNotExistsContext *ifNotExists();
    antlr4::tree::TerminalNode *COMPLETION_SYMBOL();
    antlr4::tree::TerminalNode *PRESERVE_SYMBOL();
    antlr4::tree::TerminalNode *ENABLE_SYMBOL();
    antlr4::tree::TerminalNode *DISABLE_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *NOT_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateEventContext* createEvent();

  class  CreateRoleContext : public antlr4::ParserRuleContext {
  public:
    CreateRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    RoleListContext *roleList();
    IfNotExistsContext *ifNotExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateRoleContext* createRole();

  class  CreateSpatialReferenceContext : public antlr4::ParserRuleContext {
  public:
    CreateSpatialReferenceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OR_SYMBOL();
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    antlr4::tree::TerminalNode *SPATIAL_SYMBOL();
    antlr4::tree::TerminalNode *REFERENCE_SYMBOL();
    antlr4::tree::TerminalNode *SYSTEM_SYMBOL();
    Real_ulonglong_numberContext *real_ulonglong_number();
    std::vector<SrsAttributeContext *> srsAttribute();
    SrsAttributeContext* srsAttribute(size_t i);
    IfNotExistsContext *ifNotExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateSpatialReferenceContext* createSpatialReference();

  class  SrsAttributeContext : public antlr4::ParserRuleContext {
  public:
    SrsAttributeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NAME_SYMBOL();
    antlr4::tree::TerminalNode *TEXT_SYMBOL();
    TextStringNoLinebreakContext *textStringNoLinebreak();
    antlr4::tree::TerminalNode *DEFINITION_SYMBOL();
    antlr4::tree::TerminalNode *ORGANIZATION_SYMBOL();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    Real_ulonglong_numberContext *real_ulonglong_number();
    antlr4::tree::TerminalNode *DESCRIPTION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SrsAttributeContext* srsAttribute();

  class  DropStatementContext : public antlr4::ParserRuleContext {
  public:
    DropStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    DropDatabaseContext *dropDatabase();
    DropEventContext *dropEvent();
    DropFunctionContext *dropFunction();
    DropProcedureContext *dropProcedure();
    DropIndexContext *dropIndex();
    DropLogfileGroupContext *dropLogfileGroup();
    DropServerContext *dropServer();
    DropTableContext *dropTable();
    DropTableSpaceContext *dropTableSpace();
    DropTriggerContext *dropTrigger();
    DropViewContext *dropView();
    DropRoleContext *dropRole();
    DropSpatialReferenceContext *dropSpatialReference();
    DropUndoTablespaceContext *dropUndoTablespace();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropStatementContext* dropStatement();

  class  DropDatabaseContext : public antlr4::ParserRuleContext {
  public:
    DropDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATABASE_SYMBOL();
    SchemaRefContext *schemaRef();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropDatabaseContext* dropDatabase();

  class  DropEventContext : public antlr4::ParserRuleContext {
  public:
    DropEventContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    EventRefContext *eventRef();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropEventContext* dropEvent();

  class  DropFunctionContext : public antlr4::ParserRuleContext {
  public:
    DropFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    FunctionRefContext *functionRef();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropFunctionContext* dropFunction();

  class  DropProcedureContext : public antlr4::ParserRuleContext {
  public:
    DropProcedureContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROCEDURE_SYMBOL();
    ProcedureRefContext *procedureRef();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropProcedureContext* dropProcedure();

  class  DropIndexContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    DropIndexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IndexRefContext *indexRef();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    OnlineOptionContext *onlineOption();
    IndexLockAndAlgorithmContext *indexLockAndAlgorithm();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropIndexContext* dropIndex();

  class  DropLogfileGroupContext : public antlr4::ParserRuleContext {
  public:
    DropLogfileGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOGFILE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    LogfileGroupRefContext *logfileGroupRef();
    std::vector<DropLogfileGroupOptionContext *> dropLogfileGroupOption();
    DropLogfileGroupOptionContext* dropLogfileGroupOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropLogfileGroupContext* dropLogfileGroup();

  class  DropLogfileGroupOptionContext : public antlr4::ParserRuleContext {
  public:
    DropLogfileGroupOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TsOptionWaitContext *tsOptionWait();
    TsOptionEngineContext *tsOptionEngine();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropLogfileGroupOptionContext* dropLogfileGroupOption();

  class  DropServerContext : public antlr4::ParserRuleContext {
  public:
    DropServerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SERVER_SYMBOL();
    ServerRefContext *serverRef();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropServerContext* dropServer();

  class  DropTableContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    DropTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefListContext *tableRefList();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    antlr4::tree::TerminalNode *TEMPORARY_SYMBOL();
    IfExistsContext *ifExists();
    antlr4::tree::TerminalNode *RESTRICT_SYMBOL();
    antlr4::tree::TerminalNode *CASCADE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropTableContext* dropTable();

  class  DropTableSpaceContext : public antlr4::ParserRuleContext {
  public:
    DropTableSpaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    TablespaceRefContext *tablespaceRef();
    std::vector<DropLogfileGroupOptionContext *> dropLogfileGroupOption();
    DropLogfileGroupOptionContext* dropLogfileGroupOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropTableSpaceContext* dropTableSpace();

  class  DropTriggerContext : public antlr4::ParserRuleContext {
  public:
    DropTriggerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRIGGER_SYMBOL();
    TriggerRefContext *triggerRef();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropTriggerContext* dropTrigger();

  class  DropViewContext : public antlr4::ParserRuleContext {
  public:
    DropViewContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    ViewRefListContext *viewRefList();
    IfExistsContext *ifExists();
    antlr4::tree::TerminalNode *RESTRICT_SYMBOL();
    antlr4::tree::TerminalNode *CASCADE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropViewContext* dropView();

  class  DropRoleContext : public antlr4::ParserRuleContext {
  public:
    DropRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    RoleListContext *roleList();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropRoleContext* dropRole();

  class  DropSpatialReferenceContext : public antlr4::ParserRuleContext {
  public:
    DropSpatialReferenceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SPATIAL_SYMBOL();
    antlr4::tree::TerminalNode *REFERENCE_SYMBOL();
    antlr4::tree::TerminalNode *SYSTEM_SYMBOL();
    Real_ulonglong_numberContext *real_ulonglong_number();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropSpatialReferenceContext* dropSpatialReference();

  class  DropUndoTablespaceContext : public antlr4::ParserRuleContext {
  public:
    DropUndoTablespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNDO_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    TablespaceRefContext *tablespaceRef();
    UndoTableSpaceOptionsContext *undoTableSpaceOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropUndoTablespaceContext* dropUndoTablespace();

  class  RenameTableStatementContext : public antlr4::ParserRuleContext {
  public:
    RenameTableStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME_SYMBOL();
    std::vector<RenamePairContext *> renamePair();
    RenamePairContext* renamePair(size_t i);
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RenameTableStatementContext* renameTableStatement();

  class  RenamePairContext : public antlr4::ParserRuleContext {
  public:
    RenamePairContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    TableNameContext *tableName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RenamePairContext* renamePair();

  class  TruncateTableStatementContext : public antlr4::ParserRuleContext {
  public:
    TruncateTableStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRUNCATE_SYMBOL();
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TruncateTableStatementContext* truncateTableStatement();

  class  ImportStatementContext : public antlr4::ParserRuleContext {
  public:
    ImportStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IMPORT_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    TextStringLiteralListContext *textStringLiteralList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ImportStatementContext* importStatement();

  class  CallStatementContext : public antlr4::ParserRuleContext {
  public:
    CallStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL_SYMBOL();
    ProcedureRefContext *procedureRef();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    ExprListContext *exprList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CallStatementContext* callStatement();

  class  DeleteStatementContext : public antlr4::ParserRuleContext {
  public:
    DeleteStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DELETE_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    TableAliasRefListContext *tableAliasRefList();
    TableReferenceListContext *tableReferenceList();
    WithClauseContext *withClause();
    std::vector<DeleteStatementOptionContext *> deleteStatementOption();
    DeleteStatementOptionContext* deleteStatementOption(size_t i);
    antlr4::tree::TerminalNode *USING_SYMBOL();
    TableRefContext *tableRef();
    WhereClauseContext *whereClause();
    TableAliasContext *tableAlias();
    PartitionDeleteContext *partitionDelete();
    OrderClauseContext *orderClause();
    SimpleLimitClauseContext *simpleLimitClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DeleteStatementContext* deleteStatement();

  class  PartitionDeleteContext : public antlr4::ParserRuleContext {
  public:
    PartitionDeleteContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionDeleteContext* partitionDelete();

  class  DeleteStatementOptionContext : public antlr4::ParserRuleContext {
  public:
    DeleteStatementOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *QUICK_SYMBOL();
    antlr4::tree::TerminalNode *LOW_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DeleteStatementOptionContext* deleteStatementOption();

  class  DoStatementContext : public antlr4::ParserRuleContext {
  public:
    DoStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DO_SYMBOL();
    ExprListContext *exprList();
    SelectItemListContext *selectItemList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DoStatementContext* doStatement();

  class  HandlerStatementContext : public antlr4::ParserRuleContext {
  public:
    HandlerStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *HANDLER_SYMBOL();
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *OPEN_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *CLOSE_SYMBOL();
    antlr4::tree::TerminalNode *READ_SYMBOL();
    HandlerReadOrScanContext *handlerReadOrScan();
    TableAliasContext *tableAlias();
    WhereClauseContext *whereClause();
    LimitClauseContext *limitClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HandlerStatementContext* handlerStatement();

  class  HandlerReadOrScanContext : public antlr4::ParserRuleContext {
  public:
    HandlerReadOrScanContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FIRST_SYMBOL();
    antlr4::tree::TerminalNode *NEXT_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ValuesContext *values();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *PREV_SYMBOL();
    antlr4::tree::TerminalNode *LAST_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *LESS_THAN_OPERATOR();
    antlr4::tree::TerminalNode *GREATER_THAN_OPERATOR();
    antlr4::tree::TerminalNode *LESS_OR_EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *GREATER_OR_EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HandlerReadOrScanContext* handlerReadOrScan();

  class  InsertStatementContext : public antlr4::ParserRuleContext {
  public:
    InsertStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INSERT_SYMBOL();
    TableRefContext *tableRef();
    InsertFromConstructorContext *insertFromConstructor();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    UpdateListContext *updateList();
    InsertQueryExpressionContext *insertQueryExpression();
    InsertLockOptionContext *insertLockOption();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    UsePartitionContext *usePartition();
    InsertUpdateListContext *insertUpdateList();
    ValuesReferenceContext *valuesReference();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertStatementContext* insertStatement();

  class  InsertLockOptionContext : public antlr4::ParserRuleContext {
  public:
    InsertLockOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOW_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *DELAYED_SYMBOL();
    antlr4::tree::TerminalNode *HIGH_PRIORITY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertLockOptionContext* insertLockOption();

  class  InsertFromConstructorContext : public antlr4::ParserRuleContext {
  public:
    InsertFromConstructorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InsertValuesContext *insertValues();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    FieldsContext *fields();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertFromConstructorContext* insertFromConstructor();

  class  FieldsContext : public antlr4::ParserRuleContext {
  public:
    FieldsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<InsertIdentifierContext *> insertIdentifier();
    InsertIdentifierContext* insertIdentifier(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldsContext* fields();

  class  InsertValuesContext : public antlr4::ParserRuleContext {
  public:
    InsertValuesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ValueListContext *valueList();
    antlr4::tree::TerminalNode *VALUES_SYMBOL();
    antlr4::tree::TerminalNode *VALUE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertValuesContext* insertValues();

  class  InsertQueryExpressionContext : public antlr4::ParserRuleContext {
  public:
    InsertQueryExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionOrParensContext *queryExpressionOrParens();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    FieldsContext *fields();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertQueryExpressionContext* insertQueryExpression();

  class  ValueListContext : public antlr4::ParserRuleContext {
  public:
    ValueListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode* OPEN_PAR_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode* CLOSE_PAR_SYMBOL(size_t i);
    std::vector<ValuesContext *> values();
    ValuesContext* values(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ValueListContext* valueList();

  class  ValuesContext : public antlr4::ParserRuleContext {
  public:
    ValuesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode* DEFAULT_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ValuesContext* values();

  class  ValuesReferenceContext : public antlr4::ParserRuleContext {
  public:
    ValuesReferenceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AS_SYMBOL();
    IdentifierContext *identifier();
    ColumnInternalRefListContext *columnInternalRefList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ValuesReferenceContext* valuesReference();

  class  InsertUpdateListContext : public antlr4::ParserRuleContext {
  public:
    InsertUpdateListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON_SYMBOL();
    antlr4::tree::TerminalNode *DUPLICATE_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    UpdateListContext *updateList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertUpdateListContext* insertUpdateList();

  class  LoadStatementContext : public antlr4::ParserRuleContext {
  public:
    LoadStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOAD_SYMBOL();
    DataOrXmlContext *dataOrXml();
    antlr4::tree::TerminalNode *INFILE_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    TableRefContext *tableRef();
    LoadDataFileTailContext *loadDataFileTail();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    UsePartitionContext *usePartition();
    CharsetClauseContext *charsetClause();
    XmlRowsIdentifiedByContext *xmlRowsIdentifiedBy();
    FieldsClauseContext *fieldsClause();
    LinesClauseContext *linesClause();
    antlr4::tree::TerminalNode *LOW_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *CONCURRENT_SYMBOL();
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LoadStatementContext* loadStatement();

  class  DataOrXmlContext : public antlr4::ParserRuleContext {
  public:
    DataOrXmlContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *XML_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DataOrXmlContext* dataOrXml();

  class  XmlRowsIdentifiedByContext : public antlr4::ParserRuleContext {
  public:
    XmlRowsIdentifiedByContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ROWS_SYMBOL();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    TextStringContext *textString();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  XmlRowsIdentifiedByContext* xmlRowsIdentifiedBy();

  class  LoadDataFileTailContext : public antlr4::ParserRuleContext {
  public:
    LoadDataFileTailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();
    antlr4::tree::TerminalNode *INT_NUMBER();
    LoadDataFileTargetListContext *loadDataFileTargetList();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    UpdateListContext *updateList();
    antlr4::tree::TerminalNode *LINES_SYMBOL();
    antlr4::tree::TerminalNode *ROWS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LoadDataFileTailContext* loadDataFileTail();

  class  LoadDataFileTargetListContext : public antlr4::ParserRuleContext {
  public:
    LoadDataFileTargetListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    FieldOrVariableListContext *fieldOrVariableList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LoadDataFileTargetListContext* loadDataFileTargetList();

  class  FieldOrVariableListContext : public antlr4::ParserRuleContext {
  public:
    FieldOrVariableListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ColumnRefContext *> columnRef();
    ColumnRefContext* columnRef(size_t i);
    std::vector<UserVariableContext *> userVariable();
    UserVariableContext* userVariable(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldOrVariableListContext* fieldOrVariableList();

  class  ReplaceStatementContext : public antlr4::ParserRuleContext {
  public:
    ReplaceStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    TableRefContext *tableRef();
    InsertFromConstructorContext *insertFromConstructor();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    UpdateListContext *updateList();
    InsertQueryExpressionContext *insertQueryExpression();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    UsePartitionContext *usePartition();
    antlr4::tree::TerminalNode *LOW_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *DELAYED_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReplaceStatementContext* replaceStatement();

  class  SelectStatementContext : public antlr4::ParserRuleContext {
  public:
    SelectStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionContext *queryExpression();
    QueryExpressionParensContext *queryExpressionParens();
    SelectStatementWithIntoContext *selectStatementWithInto();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectStatementContext* selectStatement();

  class  SelectStatementWithIntoContext : public antlr4::ParserRuleContext {
  public:
    SelectStatementWithIntoContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    SelectStatementWithIntoContext *selectStatementWithInto();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    QueryExpressionContext *queryExpression();
    IntoClauseContext *intoClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectStatementWithIntoContext* selectStatementWithInto();

  class  QueryExpressionContext : public antlr4::ParserRuleContext {
  public:
    QueryExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionBodyContext *queryExpressionBody();
    QueryExpressionParensContext *queryExpressionParens();
    WithClauseContext *withClause();
    ProcedureAnalyseClauseContext *procedureAnalyseClause();
    LockingClauseContext *lockingClause();
    OrderClauseContext *orderClause();
    LimitClauseContext *limitClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryExpressionContext* queryExpression();

  class  QueryExpressionBodyContext : public antlr4::ParserRuleContext {
  public:
    QueryExpressionBodyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QuerySpecificationContext *querySpecification();
    std::vector<QueryExpressionParensContext *> queryExpressionParens();
    QueryExpressionParensContext* queryExpressionParens(size_t i);
    antlr4::tree::TerminalNode *UNION_SYMBOL();
    UnionOptionContext *unionOption();
    QueryExpressionBodyContext *queryExpressionBody();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryExpressionBodyContext* queryExpressionBody();
  QueryExpressionBodyContext* queryExpressionBody(int precedence);
  class  QueryExpressionParensContext : public antlr4::ParserRuleContext {
  public:
    QueryExpressionParensContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    QueryExpressionParensContext *queryExpressionParens();
    QueryExpressionContext *queryExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryExpressionParensContext* queryExpressionParens();

  class  QuerySpecificationContext : public antlr4::ParserRuleContext {
  public:
    QuerySpecificationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SELECT_SYMBOL();
    SelectItemListContext *selectItemList();
    std::vector<SelectOptionContext *> selectOption();
    SelectOptionContext* selectOption(size_t i);
    IntoClauseContext *intoClause();
    FromClauseContext *fromClause();
    WhereClauseContext *whereClause();
    GroupByClauseContext *groupByClause();
    HavingClauseContext *havingClause();
    WindowClauseContext *windowClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QuerySpecificationContext* querySpecification();

  class  SubqueryContext : public antlr4::ParserRuleContext {
  public:
    SubqueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryExpressionParensContext *queryExpressionParens();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SubqueryContext* subquery();

  class  QuerySpecOptionContext : public antlr4::ParserRuleContext {
  public:
    QuerySpecOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *DISTINCT_SYMBOL();
    antlr4::tree::TerminalNode *STRAIGHT_JOIN_SYMBOL();
    antlr4::tree::TerminalNode *HIGH_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *SQL_SMALL_RESULT_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BIG_RESULT_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BUFFER_RESULT_SYMBOL();
    antlr4::tree::TerminalNode *SQL_CALC_FOUND_ROWS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QuerySpecOptionContext* querySpecOption();

  class  LimitClauseContext : public antlr4::ParserRuleContext {
  public:
    LimitClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT_SYMBOL();
    LimitOptionsContext *limitOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LimitClauseContext* limitClause();

  class  SimpleLimitClauseContext : public antlr4::ParserRuleContext {
  public:
    SimpleLimitClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT_SYMBOL();
    LimitOptionContext *limitOption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SimpleLimitClauseContext* simpleLimitClause();

  class  LimitOptionsContext : public antlr4::ParserRuleContext {
  public:
    LimitOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<LimitOptionContext *> limitOption();
    LimitOptionContext* limitOption(size_t i);
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    antlr4::tree::TerminalNode *OFFSET_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LimitOptionsContext* limitOptions();

  class  LimitOptionContext : public antlr4::ParserRuleContext {
  public:
    LimitOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *PARAM_MARKER();
    antlr4::tree::TerminalNode *ULONGLONG_NUMBER();
    antlr4::tree::TerminalNode *LONG_NUMBER();
    antlr4::tree::TerminalNode *INT_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LimitOptionContext* limitOption();

  class  IntoClauseContext : public antlr4::ParserRuleContext {
  public:
    IntoClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    antlr4::tree::TerminalNode *OUTFILE_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *DUMPFILE_SYMBOL();
    std::vector<TextOrIdentifierContext *> textOrIdentifier();
    TextOrIdentifierContext* textOrIdentifier(size_t i);
    std::vector<UserVariableContext *> userVariable();
    UserVariableContext* userVariable(size_t i);
    CharsetClauseContext *charsetClause();
    FieldsClauseContext *fieldsClause();
    LinesClauseContext *linesClause();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IntoClauseContext* intoClause();

  class  ProcedureAnalyseClauseContext : public antlr4::ParserRuleContext {
  public:
    ProcedureAnalyseClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROCEDURE_SYMBOL();
    antlr4::tree::TerminalNode *ANALYSE_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> INT_NUMBER();
    antlr4::tree::TerminalNode* INT_NUMBER(size_t i);
    antlr4::tree::TerminalNode *COMMA_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ProcedureAnalyseClauseContext* procedureAnalyseClause();

  class  HavingClauseContext : public antlr4::ParserRuleContext {
  public:
    HavingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *HAVING_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HavingClauseContext* havingClause();

  class  WindowClauseContext : public antlr4::ParserRuleContext {
  public:
    WindowClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WINDOW_SYMBOL();
    std::vector<WindowDefinitionContext *> windowDefinition();
    WindowDefinitionContext* windowDefinition(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowClauseContext* windowClause();

  class  WindowDefinitionContext : public antlr4::ParserRuleContext {
  public:
    WindowDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WindowNameContext *windowName();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    WindowSpecContext *windowSpec();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowDefinitionContext* windowDefinition();

  class  WindowSpecContext : public antlr4::ParserRuleContext {
  public:
    WindowSpecContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    WindowSpecDetailsContext *windowSpecDetails();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowSpecContext* windowSpec();

  class  WindowSpecDetailsContext : public antlr4::ParserRuleContext {
  public:
    WindowSpecDetailsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WindowNameContext *windowName();
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    OrderListContext *orderList();
    OrderClauseContext *orderClause();
    WindowFrameClauseContext *windowFrameClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowSpecDetailsContext* windowSpecDetails();

  class  WindowFrameClauseContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WindowFrameUnitsContext *windowFrameUnits();
    WindowFrameExtentContext *windowFrameExtent();
    WindowFrameExclusionContext *windowFrameExclusion();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameClauseContext* windowFrameClause();

  class  WindowFrameUnitsContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameUnitsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ROWS_SYMBOL();
    antlr4::tree::TerminalNode *RANGE_SYMBOL();
    antlr4::tree::TerminalNode *GROUPS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameUnitsContext* windowFrameUnits();

  class  WindowFrameExtentContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameExtentContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WindowFrameStartContext *windowFrameStart();
    WindowFrameBetweenContext *windowFrameBetween();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameExtentContext* windowFrameExtent();

  class  WindowFrameStartContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameStartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNBOUNDED_SYMBOL();
    antlr4::tree::TerminalNode *PRECEDING_SYMBOL();
    Ulonglong_numberContext *ulonglong_number();
    antlr4::tree::TerminalNode *PARAM_MARKER();
    antlr4::tree::TerminalNode *INTERVAL_SYMBOL();
    ExprContext *expr();
    IntervalContext *interval();
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    antlr4::tree::TerminalNode *ROW_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameStartContext* windowFrameStart();

  class  WindowFrameBetweenContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameBetweenContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BETWEEN_SYMBOL();
    std::vector<WindowFrameBoundContext *> windowFrameBound();
    WindowFrameBoundContext* windowFrameBound(size_t i);
    antlr4::tree::TerminalNode *AND_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameBetweenContext* windowFrameBetween();

  class  WindowFrameBoundContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameBoundContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WindowFrameStartContext *windowFrameStart();
    antlr4::tree::TerminalNode *UNBOUNDED_SYMBOL();
    antlr4::tree::TerminalNode *FOLLOWING_SYMBOL();
    Ulonglong_numberContext *ulonglong_number();
    antlr4::tree::TerminalNode *PARAM_MARKER();
    antlr4::tree::TerminalNode *INTERVAL_SYMBOL();
    ExprContext *expr();
    IntervalContext *interval();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameBoundContext* windowFrameBound();

  class  WindowFrameExclusionContext : public antlr4::ParserRuleContext {
  public:
    WindowFrameExclusionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXCLUDE_SYMBOL();
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    antlr4::tree::TerminalNode *ROW_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    antlr4::tree::TerminalNode *TIES_SYMBOL();
    antlr4::tree::TerminalNode *NO_SYMBOL();
    antlr4::tree::TerminalNode *OTHERS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFrameExclusionContext* windowFrameExclusion();

  class  WithClauseContext : public antlr4::ParserRuleContext {
  public:
    WithClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    std::vector<CommonTableExpressionContext *> commonTableExpression();
    CommonTableExpressionContext* commonTableExpression(size_t i);
    antlr4::tree::TerminalNode *RECURSIVE_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WithClauseContext* withClause();

  class  CommonTableExpressionContext : public antlr4::ParserRuleContext {
  public:
    CommonTableExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    SubqueryContext *subquery();
    ColumnInternalRefListContext *columnInternalRefList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CommonTableExpressionContext* commonTableExpression();

  class  GroupByClauseContext : public antlr4::ParserRuleContext {
  public:
    GroupByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    OrderListContext *orderList();
    OlapOptionContext *olapOption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GroupByClauseContext* groupByClause();

  class  OlapOptionContext : public antlr4::ParserRuleContext {
  public:
    OlapOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *ROLLUP_SYMBOL();
    antlr4::tree::TerminalNode *CUBE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OlapOptionContext* olapOption();

  class  OrderClauseContext : public antlr4::ParserRuleContext {
  public:
    OrderClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ORDER_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    OrderListContext *orderList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OrderClauseContext* orderClause();

  class  DirectionContext : public antlr4::ParserRuleContext {
  public:
    DirectionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASC_SYMBOL();
    antlr4::tree::TerminalNode *DESC_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DirectionContext* direction();

  class  FromClauseContext : public antlr4::ParserRuleContext {
  public:
    FromClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *DUAL_SYMBOL();
    TableReferenceListContext *tableReferenceList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromClauseContext* fromClause();

  class  TableReferenceListContext : public antlr4::ParserRuleContext {
  public:
    TableReferenceListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TableReferenceContext *> tableReference();
    TableReferenceContext* tableReference(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableReferenceListContext* tableReferenceList();

  class  SelectOptionContext : public antlr4::ParserRuleContext {
  public:
    SelectOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QuerySpecOptionContext *querySpecOption();
    antlr4::tree::TerminalNode *SQL_NO_CACHE_SYMBOL();
    antlr4::tree::TerminalNode *SQL_CACHE_SYMBOL();
    antlr4::tree::TerminalNode *MAX_STATEMENT_TIME_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    Real_ulong_numberContext *real_ulong_number();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectOptionContext* selectOption();

  class  LockingClauseContext : public antlr4::ParserRuleContext {
  public:
    LockingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    LockStrenghContext *lockStrengh();
    antlr4::tree::TerminalNode *OF_SYMBOL();
    TableAliasRefListContext *tableAliasRefList();
    LockedRowActionContext *lockedRowAction();
    antlr4::tree::TerminalNode *LOCK_SYMBOL();
    antlr4::tree::TerminalNode *IN_SYMBOL();
    antlr4::tree::TerminalNode *SHARE_SYMBOL();
    antlr4::tree::TerminalNode *MODE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LockingClauseContext* lockingClause();

  class  LockStrenghContext : public antlr4::ParserRuleContext {
  public:
    LockStrenghContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    antlr4::tree::TerminalNode *SHARE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LockStrenghContext* lockStrengh();

  class  LockedRowActionContext : public antlr4::ParserRuleContext {
  public:
    LockedRowActionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SKIP_SYMBOL();
    antlr4::tree::TerminalNode *LOCKED_SYMBOL();
    antlr4::tree::TerminalNode *NOWAIT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LockedRowActionContext* lockedRowAction();

  class  SelectItemListContext : public antlr4::ParserRuleContext {
  public:
    SelectItemListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SelectItemContext *> selectItem();
    SelectItemContext* selectItem(size_t i);
    antlr4::tree::TerminalNode *MULT_OPERATOR();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectItemListContext* selectItemList();

  class  SelectItemContext : public antlr4::ParserRuleContext {
  public:
    SelectItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableWildContext *tableWild();
    ExprContext *expr();
    SelectAliasContext *selectAlias();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectItemContext* selectItem();

  class  SelectAliasContext : public antlr4::ParserRuleContext {
  public:
    SelectAliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *AS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectAliasContext* selectAlias();

  class  WhereClauseContext : public antlr4::ParserRuleContext {
  public:
    WhereClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHERE_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WhereClauseContext* whereClause();

  class  TableReferenceContext : public antlr4::ParserRuleContext {
  public:
    TableReferenceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableFactorContext *tableFactor();
    antlr4::tree::TerminalNode *OPEN_CURLY_SYMBOL();
    EscapedTableReferenceContext *escapedTableReference();
    antlr4::tree::TerminalNode *CLOSE_CURLY_SYMBOL();
    std::vector<JoinedTableContext *> joinedTable();
    JoinedTableContext* joinedTable(size_t i);
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *OJ_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableReferenceContext* tableReference();

  class  EscapedTableReferenceContext : public antlr4::ParserRuleContext {
  public:
    EscapedTableReferenceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableFactorContext *tableFactor();
    std::vector<JoinedTableContext *> joinedTable();
    JoinedTableContext* joinedTable(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  EscapedTableReferenceContext* escapedTableReference();

  class  JoinedTableContext : public antlr4::ParserRuleContext {
  public:
    JoinedTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InnerJoinTypeContext *innerJoinType();
    TableReferenceContext *tableReference();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    IdentifierListWithParenthesesContext *identifierListWithParentheses();
    OuterJoinTypeContext *outerJoinType();
    NaturalJoinTypeContext *naturalJoinType();
    TableFactorContext *tableFactor();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JoinedTableContext* joinedTable();

  class  NaturalJoinTypeContext : public antlr4::ParserRuleContext {
  public:
    NaturalJoinTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NATURAL_SYMBOL();
    antlr4::tree::TerminalNode *JOIN_SYMBOL();
    antlr4::tree::TerminalNode *INNER_SYMBOL();
    antlr4::tree::TerminalNode *LEFT_SYMBOL();
    antlr4::tree::TerminalNode *RIGHT_SYMBOL();
    antlr4::tree::TerminalNode *OUTER_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NaturalJoinTypeContext* naturalJoinType();

  class  InnerJoinTypeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    InnerJoinTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JOIN_SYMBOL();
    antlr4::tree::TerminalNode *INNER_SYMBOL();
    antlr4::tree::TerminalNode *CROSS_SYMBOL();
    antlr4::tree::TerminalNode *STRAIGHT_JOIN_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InnerJoinTypeContext* innerJoinType();

  class  OuterJoinTypeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    OuterJoinTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JOIN_SYMBOL();
    antlr4::tree::TerminalNode *LEFT_SYMBOL();
    antlr4::tree::TerminalNode *RIGHT_SYMBOL();
    antlr4::tree::TerminalNode *OUTER_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OuterJoinTypeContext* outerJoinType();

  class  TableFactorContext : public antlr4::ParserRuleContext {
  public:
    TableFactorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SingleTableContext *singleTable();
    SingleTableParensContext *singleTableParens();
    DerivedTableContext *derivedTable();
    TableReferenceListParensContext *tableReferenceListParens();
    TableFunctionContext *tableFunction();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableFactorContext* tableFactor();

  class  SingleTableContext : public antlr4::ParserRuleContext {
  public:
    SingleTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    UsePartitionContext *usePartition();
    TableAliasContext *tableAlias();
    IndexHintListContext *indexHintList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SingleTableContext* singleTable();

  class  SingleTableParensContext : public antlr4::ParserRuleContext {
  public:
    SingleTableParensContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    SingleTableContext *singleTable();
    SingleTableParensContext *singleTableParens();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SingleTableParensContext* singleTableParens();

  class  DerivedTableContext : public antlr4::ParserRuleContext {
  public:
    DerivedTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SubqueryContext *subquery();
    TableAliasContext *tableAlias();
    ColumnInternalRefListContext *columnInternalRefList();
    antlr4::tree::TerminalNode *LATERAL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DerivedTableContext* derivedTable();

  class  TableReferenceListParensContext : public antlr4::ParserRuleContext {
  public:
    TableReferenceListParensContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    TableReferenceListContext *tableReferenceList();
    TableReferenceListParensContext *tableReferenceListParens();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableReferenceListParensContext* tableReferenceListParens();

  class  TableFunctionContext : public antlr4::ParserRuleContext {
  public:
    TableFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JSON_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    ColumnsClauseContext *columnsClause();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    TableAliasContext *tableAlias();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableFunctionContext* tableFunction();

  class  ColumnsClauseContext : public antlr4::ParserRuleContext {
  public:
    ColumnsClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLUMNS_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<JtColumnContext *> jtColumn();
    JtColumnContext* jtColumn(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnsClauseContext* columnsClause();

  class  JtColumnContext : public antlr4::ParserRuleContext {
  public:
    JtColumnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *ORDINALITY_SYMBOL();
    DataTypeContext *dataType();
    antlr4::tree::TerminalNode *PATH_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    CollateContext *collate();
    antlr4::tree::TerminalNode *EXISTS_SYMBOL();
    OnEmptyOrErrorContext *onEmptyOrError();
    antlr4::tree::TerminalNode *NESTED_SYMBOL();
    ColumnsClauseContext *columnsClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JtColumnContext* jtColumn();

  class  OnEmptyOrErrorContext : public antlr4::ParserRuleContext {
  public:
    OnEmptyOrErrorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OnEmptyContext *onEmpty();
    OnErrorContext *onError();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OnEmptyOrErrorContext* onEmptyOrError();

  class  OnEmptyContext : public antlr4::ParserRuleContext {
  public:
    OnEmptyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    JtOnResponseContext *jtOnResponse();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    antlr4::tree::TerminalNode *EMPTY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OnEmptyContext* onEmpty();

  class  OnErrorContext : public antlr4::ParserRuleContext {
  public:
    OnErrorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    JtOnResponseContext *jtOnResponse();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    antlr4::tree::TerminalNode *ERROR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OnErrorContext* onError();

  class  JtOnResponseContext : public antlr4::ParserRuleContext {
  public:
    JtOnResponseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ERROR_SYMBOL();
    antlr4::tree::TerminalNode *NULL_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    TextStringLiteralContext *textStringLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JtOnResponseContext* jtOnResponse();

  class  UnionOptionContext : public antlr4::ParserRuleContext {
  public:
    UnionOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DISTINCT_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UnionOptionContext* unionOption();

  class  TableAliasContext : public antlr4::ParserRuleContext {
  public:
    TableAliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableAliasContext* tableAlias();

  class  IndexHintListContext : public antlr4::ParserRuleContext {
  public:
    IndexHintListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IndexHintContext *> indexHint();
    IndexHintContext* indexHint(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexHintListContext* indexHintList();

  class  IndexHintContext : public antlr4::ParserRuleContext {
  public:
    IndexHintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IndexHintTypeContext *indexHintType();
    KeyOrIndexContext *keyOrIndex();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    IndexListContext *indexList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    IndexHintClauseContext *indexHintClause();
    antlr4::tree::TerminalNode *USE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexHintContext* indexHint();

  class  IndexHintTypeContext : public antlr4::ParserRuleContext {
  public:
    IndexHintTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FORCE_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexHintTypeContext* indexHintType();

  class  KeyOrIndexContext : public antlr4::ParserRuleContext {
  public:
    KeyOrIndexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyOrIndexContext* keyOrIndex();

  class  ConstraintKeyTypeContext : public antlr4::ParserRuleContext {
  public:
    ConstraintKeyTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *UNIQUE_SYMBOL();
    KeyOrIndexContext *keyOrIndex();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConstraintKeyTypeContext* constraintKeyType();

  class  IndexHintClauseContext : public antlr4::ParserRuleContext {
  public:
    IndexHintClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *JOIN_SYMBOL();
    antlr4::tree::TerminalNode *ORDER_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexHintClauseContext* indexHintClause();

  class  IndexListContext : public antlr4::ParserRuleContext {
  public:
    IndexListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IndexListElementContext *> indexListElement();
    IndexListElementContext* indexListElement(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexListContext* indexList();

  class  IndexListElementContext : public antlr4::ParserRuleContext {
  public:
    IndexListElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexListElementContext* indexListElement();

  class  UpdateStatementContext : public antlr4::ParserRuleContext {
  public:
    UpdateStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    TableReferenceListContext *tableReferenceList();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    UpdateListContext *updateList();
    WithClauseContext *withClause();
    antlr4::tree::TerminalNode *LOW_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();
    WhereClauseContext *whereClause();
    OrderClauseContext *orderClause();
    SimpleLimitClauseContext *simpleLimitClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UpdateStatementContext* updateStatement();

  class  TransactionOrLockingStatementContext : public antlr4::ParserRuleContext {
  public:
    TransactionOrLockingStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TransactionStatementContext *transactionStatement();
    SavepointStatementContext *savepointStatement();
    LockStatementContext *lockStatement();
    XaStatementContext *xaStatement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TransactionOrLockingStatementContext* transactionOrLockingStatement();

  class  TransactionStatementContext : public antlr4::ParserRuleContext {
  public:
    TransactionStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *START_SYMBOL();
    antlr4::tree::TerminalNode *TRANSACTION_SYMBOL();
    std::vector<TransactionCharacteristicContext *> transactionCharacteristic();
    TransactionCharacteristicContext* transactionCharacteristic(size_t i);
    antlr4::tree::TerminalNode *COMMIT_SYMBOL();
    antlr4::tree::TerminalNode *WORK_SYMBOL();
    antlr4::tree::TerminalNode *AND_SYMBOL();
    antlr4::tree::TerminalNode *CHAIN_SYMBOL();
    antlr4::tree::TerminalNode *RELEASE_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> NO_SYMBOL();
    antlr4::tree::TerminalNode* NO_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TransactionStatementContext* transactionStatement();

  class  BeginWorkContext : public antlr4::ParserRuleContext {
  public:
    BeginWorkContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BEGIN_SYMBOL();
    antlr4::tree::TerminalNode *WORK_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  BeginWorkContext* beginWork();

  class  TransactionCharacteristicContext : public antlr4::ParserRuleContext {
  public:
    TransactionCharacteristicContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *CONSISTENT_SYMBOL();
    antlr4::tree::TerminalNode *SNAPSHOT_SYMBOL();
    antlr4::tree::TerminalNode *READ_SYMBOL();
    antlr4::tree::TerminalNode *WRITE_SYMBOL();
    antlr4::tree::TerminalNode *ONLY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TransactionCharacteristicContext* transactionCharacteristic();

  class  SavepointStatementContext : public antlr4::ParserRuleContext {
  public:
    SavepointStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SAVEPOINT_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *ROLLBACK_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    antlr4::tree::TerminalNode *WORK_SYMBOL();
    antlr4::tree::TerminalNode *AND_SYMBOL();
    antlr4::tree::TerminalNode *CHAIN_SYMBOL();
    antlr4::tree::TerminalNode *RELEASE_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> NO_SYMBOL();
    antlr4::tree::TerminalNode* NO_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SavepointStatementContext* savepointStatement();

  class  LockStatementContext : public antlr4::ParserRuleContext {
  public:
    LockStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOCK_SYMBOL();
    std::vector<LockItemContext *> lockItem();
    LockItemContext* lockItem(size_t i);
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *INSTANCE_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *BACKUP_SYMBOL();
    antlr4::tree::TerminalNode *UNLOCK_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LockStatementContext* lockStatement();

  class  LockItemContext : public antlr4::ParserRuleContext {
  public:
    LockItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    LockOptionContext *lockOption();
    TableAliasContext *tableAlias();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LockItemContext* lockItem();

  class  LockOptionContext : public antlr4::ParserRuleContext {
  public:
    LockOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *READ_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *WRITE_SYMBOL();
    antlr4::tree::TerminalNode *LOW_PRIORITY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LockOptionContext* lockOption();

  class  XaStatementContext : public antlr4::ParserRuleContext {
  public:
    XaStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *XA_SYMBOL();
    XidContext *xid();
    antlr4::tree::TerminalNode *END_SYMBOL();
    antlr4::tree::TerminalNode *PREPARE_SYMBOL();
    antlr4::tree::TerminalNode *COMMIT_SYMBOL();
    antlr4::tree::TerminalNode *ROLLBACK_SYMBOL();
    antlr4::tree::TerminalNode *RECOVER_SYMBOL();
    XaConvertContext *xaConvert();
    antlr4::tree::TerminalNode *START_SYMBOL();
    antlr4::tree::TerminalNode *BEGIN_SYMBOL();
    antlr4::tree::TerminalNode *SUSPEND_SYMBOL();
    antlr4::tree::TerminalNode *ONE_SYMBOL();
    antlr4::tree::TerminalNode *PHASE_SYMBOL();
    antlr4::tree::TerminalNode *JOIN_SYMBOL();
    antlr4::tree::TerminalNode *RESUME_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *MIGRATE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  XaStatementContext* xaStatement();

  class  XaConvertContext : public antlr4::ParserRuleContext {
  public:
    XaConvertContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CONVERT_SYMBOL();
    antlr4::tree::TerminalNode *XID_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  XaConvertContext* xaConvert();

  class  XidContext : public antlr4::ParserRuleContext {
  public:
    XidContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TextStringContext *> textString();
    TextStringContext* textString(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    Ulong_numberContext *ulong_number();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  XidContext* xid();

  class  ReplicationStatementContext : public antlr4::ParserRuleContext {
  public:
    ReplicationStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PURGE_SYMBOL();
    antlr4::tree::TerminalNode *LOGS_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *BEFORE_SYMBOL();
    ExprContext *expr();
    ChangeMasterContext *changeMaster();
    antlr4::tree::TerminalNode *RESET_SYMBOL();
    std::vector<ResetOptionContext *> resetOption();
    ResetOptionContext* resetOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *PERSIST_SYMBOL();
    IfExistsContext *ifExists();
    IdentifierContext *identifier();
    SlaveContext *slave();
    ChangeReplicationContext *changeReplication();
    ReplicationLoadContext *replicationLoad();
    GroupReplicationContext *groupReplication();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReplicationStatementContext* replicationStatement();

  class  ResetOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    ResetOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    MasterResetOptionsContext *masterResetOptions();
    antlr4::tree::TerminalNode *CACHE_SYMBOL();
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    ChannelContext *channel();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResetOptionContext* resetOption();

  class  MasterResetOptionsContext : public antlr4::ParserRuleContext {
  public:
    MasterResetOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TO_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    Real_ulonglong_numberContext *real_ulonglong_number();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MasterResetOptionsContext* masterResetOptions();

  class  ReplicationLoadContext : public antlr4::ParserRuleContext {
  public:
    ReplicationLoadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOAD_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    TableRefContext *tableRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReplicationLoadContext* replicationLoad();

  class  ChangeMasterContext : public antlr4::ParserRuleContext {
  public:
    ChangeMasterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CHANGE_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    ChangeMasterOptionsContext *changeMasterOptions();
    ChannelContext *channel();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ChangeMasterContext* changeMaster();

  class  ChangeMasterOptionsContext : public antlr4::ParserRuleContext {
  public:
    ChangeMasterOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MasterOptionContext *> masterOption();
    MasterOptionContext* masterOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ChangeMasterOptionsContext* changeMasterOptions();

  class  MasterOptionContext : public antlr4::ParserRuleContext {
  public:
    MasterOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MASTER_HOST_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    TextStringNoLinebreakContext *textStringNoLinebreak();
    antlr4::tree::TerminalNode *NETWORK_NAMESPACE_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_BIND_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_USER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PORT_SYMBOL();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *MASTER_CONNECT_RETRY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_RETRY_COUNT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_DELAY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CA_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CAPATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_TLS_VERSION_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CERT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_TLS_CIPHERSUITES_SYMBOL();
    MasterTlsCiphersuitesDefContext *masterTlsCiphersuitesDef();
    antlr4::tree::TerminalNode *MASTER_SSL_CIPHER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_KEY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_VERIFY_SERVER_CERT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CRL_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *MASTER_SSL_CRLPATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PUBLIC_KEY_PATH_SYMBOL();
    antlr4::tree::TerminalNode *GET_MASTER_PUBLIC_KEY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_HEARTBEAT_PERIOD_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SERVER_IDS_SYMBOL();
    ServerIdListContext *serverIdList();
    antlr4::tree::TerminalNode *MASTER_COMPRESSION_ALGORITHM_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *MASTER_ZSTD_COMPRESSION_LEVEL_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_AUTO_POSITION_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGE_CHECKS_USER_SYMBOL();
    PrivilegeCheckDefContext *privilegeCheckDef();
    MasterFileDefContext *masterFileDef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MasterOptionContext* masterOption();

  class  PrivilegeCheckDefContext : public antlr4::ParserRuleContext {
  public:
    PrivilegeCheckDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UserIdentifierOrTextContext *userIdentifierOrText();
    antlr4::tree::TerminalNode *NULL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PrivilegeCheckDefContext* privilegeCheckDef();

  class  MasterTlsCiphersuitesDefContext : public antlr4::ParserRuleContext {
  public:
    MasterTlsCiphersuitesDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringNoLinebreakContext *textStringNoLinebreak();
    antlr4::tree::TerminalNode *NULL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MasterTlsCiphersuitesDefContext* masterTlsCiphersuitesDef();

  class  MasterFileDefContext : public antlr4::ParserRuleContext {
  public:
    MasterFileDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MASTER_LOG_FILE_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    TextStringNoLinebreakContext *textStringNoLinebreak();
    antlr4::tree::TerminalNode *MASTER_LOG_POS_SYMBOL();
    Ulonglong_numberContext *ulonglong_number();
    antlr4::tree::TerminalNode *RELAY_LOG_FILE_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_LOG_POS_SYMBOL();
    Ulong_numberContext *ulong_number();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MasterFileDefContext* masterFileDef();

  class  ServerIdListContext : public antlr4::ParserRuleContext {
  public:
    ServerIdListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<Ulong_numberContext *> ulong_number();
    Ulong_numberContext* ulong_number(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ServerIdListContext* serverIdList();

  class  ChangeReplicationContext : public antlr4::ParserRuleContext {
  public:
    ChangeReplicationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CHANGE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *FILTER_SYMBOL();
    std::vector<FilterDefinitionContext *> filterDefinition();
    FilterDefinitionContext* filterDefinition(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    ChannelContext *channel();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ChangeReplicationContext* changeReplication();

  class  FilterDefinitionContext : public antlr4::ParserRuleContext {
  public:
    FilterDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REPLICATE_DO_DB_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    FilterDbListContext *filterDbList();
    antlr4::tree::TerminalNode *REPLICATE_IGNORE_DB_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_DO_TABLE_SYMBOL();
    FilterTableListContext *filterTableList();
    antlr4::tree::TerminalNode *REPLICATE_IGNORE_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_WILD_DO_TABLE_SYMBOL();
    FilterStringListContext *filterStringList();
    antlr4::tree::TerminalNode *REPLICATE_WILD_IGNORE_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_REWRITE_DB_SYMBOL();
    FilterDbPairListContext *filterDbPairList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterDefinitionContext* filterDefinition();

  class  FilterDbListContext : public antlr4::ParserRuleContext {
  public:
    FilterDbListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SchemaRefContext *> schemaRef();
    SchemaRefContext* schemaRef(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterDbListContext* filterDbList();

  class  FilterTableListContext : public antlr4::ParserRuleContext {
  public:
    FilterTableListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<FilterTableRefContext *> filterTableRef();
    FilterTableRefContext* filterTableRef(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterTableListContext* filterTableList();

  class  FilterStringListContext : public antlr4::ParserRuleContext {
  public:
    FilterStringListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<FilterWildDbTableStringContext *> filterWildDbTableString();
    FilterWildDbTableStringContext* filterWildDbTableString(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterStringListContext* filterStringList();

  class  FilterWildDbTableStringContext : public antlr4::ParserRuleContext {
  public:
    FilterWildDbTableStringContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringNoLinebreakContext *textStringNoLinebreak();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterWildDbTableStringContext* filterWildDbTableString();

  class  FilterDbPairListContext : public antlr4::ParserRuleContext {
  public:
    FilterDbPairListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SchemaIdentifierPairContext *> schemaIdentifierPair();
    SchemaIdentifierPairContext* schemaIdentifierPair(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterDbPairListContext* filterDbPairList();

  class  SlaveContext : public antlr4::ParserRuleContext {
  public:
    SlaveContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *START_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();
    SlaveConnectionOptionsContext *slaveConnectionOptions();
    SlaveThreadOptionsContext *slaveThreadOptions();
    antlr4::tree::TerminalNode *UNTIL_SYMBOL();
    SlaveUntilOptionsContext *slaveUntilOptions();
    ChannelContext *channel();
    antlr4::tree::TerminalNode *STOP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SlaveContext* slave();

  class  SlaveUntilOptionsContext : public antlr4::ParserRuleContext {
  public:
    SlaveUntilOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MasterFileDefContext *> masterFileDef();
    MasterFileDefContext* masterFileDef(size_t i);
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *SQL_AFTER_MTS_GAPS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BEFORE_GTIDS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_AFTER_GTIDS_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SlaveUntilOptionsContext* slaveUntilOptions();

  class  SlaveConnectionOptionsContext : public antlr4::ParserRuleContext {
  public:
    SlaveConnectionOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *USER_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> EQUAL_OPERATOR();
    antlr4::tree::TerminalNode* EQUAL_OPERATOR(size_t i);
    std::vector<TextStringContext *> textString();
    TextStringContext* textString(size_t i);
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_AUTH_SYMBOL();
    antlr4::tree::TerminalNode *PLUGIN_DIR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SlaveConnectionOptionsContext* slaveConnectionOptions();

  class  SlaveThreadOptionsContext : public antlr4::ParserRuleContext {
  public:
    SlaveThreadOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SlaveThreadOptionContext *> slaveThreadOption();
    SlaveThreadOptionContext* slaveThreadOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SlaveThreadOptionsContext* slaveThreadOptions();

  class  SlaveThreadOptionContext : public antlr4::ParserRuleContext {
  public:
    SlaveThreadOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RELAY_THREAD_SYMBOL();
    antlr4::tree::TerminalNode *SQL_THREAD_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SlaveThreadOptionContext* slaveThreadOption();

  class  GroupReplicationContext : public antlr4::ParserRuleContext {
  public:
    GroupReplicationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUP_REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *START_SYMBOL();
    antlr4::tree::TerminalNode *STOP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GroupReplicationContext* groupReplication();

  class  PreparedStatementContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    PreparedStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *PREPARE_SYMBOL();
    TextLiteralContext *textLiteral();
    UserVariableContext *userVariable();
    ExecuteStatementContext *executeStatement();
    antlr4::tree::TerminalNode *DEALLOCATE_SYMBOL();
    antlr4::tree::TerminalNode *DROP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PreparedStatementContext* preparedStatement();

  class  ExecuteStatementContext : public antlr4::ParserRuleContext {
  public:
    ExecuteStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXECUTE_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    ExecuteVarListContext *executeVarList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExecuteStatementContext* executeStatement();

  class  ExecuteVarListContext : public antlr4::ParserRuleContext {
  public:
    ExecuteVarListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UserVariableContext *> userVariable();
    UserVariableContext* userVariable(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExecuteVarListContext* executeVarList();

  class  CloneStatementContext : public antlr4::ParserRuleContext {
  public:
    CloneStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CLONE_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *REMOTE_SYMBOL();
    antlr4::tree::TerminalNode *INSTANCE_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    UserContext *user();
    antlr4::tree::TerminalNode *COLON_SYMBOL();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    EqualContext *equal();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATION_SYMBOL();
    DataDirSSLContext *dataDirSSL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CloneStatementContext* cloneStatement();

  class  DataDirSSLContext : public antlr4::ParserRuleContext {
  public:
    DataDirSSLContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SslContext *ssl();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    EqualContext *equal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DataDirSSLContext* dataDirSSL();

  class  SslContext : public antlr4::ParserRuleContext {
  public:
    SslContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REQUIRE_SYMBOL();
    antlr4::tree::TerminalNode *SSL_SYMBOL();
    antlr4::tree::TerminalNode *NO_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SslContext* ssl();

  class  AccountManagementStatementContext : public antlr4::ParserRuleContext {
  public:
    AccountManagementStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AlterUserContext *alterUser();
    CreateUserContext *createUser();
    DropUserContext *dropUser();
    GrantContext *grant();
    RenameUserContext *renameUser();
    RevokeContext *revoke();
    SetRoleContext *setRole();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AccountManagementStatementContext* accountManagementStatement();

  class  AlterUserContext : public antlr4::ParserRuleContext {
  public:
    AlterUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALTER_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    AlterUserTailContext *alterUserTail();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterUserContext* alterUser();

  class  AlterUserTailContext : public antlr4::ParserRuleContext {
  public:
    AlterUserTailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateUserTailContext *createUserTail();
    CreateUserListContext *createUserList();
    AlterUserListContext *alterUserList();
    UserContext *user();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    TextStringContext *textString();
    ReplacePasswordContext *replacePassword();
    RetainCurrentPasswordContext *retainCurrentPassword();
    DiscardOldPasswordContext *discardOldPassword();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *NONE_SYMBOL();
    RoleListContext *roleList();
    antlr4::tree::TerminalNode *RANDOM_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterUserTailContext* alterUserTail();

  class  UserFunctionContext : public antlr4::ParserRuleContext {
  public:
    UserFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *USER_SYMBOL();
    ParenthesesContext *parentheses();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UserFunctionContext* userFunction();

  class  CreateUserContext : public antlr4::ParserRuleContext {
  public:
    CreateUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    CreateUserListContext *createUserList();
    DefaultRoleClauseContext *defaultRoleClause();
    CreateUserTailContext *createUserTail();
    IfNotExistsContext *ifNotExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateUserContext* createUser();

  class  CreateUserTailContext : public antlr4::ParserRuleContext {
  public:
    CreateUserTailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RequireClauseContext *requireClause();
    ConnectOptionsContext *connectOptions();
    std::vector<AccountLockPasswordExpireOptionsContext *> accountLockPasswordExpireOptions();
    AccountLockPasswordExpireOptionsContext* accountLockPasswordExpireOptions(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateUserTailContext* createUserTail();

  class  DefaultRoleClauseContext : public antlr4::ParserRuleContext {
  public:
    DefaultRoleClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    RoleListContext *roleList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DefaultRoleClauseContext* defaultRoleClause();

  class  RequireClauseContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    RequireClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REQUIRE_SYMBOL();
    RequireListContext *requireList();
    antlr4::tree::TerminalNode *SSL_SYMBOL();
    antlr4::tree::TerminalNode *X509_SYMBOL();
    antlr4::tree::TerminalNode *NONE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RequireClauseContext* requireClause();

  class  ConnectOptionsContext : public antlr4::ParserRuleContext {
  public:
    ConnectOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> MAX_QUERIES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode* MAX_QUERIES_PER_HOUR_SYMBOL(size_t i);
    std::vector<Ulong_numberContext *> ulong_number();
    Ulong_numberContext* ulong_number(size_t i);
    std::vector<antlr4::tree::TerminalNode *> MAX_UPDATES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode* MAX_UPDATES_PER_HOUR_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> MAX_CONNECTIONS_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode* MAX_CONNECTIONS_PER_HOUR_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> MAX_USER_CONNECTIONS_SYMBOL();
    antlr4::tree::TerminalNode* MAX_USER_CONNECTIONS_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConnectOptionsContext* connectOptions();

  class  AccountLockPasswordExpireOptionsContext : public antlr4::ParserRuleContext {
  public:
    AccountLockPasswordExpireOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ACCOUNT_SYMBOL();
    antlr4::tree::TerminalNode *LOCK_SYMBOL();
    antlr4::tree::TerminalNode *UNLOCK_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *EXPIRE_SYMBOL();
    antlr4::tree::TerminalNode *HISTORY_SYMBOL();
    antlr4::tree::TerminalNode *REUSE_SYMBOL();
    antlr4::tree::TerminalNode *INTERVAL_SYMBOL();
    antlr4::tree::TerminalNode *REQUIRE_SYMBOL();
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *DAY_SYMBOL();
    antlr4::tree::TerminalNode *NEVER_SYMBOL();
    antlr4::tree::TerminalNode *OPTIONAL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AccountLockPasswordExpireOptionsContext* accountLockPasswordExpireOptions();

  class  DropUserContext : public antlr4::ParserRuleContext {
  public:
    DropUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    UserListContext *userList();
    IfExistsContext *ifExists();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropUserContext* dropUser();

  class  GrantContext : public antlr4::ParserRuleContext {
  public:
    GrantContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> GRANT_SYMBOL();
    antlr4::tree::TerminalNode* GRANT_SYMBOL(size_t i);
    RoleOrPrivilegesListContext *roleOrPrivilegesList();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    UserListContext *userList();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    GrantIdentifierContext *grantIdentifier();
    GrantTargetListContext *grantTargetList();
    antlr4::tree::TerminalNode *PROXY_SYMBOL();
    UserContext *user();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *ADMIN_SYMBOL();
    antlr4::tree::TerminalNode *OPTION_SYMBOL();
    AclTypeContext *aclType();
    VersionedRequireClauseContext *versionedRequireClause();
    GrantOptionsContext *grantOptions();
    GrantAsContext *grantAs();
    antlr4::tree::TerminalNode *PRIVILEGES_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GrantContext* grant();

  class  GrantTargetListContext : public antlr4::ParserRuleContext {
  public:
    GrantTargetListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateUserListContext *createUserList();
    UserListContext *userList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GrantTargetListContext* grantTargetList();

  class  GrantOptionsContext : public antlr4::ParserRuleContext {
  public:
    GrantOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    std::vector<GrantOptionContext *> grantOption();
    GrantOptionContext* grantOption(size_t i);
    antlr4::tree::TerminalNode *GRANT_SYMBOL();
    antlr4::tree::TerminalNode *OPTION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GrantOptionsContext* grantOptions();

  class  ExceptRoleListContext : public antlr4::ParserRuleContext {
  public:
    ExceptRoleListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXCEPT_SYMBOL();
    RoleListContext *roleList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExceptRoleListContext* exceptRoleList();

  class  WithRolesContext : public antlr4::ParserRuleContext {
  public:
    WithRolesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    RoleListContext *roleList();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *NONE_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    ExceptRoleListContext *exceptRoleList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WithRolesContext* withRoles();

  class  GrantAsContext : public antlr4::ParserRuleContext {
  public:
    GrantAsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AS_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    WithRolesContext *withRoles();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GrantAsContext* grantAs();

  class  VersionedRequireClauseContext : public antlr4::ParserRuleContext {
  public:
    VersionedRequireClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RequireClauseContext *requireClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  VersionedRequireClauseContext* versionedRequireClause();

  class  RenameUserContext : public antlr4::ParserRuleContext {
  public:
    RenameUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    std::vector<UserContext *> user();
    UserContext* user(size_t i);
    std::vector<antlr4::tree::TerminalNode *> TO_SYMBOL();
    antlr4::tree::TerminalNode* TO_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RenameUserContext* renameUser();

  class  RevokeContext : public antlr4::ParserRuleContext {
  public:
    RevokeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REVOKE_SYMBOL();
    RoleOrPrivilegesListContext *roleOrPrivilegesList();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    UserListContext *userList();
    OnTypeToContext *onTypeTo();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *PROXY_SYMBOL();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    UserContext *user();
    GrantIdentifierContext *grantIdentifier();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    antlr4::tree::TerminalNode *GRANT_SYMBOL();
    antlr4::tree::TerminalNode *OPTION_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGES_SYMBOL();
    AclTypeContext *aclType();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RevokeContext* revoke();

  class  OnTypeToContext : public antlr4::ParserRuleContext {
  public:
    OnTypeToContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON_SYMBOL();
    GrantIdentifierContext *grantIdentifier();
    AclTypeContext *aclType();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OnTypeToContext* onTypeTo();

  class  AclTypeContext : public antlr4::ParserRuleContext {
  public:
    AclTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    antlr4::tree::TerminalNode *PROCEDURE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AclTypeContext* aclType();

  class  RoleOrPrivilegesListContext : public antlr4::ParserRuleContext {
  public:
    RoleOrPrivilegesListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<RoleOrPrivilegeContext *> roleOrPrivilege();
    RoleOrPrivilegeContext* roleOrPrivilege(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleOrPrivilegesListContext* roleOrPrivilegesList();

  class  RoleOrPrivilegeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *object = nullptr;
    RoleOrPrivilegeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoleIdentifierOrTextContext *roleIdentifierOrText();
    antlr4::tree::TerminalNode *AT_TEXT_SUFFIX();
    antlr4::tree::TerminalNode *AT_SIGN_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    ColumnInternalRefListContext *columnInternalRefList();
    antlr4::tree::TerminalNode *SELECT_SYMBOL();
    antlr4::tree::TerminalNode *INSERT_SYMBOL();
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    antlr4::tree::TerminalNode *REFERENCES_SYMBOL();
    antlr4::tree::TerminalNode *DELETE_SYMBOL();
    antlr4::tree::TerminalNode *USAGE_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    antlr4::tree::TerminalNode *EXECUTE_SYMBOL();
    antlr4::tree::TerminalNode *RELOAD_SYMBOL();
    antlr4::tree::TerminalNode *SHUTDOWN_SYMBOL();
    antlr4::tree::TerminalNode *PROCESS_SYMBOL();
    antlr4::tree::TerminalNode *FILE_SYMBOL();
    antlr4::tree::TerminalNode *PROXY_SYMBOL();
    antlr4::tree::TerminalNode *SUPER_SYMBOL();
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    antlr4::tree::TerminalNode *TRIGGER_SYMBOL();
    antlr4::tree::TerminalNode *GRANT_SYMBOL();
    antlr4::tree::TerminalNode *OPTION_SYMBOL();
    antlr4::tree::TerminalNode *SHOW_SYMBOL();
    antlr4::tree::TerminalNode *DATABASES_SYMBOL();
    antlr4::tree::TerminalNode *CREATE_SYMBOL();
    antlr4::tree::TerminalNode *TEMPORARY_SYMBOL();
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    antlr4::tree::TerminalNode *ROUTINE_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    antlr4::tree::TerminalNode *LOCK_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *CLIENT_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();
    antlr4::tree::TerminalNode *ALTER_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleOrPrivilegeContext* roleOrPrivilege();

  class  GrantIdentifierContext : public antlr4::ParserRuleContext {
  public:
    GrantIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> MULT_OPERATOR();
    antlr4::tree::TerminalNode* MULT_OPERATOR(size_t i);
    antlr4::tree::TerminalNode *DOT_SYMBOL();
    SchemaRefContext *schemaRef();
    TableRefContext *tableRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GrantIdentifierContext* grantIdentifier();

  class  RequireListContext : public antlr4::ParserRuleContext {
  public:
    RequireListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<RequireListElementContext *> requireListElement();
    RequireListElementContext* requireListElement(size_t i);
    std::vector<antlr4::tree::TerminalNode *> AND_SYMBOL();
    antlr4::tree::TerminalNode* AND_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RequireListContext* requireList();

  class  RequireListElementContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *element = nullptr;
    RequireListElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringContext *textString();
    antlr4::tree::TerminalNode *CIPHER_SYMBOL();
    antlr4::tree::TerminalNode *ISSUER_SYMBOL();
    antlr4::tree::TerminalNode *SUBJECT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RequireListElementContext* requireListElement();

  class  GrantOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    GrantOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPTION_SYMBOL();
    antlr4::tree::TerminalNode *GRANT_SYMBOL();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *MAX_QUERIES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_UPDATES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_CONNECTIONS_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_USER_CONNECTIONS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GrantOptionContext* grantOption();

  class  SetRoleContext : public antlr4::ParserRuleContext {
  public:
    SetRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    std::vector<RoleListContext *> roleList();
    RoleListContext* roleList(size_t i);
    antlr4::tree::TerminalNode *NONE_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *EXCEPT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SetRoleContext* setRole();

  class  RoleListContext : public antlr4::ParserRuleContext {
  public:
    RoleListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<RoleContext *> role();
    RoleContext* role(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleListContext* roleList();

  class  RoleContext : public antlr4::ParserRuleContext {
  public:
    RoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoleIdentifierOrTextContext *roleIdentifierOrText();
    antlr4::tree::TerminalNode *AT_SIGN_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *AT_TEXT_SUFFIX();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleContext* role();

  class  TableAdministrationStatementContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    TableAdministrationStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    TableRefListContext *tableRefList();
    antlr4::tree::TerminalNode *ANALYZE_SYMBOL();
    NoWriteToBinLogContext *noWriteToBinLog();
    HistogramContext *histogram();
    antlr4::tree::TerminalNode *CHECK_SYMBOL();
    std::vector<CheckOptionContext *> checkOption();
    CheckOptionContext* checkOption(size_t i);
    antlr4::tree::TerminalNode *CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *QUICK_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    antlr4::tree::TerminalNode *OPTIMIZE_SYMBOL();
    antlr4::tree::TerminalNode *REPAIR_SYMBOL();
    std::vector<RepairTypeContext *> repairType();
    RepairTypeContext* repairType(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableAdministrationStatementContext* tableAdministrationStatement();

  class  HistogramContext : public antlr4::ParserRuleContext {
  public:
    HistogramContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    antlr4::tree::TerminalNode *HISTOGRAM_SYMBOL();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *BUCKETS_SYMBOL();
    antlr4::tree::TerminalNode *DROP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HistogramContext* histogram();

  class  CheckOptionContext : public antlr4::ParserRuleContext {
  public:
    CheckOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *UPGRADE_SYMBOL();
    antlr4::tree::TerminalNode *QUICK_SYMBOL();
    antlr4::tree::TerminalNode *FAST_SYMBOL();
    antlr4::tree::TerminalNode *MEDIUM_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    antlr4::tree::TerminalNode *CHANGED_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CheckOptionContext* checkOption();

  class  RepairTypeContext : public antlr4::ParserRuleContext {
  public:
    RepairTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *QUICK_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    antlr4::tree::TerminalNode *USE_FRM_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RepairTypeContext* repairType();

  class  InstallUninstallStatmentContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *action = nullptr;
    antlr4::Token *type = nullptr;
    InstallUninstallStatmentContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *SONAME_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *INSTALL_SYMBOL();
    antlr4::tree::TerminalNode *PLUGIN_SYMBOL();
    TextStringLiteralListContext *textStringLiteralList();
    antlr4::tree::TerminalNode *COMPONENT_SYMBOL();
    PluginRefContext *pluginRef();
    antlr4::tree::TerminalNode *UNINSTALL_SYMBOL();
    std::vector<ComponentRefContext *> componentRef();
    ComponentRefContext* componentRef(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InstallUninstallStatmentContext* installUninstallStatment();

  class  SetStatementContext : public antlr4::ParserRuleContext {
  public:
    SetStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET_SYMBOL();
    StartOptionValueListContext *startOptionValueList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SetStatementContext* setStatement();

  class  StartOptionValueListContext : public antlr4::ParserRuleContext {
  public:
    StartOptionValueListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OptionValueNoOptionTypeContext *optionValueNoOptionType();
    OptionValueListContinuedContext *optionValueListContinued();
    antlr4::tree::TerminalNode *TRANSACTION_SYMBOL();
    TransactionCharacteristicsContext *transactionCharacteristics();
    OptionTypeContext *optionType();
    StartOptionValueListFollowingOptionTypeContext *startOptionValueListFollowingOptionType();
    std::vector<antlr4::tree::TerminalNode *> PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode* PASSWORD_SYMBOL(size_t i);
    EqualContext *equal();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *OLD_PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    UserContext *user();
    ReplacePasswordContext *replacePassword();
    RetainCurrentPasswordContext *retainCurrentPassword();
    antlr4::tree::TerminalNode *TO_SYMBOL();
    antlr4::tree::TerminalNode *RANDOM_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StartOptionValueListContext* startOptionValueList();

  class  TransactionCharacteristicsContext : public antlr4::ParserRuleContext {
  public:
    TransactionCharacteristicsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TransactionAccessModeContext *transactionAccessMode();
    IsolationLevelContext *isolationLevel();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TransactionCharacteristicsContext* transactionCharacteristics();

  class  TransactionAccessModeContext : public antlr4::ParserRuleContext {
  public:
    TransactionAccessModeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *READ_SYMBOL();
    antlr4::tree::TerminalNode *WRITE_SYMBOL();
    antlr4::tree::TerminalNode *ONLY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TransactionAccessModeContext* transactionAccessMode();

  class  IsolationLevelContext : public antlr4::ParserRuleContext {
  public:
    IsolationLevelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ISOLATION_SYMBOL();
    antlr4::tree::TerminalNode *LEVEL_SYMBOL();
    antlr4::tree::TerminalNode *REPEATABLE_SYMBOL();
    antlr4::tree::TerminalNode *READ_SYMBOL();
    antlr4::tree::TerminalNode *SERIALIZABLE_SYMBOL();
    antlr4::tree::TerminalNode *COMMITTED_SYMBOL();
    antlr4::tree::TerminalNode *UNCOMMITTED_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IsolationLevelContext* isolationLevel();

  class  OptionValueListContinuedContext : public antlr4::ParserRuleContext {
  public:
    OptionValueListContinuedContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    std::vector<OptionValueContext *> optionValue();
    OptionValueContext* optionValue(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OptionValueListContinuedContext* optionValueListContinued();

  class  OptionValueNoOptionTypeContext : public antlr4::ParserRuleContext {
  public:
    OptionValueNoOptionTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InternalVariableNameContext *internalVariableName();
    EqualContext *equal();
    SetExprOrDefaultContext *setExprOrDefault();
    CharsetClauseContext *charsetClause();
    UserVariableContext *userVariable();
    ExprContext *expr();
    SetSystemVariableContext *setSystemVariable();
    antlr4::tree::TerminalNode *NAMES_SYMBOL();
    CharsetNameContext *charsetName();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    CollateContext *collate();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OptionValueNoOptionTypeContext* optionValueNoOptionType();

  class  OptionValueContext : public antlr4::ParserRuleContext {
  public:
    OptionValueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OptionTypeContext *optionType();
    InternalVariableNameContext *internalVariableName();
    EqualContext *equal();
    SetExprOrDefaultContext *setExprOrDefault();
    OptionValueNoOptionTypeContext *optionValueNoOptionType();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OptionValueContext* optionValue();

  class  SetSystemVariableContext : public antlr4::ParserRuleContext {
  public:
    SetSystemVariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AT_AT_SIGN_SYMBOL();
    InternalVariableNameContext *internalVariableName();
    SetVarIdentTypeContext *setVarIdentType();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SetSystemVariableContext* setSystemVariable();

  class  StartOptionValueListFollowingOptionTypeContext : public antlr4::ParserRuleContext {
  public:
    StartOptionValueListFollowingOptionTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OptionValueFollowingOptionTypeContext *optionValueFollowingOptionType();
    OptionValueListContinuedContext *optionValueListContinued();
    antlr4::tree::TerminalNode *TRANSACTION_SYMBOL();
    TransactionCharacteristicsContext *transactionCharacteristics();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StartOptionValueListFollowingOptionTypeContext* startOptionValueListFollowingOptionType();

  class  OptionValueFollowingOptionTypeContext : public antlr4::ParserRuleContext {
  public:
    OptionValueFollowingOptionTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InternalVariableNameContext *internalVariableName();
    EqualContext *equal();
    SetExprOrDefaultContext *setExprOrDefault();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OptionValueFollowingOptionTypeContext* optionValueFollowingOptionType();

  class  SetExprOrDefaultContext : public antlr4::ParserRuleContext {
  public:
    SetExprOrDefaultContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    antlr4::tree::TerminalNode *ROW_SYMBOL();
    antlr4::tree::TerminalNode *SYSTEM_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SetExprOrDefaultContext* setExprOrDefault();

  class  ShowStatementContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *value = nullptr;
    antlr4::Token *object = nullptr;
    ShowStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW_SYMBOL();
    antlr4::tree::TerminalNode *STATUS_SYMBOL();
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *EVENTS_SYMBOL();
    FromOrInContext *fromOrIn();
    antlr4::tree::TerminalNode *COUNT_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *MULT_OPERATOR();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    CharsetContext *charset();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    UserContext *user();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    UserListContext *userList();
    antlr4::tree::TerminalNode *CODE_SYMBOL();
    ProcedureRefContext *procedureRef();
    FunctionRefContext *functionRef();
    antlr4::tree::TerminalNode *AUTHORS_SYMBOL();
    antlr4::tree::TerminalNode *DATABASES_SYMBOL();
    antlr4::tree::TerminalNode *TRIGGERS_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_SYMBOL();
    antlr4::tree::TerminalNode *PLUGINS_SYMBOL();
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *MUTEX_SYMBOL();
    antlr4::tree::TerminalNode *LOGS_SYMBOL();
    antlr4::tree::TerminalNode *COLUMNS_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *IN_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();
    antlr4::tree::TerminalNode *ENGINES_SYMBOL();
    antlr4::tree::TerminalNode *WARNINGS_SYMBOL();
    antlr4::tree::TerminalNode *ERRORS_SYMBOL();
    antlr4::tree::TerminalNode *PROFILES_SYMBOL();
    antlr4::tree::TerminalNode *PROFILE_SYMBOL();
    antlr4::tree::TerminalNode *PROCESSLIST_SYMBOL();
    antlr4::tree::TerminalNode *COLLATION_SYMBOL();
    antlr4::tree::TerminalNode *CONTRIBUTORS_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGES_SYMBOL();
    antlr4::tree::TerminalNode *GRANTS_SYMBOL();
    antlr4::tree::TerminalNode *CREATE_SYMBOL();
    antlr4::tree::TerminalNode *PROCEDURE_SYMBOL();
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    EngineRefContext *engineRef();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *HOSTS_SYMBOL();
    NonBlockingContext *nonBlocking();
    antlr4::tree::TerminalNode *BINLOG_SYMBOL();
    antlr4::tree::TerminalNode *RELAYLOG_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    antlr4::tree::TerminalNode *INDEXES_SYMBOL();
    antlr4::tree::TerminalNode *KEYS_SYMBOL();
    antlr4::tree::TerminalNode *VARIABLES_SYMBOL();
    SchemaRefContext *schemaRef();
    EventRefContext *eventRef();
    TriggerRefContext *triggerRef();
    ViewRefContext *viewRef();
    LikeOrWhereContext *likeOrWhere();
    ShowCommandTypeContext *showCommandType();
    InDbContext *inDb();
    antlr4::tree::TerminalNode *FULL_SYMBOL();
    TextStringContext *textString();
    Ulonglong_numberContext *ulonglong_number();
    LimitClauseContext *limitClause();
    ChannelContext *channel();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    WhereClauseContext *whereClause();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    std::vector<ProfileTypeContext *> profileType();
    ProfileTypeContext* profileType(size_t i);
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *INT_NUMBER();
    OptionTypeContext *optionType();
    antlr4::tree::TerminalNode *DATABASE_SYMBOL();
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    antlr4::tree::TerminalNode *TRIGGER_SYMBOL();
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    IfNotExistsContext *ifNotExists();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ShowStatementContext* showStatement();

  class  ShowCommandTypeContext : public antlr4::ParserRuleContext {
  public:
    ShowCommandTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FULL_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ShowCommandTypeContext* showCommandType();

  class  NonBlockingContext : public antlr4::ParserRuleContext {
  public:
    NonBlockingContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NONBLOCKING_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NonBlockingContext* nonBlocking();

  class  FromOrInContext : public antlr4::ParserRuleContext {
  public:
    FromOrInContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *IN_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromOrInContext* fromOrIn();

  class  InDbContext : public antlr4::ParserRuleContext {
  public:
    InDbContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FromOrInContext *fromOrIn();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InDbContext* inDb();

  class  ProfileTypeContext : public antlr4::ParserRuleContext {
  public:
    ProfileTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BLOCK_SYMBOL();
    antlr4::tree::TerminalNode *IO_SYMBOL();
    antlr4::tree::TerminalNode *CONTEXT_SYMBOL();
    antlr4::tree::TerminalNode *SWITCHES_SYMBOL();
    antlr4::tree::TerminalNode *PAGE_SYMBOL();
    antlr4::tree::TerminalNode *FAULTS_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *CPU_SYMBOL();
    antlr4::tree::TerminalNode *IPC_SYMBOL();
    antlr4::tree::TerminalNode *MEMORY_SYMBOL();
    antlr4::tree::TerminalNode *SOURCE_SYMBOL();
    antlr4::tree::TerminalNode *SWAPS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ProfileTypeContext* profileType();

  class  OtherAdministrativeStatementContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    OtherAdministrativeStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *BINLOG_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    KeyCacheListOrPartsContext *keyCacheListOrParts();
    antlr4::tree::TerminalNode *IN_SYMBOL();
    antlr4::tree::TerminalNode *CACHE_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *FLUSH_SYMBOL();
    FlushTablesContext *flushTables();
    std::vector<FlushOptionContext *> flushOption();
    FlushOptionContext* flushOption(size_t i);
    NoWriteToBinLogContext *noWriteToBinLog();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    ExprContext *expr();
    antlr4::tree::TerminalNode *KILL_SYMBOL();
    antlr4::tree::TerminalNode *CONNECTION_SYMBOL();
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    PreloadTailContext *preloadTail();
    antlr4::tree::TerminalNode *LOAD_SYMBOL();
    antlr4::tree::TerminalNode *SHUTDOWN_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OtherAdministrativeStatementContext* otherAdministrativeStatement();

  class  KeyCacheListOrPartsContext : public antlr4::ParserRuleContext {
  public:
    KeyCacheListOrPartsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KeyCacheListContext *keyCacheList();
    AssignToKeycachePartitionContext *assignToKeycachePartition();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyCacheListOrPartsContext* keyCacheListOrParts();

  class  KeyCacheListContext : public antlr4::ParserRuleContext {
  public:
    KeyCacheListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AssignToKeycacheContext *> assignToKeycache();
    AssignToKeycacheContext* assignToKeycache(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyCacheListContext* keyCacheList();

  class  AssignToKeycacheContext : public antlr4::ParserRuleContext {
  public:
    AssignToKeycacheContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    CacheKeyListContext *cacheKeyList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AssignToKeycacheContext* assignToKeycache();

  class  AssignToKeycachePartitionContext : public antlr4::ParserRuleContext {
  public:
    AssignToKeycachePartitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    AllOrPartitionNameListContext *allOrPartitionNameList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    CacheKeyListContext *cacheKeyList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AssignToKeycachePartitionContext* assignToKeycachePartition();

  class  CacheKeyListContext : public antlr4::ParserRuleContext {
  public:
    CacheKeyListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KeyOrIndexContext *keyOrIndex();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    KeyUsageListContext *keyUsageList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CacheKeyListContext* cacheKeyList();

  class  KeyUsageElementContext : public antlr4::ParserRuleContext {
  public:
    KeyUsageElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyUsageElementContext* keyUsageElement();

  class  KeyUsageListContext : public antlr4::ParserRuleContext {
  public:
    KeyUsageListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KeyUsageElementContext *> keyUsageElement();
    KeyUsageElementContext* keyUsageElement(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyUsageListContext* keyUsageList();

  class  FlushOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    FlushOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DES_KEY_FILE_SYMBOL();
    antlr4::tree::TerminalNode *HOSTS_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGES_SYMBOL();
    antlr4::tree::TerminalNode *STATUS_SYMBOL();
    antlr4::tree::TerminalNode *USER_RESOURCES_SYMBOL();
    antlr4::tree::TerminalNode *LOGS_SYMBOL();
    LogTypeContext *logType();
    antlr4::tree::TerminalNode *RELAY_SYMBOL();
    ChannelContext *channel();
    antlr4::tree::TerminalNode *CACHE_SYMBOL();
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *OPTIMIZER_COSTS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FlushOptionContext* flushOption();

  class  LogTypeContext : public antlr4::ParserRuleContext {
  public:
    LogTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *ERROR_SYMBOL();
    antlr4::tree::TerminalNode *GENERAL_SYMBOL();
    antlr4::tree::TerminalNode *SLOW_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LogTypeContext* logType();

  class  FlushTablesContext : public antlr4::ParserRuleContext {
  public:
    FlushTablesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_SYMBOL();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *READ_SYMBOL();
    antlr4::tree::TerminalNode *LOCK_SYMBOL();
    IdentifierListContext *identifierList();
    FlushTablesOptionsContext *flushTablesOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FlushTablesContext* flushTables();

  class  FlushTablesOptionsContext : public antlr4::ParserRuleContext {
  public:
    FlushTablesOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *EXPORT_SYMBOL();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *READ_SYMBOL();
    antlr4::tree::TerminalNode *LOCK_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FlushTablesOptionsContext* flushTablesOptions();

  class  PreloadTailContext : public antlr4::ParserRuleContext {
  public:
    PreloadTailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    AdminPartitionContext *adminPartition();
    CacheKeyListContext *cacheKeyList();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();
    antlr4::tree::TerminalNode *LEAVES_SYMBOL();
    PreloadListContext *preloadList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PreloadTailContext* preloadTail();

  class  PreloadListContext : public antlr4::ParserRuleContext {
  public:
    PreloadListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PreloadKeysContext *> preloadKeys();
    PreloadKeysContext* preloadKeys(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PreloadListContext* preloadList();

  class  PreloadKeysContext : public antlr4::ParserRuleContext {
  public:
    PreloadKeysContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    CacheKeyListContext *cacheKeyList();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();
    antlr4::tree::TerminalNode *LEAVES_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PreloadKeysContext* preloadKeys();

  class  AdminPartitionContext : public antlr4::ParserRuleContext {
  public:
    AdminPartitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    AllOrPartitionNameListContext *allOrPartitionNameList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AdminPartitionContext* adminPartition();

  class  ResourceGroupManagementContext : public antlr4::ParserRuleContext {
  public:
    ResourceGroupManagementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateResourceGroupContext *createResourceGroup();
    AlterResourceGroupContext *alterResourceGroup();
    SetResourceGroupContext *setResourceGroup();
    DropResourceGroupContext *dropResourceGroup();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResourceGroupManagementContext* resourceGroupManagement();

  class  CreateResourceGroupContext : public antlr4::ParserRuleContext {
  public:
    CreateResourceGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE_SYMBOL();
    antlr4::tree::TerminalNode *RESOURCE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *TYPE_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    antlr4::tree::TerminalNode *SYSTEM_SYMBOL();
    EqualContext *equal();
    ResourceGroupVcpuListContext *resourceGroupVcpuList();
    ResourceGroupPriorityContext *resourceGroupPriority();
    ResourceGroupEnableDisableContext *resourceGroupEnableDisable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateResourceGroupContext* createResourceGroup();

  class  ResourceGroupVcpuListContext : public antlr4::ParserRuleContext {
  public:
    ResourceGroupVcpuListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VCPU_SYMBOL();
    std::vector<VcpuNumOrRangeContext *> vcpuNumOrRange();
    VcpuNumOrRangeContext* vcpuNumOrRange(size_t i);
    EqualContext *equal();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResourceGroupVcpuListContext* resourceGroupVcpuList();

  class  VcpuNumOrRangeContext : public antlr4::ParserRuleContext {
  public:
    VcpuNumOrRangeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> INT_NUMBER();
    antlr4::tree::TerminalNode* INT_NUMBER(size_t i);
    antlr4::tree::TerminalNode *MINUS_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  VcpuNumOrRangeContext* vcpuNumOrRange();

  class  ResourceGroupPriorityContext : public antlr4::ParserRuleContext {
  public:
    ResourceGroupPriorityContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *THREAD_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *INT_NUMBER();
    EqualContext *equal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResourceGroupPriorityContext* resourceGroupPriority();

  class  ResourceGroupEnableDisableContext : public antlr4::ParserRuleContext {
  public:
    ResourceGroupEnableDisableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ENABLE_SYMBOL();
    antlr4::tree::TerminalNode *DISABLE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResourceGroupEnableDisableContext* resourceGroupEnableDisable();

  class  AlterResourceGroupContext : public antlr4::ParserRuleContext {
  public:
    AlterResourceGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALTER_SYMBOL();
    antlr4::tree::TerminalNode *RESOURCE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    ResourceGroupRefContext *resourceGroupRef();
    ResourceGroupVcpuListContext *resourceGroupVcpuList();
    ResourceGroupPriorityContext *resourceGroupPriority();
    ResourceGroupEnableDisableContext *resourceGroupEnableDisable();
    antlr4::tree::TerminalNode *FORCE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterResourceGroupContext* alterResourceGroup();

  class  SetResourceGroupContext : public antlr4::ParserRuleContext {
  public:
    SetResourceGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET_SYMBOL();
    antlr4::tree::TerminalNode *RESOURCE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    ThreadIdListContext *threadIdList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SetResourceGroupContext* setResourceGroup();

  class  ThreadIdListContext : public antlr4::ParserRuleContext {
  public:
    ThreadIdListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Real_ulong_numberContext *> real_ulong_number();
    Real_ulong_numberContext* real_ulong_number(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ThreadIdListContext* threadIdList();

  class  DropResourceGroupContext : public antlr4::ParserRuleContext {
  public:
    DropResourceGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP_SYMBOL();
    antlr4::tree::TerminalNode *RESOURCE_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_SYMBOL();
    ResourceGroupRefContext *resourceGroupRef();
    antlr4::tree::TerminalNode *FORCE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropResourceGroupContext* dropResourceGroup();

  class  UtilityStatementContext : public antlr4::ParserRuleContext {
  public:
    UtilityStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DescribeCommandContext *describeCommand();
    ExplainCommandContext *explainCommand();
    HelpCommandContext *helpCommand();
    UseCommandContext *useCommand();
    RestartServerContext *restartServer();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UtilityStatementContext* utilityStatement();

  class  DescribeCommandContext : public antlr4::ParserRuleContext {
  public:
    DescribeCommandContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableRefContext *tableRef();
    antlr4::tree::TerminalNode *EXPLAIN_SYMBOL();
    antlr4::tree::TerminalNode *DESCRIBE_SYMBOL();
    antlr4::tree::TerminalNode *DESC_SYMBOL();
    TextStringContext *textString();
    ColumnRefContext *columnRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DescribeCommandContext* describeCommand();

  class  ExplainCommandContext : public antlr4::ParserRuleContext {
  public:
    ExplainCommandContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExplainableStatementContext *explainableStatement();
    antlr4::tree::TerminalNode *EXPLAIN_SYMBOL();
    antlr4::tree::TerminalNode *DESCRIBE_SYMBOL();
    antlr4::tree::TerminalNode *DESC_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONS_SYMBOL();
    antlr4::tree::TerminalNode *FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *ANALYZE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExplainCommandContext* explainCommand();

  class  ExplainableStatementContext : public antlr4::ParserRuleContext {
  public:
    ExplainableStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectStatementContext *selectStatement();
    DeleteStatementContext *deleteStatement();
    InsertStatementContext *insertStatement();
    ReplaceStatementContext *replaceStatement();
    UpdateStatementContext *updateStatement();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *CONNECTION_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExplainableStatementContext* explainableStatement();

  class  HelpCommandContext : public antlr4::ParserRuleContext {
  public:
    HelpCommandContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *HELP_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HelpCommandContext* helpCommand();

  class  UseCommandContext : public antlr4::ParserRuleContext {
  public:
    UseCommandContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *USE_SYMBOL();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UseCommandContext* useCommand();

  class  RestartServerContext : public antlr4::ParserRuleContext {
  public:
    RestartServerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RESTART_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RestartServerContext* restartServer();

  class  ExprContext : public antlr4::ParserRuleContext {
  public:
    ExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ExprContext() = default;
    void copyFrom(ExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ExprOrContext : public ExprContext {
  public:
    ExprOrContext(ExprContext *ctx);

    antlr4::Token *op = nullptr;
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *OR_SYMBOL();
    antlr4::tree::TerminalNode *LOGICAL_OR_OPERATOR();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ExprNotContext : public ExprContext {
  public:
    ExprNotContext(ExprContext *ctx);

    antlr4::tree::TerminalNode *NOT_SYMBOL();
    ExprContext *expr();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ExprIsContext : public ExprContext {
  public:
    ExprIsContext(ExprContext *ctx);

    antlr4::Token *type = nullptr;
    BoolPriContext *boolPri();
    antlr4::tree::TerminalNode *IS_SYMBOL();
    antlr4::tree::TerminalNode *TRUE_SYMBOL();
    antlr4::tree::TerminalNode *FALSE_SYMBOL();
    antlr4::tree::TerminalNode *UNKNOWN_SYMBOL();
    NotRuleContext *notRule();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ExprAndContext : public ExprContext {
  public:
    ExprAndContext(ExprContext *ctx);

    antlr4::Token *op = nullptr;
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *AND_SYMBOL();
    antlr4::tree::TerminalNode *LOGICAL_AND_OPERATOR();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ExprXorContext : public ExprContext {
  public:
    ExprXorContext(ExprContext *ctx);

    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *XOR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ExprContext* expr();
  ExprContext* expr(int precedence);
  class  BoolPriContext : public antlr4::ParserRuleContext {
  public:
    BoolPriContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    BoolPriContext() = default;
    void copyFrom(BoolPriContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  PrimaryExprPredicateContext : public BoolPriContext {
  public:
    PrimaryExprPredicateContext(BoolPriContext *ctx);

    PredicateContext *predicate();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PrimaryExprCompareContext : public BoolPriContext {
  public:
    PrimaryExprCompareContext(BoolPriContext *ctx);

    BoolPriContext *boolPri();
    CompOpContext *compOp();
    PredicateContext *predicate();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PrimaryExprAllAnyContext : public BoolPriContext {
  public:
    PrimaryExprAllAnyContext(BoolPriContext *ctx);

    BoolPriContext *boolPri();
    CompOpContext *compOp();
    SubqueryContext *subquery();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    antlr4::tree::TerminalNode *ANY_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PrimaryExprIsNullContext : public BoolPriContext {
  public:
    PrimaryExprIsNullContext(BoolPriContext *ctx);

    BoolPriContext *boolPri();
    antlr4::tree::TerminalNode *IS_SYMBOL();
    antlr4::tree::TerminalNode *NULL_SYMBOL();
    NotRuleContext *notRule();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  BoolPriContext* boolPri();
  BoolPriContext* boolPri(int precedence);
  class  CompOpContext : public antlr4::ParserRuleContext {
  public:
    CompOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *NULL_SAFE_EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *GREATER_OR_EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *GREATER_THAN_OPERATOR();
    antlr4::tree::TerminalNode *LESS_OR_EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *LESS_THAN_OPERATOR();
    antlr4::tree::TerminalNode *NOT_EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CompOpContext* compOp();

  class  PredicateContext : public antlr4::ParserRuleContext {
  public:
    PredicateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<BitExprContext *> bitExpr();
    BitExprContext* bitExpr(size_t i);
    PredicateOperationsContext *predicateOperations();
    antlr4::tree::TerminalNode *MEMBER_SYMBOL();
    SimpleExprWithParenthesesContext *simpleExprWithParentheses();
    antlr4::tree::TerminalNode *SOUNDS_SYMBOL();
    antlr4::tree::TerminalNode *LIKE_SYMBOL();
    NotRuleContext *notRule();
    antlr4::tree::TerminalNode *OF_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PredicateContext* predicate();

  class  PredicateOperationsContext : public antlr4::ParserRuleContext {
  public:
    PredicateOperationsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    PredicateOperationsContext() = default;
    void copyFrom(PredicateOperationsContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  PredicateExprRegexContext : public PredicateOperationsContext {
  public:
    PredicateExprRegexContext(PredicateOperationsContext *ctx);

    antlr4::tree::TerminalNode *REGEXP_SYMBOL();
    BitExprContext *bitExpr();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PredicateExprBetweenContext : public PredicateOperationsContext {
  public:
    PredicateExprBetweenContext(PredicateOperationsContext *ctx);

    antlr4::tree::TerminalNode *BETWEEN_SYMBOL();
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *AND_SYMBOL();
    PredicateContext *predicate();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PredicateExprInContext : public PredicateOperationsContext {
  public:
    PredicateExprInContext(PredicateOperationsContext *ctx);

    antlr4::tree::TerminalNode *IN_SYMBOL();
    SubqueryContext *subquery();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprListContext *exprList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PredicateExprLikeContext : public PredicateOperationsContext {
  public:
    PredicateExprLikeContext(PredicateOperationsContext *ctx);

    antlr4::tree::TerminalNode *LIKE_SYMBOL();
    std::vector<SimpleExprContext *> simpleExpr();
    SimpleExprContext* simpleExpr(size_t i);
    antlr4::tree::TerminalNode *ESCAPE_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  PredicateOperationsContext* predicateOperations();

  class  BitExprContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *op = nullptr;
    BitExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SimpleExprContext *simpleExpr();
    std::vector<BitExprContext *> bitExpr();
    BitExprContext* bitExpr(size_t i);
    antlr4::tree::TerminalNode *BITWISE_XOR_OPERATOR();
    antlr4::tree::TerminalNode *MULT_OPERATOR();
    antlr4::tree::TerminalNode *DIV_OPERATOR();
    antlr4::tree::TerminalNode *MOD_OPERATOR();
    antlr4::tree::TerminalNode *DIV_SYMBOL();
    antlr4::tree::TerminalNode *MOD_SYMBOL();
    antlr4::tree::TerminalNode *PLUS_OPERATOR();
    antlr4::tree::TerminalNode *MINUS_OPERATOR();
    antlr4::tree::TerminalNode *SHIFT_LEFT_OPERATOR();
    antlr4::tree::TerminalNode *SHIFT_RIGHT_OPERATOR();
    antlr4::tree::TerminalNode *BITWISE_AND_OPERATOR();
    antlr4::tree::TerminalNode *BITWISE_OR_OPERATOR();
    antlr4::tree::TerminalNode *INTERVAL_SYMBOL();
    ExprContext *expr();
    IntervalContext *interval();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  BitExprContext* bitExpr();
  BitExprContext* bitExpr(int precedence);
  class  SimpleExprContext : public antlr4::ParserRuleContext {
  public:
    SimpleExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    SimpleExprContext() = default;
    void copyFrom(SimpleExprContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  SimpleExprConvertContext : public SimpleExprContext {
  public:
    SimpleExprConvertContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *CONVERT_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    CastTypeContext *castType();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprVariableContext : public SimpleExprContext {
  public:
    SimpleExprVariableContext(SimpleExprContext *ctx);

    VariableContext *variable();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprCastContext : public SimpleExprContext {
  public:
    SimpleExprCastContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *CAST_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    CastTypeContext *castType();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    ArrayCastContext *arrayCast();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprUnaryContext : public SimpleExprContext {
  public:
    SimpleExprUnaryContext(SimpleExprContext *ctx);

    antlr4::Token *op = nullptr;
    SimpleExprContext *simpleExpr();
    antlr4::tree::TerminalNode *PLUS_OPERATOR();
    antlr4::tree::TerminalNode *MINUS_OPERATOR();
    antlr4::tree::TerminalNode *BITWISE_NOT_OPERATOR();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprOdbcContext : public SimpleExprContext {
  public:
    SimpleExprOdbcContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *OPEN_CURLY_SYMBOL();
    IdentifierContext *identifier();
    ExprContext *expr();
    antlr4::tree::TerminalNode *CLOSE_CURLY_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprRuntimeFunctionContext : public SimpleExprContext {
  public:
    SimpleExprRuntimeFunctionContext(SimpleExprContext *ctx);

    RuntimeFunctionCallContext *runtimeFunctionCall();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprFunctionContext : public SimpleExprContext {
  public:
    SimpleExprFunctionContext(SimpleExprContext *ctx);

    FunctionCallContext *functionCall();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprCollateContext : public SimpleExprContext {
  public:
    SimpleExprCollateContext(SimpleExprContext *ctx);

    SimpleExprContext *simpleExpr();
    antlr4::tree::TerminalNode *COLLATE_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprMatchContext : public SimpleExprContext {
  public:
    SimpleExprMatchContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *MATCH_SYMBOL();
    IdentListArgContext *identListArg();
    antlr4::tree::TerminalNode *AGAINST_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    FulltextOptionsContext *fulltextOptions();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprWindowingFunctionContext : public SimpleExprContext {
  public:
    SimpleExprWindowingFunctionContext(SimpleExprContext *ctx);

    WindowFunctionCallContext *windowFunctionCall();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprBinaryContext : public SimpleExprContext {
  public:
    SimpleExprBinaryContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    SimpleExprContext *simpleExpr();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprColumnRefContext : public SimpleExprContext {
  public:
    SimpleExprColumnRefContext(SimpleExprContext *ctx);

    ColumnRefContext *columnRef();
    JsonOperatorContext *jsonOperator();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprParamMarkerContext : public SimpleExprContext {
  public:
    SimpleExprParamMarkerContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *PARAM_MARKER();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprSumContext : public SimpleExprContext {
  public:
    SimpleExprSumContext(SimpleExprContext *ctx);

    SumExprContext *sumExpr();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprConvertUsingContext : public SimpleExprContext {
  public:
    SimpleExprConvertUsingContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *CONVERT_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    CharsetNameContext *charsetName();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprSubQueryContext : public SimpleExprContext {
  public:
    SimpleExprSubQueryContext(SimpleExprContext *ctx);

    SubqueryContext *subquery();
    antlr4::tree::TerminalNode *EXISTS_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprGroupingOperationContext : public SimpleExprContext {
  public:
    SimpleExprGroupingOperationContext(SimpleExprContext *ctx);

    GroupingOperationContext *groupingOperation();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprNotContext : public SimpleExprContext {
  public:
    SimpleExprNotContext(SimpleExprContext *ctx);

    Not2RuleContext *not2Rule();
    SimpleExprContext *simpleExpr();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprValuesContext : public SimpleExprContext {
  public:
    SimpleExprValuesContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *VALUES_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    SimpleIdentifierContext *simpleIdentifier();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprDefaultContext : public SimpleExprContext {
  public:
    SimpleExprDefaultContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    SimpleIdentifierContext *simpleIdentifier();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprListContext : public SimpleExprContext {
  public:
    SimpleExprListContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprListContext *exprList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *ROW_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprIntervalContext : public SimpleExprContext {
  public:
    SimpleExprIntervalContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *INTERVAL_SYMBOL();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    IntervalContext *interval();
    antlr4::tree::TerminalNode *PLUS_OPERATOR();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprCaseContext : public SimpleExprContext {
  public:
    SimpleExprCaseContext(SimpleExprContext *ctx);

    antlr4::tree::TerminalNode *CASE_SYMBOL();
    antlr4::tree::TerminalNode *END_SYMBOL();
    ExprContext *expr();
    std::vector<WhenExpressionContext *> whenExpression();
    WhenExpressionContext* whenExpression(size_t i);
    std::vector<ThenExpressionContext *> thenExpression();
    ThenExpressionContext* thenExpression(size_t i);
    ElseExpressionContext *elseExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprConcatContext : public SimpleExprContext {
  public:
    SimpleExprConcatContext(SimpleExprContext *ctx);

    std::vector<SimpleExprContext *> simpleExpr();
    SimpleExprContext* simpleExpr(size_t i);
    antlr4::tree::TerminalNode *CONCAT_PIPES_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SimpleExprLiteralContext : public SimpleExprContext {
  public:
    SimpleExprLiteralContext(SimpleExprContext *ctx);

    LiteralContext *literal();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  SimpleExprContext* simpleExpr();
  SimpleExprContext* simpleExpr(int precedence);
  class  ArrayCastContext : public antlr4::ParserRuleContext {
  public:
    ArrayCastContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ARRAY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ArrayCastContext* arrayCast();

  class  JsonOperatorContext : public antlr4::ParserRuleContext {
  public:
    JsonOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JSON_SEPARATOR_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *JSON_UNQUOTED_SEPARATOR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JsonOperatorContext* jsonOperator();

  class  SumExprContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *name = nullptr;
    SumExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    InSumExprContext *inSumExpr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *AVG_SYMBOL();
    antlr4::tree::TerminalNode *DISTINCT_SYMBOL();
    WindowingClauseContext *windowingClause();
    antlr4::tree::TerminalNode *BIT_AND_SYMBOL();
    antlr4::tree::TerminalNode *BIT_OR_SYMBOL();
    antlr4::tree::TerminalNode *BIT_XOR_SYMBOL();
    JsonFunctionContext *jsonFunction();
    antlr4::tree::TerminalNode *MULT_OPERATOR();
    antlr4::tree::TerminalNode *COUNT_SYMBOL();
    antlr4::tree::TerminalNode *ALL_SYMBOL();
    ExprListContext *exprList();
    antlr4::tree::TerminalNode *MIN_SYMBOL();
    antlr4::tree::TerminalNode *MAX_SYMBOL();
    antlr4::tree::TerminalNode *STD_SYMBOL();
    antlr4::tree::TerminalNode *VARIANCE_SYMBOL();
    antlr4::tree::TerminalNode *STDDEV_SAMP_SYMBOL();
    antlr4::tree::TerminalNode *VAR_SAMP_SYMBOL();
    antlr4::tree::TerminalNode *SUM_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_CONCAT_SYMBOL();
    OrderClauseContext *orderClause();
    antlr4::tree::TerminalNode *SEPARATOR_SYMBOL();
    TextStringContext *textString();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SumExprContext* sumExpr();

  class  GroupingOperationContext : public antlr4::ParserRuleContext {
  public:
    GroupingOperationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUPING_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprListContext *exprList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GroupingOperationContext* groupingOperation();

  class  WindowFunctionCallContext : public antlr4::ParserRuleContext {
  public:
    WindowFunctionCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ParenthesesContext *parentheses();
    WindowingClauseContext *windowingClause();
    antlr4::tree::TerminalNode *ROW_NUMBER_SYMBOL();
    antlr4::tree::TerminalNode *RANK_SYMBOL();
    antlr4::tree::TerminalNode *DENSE_RANK_SYMBOL();
    antlr4::tree::TerminalNode *CUME_DIST_SYMBOL();
    antlr4::tree::TerminalNode *PERCENT_RANK_SYMBOL();
    antlr4::tree::TerminalNode *NTILE_SYMBOL();
    SimpleExprWithParenthesesContext *simpleExprWithParentheses();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *LEAD_SYMBOL();
    antlr4::tree::TerminalNode *LAG_SYMBOL();
    LeadLagInfoContext *leadLagInfo();
    NullTreatmentContext *nullTreatment();
    ExprWithParenthesesContext *exprWithParentheses();
    antlr4::tree::TerminalNode *FIRST_VALUE_SYMBOL();
    antlr4::tree::TerminalNode *LAST_VALUE_SYMBOL();
    antlr4::tree::TerminalNode *NTH_VALUE_SYMBOL();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    SimpleExprContext *simpleExpr();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *FIRST_SYMBOL();
    antlr4::tree::TerminalNode *LAST_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowFunctionCallContext* windowFunctionCall();

  class  WindowingClauseContext : public antlr4::ParserRuleContext {
  public:
    WindowingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OVER_SYMBOL();
    WindowNameContext *windowName();
    WindowSpecContext *windowSpec();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowingClauseContext* windowingClause();

  class  LeadLagInfoContext : public antlr4::ParserRuleContext {
  public:
    LeadLagInfoContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    Ulonglong_numberContext *ulonglong_number();
    antlr4::tree::TerminalNode *PARAM_MARKER();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LeadLagInfoContext* leadLagInfo();

  class  NullTreatmentContext : public antlr4::ParserRuleContext {
  public:
    NullTreatmentContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NULLS_SYMBOL();
    antlr4::tree::TerminalNode *RESPECT_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NullTreatmentContext* nullTreatment();

  class  JsonFunctionContext : public antlr4::ParserRuleContext {
  public:
    JsonFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JSON_ARRAYAGG_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<InSumExprContext *> inSumExpr();
    InSumExprContext* inSumExpr(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    WindowingClauseContext *windowingClause();
    antlr4::tree::TerminalNode *JSON_OBJECTAGG_SYMBOL();
    antlr4::tree::TerminalNode *COMMA_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JsonFunctionContext* jsonFunction();

  class  InSumExprContext : public antlr4::ParserRuleContext {
  public:
    InSumExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();
    antlr4::tree::TerminalNode *ALL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InSumExprContext* inSumExpr();

  class  IdentListArgContext : public antlr4::ParserRuleContext {
  public:
    IdentListArgContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentListContext *identList();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentListArgContext* identListArg();

  class  IdentListContext : public antlr4::ParserRuleContext {
  public:
    IdentListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SimpleIdentifierContext *> simpleIdentifier();
    SimpleIdentifierContext* simpleIdentifier(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentListContext* identList();

  class  FulltextOptionsContext : public antlr4::ParserRuleContext {
  public:
    FulltextOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IN_SYMBOL();
    antlr4::tree::TerminalNode *BOOLEAN_SYMBOL();
    antlr4::tree::TerminalNode *MODE_SYMBOL();
    antlr4::tree::TerminalNode *NATURAL_SYMBOL();
    antlr4::tree::TerminalNode *LANGUAGE_SYMBOL();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *EXPANSION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FulltextOptionsContext* fulltextOptions();

  class  RuntimeFunctionCallContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *name = nullptr;
    RuntimeFunctionCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprListContext *exprList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CHAR_SYMBOL();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    CharsetNameContext *charsetName();
    antlr4::tree::TerminalNode *CURRENT_USER_SYMBOL();
    ParenthesesContext *parentheses();
    ExprWithParenthesesContext *exprWithParentheses();
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *DAY_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_SYMBOL();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *INSERT_SYMBOL();
    antlr4::tree::TerminalNode *INTERVAL_SYMBOL();
    antlr4::tree::TerminalNode *LEFT_SYMBOL();
    antlr4::tree::TerminalNode *MINUTE_SYMBOL();
    antlr4::tree::TerminalNode *MONTH_SYMBOL();
    antlr4::tree::TerminalNode *RIGHT_SYMBOL();
    antlr4::tree::TerminalNode *SECOND_SYMBOL();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_SYMBOL();
    TrimFunctionContext *trimFunction();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    antlr4::tree::TerminalNode *VALUES_SYMBOL();
    antlr4::tree::TerminalNode *YEAR_SYMBOL();
    antlr4::tree::TerminalNode *ADDDATE_SYMBOL();
    antlr4::tree::TerminalNode *SUBDATE_SYMBOL();
    IntervalContext *interval();
    antlr4::tree::TerminalNode *CURDATE_SYMBOL();
    antlr4::tree::TerminalNode *CURTIME_SYMBOL();
    TimeFunctionParametersContext *timeFunctionParameters();
    antlr4::tree::TerminalNode *DATE_ADD_SYMBOL();
    antlr4::tree::TerminalNode *DATE_SUB_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *EXTRACT_SYMBOL();
    DateTimeTtypeContext *dateTimeTtype();
    antlr4::tree::TerminalNode *GET_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *NOW_SYMBOL();
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *IN_SYMBOL();
    antlr4::tree::TerminalNode *POSITION_SYMBOL();
    SubstringFunctionContext *substringFunction();
    antlr4::tree::TerminalNode *SYSDATE_SYMBOL();
    IntervalTimeStampContext *intervalTimeStamp();
    antlr4::tree::TerminalNode *TIMESTAMP_ADD_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_DIFF_SYMBOL();
    antlr4::tree::TerminalNode *UTC_DATE_SYMBOL();
    antlr4::tree::TerminalNode *UTC_TIME_SYMBOL();
    antlr4::tree::TerminalNode *UTC_TIMESTAMP_SYMBOL();
    antlr4::tree::TerminalNode *ASCII_SYMBOL();
    antlr4::tree::TerminalNode *CHARSET_SYMBOL();
    ExprListWithParenthesesContext *exprListWithParentheses();
    antlr4::tree::TerminalNode *COALESCE_SYMBOL();
    antlr4::tree::TerminalNode *COLLATION_SYMBOL();
    antlr4::tree::TerminalNode *DATABASE_SYMBOL();
    antlr4::tree::TerminalNode *IF_SYMBOL();
    antlr4::tree::TerminalNode *FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *MOD_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *OLD_PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *QUARTER_SYMBOL();
    antlr4::tree::TerminalNode *REPEAT_SYMBOL();
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    antlr4::tree::TerminalNode *REVERSE_SYMBOL();
    antlr4::tree::TerminalNode *ROW_COUNT_SYMBOL();
    antlr4::tree::TerminalNode *TRUNCATE_SYMBOL();
    antlr4::tree::TerminalNode *WEEK_SYMBOL();
    antlr4::tree::TerminalNode *WEIGHT_STRING_SYMBOL();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    WsNumCodepointsContext *wsNumCodepoints();
    std::vector<Ulong_numberContext *> ulong_number();
    Ulong_numberContext* ulong_number(size_t i);
    WeightStringLevelsContext *weightStringLevels();
    GeometryFunctionContext *geometryFunction();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RuntimeFunctionCallContext* runtimeFunctionCall();

  class  GeometryFunctionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *name = nullptr;
    GeometryFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CONTAINS_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRYCOLLECTION_SYMBOL();
    ExprListContext *exprList();
    ExprListWithParenthesesContext *exprListWithParentheses();
    antlr4::tree::TerminalNode *LINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *MULTILINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOINT_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOLYGON_SYMBOL();
    antlr4::tree::TerminalNode *POINT_SYMBOL();
    antlr4::tree::TerminalNode *POLYGON_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GeometryFunctionContext* geometryFunction();

  class  TimeFunctionParametersContext : public antlr4::ParserRuleContext {
  public:
    TimeFunctionParametersContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    FractionalPrecisionContext *fractionalPrecision();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TimeFunctionParametersContext* timeFunctionParameters();

  class  FractionalPrecisionContext : public antlr4::ParserRuleContext {
  public:
    FractionalPrecisionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FractionalPrecisionContext* fractionalPrecision();

  class  WeightStringLevelsContext : public antlr4::ParserRuleContext {
  public:
    WeightStringLevelsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LEVEL_SYMBOL();
    std::vector<Real_ulong_numberContext *> real_ulong_number();
    Real_ulong_numberContext* real_ulong_number(size_t i);
    antlr4::tree::TerminalNode *MINUS_OPERATOR();
    std::vector<WeightStringLevelListItemContext *> weightStringLevelListItem();
    WeightStringLevelListItemContext* weightStringLevelListItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WeightStringLevelsContext* weightStringLevels();

  class  WeightStringLevelListItemContext : public antlr4::ParserRuleContext {
  public:
    WeightStringLevelListItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Real_ulong_numberContext *real_ulong_number();
    antlr4::tree::TerminalNode *REVERSE_SYMBOL();
    antlr4::tree::TerminalNode *ASC_SYMBOL();
    antlr4::tree::TerminalNode *DESC_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WeightStringLevelListItemContext* weightStringLevelListItem();

  class  DateTimeTtypeContext : public antlr4::ParserRuleContext {
  public:
    DateTimeTtypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    antlr4::tree::TerminalNode *DATETIME_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DateTimeTtypeContext* dateTimeTtype();

  class  TrimFunctionContext : public antlr4::ParserRuleContext {
  public:
    TrimFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRIM_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *LEADING_SYMBOL();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *TRAILING_SYMBOL();
    antlr4::tree::TerminalNode *BOTH_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TrimFunctionContext* trimFunction();

  class  SubstringFunctionContext : public antlr4::ParserRuleContext {
  public:
    SubstringFunctionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SUBSTRING_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SubstringFunctionContext* substringFunction();

  class  FunctionCallContext : public antlr4::ParserRuleContext {
  public:
    FunctionCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PureIdentifierContext *pureIdentifier();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    UdfExprListContext *udfExprList();
    QualifiedIdentifierContext *qualifiedIdentifier();
    ExprListContext *exprList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FunctionCallContext* functionCall();

  class  UdfExprListContext : public antlr4::ParserRuleContext {
  public:
    UdfExprListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UdfExprContext *> udfExpr();
    UdfExprContext* udfExpr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UdfExprListContext* udfExprList();

  class  UdfExprContext : public antlr4::ParserRuleContext {
  public:
    UdfExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();
    SelectAliasContext *selectAlias();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UdfExprContext* udfExpr();

  class  VariableContext : public antlr4::ParserRuleContext {
  public:
    VariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UserVariableContext *userVariable();
    antlr4::tree::TerminalNode *ASSIGN_OPERATOR();
    ExprContext *expr();
    SystemVariableContext *systemVariable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  VariableContext* variable();

  class  UserVariableContext : public antlr4::ParserRuleContext {
  public:
    UserVariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AT_SIGN_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *AT_TEXT_SUFFIX();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UserVariableContext* userVariable();

  class  SystemVariableContext : public antlr4::ParserRuleContext {
  public:
    SystemVariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AT_AT_SIGN_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    VarIdentTypeContext *varIdentType();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SystemVariableContext* systemVariable();

  class  InternalVariableNameContext : public antlr4::ParserRuleContext {
  public:
    InternalVariableNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    LValueIdentifierContext *lValueIdentifier();
    DotIdentifierContext *dotIdentifier();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InternalVariableNameContext* internalVariableName();

  class  WhenExpressionContext : public antlr4::ParserRuleContext {
  public:
    WhenExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHEN_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WhenExpressionContext* whenExpression();

  class  ThenExpressionContext : public antlr4::ParserRuleContext {
  public:
    ThenExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *THEN_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ThenExpressionContext* thenExpression();

  class  ElseExpressionContext : public antlr4::ParserRuleContext {
  public:
    ElseExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ELSE_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ElseExpressionContext* elseExpression();

  class  CastTypeContext : public antlr4::ParserRuleContext {
  public:
    CastTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    FieldLengthContext *fieldLength();
    antlr4::tree::TerminalNode *CHAR_SYMBOL();
    CharsetWithOptBinaryContext *charsetWithOptBinary();
    NcharContext *nchar();
    antlr4::tree::TerminalNode *SIGNED_SYMBOL();
    antlr4::tree::TerminalNode *INT_SYMBOL();
    antlr4::tree::TerminalNode *UNSIGNED_SYMBOL();
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    TypeDatetimePrecisionContext *typeDatetimePrecision();
    antlr4::tree::TerminalNode *DATETIME_SYMBOL();
    antlr4::tree::TerminalNode *DECIMAL_SYMBOL();
    FloatOptionsContext *floatOptions();
    antlr4::tree::TerminalNode *JSON_SYMBOL();
    RealTypeContext *realType();
    antlr4::tree::TerminalNode *FLOAT_SYMBOL();
    StandardFloatOptionsContext *standardFloatOptions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CastTypeContext* castType();

  class  ExprListContext : public antlr4::ParserRuleContext {
  public:
    ExprListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExprListContext* exprList();

  class  CharsetContext : public antlr4::ParserRuleContext {
  public:
    CharsetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CHAR_SYMBOL();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    antlr4::tree::TerminalNode *CHARSET_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CharsetContext* charset();

  class  NotRuleContext : public antlr4::ParserRuleContext {
  public:
    NotRuleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NOT_SYMBOL();
    antlr4::tree::TerminalNode *NOT2_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NotRuleContext* notRule();

  class  Not2RuleContext : public antlr4::ParserRuleContext {
  public:
    Not2RuleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOGICAL_NOT_OPERATOR();
    antlr4::tree::TerminalNode *NOT2_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  Not2RuleContext* not2Rule();

  class  IntervalContext : public antlr4::ParserRuleContext {
  public:
    IntervalContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IntervalTimeStampContext *intervalTimeStamp();
    antlr4::tree::TerminalNode *SECOND_MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *MINUTE_MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *MINUTE_SECOND_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_SECOND_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_MINUTE_SYMBOL();
    antlr4::tree::TerminalNode *DAY_MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *DAY_SECOND_SYMBOL();
    antlr4::tree::TerminalNode *DAY_MINUTE_SYMBOL();
    antlr4::tree::TerminalNode *DAY_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *YEAR_MONTH_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IntervalContext* interval();

  class  IntervalTimeStampContext : public antlr4::ParserRuleContext {
  public:
    IntervalTimeStampContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *SECOND_SYMBOL();
    antlr4::tree::TerminalNode *MINUTE_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_SYMBOL();
    antlr4::tree::TerminalNode *DAY_SYMBOL();
    antlr4::tree::TerminalNode *WEEK_SYMBOL();
    antlr4::tree::TerminalNode *MONTH_SYMBOL();
    antlr4::tree::TerminalNode *QUARTER_SYMBOL();
    antlr4::tree::TerminalNode *YEAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IntervalTimeStampContext* intervalTimeStamp();

  class  ExprListWithParenthesesContext : public antlr4::ParserRuleContext {
  public:
    ExprListWithParenthesesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprListContext *exprList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExprListWithParenthesesContext* exprListWithParentheses();

  class  ExprWithParenthesesContext : public antlr4::ParserRuleContext {
  public:
    ExprWithParenthesesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExprWithParenthesesContext* exprWithParentheses();

  class  SimpleExprWithParenthesesContext : public antlr4::ParserRuleContext {
  public:
    SimpleExprWithParenthesesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    SimpleExprContext *simpleExpr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SimpleExprWithParenthesesContext* simpleExprWithParentheses();

  class  OrderListContext : public antlr4::ParserRuleContext {
  public:
    OrderListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OrderExpressionContext *> orderExpression();
    OrderExpressionContext* orderExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OrderListContext* orderList();

  class  OrderExpressionContext : public antlr4::ParserRuleContext {
  public:
    OrderExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();
    DirectionContext *direction();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OrderExpressionContext* orderExpression();

  class  GroupListContext : public antlr4::ParserRuleContext {
  public:
    GroupListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<GroupingExpressionContext *> groupingExpression();
    GroupingExpressionContext* groupingExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GroupListContext* groupList();

  class  GroupingExpressionContext : public antlr4::ParserRuleContext {
  public:
    GroupingExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GroupingExpressionContext* groupingExpression();

  class  ChannelContext : public antlr4::ParserRuleContext {
  public:
    ChannelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    antlr4::tree::TerminalNode *CHANNEL_SYMBOL();
    TextStringNoLinebreakContext *textStringNoLinebreak();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ChannelContext* channel();

  class  CompoundStatementContext : public antlr4::ParserRuleContext {
  public:
    CompoundStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SimpleStatementContext *simpleStatement();
    ReturnStatementContext *returnStatement();
    IfStatementContext *ifStatement();
    CaseStatementContext *caseStatement();
    LabeledBlockContext *labeledBlock();
    UnlabeledBlockContext *unlabeledBlock();
    LabeledControlContext *labeledControl();
    UnlabeledControlContext *unlabeledControl();
    LeaveStatementContext *leaveStatement();
    IterateStatementContext *iterateStatement();
    CursorOpenContext *cursorOpen();
    CursorFetchContext *cursorFetch();
    CursorCloseContext *cursorClose();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CompoundStatementContext* compoundStatement();

  class  ReturnStatementContext : public antlr4::ParserRuleContext {
  public:
    ReturnStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RETURN_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReturnStatementContext* returnStatement();

  class  IfStatementContext : public antlr4::ParserRuleContext {
  public:
    IfStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> IF_SYMBOL();
    antlr4::tree::TerminalNode* IF_SYMBOL(size_t i);
    IfBodyContext *ifBody();
    antlr4::tree::TerminalNode *END_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IfStatementContext* ifStatement();

  class  IfBodyContext : public antlr4::ParserRuleContext {
  public:
    IfBodyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExprContext *expr();
    ThenStatementContext *thenStatement();
    antlr4::tree::TerminalNode *ELSEIF_SYMBOL();
    IfBodyContext *ifBody();
    antlr4::tree::TerminalNode *ELSE_SYMBOL();
    CompoundStatementListContext *compoundStatementList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IfBodyContext* ifBody();

  class  ThenStatementContext : public antlr4::ParserRuleContext {
  public:
    ThenStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *THEN_SYMBOL();
    CompoundStatementListContext *compoundStatementList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ThenStatementContext* thenStatement();

  class  CompoundStatementListContext : public antlr4::ParserRuleContext {
  public:
    CompoundStatementListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<CompoundStatementContext *> compoundStatement();
    CompoundStatementContext* compoundStatement(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SEMICOLON_SYMBOL();
    antlr4::tree::TerminalNode* SEMICOLON_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CompoundStatementListContext* compoundStatementList();

  class  CaseStatementContext : public antlr4::ParserRuleContext {
  public:
    CaseStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> CASE_SYMBOL();
    antlr4::tree::TerminalNode* CASE_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *END_SYMBOL();
    ExprContext *expr();
    std::vector<WhenExpressionContext *> whenExpression();
    WhenExpressionContext* whenExpression(size_t i);
    std::vector<ThenStatementContext *> thenStatement();
    ThenStatementContext* thenStatement(size_t i);
    ElseStatementContext *elseStatement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CaseStatementContext* caseStatement();

  class  ElseStatementContext : public antlr4::ParserRuleContext {
  public:
    ElseStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ELSE_SYMBOL();
    CompoundStatementListContext *compoundStatementList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ElseStatementContext* elseStatement();

  class  LabeledBlockContext : public antlr4::ParserRuleContext {
  public:
    LabeledBlockContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelContext *label();
    BeginEndBlockContext *beginEndBlock();
    LabelRefContext *labelRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LabeledBlockContext* labeledBlock();

  class  UnlabeledBlockContext : public antlr4::ParserRuleContext {
  public:
    UnlabeledBlockContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BeginEndBlockContext *beginEndBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UnlabeledBlockContext* unlabeledBlock();

  class  LabelContext : public antlr4::ParserRuleContext {
  public:
    LabelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelIdentifierContext *labelIdentifier();
    antlr4::tree::TerminalNode *COLON_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LabelContext* label();

  class  BeginEndBlockContext : public antlr4::ParserRuleContext {
  public:
    BeginEndBlockContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BEGIN_SYMBOL();
    antlr4::tree::TerminalNode *END_SYMBOL();
    SpDeclarationsContext *spDeclarations();
    CompoundStatementListContext *compoundStatementList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  BeginEndBlockContext* beginEndBlock();

  class  LabeledControlContext : public antlr4::ParserRuleContext {
  public:
    LabeledControlContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelContext *label();
    UnlabeledControlContext *unlabeledControl();
    LabelRefContext *labelRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LabeledControlContext* labeledControl();

  class  UnlabeledControlContext : public antlr4::ParserRuleContext {
  public:
    UnlabeledControlContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LoopBlockContext *loopBlock();
    WhileDoBlockContext *whileDoBlock();
    RepeatUntilBlockContext *repeatUntilBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UnlabeledControlContext* unlabeledControl();

  class  LoopBlockContext : public antlr4::ParserRuleContext {
  public:
    LoopBlockContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> LOOP_SYMBOL();
    antlr4::tree::TerminalNode* LOOP_SYMBOL(size_t i);
    CompoundStatementListContext *compoundStatementList();
    antlr4::tree::TerminalNode *END_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LoopBlockContext* loopBlock();

  class  WhileDoBlockContext : public antlr4::ParserRuleContext {
  public:
    WhileDoBlockContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> WHILE_SYMBOL();
    antlr4::tree::TerminalNode* WHILE_SYMBOL(size_t i);
    ExprContext *expr();
    antlr4::tree::TerminalNode *DO_SYMBOL();
    CompoundStatementListContext *compoundStatementList();
    antlr4::tree::TerminalNode *END_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WhileDoBlockContext* whileDoBlock();

  class  RepeatUntilBlockContext : public antlr4::ParserRuleContext {
  public:
    RepeatUntilBlockContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> REPEAT_SYMBOL();
    antlr4::tree::TerminalNode* REPEAT_SYMBOL(size_t i);
    CompoundStatementListContext *compoundStatementList();
    antlr4::tree::TerminalNode *UNTIL_SYMBOL();
    ExprContext *expr();
    antlr4::tree::TerminalNode *END_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RepeatUntilBlockContext* repeatUntilBlock();

  class  SpDeclarationsContext : public antlr4::ParserRuleContext {
  public:
    SpDeclarationsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SpDeclarationContext *> spDeclaration();
    SpDeclarationContext* spDeclaration(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SEMICOLON_SYMBOL();
    antlr4::tree::TerminalNode* SEMICOLON_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SpDeclarationsContext* spDeclarations();

  class  SpDeclarationContext : public antlr4::ParserRuleContext {
  public:
    SpDeclarationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VariableDeclarationContext *variableDeclaration();
    ConditionDeclarationContext *conditionDeclaration();
    HandlerDeclarationContext *handlerDeclaration();
    CursorDeclarationContext *cursorDeclaration();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SpDeclarationContext* spDeclaration();

  class  VariableDeclarationContext : public antlr4::ParserRuleContext {
  public:
    VariableDeclarationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DECLARE_SYMBOL();
    IdentifierListContext *identifierList();
    DataTypeContext *dataType();
    CollateContext *collate();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    ExprContext *expr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  VariableDeclarationContext* variableDeclaration();

  class  ConditionDeclarationContext : public antlr4::ParserRuleContext {
  public:
    ConditionDeclarationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DECLARE_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *CONDITION_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    SpConditionContext *spCondition();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConditionDeclarationContext* conditionDeclaration();

  class  SpConditionContext : public antlr4::ParserRuleContext {
  public:
    SpConditionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Ulong_numberContext *ulong_number();
    SqlstateContext *sqlstate();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SpConditionContext* spCondition();

  class  SqlstateContext : public antlr4::ParserRuleContext {
  public:
    SqlstateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SQLSTATE_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *VALUE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SqlstateContext* sqlstate();

  class  HandlerDeclarationContext : public antlr4::ParserRuleContext {
  public:
    HandlerDeclarationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DECLARE_SYMBOL();
    antlr4::tree::TerminalNode *HANDLER_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    std::vector<HandlerConditionContext *> handlerCondition();
    HandlerConditionContext* handlerCondition(size_t i);
    CompoundStatementContext *compoundStatement();
    antlr4::tree::TerminalNode *CONTINUE_SYMBOL();
    antlr4::tree::TerminalNode *EXIT_SYMBOL();
    antlr4::tree::TerminalNode *UNDO_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HandlerDeclarationContext* handlerDeclaration();

  class  HandlerConditionContext : public antlr4::ParserRuleContext {
  public:
    HandlerConditionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SpConditionContext *spCondition();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *SQLWARNING_SYMBOL();
    NotRuleContext *notRule();
    antlr4::tree::TerminalNode *FOUND_SYMBOL();
    antlr4::tree::TerminalNode *SQLEXCEPTION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HandlerConditionContext* handlerCondition();

  class  CursorDeclarationContext : public antlr4::ParserRuleContext {
  public:
    CursorDeclarationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DECLARE_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *CURSOR_SYMBOL();
    antlr4::tree::TerminalNode *FOR_SYMBOL();
    SelectStatementContext *selectStatement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CursorDeclarationContext* cursorDeclaration();

  class  IterateStatementContext : public antlr4::ParserRuleContext {
  public:
    IterateStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ITERATE_SYMBOL();
    LabelRefContext *labelRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IterateStatementContext* iterateStatement();

  class  LeaveStatementContext : public antlr4::ParserRuleContext {
  public:
    LeaveStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LEAVE_SYMBOL();
    LabelRefContext *labelRef();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LeaveStatementContext* leaveStatement();

  class  GetDiagnosticsContext : public antlr4::ParserRuleContext {
  public:
    GetDiagnosticsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GET_SYMBOL();
    antlr4::tree::TerminalNode *DIAGNOSTICS_SYMBOL();
    std::vector<StatementInformationItemContext *> statementInformationItem();
    StatementInformationItemContext* statementInformationItem(size_t i);
    antlr4::tree::TerminalNode *CONDITION_SYMBOL();
    SignalAllowedExprContext *signalAllowedExpr();
    std::vector<ConditionInformationItemContext *> conditionInformationItem();
    ConditionInformationItemContext* conditionInformationItem(size_t i);
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    antlr4::tree::TerminalNode *STACKED_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GetDiagnosticsContext* getDiagnostics();

  class  SignalAllowedExprContext : public antlr4::ParserRuleContext {
  public:
    SignalAllowedExprContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();
    VariableContext *variable();
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SignalAllowedExprContext* signalAllowedExpr();

  class  StatementInformationItemContext : public antlr4::ParserRuleContext {
  public:
    StatementInformationItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *NUMBER_SYMBOL();
    antlr4::tree::TerminalNode *ROW_COUNT_SYMBOL();
    VariableContext *variable();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StatementInformationItemContext* statementInformationItem();

  class  ConditionInformationItemContext : public antlr4::ParserRuleContext {
  public:
    ConditionInformationItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    VariableContext *variable();
    IdentifierContext *identifier();
    SignalInformationItemNameContext *signalInformationItemName();
    antlr4::tree::TerminalNode *RETURNED_SQLSTATE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConditionInformationItemContext* conditionInformationItem();

  class  SignalInformationItemNameContext : public antlr4::ParserRuleContext {
  public:
    SignalInformationItemNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CLASS_ORIGIN_SYMBOL();
    antlr4::tree::TerminalNode *SUBCLASS_ORIGIN_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_CATALOG_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_SCHEMA_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_NAME_SYMBOL();
    antlr4::tree::TerminalNode *CATALOG_NAME_SYMBOL();
    antlr4::tree::TerminalNode *SCHEMA_NAME_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_NAME_SYMBOL();
    antlr4::tree::TerminalNode *COLUMN_NAME_SYMBOL();
    antlr4::tree::TerminalNode *CURSOR_NAME_SYMBOL();
    antlr4::tree::TerminalNode *MESSAGE_TEXT_SYMBOL();
    antlr4::tree::TerminalNode *MYSQL_ERRNO_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SignalInformationItemNameContext* signalInformationItemName();

  class  SignalStatementContext : public antlr4::ParserRuleContext {
  public:
    SignalStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SIGNAL_SYMBOL();
    IdentifierContext *identifier();
    SqlstateContext *sqlstate();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    std::vector<SignalInformationItemContext *> signalInformationItem();
    SignalInformationItemContext* signalInformationItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SignalStatementContext* signalStatement();

  class  ResignalStatementContext : public antlr4::ParserRuleContext {
  public:
    ResignalStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RESIGNAL_SYMBOL();
    antlr4::tree::TerminalNode *SQLSTATE_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    std::vector<SignalInformationItemContext *> signalInformationItem();
    SignalInformationItemContext* signalInformationItem(size_t i);
    antlr4::tree::TerminalNode *VALUE_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResignalStatementContext* resignalStatement();

  class  SignalInformationItemContext : public antlr4::ParserRuleContext {
  public:
    SignalInformationItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SignalInformationItemNameContext *signalInformationItemName();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    SignalAllowedExprContext *signalAllowedExpr();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SignalInformationItemContext* signalInformationItem();

  class  CursorOpenContext : public antlr4::ParserRuleContext {
  public:
    CursorOpenContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_SYMBOL();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CursorOpenContext* cursorOpen();

  class  CursorCloseContext : public antlr4::ParserRuleContext {
  public:
    CursorCloseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CLOSE_SYMBOL();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CursorCloseContext* cursorClose();

  class  CursorFetchContext : public antlr4::ParserRuleContext {
  public:
    CursorFetchContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FETCH_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *INTO_SYMBOL();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *FROM_SYMBOL();
    antlr4::tree::TerminalNode *NEXT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CursorFetchContext* cursorFetch();

  class  ScheduleContext : public antlr4::ParserRuleContext {
  public:
    ScheduleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AT_SYMBOL();
    std::vector<ExprContext *> expr();
    ExprContext* expr(size_t i);
    antlr4::tree::TerminalNode *EVERY_SYMBOL();
    IntervalContext *interval();
    antlr4::tree::TerminalNode *STARTS_SYMBOL();
    antlr4::tree::TerminalNode *ENDS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ScheduleContext* schedule();

  class  ColumnDefinitionContext : public antlr4::ParserRuleContext {
  public:
    ColumnDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnNameContext *columnName();
    FieldDefinitionContext *fieldDefinition();
    CheckOrReferencesContext *checkOrReferences();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnDefinitionContext* columnDefinition();

  class  CheckOrReferencesContext : public antlr4::ParserRuleContext {
  public:
    CheckOrReferencesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CheckConstraintContext *checkConstraint();
    ReferencesContext *references();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CheckOrReferencesContext* checkOrReferences();

  class  CheckConstraintContext : public antlr4::ParserRuleContext {
  public:
    CheckConstraintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CHECK_SYMBOL();
    ExprWithParenthesesContext *exprWithParentheses();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CheckConstraintContext* checkConstraint();

  class  ConstraintEnforcementContext : public antlr4::ParserRuleContext {
  public:
    ConstraintEnforcementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ENFORCED_SYMBOL();
    antlr4::tree::TerminalNode *NOT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConstraintEnforcementContext* constraintEnforcement();

  class  TableConstraintDefContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    TableConstraintDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KeyListVariantsContext *keyListVariants();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    IndexNameAndTypeContext *indexNameAndType();
    std::vector<IndexOptionContext *> indexOption();
    IndexOptionContext* indexOption(size_t i);
    antlr4::tree::TerminalNode *FULLTEXT_SYMBOL();
    KeyOrIndexContext *keyOrIndex();
    IndexNameContext *indexName();
    std::vector<FulltextIndexOptionContext *> fulltextIndexOption();
    FulltextIndexOptionContext* fulltextIndexOption(size_t i);
    antlr4::tree::TerminalNode *SPATIAL_SYMBOL();
    std::vector<SpatialIndexOptionContext *> spatialIndexOption();
    SpatialIndexOptionContext* spatialIndexOption(size_t i);
    KeyListContext *keyList();
    ReferencesContext *references();
    CheckConstraintContext *checkConstraint();
    ConstraintNameContext *constraintName();
    antlr4::tree::TerminalNode *FOREIGN_SYMBOL();
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();
    antlr4::tree::TerminalNode *UNIQUE_SYMBOL();
    ConstraintEnforcementContext *constraintEnforcement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableConstraintDefContext* tableConstraintDef();

  class  ConstraintNameContext : public antlr4::ParserRuleContext {
  public:
    ConstraintNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CONSTRAINT_SYMBOL();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConstraintNameContext* constraintName();

  class  FieldDefinitionContext : public antlr4::ParserRuleContext {
  public:
    FieldDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext *dataType();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    ExprWithParenthesesContext *exprWithParentheses();
    std::vector<ColumnAttributeContext *> columnAttribute();
    ColumnAttributeContext* columnAttribute(size_t i);
    CollateContext *collate();
    antlr4::tree::TerminalNode *GENERATED_SYMBOL();
    antlr4::tree::TerminalNode *ALWAYS_SYMBOL();
    antlr4::tree::TerminalNode *VIRTUAL_SYMBOL();
    antlr4::tree::TerminalNode *STORED_SYMBOL();
    std::vector<GcolAttributeContext *> gcolAttribute();
    GcolAttributeContext* gcolAttribute(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldDefinitionContext* fieldDefinition();

  class  ColumnAttributeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *value = nullptr;
    ColumnAttributeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NullLiteralContext *nullLiteral();
    antlr4::tree::TerminalNode *NOT_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    SignedLiteralContext *signedLiteral();
    antlr4::tree::TerminalNode *NOW_SYMBOL();
    ExprWithParenthesesContext *exprWithParentheses();
    TimeFunctionParametersContext *timeFunctionParameters();
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    antlr4::tree::TerminalNode *ON_SYMBOL();
    antlr4::tree::TerminalNode *AUTO_INCREMENT_SYMBOL();
    antlr4::tree::TerminalNode *VALUE_SYMBOL();
    antlr4::tree::TerminalNode *SERIAL_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();
    antlr4::tree::TerminalNode *UNIQUE_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    CollateContext *collate();
    ColumnFormatContext *columnFormat();
    antlr4::tree::TerminalNode *COLUMN_FORMAT_SYMBOL();
    StorageMediaContext *storageMedia();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    Real_ulonglong_numberContext *real_ulonglong_number();
    antlr4::tree::TerminalNode *SRID_SYMBOL();
    CheckConstraintContext *checkConstraint();
    ConstraintNameContext *constraintName();
    ConstraintEnforcementContext *constraintEnforcement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnAttributeContext* columnAttribute();

  class  ColumnFormatContext : public antlr4::ParserRuleContext {
  public:
    ColumnFormatContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FIXED_SYMBOL();
    antlr4::tree::TerminalNode *DYNAMIC_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnFormatContext* columnFormat();

  class  StorageMediaContext : public antlr4::ParserRuleContext {
  public:
    StorageMediaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DISK_SYMBOL();
    antlr4::tree::TerminalNode *MEMORY_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StorageMediaContext* storageMedia();

  class  GcolAttributeContext : public antlr4::ParserRuleContext {
  public:
    GcolAttributeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNIQUE_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *NULL_SYMBOL();
    NotRuleContext *notRule();
    antlr4::tree::TerminalNode *PRIMARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GcolAttributeContext* gcolAttribute();

  class  ReferencesContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *match = nullptr;
    antlr4::Token *option = nullptr;
    ReferencesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REFERENCES_SYMBOL();
    TableRefContext *tableRef();
    IdentifierListWithParenthesesContext *identifierListWithParentheses();
    antlr4::tree::TerminalNode *MATCH_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> ON_SYMBOL();
    antlr4::tree::TerminalNode* ON_SYMBOL(size_t i);
    std::vector<DeleteOptionContext *> deleteOption();
    DeleteOptionContext* deleteOption(size_t i);
    antlr4::tree::TerminalNode *UPDATE_SYMBOL();
    antlr4::tree::TerminalNode *DELETE_SYMBOL();
    antlr4::tree::TerminalNode *FULL_SYMBOL();
    antlr4::tree::TerminalNode *PARTIAL_SYMBOL();
    antlr4::tree::TerminalNode *SIMPLE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReferencesContext* references();

  class  DeleteOptionContext : public antlr4::ParserRuleContext {
  public:
    DeleteOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RESTRICT_SYMBOL();
    antlr4::tree::TerminalNode *CASCADE_SYMBOL();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    NullLiteralContext *nullLiteral();
    antlr4::tree::TerminalNode *NO_SYMBOL();
    antlr4::tree::TerminalNode *ACTION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DeleteOptionContext* deleteOption();

  class  KeyListContext : public antlr4::ParserRuleContext {
  public:
    KeyListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<KeyPartContext *> keyPart();
    KeyPartContext* keyPart(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyListContext* keyList();

  class  KeyPartContext : public antlr4::ParserRuleContext {
  public:
    KeyPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    FieldLengthContext *fieldLength();
    DirectionContext *direction();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyPartContext* keyPart();

  class  KeyListWithExpressionContext : public antlr4::ParserRuleContext {
  public:
    KeyListWithExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<KeyPartOrExpressionContext *> keyPartOrExpression();
    KeyPartOrExpressionContext* keyPartOrExpression(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyListWithExpressionContext* keyListWithExpression();

  class  KeyPartOrExpressionContext : public antlr4::ParserRuleContext {
  public:
    KeyPartOrExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KeyPartContext *keyPart();
    ExprWithParenthesesContext *exprWithParentheses();
    DirectionContext *direction();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyPartOrExpressionContext* keyPartOrExpression();

  class  KeyListVariantsContext : public antlr4::ParserRuleContext {
  public:
    KeyListVariantsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KeyListWithExpressionContext *keyListWithExpression();
    KeyListContext *keyList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KeyListVariantsContext* keyListVariants();

  class  IndexTypeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *algorithm = nullptr;
    IndexTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BTREE_SYMBOL();
    antlr4::tree::TerminalNode *RTREE_SYMBOL();
    antlr4::tree::TerminalNode *HASH_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexTypeContext* indexType();

  class  IndexOptionContext : public antlr4::ParserRuleContext {
  public:
    IndexOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CommonIndexOptionContext *commonIndexOption();
    IndexTypeClauseContext *indexTypeClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexOptionContext* indexOption();

  class  CommonIndexOptionContext : public antlr4::ParserRuleContext {
  public:
    CommonIndexOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *KEY_BLOCK_SIZE_SYMBOL();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    TextLiteralContext *textLiteral();
    VisibilityContext *visibility();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CommonIndexOptionContext* commonIndexOption();

  class  VisibilityContext : public antlr4::ParserRuleContext {
  public:
    VisibilityContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VISIBLE_SYMBOL();
    antlr4::tree::TerminalNode *INVISIBLE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  VisibilityContext* visibility();

  class  IndexTypeClauseContext : public antlr4::ParserRuleContext {
  public:
    IndexTypeClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IndexTypeContext *indexType();
    antlr4::tree::TerminalNode *USING_SYMBOL();
    antlr4::tree::TerminalNode *TYPE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexTypeClauseContext* indexTypeClause();

  class  FulltextIndexOptionContext : public antlr4::ParserRuleContext {
  public:
    FulltextIndexOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CommonIndexOptionContext *commonIndexOption();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    antlr4::tree::TerminalNode *PARSER_SYMBOL();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FulltextIndexOptionContext* fulltextIndexOption();

  class  SpatialIndexOptionContext : public antlr4::ParserRuleContext {
  public:
    SpatialIndexOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CommonIndexOptionContext *commonIndexOption();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SpatialIndexOptionContext* spatialIndexOption();

  class  DataTypeDefinitionContext : public antlr4::ParserRuleContext {
  public:
    DataTypeDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext *dataType();
    antlr4::tree::TerminalNode *EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DataTypeDefinitionContext* dataTypeDefinition();

  class  DataTypeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    DataTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_SYMBOL();
    antlr4::tree::TerminalNode *TINYINT_SYMBOL();
    antlr4::tree::TerminalNode *SMALLINT_SYMBOL();
    antlr4::tree::TerminalNode *MEDIUMINT_SYMBOL();
    antlr4::tree::TerminalNode *BIGINT_SYMBOL();
    FieldLengthContext *fieldLength();
    FieldOptionsContext *fieldOptions();
    antlr4::tree::TerminalNode *REAL_SYMBOL();
    antlr4::tree::TerminalNode *DOUBLE_SYMBOL();
    PrecisionContext *precision();
    antlr4::tree::TerminalNode *PRECISION_SYMBOL();
    antlr4::tree::TerminalNode *FLOAT_SYMBOL();
    antlr4::tree::TerminalNode *DECIMAL_SYMBOL();
    antlr4::tree::TerminalNode *NUMERIC_SYMBOL();
    antlr4::tree::TerminalNode *FIXED_SYMBOL();
    FloatOptionsContext *floatOptions();
    antlr4::tree::TerminalNode *BIT_SYMBOL();
    antlr4::tree::TerminalNode *BOOL_SYMBOL();
    antlr4::tree::TerminalNode *BOOLEAN_SYMBOL();
    antlr4::tree::TerminalNode *CHAR_SYMBOL();
    CharsetWithOptBinaryContext *charsetWithOptBinary();
    NcharContext *nchar();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    antlr4::tree::TerminalNode *VARYING_SYMBOL();
    antlr4::tree::TerminalNode *VARCHAR_SYMBOL();
    antlr4::tree::TerminalNode *NATIONAL_SYMBOL();
    antlr4::tree::TerminalNode *NVARCHAR_SYMBOL();
    antlr4::tree::TerminalNode *NCHAR_SYMBOL();
    antlr4::tree::TerminalNode *VARBINARY_SYMBOL();
    antlr4::tree::TerminalNode *YEAR_SYMBOL();
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    TypeDatetimePrecisionContext *typeDatetimePrecision();
    antlr4::tree::TerminalNode *TIMESTAMP_SYMBOL();
    antlr4::tree::TerminalNode *DATETIME_SYMBOL();
    antlr4::tree::TerminalNode *TINYBLOB_SYMBOL();
    antlr4::tree::TerminalNode *BLOB_SYMBOL();
    antlr4::tree::TerminalNode *MEDIUMBLOB_SYMBOL();
    antlr4::tree::TerminalNode *LONGBLOB_SYMBOL();
    antlr4::tree::TerminalNode *LONG_SYMBOL();
    antlr4::tree::TerminalNode *TINYTEXT_SYMBOL();
    antlr4::tree::TerminalNode *TEXT_SYMBOL();
    antlr4::tree::TerminalNode *MEDIUMTEXT_SYMBOL();
    antlr4::tree::TerminalNode *LONGTEXT_SYMBOL();
    StringListContext *stringList();
    antlr4::tree::TerminalNode *ENUM_SYMBOL();
    antlr4::tree::TerminalNode *SET_SYMBOL();
    antlr4::tree::TerminalNode *SERIAL_SYMBOL();
    antlr4::tree::TerminalNode *JSON_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRY_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRYCOLLECTION_SYMBOL();
    antlr4::tree::TerminalNode *POINT_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOINT_SYMBOL();
    antlr4::tree::TerminalNode *LINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *MULTILINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *POLYGON_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOLYGON_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DataTypeContext* dataType();

  class  NcharContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    NcharContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NCHAR_SYMBOL();
    antlr4::tree::TerminalNode *CHAR_SYMBOL();
    antlr4::tree::TerminalNode *NATIONAL_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NcharContext* nchar();

  class  RealTypeContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    RealTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REAL_SYMBOL();
    antlr4::tree::TerminalNode *DOUBLE_SYMBOL();
    antlr4::tree::TerminalNode *PRECISION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RealTypeContext* realType();

  class  FieldLengthContext : public antlr4::ParserRuleContext {
  public:
    FieldLengthContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    Real_ulonglong_numberContext *real_ulonglong_number();
    antlr4::tree::TerminalNode *DECIMAL_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldLengthContext* fieldLength();

  class  FieldOptionsContext : public antlr4::ParserRuleContext {
  public:
    FieldOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SIGNED_SYMBOL();
    antlr4::tree::TerminalNode* SIGNED_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> UNSIGNED_SYMBOL();
    antlr4::tree::TerminalNode* UNSIGNED_SYMBOL(size_t i);
    std::vector<antlr4::tree::TerminalNode *> ZEROFILL_SYMBOL();
    antlr4::tree::TerminalNode* ZEROFILL_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldOptionsContext* fieldOptions();

  class  CharsetWithOptBinaryContext : public antlr4::ParserRuleContext {
  public:
    CharsetWithOptBinaryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AsciiContext *ascii();
    UnicodeContext *unicode();
    antlr4::tree::TerminalNode *BYTE_SYMBOL();
    CharsetContext *charset();
    CharsetNameContext *charsetName();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CharsetWithOptBinaryContext* charsetWithOptBinary();

  class  AsciiContext : public antlr4::ParserRuleContext {
  public:
    AsciiContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASCII_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AsciiContext* ascii();

  class  UnicodeContext : public antlr4::ParserRuleContext {
  public:
    UnicodeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNICODE_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UnicodeContext* unicode();

  class  WsNumCodepointsContext : public antlr4::ParserRuleContext {
  public:
    WsNumCodepointsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WsNumCodepointsContext* wsNumCodepoints();

  class  TypeDatetimePrecisionContext : public antlr4::ParserRuleContext {
  public:
    TypeDatetimePrecisionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TypeDatetimePrecisionContext* typeDatetimePrecision();

  class  CharsetNameContext : public antlr4::ParserRuleContext {
  public:
    CharsetNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CharsetNameContext* charsetName();

  class  CollationNameContext : public antlr4::ParserRuleContext {
  public:
    CollationNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *BINARY_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CollationNameContext* collationName();

  class  CreateTableOptionsContext : public antlr4::ParserRuleContext {
  public:
    CreateTableOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<CreateTableOptionContext *> createTableOption();
    CreateTableOptionContext* createTableOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateTableOptionsContext* createTableOptions();

  class  CreateTableOptionsSpaceSeparatedContext : public antlr4::ParserRuleContext {
  public:
    CreateTableOptionsSpaceSeparatedContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<CreateTableOptionContext *> createTableOption();
    CreateTableOptionContext* createTableOption(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateTableOptionsSpaceSeparatedContext* createTableOptionsSpaceSeparated();

  class  CreateTableOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    antlr4::Token *format = nullptr;
    antlr4::Token *method = nullptr;
    CreateTableOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    EngineRefContext *engineRef();
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *SECONDARY_ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *NULL_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    EqualContext *equal();
    Ulonglong_numberContext *ulonglong_number();
    antlr4::tree::TerminalNode *MAX_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *MIN_ROWS_SYMBOL();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *AVG_ROW_LENGTH_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *COMPRESSION_SYMBOL();
    antlr4::tree::TerminalNode *ENCRYPTION_SYMBOL();
    antlr4::tree::TerminalNode *AUTO_INCREMENT_SYMBOL();
    TernaryOptionContext *ternaryOption();
    antlr4::tree::TerminalNode *PACK_KEYS_SYMBOL();
    antlr4::tree::TerminalNode *STATS_AUTO_RECALC_SYMBOL();
    antlr4::tree::TerminalNode *STATS_PERSISTENT_SYMBOL();
    antlr4::tree::TerminalNode *STATS_SAMPLE_PAGES_SYMBOL();
    antlr4::tree::TerminalNode *CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *DELAY_KEY_WRITE_SYMBOL();
    antlr4::tree::TerminalNode *ROW_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *DYNAMIC_SYMBOL();
    antlr4::tree::TerminalNode *FIXED_SYMBOL();
    antlr4::tree::TerminalNode *COMPRESSED_SYMBOL();
    antlr4::tree::TerminalNode *REDUNDANT_SYMBOL();
    antlr4::tree::TerminalNode *COMPACT_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    TableRefListContext *tableRefList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *UNION_SYMBOL();
    DefaultCharsetContext *defaultCharset();
    DefaultCollationContext *defaultCollation();
    antlr4::tree::TerminalNode *INSERT_METHOD_SYMBOL();
    antlr4::tree::TerminalNode *NO_SYMBOL();
    antlr4::tree::TerminalNode *FIRST_SYMBOL();
    antlr4::tree::TerminalNode *LAST_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    antlr4::tree::TerminalNode *DISK_SYMBOL();
    antlr4::tree::TerminalNode *MEMORY_SYMBOL();
    antlr4::tree::TerminalNode *CONNECTION_SYMBOL();
    antlr4::tree::TerminalNode *KEY_BLOCK_SIZE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateTableOptionContext* createTableOption();

  class  TernaryOptionContext : public antlr4::ParserRuleContext {
  public:
    TernaryOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TernaryOptionContext* ternaryOption();

  class  DefaultCollationContext : public antlr4::ParserRuleContext {
  public:
    DefaultCollationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLLATE_SYMBOL();
    CollationNameContext *collationName();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DefaultCollationContext* defaultCollation();

  class  DefaultEncryptionContext : public antlr4::ParserRuleContext {
  public:
    DefaultEncryptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ENCRYPTION_SYMBOL();
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DefaultEncryptionContext* defaultEncryption();

  class  DefaultCharsetContext : public antlr4::ParserRuleContext {
  public:
    DefaultCharsetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CharsetContext *charset();
    CharsetNameContext *charsetName();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DefaultCharsetContext* defaultCharset();

  class  PartitionClauseContext : public antlr4::ParserRuleContext {
  public:
    PartitionClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    PartitionTypeDefContext *partitionTypeDef();
    antlr4::tree::TerminalNode *PARTITIONS_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    SubPartitionsContext *subPartitions();
    PartitionDefinitionsContext *partitionDefinitions();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionClauseContext* partitionClause();

  class  PartitionTypeDefContext : public antlr4::ParserRuleContext {
  public:
    PartitionTypeDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    PartitionTypeDefContext() = default;
    void copyFrom(PartitionTypeDefContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  PartitionDefRangeListContext : public PartitionTypeDefContext {
  public:
    PartitionDefRangeListContext(PartitionTypeDefContext *ctx);

    antlr4::tree::TerminalNode *RANGE_SYMBOL();
    antlr4::tree::TerminalNode *LIST_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *COLUMNS_SYMBOL();
    IdentifierListContext *identifierList();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PartitionDefKeyContext : public PartitionTypeDefContext {
  public:
    PartitionDefKeyContext(PartitionTypeDefContext *ctx);

    antlr4::tree::TerminalNode *KEY_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *LINEAR_SYMBOL();
    PartitionKeyAlgorithmContext *partitionKeyAlgorithm();
    IdentifierListContext *identifierList();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PartitionDefHashContext : public PartitionTypeDefContext {
  public:
    PartitionDefHashContext(PartitionTypeDefContext *ctx);

    antlr4::tree::TerminalNode *HASH_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *LINEAR_SYMBOL();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  PartitionTypeDefContext* partitionTypeDef();

  class  SubPartitionsContext : public antlr4::ParserRuleContext {
  public:
    SubPartitionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SUBPARTITION_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    antlr4::tree::TerminalNode *HASH_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    antlr4::tree::TerminalNode *KEY_SYMBOL();
    IdentifierListWithParenthesesContext *identifierListWithParentheses();
    antlr4::tree::TerminalNode *LINEAR_SYMBOL();
    antlr4::tree::TerminalNode *SUBPARTITIONS_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    PartitionKeyAlgorithmContext *partitionKeyAlgorithm();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SubPartitionsContext* subPartitions();

  class  PartitionKeyAlgorithmContext : public antlr4::ParserRuleContext {
  public:
    PartitionKeyAlgorithmContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALGORITHM_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    Real_ulong_numberContext *real_ulong_number();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionKeyAlgorithmContext* partitionKeyAlgorithm();

  class  PartitionDefinitionsContext : public antlr4::ParserRuleContext {
  public:
    PartitionDefinitionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<PartitionDefinitionContext *> partitionDefinition();
    PartitionDefinitionContext* partitionDefinition(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionDefinitionsContext* partitionDefinitions();

  class  PartitionDefinitionContext : public antlr4::ParserRuleContext {
  public:
    PartitionDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *VALUES_SYMBOL();
    antlr4::tree::TerminalNode *LESS_SYMBOL();
    antlr4::tree::TerminalNode *THAN_SYMBOL();
    antlr4::tree::TerminalNode *IN_SYMBOL();
    PartitionValuesInContext *partitionValuesIn();
    std::vector<PartitionOptionContext *> partitionOption();
    PartitionOptionContext* partitionOption(size_t i);
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<SubpartitionDefinitionContext *> subpartitionDefinition();
    SubpartitionDefinitionContext* subpartitionDefinition(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    PartitionValueItemListParenContext *partitionValueItemListParen();
    antlr4::tree::TerminalNode *MAXVALUE_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionDefinitionContext* partitionDefinition();

  class  PartitionValuesInContext : public antlr4::ParserRuleContext {
  public:
    PartitionValuesInContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PartitionValueItemListParenContext *> partitionValueItemListParen();
    PartitionValueItemListParenContext* partitionValueItemListParen(size_t i);
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionValuesInContext* partitionValuesIn();

  class  PartitionOptionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *option = nullptr;
    PartitionOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    EngineRefContext *engineRef();
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    Real_ulong_numberContext *real_ulong_number();
    antlr4::tree::TerminalNode *NODEGROUP_SYMBOL();
    antlr4::tree::TerminalNode *MAX_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *MIN_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    TextLiteralContext *textLiteral();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *INDEX_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionOptionContext* partitionOption();

  class  SubpartitionDefinitionContext : public antlr4::ParserRuleContext {
  public:
    SubpartitionDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SUBPARTITION_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    std::vector<PartitionOptionContext *> partitionOption();
    PartitionOptionContext* partitionOption(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SubpartitionDefinitionContext* subpartitionDefinition();

  class  PartitionValueItemListParenContext : public antlr4::ParserRuleContext {
  public:
    PartitionValueItemListParenContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<PartitionValueItemContext *> partitionValueItem();
    PartitionValueItemContext* partitionValueItem(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionValueItemListParenContext* partitionValueItemListParen();

  class  PartitionValueItemContext : public antlr4::ParserRuleContext {
  public:
    PartitionValueItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BitExprContext *bitExpr();
    antlr4::tree::TerminalNode *MAXVALUE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PartitionValueItemContext* partitionValueItem();

  class  DefinerClauseContext : public antlr4::ParserRuleContext {
  public:
    DefinerClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DEFINER_SYMBOL();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    UserContext *user();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DefinerClauseContext* definerClause();

  class  IfExistsContext : public antlr4::ParserRuleContext {
  public:
    IfExistsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IF_SYMBOL();
    antlr4::tree::TerminalNode *EXISTS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IfExistsContext* ifExists();

  class  IfNotExistsContext : public antlr4::ParserRuleContext {
  public:
    IfNotExistsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IF_SYMBOL();
    NotRuleContext *notRule();
    antlr4::tree::TerminalNode *EXISTS_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IfNotExistsContext* ifNotExists();

  class  ProcedureParameterContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *type = nullptr;
    ProcedureParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FunctionParameterContext *functionParameter();
    antlr4::tree::TerminalNode *IN_SYMBOL();
    antlr4::tree::TerminalNode *OUT_SYMBOL();
    antlr4::tree::TerminalNode *INOUT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ProcedureParameterContext* procedureParameter();

  class  FunctionParameterContext : public antlr4::ParserRuleContext {
  public:
    FunctionParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ParameterNameContext *parameterName();
    TypeWithOptCollateContext *typeWithOptCollate();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FunctionParameterContext* functionParameter();

  class  CollateContext : public antlr4::ParserRuleContext {
  public:
    CollateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLLATE_SYMBOL();
    CollationNameContext *collationName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CollateContext* collate();

  class  TypeWithOptCollateContext : public antlr4::ParserRuleContext {
  public:
    TypeWithOptCollateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext *dataType();
    CollateContext *collate();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TypeWithOptCollateContext* typeWithOptCollate();

  class  SchemaIdentifierPairContext : public antlr4::ParserRuleContext {
  public:
    SchemaIdentifierPairContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<SchemaRefContext *> schemaRef();
    SchemaRefContext* schemaRef(size_t i);
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SchemaIdentifierPairContext* schemaIdentifierPair();

  class  ViewRefListContext : public antlr4::ParserRuleContext {
  public:
    ViewRefListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ViewRefContext *> viewRef();
    ViewRefContext* viewRef(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewRefListContext* viewRefList();

  class  UpdateListContext : public antlr4::ParserRuleContext {
  public:
    UpdateListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UpdateElementContext *> updateElement();
    UpdateElementContext* updateElement(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UpdateListContext* updateList();

  class  UpdateElementContext : public antlr4::ParserRuleContext {
  public:
    UpdateElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnRefContext *columnRef();
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    ExprContext *expr();
    antlr4::tree::TerminalNode *DEFAULT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UpdateElementContext* updateElement();

  class  CharsetClauseContext : public antlr4::ParserRuleContext {
  public:
    CharsetClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CharsetContext *charset();
    CharsetNameContext *charsetName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CharsetClauseContext* charsetClause();

  class  FieldsClauseContext : public antlr4::ParserRuleContext {
  public:
    FieldsClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLUMNS_SYMBOL();
    std::vector<FieldTermContext *> fieldTerm();
    FieldTermContext* fieldTerm(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldsClauseContext* fieldsClause();

  class  FieldTermContext : public antlr4::ParserRuleContext {
  public:
    FieldTermContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TERMINATED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *ENCLOSED_SYMBOL();
    antlr4::tree::TerminalNode *OPTIONALLY_SYMBOL();
    antlr4::tree::TerminalNode *ESCAPED_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldTermContext* fieldTerm();

  class  LinesClauseContext : public antlr4::ParserRuleContext {
  public:
    LinesClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LINES_SYMBOL();
    std::vector<LineTermContext *> lineTerm();
    LineTermContext* lineTerm(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LinesClauseContext* linesClause();

  class  LineTermContext : public antlr4::ParserRuleContext {
  public:
    LineTermContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BY_SYMBOL();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *TERMINATED_SYMBOL();
    antlr4::tree::TerminalNode *STARTING_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LineTermContext* lineTerm();

  class  UserListContext : public antlr4::ParserRuleContext {
  public:
    UserListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UserContext *> user();
    UserContext* user(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UserListContext* userList();

  class  CreateUserListContext : public antlr4::ParserRuleContext {
  public:
    CreateUserListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<CreateUserEntryContext *> createUserEntry();
    CreateUserEntryContext* createUserEntry(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateUserListContext* createUserList();

  class  AlterUserListContext : public antlr4::ParserRuleContext {
  public:
    AlterUserListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AlterUserEntryContext *> alterUserEntry();
    AlterUserEntryContext* alterUserEntry(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterUserListContext* alterUserList();

  class  CreateUserEntryContext : public antlr4::ParserRuleContext {
  public:
    CreateUserEntryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UserContext *user();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    TextStringContext *textString();
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    antlr4::tree::TerminalNode *RANDOM_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    TextStringHashContext *textStringHash();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateUserEntryContext* createUserEntry();

  class  AlterUserEntryContext : public antlr4::ParserRuleContext {
  public:
    AlterUserEntryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UserContext *user();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *BY_SYMBOL();
    std::vector<TextStringContext *> textString();
    TextStringContext* textString(size_t i);
    antlr4::tree::TerminalNode *WITH_SYMBOL();
    TextOrIdentifierContext *textOrIdentifier();
    DiscardOldPasswordContext *discardOldPassword();
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    RetainCurrentPasswordContext *retainCurrentPassword();
    antlr4::tree::TerminalNode *AS_SYMBOL();
    TextStringHashContext *textStringHash();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AlterUserEntryContext* alterUserEntry();

  class  RetainCurrentPasswordContext : public antlr4::ParserRuleContext {
  public:
    RetainCurrentPasswordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RETAIN_SYMBOL();
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RetainCurrentPasswordContext* retainCurrentPassword();

  class  DiscardOldPasswordContext : public antlr4::ParserRuleContext {
  public:
    DiscardOldPasswordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DISCARD_SYMBOL();
    antlr4::tree::TerminalNode *OLD_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DiscardOldPasswordContext* discardOldPassword();

  class  ReplacePasswordContext : public antlr4::ParserRuleContext {
  public:
    ReplacePasswordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REPLACE_SYMBOL();
    TextStringContext *textString();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ReplacePasswordContext* replacePassword();

  class  UserIdentifierOrTextContext : public antlr4::ParserRuleContext {
  public:
    UserIdentifierOrTextContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TextOrIdentifierContext *> textOrIdentifier();
    TextOrIdentifierContext* textOrIdentifier(size_t i);
    antlr4::tree::TerminalNode *AT_SIGN_SYMBOL();
    antlr4::tree::TerminalNode *AT_TEXT_SUFFIX();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UserIdentifierOrTextContext* userIdentifierOrText();

  class  UserContext : public antlr4::ParserRuleContext {
  public:
    UserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UserIdentifierOrTextContext *userIdentifierOrText();
    antlr4::tree::TerminalNode *CURRENT_USER_SYMBOL();
    ParenthesesContext *parentheses();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UserContext* user();

  class  LikeClauseContext : public antlr4::ParserRuleContext {
  public:
    LikeClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIKE_SYMBOL();
    TextStringLiteralContext *textStringLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LikeClauseContext* likeClause();

  class  LikeOrWhereContext : public antlr4::ParserRuleContext {
  public:
    LikeOrWhereContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LikeClauseContext *likeClause();
    WhereClauseContext *whereClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LikeOrWhereContext* likeOrWhere();

  class  OnlineOptionContext : public antlr4::ParserRuleContext {
  public:
    OnlineOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ONLINE_SYMBOL();
    antlr4::tree::TerminalNode *OFFLINE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OnlineOptionContext* onlineOption();

  class  NoWriteToBinLogContext : public antlr4::ParserRuleContext {
  public:
    NoWriteToBinLogContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *NO_WRITE_TO_BINLOG_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NoWriteToBinLogContext* noWriteToBinLog();

  class  UsePartitionContext : public antlr4::ParserRuleContext {
  public:
    UsePartitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    IdentifierListWithParenthesesContext *identifierListWithParentheses();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UsePartitionContext* usePartition();

  class  FieldIdentifierContext : public antlr4::ParserRuleContext {
  public:
    FieldIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DotIdentifierContext *dotIdentifier();
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FieldIdentifierContext* fieldIdentifier();

  class  ColumnNameContext : public antlr4::ParserRuleContext {
  public:
    ColumnNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    FieldIdentifierContext *fieldIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnNameContext* columnName();

  class  ColumnInternalRefContext : public antlr4::ParserRuleContext {
  public:
    ColumnInternalRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnInternalRefContext* columnInternalRef();

  class  ColumnInternalRefListContext : public antlr4::ParserRuleContext {
  public:
    ColumnInternalRefListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<ColumnInternalRefContext *> columnInternalRef();
    ColumnInternalRefContext* columnInternalRef(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnInternalRefListContext* columnInternalRefList();

  class  ColumnRefContext : public antlr4::ParserRuleContext {
  public:
    ColumnRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FieldIdentifierContext *fieldIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnRefContext* columnRef();

  class  InsertIdentifierContext : public antlr4::ParserRuleContext {
  public:
    InsertIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColumnRefContext *columnRef();
    TableWildContext *tableWild();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InsertIdentifierContext* insertIdentifier();

  class  IndexNameContext : public antlr4::ParserRuleContext {
  public:
    IndexNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexNameContext* indexName();

  class  IndexRefContext : public antlr4::ParserRuleContext {
  public:
    IndexRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FieldIdentifierContext *fieldIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IndexRefContext* indexRef();

  class  TableWildContext : public antlr4::ParserRuleContext {
  public:
    TableWildContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode *> DOT_SYMBOL();
    antlr4::tree::TerminalNode* DOT_SYMBOL(size_t i);
    antlr4::tree::TerminalNode *MULT_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableWildContext* tableWild();

  class  SchemaNameContext : public antlr4::ParserRuleContext {
  public:
    SchemaNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SchemaNameContext* schemaName();

  class  SchemaRefContext : public antlr4::ParserRuleContext {
  public:
    SchemaRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SchemaRefContext* schemaRef();

  class  ProcedureNameContext : public antlr4::ParserRuleContext {
  public:
    ProcedureNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ProcedureNameContext* procedureName();

  class  ProcedureRefContext : public antlr4::ParserRuleContext {
  public:
    ProcedureRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ProcedureRefContext* procedureRef();

  class  FunctionNameContext : public antlr4::ParserRuleContext {
  public:
    FunctionNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FunctionNameContext* functionName();

  class  FunctionRefContext : public antlr4::ParserRuleContext {
  public:
    FunctionRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FunctionRefContext* functionRef();

  class  TriggerNameContext : public antlr4::ParserRuleContext {
  public:
    TriggerNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TriggerNameContext* triggerName();

  class  TriggerRefContext : public antlr4::ParserRuleContext {
  public:
    TriggerRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TriggerRefContext* triggerRef();

  class  ViewNameContext : public antlr4::ParserRuleContext {
  public:
    ViewNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewNameContext* viewName();

  class  ViewRefContext : public antlr4::ParserRuleContext {
  public:
    ViewRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ViewRefContext* viewRef();

  class  TablespaceNameContext : public antlr4::ParserRuleContext {
  public:
    TablespaceNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TablespaceNameContext* tablespaceName();

  class  TablespaceRefContext : public antlr4::ParserRuleContext {
  public:
    TablespaceRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TablespaceRefContext* tablespaceRef();

  class  LogfileGroupNameContext : public antlr4::ParserRuleContext {
  public:
    LogfileGroupNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LogfileGroupNameContext* logfileGroupName();

  class  LogfileGroupRefContext : public antlr4::ParserRuleContext {
  public:
    LogfileGroupRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LogfileGroupRefContext* logfileGroupRef();

  class  EventNameContext : public antlr4::ParserRuleContext {
  public:
    EventNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  EventNameContext* eventName();

  class  EventRefContext : public antlr4::ParserRuleContext {
  public:
    EventRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  EventRefContext* eventRef();

  class  UdfNameContext : public antlr4::ParserRuleContext {
  public:
    UdfNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UdfNameContext* udfName();

  class  ServerNameContext : public antlr4::ParserRuleContext {
  public:
    ServerNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextOrIdentifierContext *textOrIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ServerNameContext* serverName();

  class  ServerRefContext : public antlr4::ParserRuleContext {
  public:
    ServerRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextOrIdentifierContext *textOrIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ServerRefContext* serverRef();

  class  EngineRefContext : public antlr4::ParserRuleContext {
  public:
    EngineRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextOrIdentifierContext *textOrIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  EngineRefContext* engineRef();

  class  TableNameContext : public antlr4::ParserRuleContext {
  public:
    TableNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableNameContext* tableName();

  class  FilterTableRefContext : public antlr4::ParserRuleContext {
  public:
    FilterTableRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SchemaRefContext *schemaRef();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FilterTableRefContext* filterTableRef();

  class  TableRefWithWildcardContext : public antlr4::ParserRuleContext {
  public:
    TableRefWithWildcardContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *DOT_SYMBOL();
    antlr4::tree::TerminalNode *MULT_OPERATOR();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableRefWithWildcardContext* tableRefWithWildcard();

  class  TableRefContext : public antlr4::ParserRuleContext {
  public:
    TableRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedIdentifierContext *qualifiedIdentifier();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableRefContext* tableRef();

  class  TableRefListContext : public antlr4::ParserRuleContext {
  public:
    TableRefListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TableRefContext *> tableRef();
    TableRefContext* tableRef(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableRefListContext* tableRefList();

  class  TableAliasRefListContext : public antlr4::ParserRuleContext {
  public:
    TableAliasRefListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TableRefWithWildcardContext *> tableRefWithWildcard();
    TableRefWithWildcardContext* tableRefWithWildcard(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableAliasRefListContext* tableAliasRefList();

  class  ParameterNameContext : public antlr4::ParserRuleContext {
  public:
    ParameterNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ParameterNameContext* parameterName();

  class  LabelIdentifierContext : public antlr4::ParserRuleContext {
  public:
    LabelIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PureIdentifierContext *pureIdentifier();
    LabelKeywordContext *labelKeyword();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LabelIdentifierContext* labelIdentifier();

  class  LabelRefContext : public antlr4::ParserRuleContext {
  public:
    LabelRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelIdentifierContext *labelIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LabelRefContext* labelRef();

  class  RoleIdentifierContext : public antlr4::ParserRuleContext {
  public:
    RoleIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PureIdentifierContext *pureIdentifier();
    RoleKeywordContext *roleKeyword();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleIdentifierContext* roleIdentifier();

  class  RoleRefContext : public antlr4::ParserRuleContext {
  public:
    RoleRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoleIdentifierContext *roleIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleRefContext* roleRef();

  class  PluginRefContext : public antlr4::ParserRuleContext {
  public:
    PluginRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PluginRefContext* pluginRef();

  class  ComponentRefContext : public antlr4::ParserRuleContext {
  public:
    ComponentRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringLiteralContext *textStringLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ComponentRefContext* componentRef();

  class  ResourceGroupRefContext : public antlr4::ParserRuleContext {
  public:
    ResourceGroupRefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ResourceGroupRefContext* resourceGroupRef();

  class  WindowNameContext : public antlr4::ParserRuleContext {
  public:
    WindowNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowNameContext* windowName();

  class  PureIdentifierContext : public antlr4::ParserRuleContext {
  public:
    PureIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();
    antlr4::tree::TerminalNode *BACK_TICK_QUOTED_ID();
    antlr4::tree::TerminalNode *DOUBLE_QUOTED_TEXT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PureIdentifierContext* pureIdentifier();

  class  IdentifierContext : public antlr4::ParserRuleContext {
  public:
    IdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PureIdentifierContext *pureIdentifier();
    IdentifierKeywordContext *identifierKeyword();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierContext* identifier();

  class  IdentifierListContext : public antlr4::ParserRuleContext {
  public:
    IdentifierListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierListContext* identifierList();

  class  IdentifierListWithParenthesesContext : public antlr4::ParserRuleContext {
  public:
    IdentifierListWithParenthesesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierListWithParenthesesContext* identifierListWithParentheses();

  class  QualifiedIdentifierContext : public antlr4::ParserRuleContext {
  public:
    QualifiedIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    DotIdentifierContext *dotIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QualifiedIdentifierContext* qualifiedIdentifier();

  class  SimpleIdentifierContext : public antlr4::ParserRuleContext {
  public:
    SimpleIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    std::vector<DotIdentifierContext *> dotIdentifier();
    DotIdentifierContext* dotIdentifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SimpleIdentifierContext* simpleIdentifier();

  class  DotIdentifierContext : public antlr4::ParserRuleContext {
  public:
    DotIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DOT_SYMBOL();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DotIdentifierContext* dotIdentifier();

  class  Ulong_numberContext : public antlr4::ParserRuleContext {
  public:
    Ulong_numberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *HEX_NUMBER();
    antlr4::tree::TerminalNode *LONG_NUMBER();
    antlr4::tree::TerminalNode *ULONGLONG_NUMBER();
    antlr4::tree::TerminalNode *DECIMAL_NUMBER();
    antlr4::tree::TerminalNode *FLOAT_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  Ulong_numberContext* ulong_number();

  class  Real_ulong_numberContext : public antlr4::ParserRuleContext {
  public:
    Real_ulong_numberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *HEX_NUMBER();
    antlr4::tree::TerminalNode *LONG_NUMBER();
    antlr4::tree::TerminalNode *ULONGLONG_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  Real_ulong_numberContext* real_ulong_number();

  class  Ulonglong_numberContext : public antlr4::ParserRuleContext {
  public:
    Ulonglong_numberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *LONG_NUMBER();
    antlr4::tree::TerminalNode *ULONGLONG_NUMBER();
    antlr4::tree::TerminalNode *DECIMAL_NUMBER();
    antlr4::tree::TerminalNode *FLOAT_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  Ulonglong_numberContext* ulonglong_number();

  class  Real_ulonglong_numberContext : public antlr4::ParserRuleContext {
  public:
    Real_ulonglong_numberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *HEX_NUMBER();
    antlr4::tree::TerminalNode *ULONGLONG_NUMBER();
    antlr4::tree::TerminalNode *LONG_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  Real_ulonglong_numberContext* real_ulonglong_number();

  class  LiteralContext : public antlr4::ParserRuleContext {
  public:
    LiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextLiteralContext *textLiteral();
    NumLiteralContext *numLiteral();
    TemporalLiteralContext *temporalLiteral();
    NullLiteralContext *nullLiteral();
    BoolLiteralContext *boolLiteral();
    antlr4::tree::TerminalNode *HEX_NUMBER();
    antlr4::tree::TerminalNode *BIN_NUMBER();
    antlr4::tree::TerminalNode *UNDERSCORE_CHARSET();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LiteralContext* literal();

  class  SignedLiteralContext : public antlr4::ParserRuleContext {
  public:
    SignedLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();
    antlr4::tree::TerminalNode *PLUS_OPERATOR();
    Ulong_numberContext *ulong_number();
    antlr4::tree::TerminalNode *MINUS_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SignedLiteralContext* signedLiteral();

  class  StringListContext : public antlr4::ParserRuleContext {
  public:
    StringListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<TextStringContext *> textString();
    TextStringContext* textString(size_t i);
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StringListContext* stringList();

  class  TextStringLiteralContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *value = nullptr;
    TextStringLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SINGLE_QUOTED_TEXT();
    antlr4::tree::TerminalNode *DOUBLE_QUOTED_TEXT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextStringLiteralContext* textStringLiteral();

  class  TextStringContext : public antlr4::ParserRuleContext {
  public:
    TextStringContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *HEX_NUMBER();
    antlr4::tree::TerminalNode *BIN_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextStringContext* textString();

  class  TextStringHashContext : public antlr4::ParserRuleContext {
  public:
    TextStringHashContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringLiteralContext *textStringLiteral();
    antlr4::tree::TerminalNode *HEX_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextStringHashContext* textStringHash();

  class  TextLiteralContext : public antlr4::ParserRuleContext {
  public:
    TextLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TextStringLiteralContext *> textStringLiteral();
    TextStringLiteralContext* textStringLiteral(size_t i);
    antlr4::tree::TerminalNode *NCHAR_TEXT();
    antlr4::tree::TerminalNode *UNDERSCORE_CHARSET();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextLiteralContext* textLiteral();

  class  TextStringNoLinebreakContext : public antlr4::ParserRuleContext {
  public:
    TextStringNoLinebreakContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TextStringLiteralContext *textStringLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextStringNoLinebreakContext* textStringNoLinebreak();

  class  TextStringLiteralListContext : public antlr4::ParserRuleContext {
  public:
    TextStringLiteralListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TextStringLiteralContext *> textStringLiteral();
    TextStringLiteralContext* textStringLiteral(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA_SYMBOL();
    antlr4::tree::TerminalNode* COMMA_SYMBOL(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextStringLiteralListContext* textStringLiteralList();

  class  NumLiteralContext : public antlr4::ParserRuleContext {
  public:
    NumLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT_NUMBER();
    antlr4::tree::TerminalNode *LONG_NUMBER();
    antlr4::tree::TerminalNode *ULONGLONG_NUMBER();
    antlr4::tree::TerminalNode *DECIMAL_NUMBER();
    antlr4::tree::TerminalNode *FLOAT_NUMBER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NumLiteralContext* numLiteral();

  class  BoolLiteralContext : public antlr4::ParserRuleContext {
  public:
    BoolLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRUE_SYMBOL();
    antlr4::tree::TerminalNode *FALSE_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  BoolLiteralContext* boolLiteral();

  class  NullLiteralContext : public antlr4::ParserRuleContext {
  public:
    NullLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NULL_SYMBOL();
    antlr4::tree::TerminalNode *NULL2_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NullLiteralContext* nullLiteral();

  class  TemporalLiteralContext : public antlr4::ParserRuleContext {
  public:
    TemporalLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *SINGLE_QUOTED_TEXT();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TemporalLiteralContext* temporalLiteral();

  class  FloatOptionsContext : public antlr4::ParserRuleContext {
  public:
    FloatOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FieldLengthContext *fieldLength();
    PrecisionContext *precision();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FloatOptionsContext* floatOptions();

  class  StandardFloatOptionsContext : public antlr4::ParserRuleContext {
  public:
    StandardFloatOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PrecisionContext *precision();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StandardFloatOptionsContext* standardFloatOptions();

  class  PrecisionContext : public antlr4::ParserRuleContext {
  public:
    PrecisionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    std::vector<antlr4::tree::TerminalNode *> INT_NUMBER();
    antlr4::tree::TerminalNode* INT_NUMBER(size_t i);
    antlr4::tree::TerminalNode *COMMA_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PrecisionContext* precision();

  class  TextOrIdentifierContext : public antlr4::ParserRuleContext {
  public:
    TextOrIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SINGLE_QUOTED_TEXT();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TextOrIdentifierContext* textOrIdentifier();

  class  LValueIdentifierContext : public antlr4::ParserRuleContext {
  public:
    LValueIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PureIdentifierContext *pureIdentifier();
    LValueKeywordContext *lValueKeyword();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LValueIdentifierContext* lValueIdentifier();

  class  RoleIdentifierOrTextContext : public antlr4::ParserRuleContext {
  public:
    RoleIdentifierOrTextContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoleIdentifierContext *roleIdentifier();
    TextStringLiteralContext *textStringLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleIdentifierOrTextContext* roleIdentifierOrText();

  class  SizeNumberContext : public antlr4::ParserRuleContext {
  public:
    SizeNumberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Real_ulonglong_numberContext *real_ulonglong_number();
    PureIdentifierContext *pureIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SizeNumberContext* sizeNumber();

  class  ParenthesesContext : public antlr4::ParserRuleContext {
  public:
    ParenthesesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPEN_PAR_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_PAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ParenthesesContext* parentheses();

  class  EqualContext : public antlr4::ParserRuleContext {
  public:
    EqualContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQUAL_OPERATOR();
    antlr4::tree::TerminalNode *ASSIGN_OPERATOR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  EqualContext* equal();

  class  OptionTypeContext : public antlr4::ParserRuleContext {
  public:
    OptionTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PERSIST_SYMBOL();
    antlr4::tree::TerminalNode *PERSIST_ONLY_SYMBOL();
    antlr4::tree::TerminalNode *GLOBAL_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *SESSION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OptionTypeContext* optionType();

  class  VarIdentTypeContext : public antlr4::ParserRuleContext {
  public:
    VarIdentTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GLOBAL_SYMBOL();
    antlr4::tree::TerminalNode *DOT_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *SESSION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  VarIdentTypeContext* varIdentType();

  class  SetVarIdentTypeContext : public antlr4::ParserRuleContext {
  public:
    SetVarIdentTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PERSIST_SYMBOL();
    antlr4::tree::TerminalNode *DOT_SYMBOL();
    antlr4::tree::TerminalNode *PERSIST_ONLY_SYMBOL();
    antlr4::tree::TerminalNode *GLOBAL_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *SESSION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SetVarIdentTypeContext* setVarIdentType();

  class  IdentifierKeywordContext : public antlr4::ParserRuleContext {
  public:
    IdentifierKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelKeywordContext *labelKeyword();
    RoleOrIdentifierKeywordContext *roleOrIdentifierKeyword();
    antlr4::tree::TerminalNode *EXECUTE_SYMBOL();
    antlr4::tree::TerminalNode *SHUTDOWN_SYMBOL();
    antlr4::tree::TerminalNode *RESTART_SYMBOL();
    IdentifierKeywordsUnambiguousContext *identifierKeywordsUnambiguous();
    IdentifierKeywordsAmbiguous1RolesAndLabelsContext *identifierKeywordsAmbiguous1RolesAndLabels();
    IdentifierKeywordsAmbiguous2LabelsContext *identifierKeywordsAmbiguous2Labels();
    IdentifierKeywordsAmbiguous3RolesContext *identifierKeywordsAmbiguous3Roles();
    IdentifierKeywordsAmbiguous4SystemVariablesContext *identifierKeywordsAmbiguous4SystemVariables();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierKeywordContext* identifierKeyword();

  class  IdentifierKeywordsAmbiguous1RolesAndLabelsContext : public antlr4::ParserRuleContext {
  public:
    IdentifierKeywordsAmbiguous1RolesAndLabelsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXECUTE_SYMBOL();
    antlr4::tree::TerminalNode *RESTART_SYMBOL();
    antlr4::tree::TerminalNode *SHUTDOWN_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierKeywordsAmbiguous1RolesAndLabelsContext* identifierKeywordsAmbiguous1RolesAndLabels();

  class  IdentifierKeywordsAmbiguous2LabelsContext : public antlr4::ParserRuleContext {
  public:
    IdentifierKeywordsAmbiguous2LabelsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASCII_SYMBOL();
    antlr4::tree::TerminalNode *BEGIN_SYMBOL();
    antlr4::tree::TerminalNode *BYTE_SYMBOL();
    antlr4::tree::TerminalNode *CACHE_SYMBOL();
    antlr4::tree::TerminalNode *CHARSET_SYMBOL();
    antlr4::tree::TerminalNode *CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *CLONE_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    antlr4::tree::TerminalNode *COMMIT_SYMBOL();
    antlr4::tree::TerminalNode *CONTAINS_SYMBOL();
    antlr4::tree::TerminalNode *DEALLOCATE_SYMBOL();
    antlr4::tree::TerminalNode *DO_SYMBOL();
    antlr4::tree::TerminalNode *END_SYMBOL();
    antlr4::tree::TerminalNode *FLUSH_SYMBOL();
    antlr4::tree::TerminalNode *FOLLOWS_SYMBOL();
    antlr4::tree::TerminalNode *HANDLER_SYMBOL();
    antlr4::tree::TerminalNode *HELP_SYMBOL();
    antlr4::tree::TerminalNode *IMPORT_SYMBOL();
    antlr4::tree::TerminalNode *INSTALL_SYMBOL();
    antlr4::tree::TerminalNode *LANGUAGE_SYMBOL();
    antlr4::tree::TerminalNode *NO_SYMBOL();
    antlr4::tree::TerminalNode *PRECEDES_SYMBOL();
    antlr4::tree::TerminalNode *PREPARE_SYMBOL();
    antlr4::tree::TerminalNode *REPAIR_SYMBOL();
    antlr4::tree::TerminalNode *RESET_SYMBOL();
    antlr4::tree::TerminalNode *ROLLBACK_SYMBOL();
    antlr4::tree::TerminalNode *SAVEPOINT_SYMBOL();
    antlr4::tree::TerminalNode *SIGNED_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();
    antlr4::tree::TerminalNode *START_SYMBOL();
    antlr4::tree::TerminalNode *STOP_SYMBOL();
    antlr4::tree::TerminalNode *TRUNCATE_SYMBOL();
    antlr4::tree::TerminalNode *UNICODE_SYMBOL();
    antlr4::tree::TerminalNode *UNINSTALL_SYMBOL();
    antlr4::tree::TerminalNode *XA_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierKeywordsAmbiguous2LabelsContext* identifierKeywordsAmbiguous2Labels();

  class  LabelKeywordContext : public antlr4::ParserRuleContext {
  public:
    LabelKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoleOrLabelKeywordContext *roleOrLabelKeyword();
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    antlr4::tree::TerminalNode *FILE_SYMBOL();
    antlr4::tree::TerminalNode *NONE_SYMBOL();
    antlr4::tree::TerminalNode *PROCESS_SYMBOL();
    antlr4::tree::TerminalNode *PROXY_SYMBOL();
    antlr4::tree::TerminalNode *RELOAD_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *RESOURCE_SYMBOL();
    antlr4::tree::TerminalNode *SUPER_SYMBOL();
    IdentifierKeywordsUnambiguousContext *identifierKeywordsUnambiguous();
    IdentifierKeywordsAmbiguous3RolesContext *identifierKeywordsAmbiguous3Roles();
    IdentifierKeywordsAmbiguous4SystemVariablesContext *identifierKeywordsAmbiguous4SystemVariables();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LabelKeywordContext* labelKeyword();

  class  IdentifierKeywordsAmbiguous3RolesContext : public antlr4::ParserRuleContext {
  public:
    IdentifierKeywordsAmbiguous3RolesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EVENT_SYMBOL();
    antlr4::tree::TerminalNode *FILE_SYMBOL();
    antlr4::tree::TerminalNode *NONE_SYMBOL();
    antlr4::tree::TerminalNode *PROCESS_SYMBOL();
    antlr4::tree::TerminalNode *PROXY_SYMBOL();
    antlr4::tree::TerminalNode *RELOAD_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *RESOURCE_SYMBOL();
    antlr4::tree::TerminalNode *SUPER_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierKeywordsAmbiguous3RolesContext* identifierKeywordsAmbiguous3Roles();

  class  IdentifierKeywordsUnambiguousContext : public antlr4::ParserRuleContext {
  public:
    IdentifierKeywordsUnambiguousContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ACTION_SYMBOL();
    antlr4::tree::TerminalNode *ACCOUNT_SYMBOL();
    antlr4::tree::TerminalNode *ACTIVE_SYMBOL();
    antlr4::tree::TerminalNode *ADDDATE_SYMBOL();
    antlr4::tree::TerminalNode *ADMIN_SYMBOL();
    antlr4::tree::TerminalNode *AFTER_SYMBOL();
    antlr4::tree::TerminalNode *AGAINST_SYMBOL();
    antlr4::tree::TerminalNode *AGGREGATE_SYMBOL();
    antlr4::tree::TerminalNode *ALGORITHM_SYMBOL();
    antlr4::tree::TerminalNode *ALWAYS_SYMBOL();
    antlr4::tree::TerminalNode *ANY_SYMBOL();
    antlr4::tree::TerminalNode *AT_SYMBOL();
    antlr4::tree::TerminalNode *AUTOEXTEND_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *AUTO_INCREMENT_SYMBOL();
    antlr4::tree::TerminalNode *AVG_ROW_LENGTH_SYMBOL();
    antlr4::tree::TerminalNode *AVG_SYMBOL();
    antlr4::tree::TerminalNode *BACKUP_SYMBOL();
    antlr4::tree::TerminalNode *BINLOG_SYMBOL();
    antlr4::tree::TerminalNode *BIT_SYMBOL();
    antlr4::tree::TerminalNode *BLOCK_SYMBOL();
    antlr4::tree::TerminalNode *BOOLEAN_SYMBOL();
    antlr4::tree::TerminalNode *BOOL_SYMBOL();
    antlr4::tree::TerminalNode *BTREE_SYMBOL();
    antlr4::tree::TerminalNode *BUCKETS_SYMBOL();
    antlr4::tree::TerminalNode *CASCADED_SYMBOL();
    antlr4::tree::TerminalNode *CATALOG_NAME_SYMBOL();
    antlr4::tree::TerminalNode *CHAIN_SYMBOL();
    antlr4::tree::TerminalNode *CHANGED_SYMBOL();
    antlr4::tree::TerminalNode *CHANNEL_SYMBOL();
    antlr4::tree::TerminalNode *CIPHER_SYMBOL();
    antlr4::tree::TerminalNode *CLASS_ORIGIN_SYMBOL();
    antlr4::tree::TerminalNode *CLIENT_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_SYMBOL();
    antlr4::tree::TerminalNode *COALESCE_SYMBOL();
    antlr4::tree::TerminalNode *CODE_SYMBOL();
    antlr4::tree::TerminalNode *COLLATION_SYMBOL();
    antlr4::tree::TerminalNode *COLUMNS_SYMBOL();
    antlr4::tree::TerminalNode *COLUMN_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *COLUMN_NAME_SYMBOL();
    antlr4::tree::TerminalNode *COMMITTED_SYMBOL();
    antlr4::tree::TerminalNode *COMPACT_SYMBOL();
    antlr4::tree::TerminalNode *COMPLETION_SYMBOL();
    antlr4::tree::TerminalNode *COMPONENT_SYMBOL();
    antlr4::tree::TerminalNode *COMPRESSED_SYMBOL();
    antlr4::tree::TerminalNode *COMPRESSION_SYMBOL();
    antlr4::tree::TerminalNode *CONCURRENT_SYMBOL();
    antlr4::tree::TerminalNode *CONNECTION_SYMBOL();
    antlr4::tree::TerminalNode *CONSISTENT_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_CATALOG_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_NAME_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_SCHEMA_SYMBOL();
    antlr4::tree::TerminalNode *CONTEXT_SYMBOL();
    antlr4::tree::TerminalNode *CPU_SYMBOL();
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    antlr4::tree::TerminalNode *CURSOR_NAME_SYMBOL();
    antlr4::tree::TerminalNode *DATAFILE_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *DATETIME_SYMBOL();
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *DAY_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_AUTH_SYMBOL();
    antlr4::tree::TerminalNode *DEFINER_SYMBOL();
    antlr4::tree::TerminalNode *DEFINITION_SYMBOL();
    antlr4::tree::TerminalNode *DELAY_KEY_WRITE_SYMBOL();
    antlr4::tree::TerminalNode *DESCRIPTION_SYMBOL();
    antlr4::tree::TerminalNode *DIAGNOSTICS_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    antlr4::tree::TerminalNode *DISABLE_SYMBOL();
    antlr4::tree::TerminalNode *DISCARD_SYMBOL();
    antlr4::tree::TerminalNode *DISK_SYMBOL();
    antlr4::tree::TerminalNode *DUMPFILE_SYMBOL();
    antlr4::tree::TerminalNode *DUPLICATE_SYMBOL();
    antlr4::tree::TerminalNode *DYNAMIC_SYMBOL();
    antlr4::tree::TerminalNode *ENABLE_SYMBOL();
    antlr4::tree::TerminalNode *ENCRYPTION_SYMBOL();
    antlr4::tree::TerminalNode *ENDS_SYMBOL();
    antlr4::tree::TerminalNode *ENFORCED_SYMBOL();
    antlr4::tree::TerminalNode *ENGINES_SYMBOL();
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *ENUM_SYMBOL();
    antlr4::tree::TerminalNode *ERRORS_SYMBOL();
    antlr4::tree::TerminalNode *ERROR_SYMBOL();
    antlr4::tree::TerminalNode *ESCAPE_SYMBOL();
    antlr4::tree::TerminalNode *EVENTS_SYMBOL();
    antlr4::tree::TerminalNode *EVERY_SYMBOL();
    antlr4::tree::TerminalNode *EXCHANGE_SYMBOL();
    antlr4::tree::TerminalNode *EXCLUDE_SYMBOL();
    antlr4::tree::TerminalNode *EXPANSION_SYMBOL();
    antlr4::tree::TerminalNode *EXPIRE_SYMBOL();
    antlr4::tree::TerminalNode *EXPORT_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    antlr4::tree::TerminalNode *EXTENT_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *FAST_SYMBOL();
    antlr4::tree::TerminalNode *FAULTS_SYMBOL();
    antlr4::tree::TerminalNode *FILE_BLOCK_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *FILTER_SYMBOL();
    antlr4::tree::TerminalNode *FIRST_SYMBOL();
    antlr4::tree::TerminalNode *FIXED_SYMBOL();
    antlr4::tree::TerminalNode *FOLLOWING_SYMBOL();
    antlr4::tree::TerminalNode *FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *FOUND_SYMBOL();
    antlr4::tree::TerminalNode *FULL_SYMBOL();
    antlr4::tree::TerminalNode *GENERAL_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRYCOLLECTION_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRY_SYMBOL();
    antlr4::tree::TerminalNode *GET_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *GET_MASTER_PUBLIC_KEY_SYMBOL();
    antlr4::tree::TerminalNode *GRANTS_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *HASH_SYMBOL();
    antlr4::tree::TerminalNode *HISTOGRAM_SYMBOL();
    antlr4::tree::TerminalNode *HISTORY_SYMBOL();
    antlr4::tree::TerminalNode *HOSTS_SYMBOL();
    antlr4::tree::TerminalNode *HOST_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_SYMBOL();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SERVER_IDS_SYMBOL();
    antlr4::tree::TerminalNode *INACTIVE_SYMBOL();
    antlr4::tree::TerminalNode *INDEXES_SYMBOL();
    antlr4::tree::TerminalNode *INITIAL_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *INSERT_METHOD_SYMBOL();
    antlr4::tree::TerminalNode *INSTANCE_SYMBOL();
    antlr4::tree::TerminalNode *INVISIBLE_SYMBOL();
    antlr4::tree::TerminalNode *INVOKER_SYMBOL();
    antlr4::tree::TerminalNode *IO_SYMBOL();
    antlr4::tree::TerminalNode *IPC_SYMBOL();
    antlr4::tree::TerminalNode *ISOLATION_SYMBOL();
    antlr4::tree::TerminalNode *ISSUER_SYMBOL();
    antlr4::tree::TerminalNode *JSON_SYMBOL();
    antlr4::tree::TerminalNode *KEY_BLOCK_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *LAST_SYMBOL();
    antlr4::tree::TerminalNode *LEAVES_SYMBOL();
    antlr4::tree::TerminalNode *LESS_SYMBOL();
    antlr4::tree::TerminalNode *LEVEL_SYMBOL();
    antlr4::tree::TerminalNode *LINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *LIST_SYMBOL();
    antlr4::tree::TerminalNode *LOCKED_SYMBOL();
    antlr4::tree::TerminalNode *LOCKS_SYMBOL();
    antlr4::tree::TerminalNode *LOGFILE_SYMBOL();
    antlr4::tree::TerminalNode *LOGS_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_AUTO_POSITION_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_COMPRESSION_ALGORITHM_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_CONNECT_RETRY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_DELAY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_HEARTBEAT_PERIOD_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_HOST_SYMBOL();
    antlr4::tree::TerminalNode *NETWORK_NAMESPACE_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_LOG_FILE_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_LOG_POS_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PORT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PUBLIC_KEY_PATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_RETRY_COUNT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SERVER_ID_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CAPATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CA_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CERT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CIPHER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CRLPATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CRL_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_KEY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_TLS_CIPHERSUITES_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_TLS_VERSION_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_USER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_ZSTD_COMPRESSION_LEVEL_SYMBOL();
    antlr4::tree::TerminalNode *MAX_CONNECTIONS_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_QUERIES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *MAX_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *MAX_UPDATES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_USER_CONNECTIONS_SYMBOL();
    antlr4::tree::TerminalNode *MEDIUM_SYMBOL();
    antlr4::tree::TerminalNode *MEMORY_SYMBOL();
    antlr4::tree::TerminalNode *MERGE_SYMBOL();
    antlr4::tree::TerminalNode *MESSAGE_TEXT_SYMBOL();
    antlr4::tree::TerminalNode *MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *MIGRATE_SYMBOL();
    antlr4::tree::TerminalNode *MINUTE_SYMBOL();
    antlr4::tree::TerminalNode *MIN_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *MODE_SYMBOL();
    antlr4::tree::TerminalNode *MODIFY_SYMBOL();
    antlr4::tree::TerminalNode *MONTH_SYMBOL();
    antlr4::tree::TerminalNode *MULTILINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOINT_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOLYGON_SYMBOL();
    antlr4::tree::TerminalNode *MUTEX_SYMBOL();
    antlr4::tree::TerminalNode *MYSQL_ERRNO_SYMBOL();
    antlr4::tree::TerminalNode *NAMES_SYMBOL();
    antlr4::tree::TerminalNode *NAME_SYMBOL();
    antlr4::tree::TerminalNode *NATIONAL_SYMBOL();
    antlr4::tree::TerminalNode *NCHAR_SYMBOL();
    antlr4::tree::TerminalNode *NDBCLUSTER_SYMBOL();
    antlr4::tree::TerminalNode *NESTED_SYMBOL();
    antlr4::tree::TerminalNode *NEVER_SYMBOL();
    antlr4::tree::TerminalNode *NEW_SYMBOL();
    antlr4::tree::TerminalNode *NEXT_SYMBOL();
    antlr4::tree::TerminalNode *NODEGROUP_SYMBOL();
    antlr4::tree::TerminalNode *NOWAIT_SYMBOL();
    antlr4::tree::TerminalNode *NO_WAIT_SYMBOL();
    antlr4::tree::TerminalNode *NULLS_SYMBOL();
    antlr4::tree::TerminalNode *NUMBER_SYMBOL();
    antlr4::tree::TerminalNode *NVARCHAR_SYMBOL();
    antlr4::tree::TerminalNode *OFFSET_SYMBOL();
    antlr4::tree::TerminalNode *OJ_SYMBOL();
    antlr4::tree::TerminalNode *OLD_SYMBOL();
    antlr4::tree::TerminalNode *ONE_SYMBOL();
    antlr4::tree::TerminalNode *ONLY_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_SYMBOL();
    antlr4::tree::TerminalNode *OPTIONAL_SYMBOL();
    antlr4::tree::TerminalNode *OPTIONS_SYMBOL();
    antlr4::tree::TerminalNode *ORDINALITY_SYMBOL();
    antlr4::tree::TerminalNode *ORGANIZATION_SYMBOL();
    antlr4::tree::TerminalNode *OTHERS_SYMBOL();
    antlr4::tree::TerminalNode *OWNER_SYMBOL();
    antlr4::tree::TerminalNode *PACK_KEYS_SYMBOL();
    antlr4::tree::TerminalNode *PAGE_SYMBOL();
    antlr4::tree::TerminalNode *PARSER_SYMBOL();
    antlr4::tree::TerminalNode *PARTIAL_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONING_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONS_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *PATH_SYMBOL();
    antlr4::tree::TerminalNode *PHASE_SYMBOL();
    antlr4::tree::TerminalNode *PLUGINS_SYMBOL();
    antlr4::tree::TerminalNode *PLUGIN_DIR_SYMBOL();
    antlr4::tree::TerminalNode *PLUGIN_SYMBOL();
    antlr4::tree::TerminalNode *POINT_SYMBOL();
    antlr4::tree::TerminalNode *POLYGON_SYMBOL();
    antlr4::tree::TerminalNode *PORT_SYMBOL();
    antlr4::tree::TerminalNode *PRECEDING_SYMBOL();
    antlr4::tree::TerminalNode *PRESERVE_SYMBOL();
    antlr4::tree::TerminalNode *PREV_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGES_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGE_CHECKS_USER_SYMBOL();
    antlr4::tree::TerminalNode *PROCESSLIST_SYMBOL();
    antlr4::tree::TerminalNode *PROFILES_SYMBOL();
    antlr4::tree::TerminalNode *PROFILE_SYMBOL();
    antlr4::tree::TerminalNode *QUARTER_SYMBOL();
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *QUICK_SYMBOL();
    antlr4::tree::TerminalNode *READ_ONLY_SYMBOL();
    antlr4::tree::TerminalNode *REBUILD_SYMBOL();
    antlr4::tree::TerminalNode *RECOVER_SYMBOL();
    antlr4::tree::TerminalNode *REDO_BUFFER_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *REDUNDANT_SYMBOL();
    antlr4::tree::TerminalNode *REFERENCE_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_SYMBOL();
    antlr4::tree::TerminalNode *RELAYLOG_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_LOG_FILE_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_LOG_POS_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_THREAD_SYMBOL();
    antlr4::tree::TerminalNode *REMOVE_SYMBOL();
    antlr4::tree::TerminalNode *REORGANIZE_SYMBOL();
    antlr4::tree::TerminalNode *REPEATABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_DO_DB_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_DO_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_IGNORE_DB_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_IGNORE_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_REWRITE_DB_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_WILD_DO_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_WILD_IGNORE_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *USER_RESOURCES_SYMBOL();
    antlr4::tree::TerminalNode *RESPECT_SYMBOL();
    antlr4::tree::TerminalNode *RESTORE_SYMBOL();
    antlr4::tree::TerminalNode *RESUME_SYMBOL();
    antlr4::tree::TerminalNode *RETAIN_SYMBOL();
    antlr4::tree::TerminalNode *RETURNED_SQLSTATE_SYMBOL();
    antlr4::tree::TerminalNode *RETURNS_SYMBOL();
    antlr4::tree::TerminalNode *REUSE_SYMBOL();
    antlr4::tree::TerminalNode *REVERSE_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    antlr4::tree::TerminalNode *ROLLUP_SYMBOL();
    antlr4::tree::TerminalNode *ROTATE_SYMBOL();
    antlr4::tree::TerminalNode *ROUTINE_SYMBOL();
    antlr4::tree::TerminalNode *ROW_COUNT_SYMBOL();
    antlr4::tree::TerminalNode *ROW_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *RTREE_SYMBOL();
    antlr4::tree::TerminalNode *SCHEDULE_SYMBOL();
    antlr4::tree::TerminalNode *SCHEMA_NAME_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_LOAD_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_UNLOAD_SYMBOL();
    antlr4::tree::TerminalNode *SECOND_SYMBOL();
    antlr4::tree::TerminalNode *SECURITY_SYMBOL();
    antlr4::tree::TerminalNode *SERIALIZABLE_SYMBOL();
    antlr4::tree::TerminalNode *SERIAL_SYMBOL();
    antlr4::tree::TerminalNode *SERVER_SYMBOL();
    antlr4::tree::TerminalNode *SHARE_SYMBOL();
    antlr4::tree::TerminalNode *SIMPLE_SYMBOL();
    antlr4::tree::TerminalNode *SKIP_SYMBOL();
    antlr4::tree::TerminalNode *SLOW_SYMBOL();
    antlr4::tree::TerminalNode *SNAPSHOT_SYMBOL();
    antlr4::tree::TerminalNode *SOCKET_SYMBOL();
    antlr4::tree::TerminalNode *SONAME_SYMBOL();
    antlr4::tree::TerminalNode *SOUNDS_SYMBOL();
    antlr4::tree::TerminalNode *SOURCE_SYMBOL();
    antlr4::tree::TerminalNode *SQL_AFTER_GTIDS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_AFTER_MTS_GAPS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BEFORE_GTIDS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BUFFER_RESULT_SYMBOL();
    antlr4::tree::TerminalNode *SQL_NO_CACHE_SYMBOL();
    antlr4::tree::TerminalNode *SQL_THREAD_SYMBOL();
    antlr4::tree::TerminalNode *SRID_SYMBOL();
    antlr4::tree::TerminalNode *STACKED_SYMBOL();
    antlr4::tree::TerminalNode *STARTS_SYMBOL();
    antlr4::tree::TerminalNode *STATS_AUTO_RECALC_SYMBOL();
    antlr4::tree::TerminalNode *STATS_PERSISTENT_SYMBOL();
    antlr4::tree::TerminalNode *STATS_SAMPLE_PAGES_SYMBOL();
    antlr4::tree::TerminalNode *STATUS_SYMBOL();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    antlr4::tree::TerminalNode *STRING_SYMBOL();
    antlr4::tree::TerminalNode *SUBCLASS_ORIGIN_SYMBOL();
    antlr4::tree::TerminalNode *SUBDATE_SYMBOL();
    antlr4::tree::TerminalNode *SUBJECT_SYMBOL();
    antlr4::tree::TerminalNode *SUBPARTITIONS_SYMBOL();
    antlr4::tree::TerminalNode *SUBPARTITION_SYMBOL();
    antlr4::tree::TerminalNode *SUSPEND_SYMBOL();
    antlr4::tree::TerminalNode *SWAPS_SYMBOL();
    antlr4::tree::TerminalNode *SWITCHES_SYMBOL();
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_NAME_SYMBOL();
    antlr4::tree::TerminalNode *TEMPORARY_SYMBOL();
    antlr4::tree::TerminalNode *TEMPTABLE_SYMBOL();
    antlr4::tree::TerminalNode *TEXT_SYMBOL();
    antlr4::tree::TerminalNode *THAN_SYMBOL();
    antlr4::tree::TerminalNode *THREAD_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *TIES_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_ADD_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_DIFF_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_SYMBOL();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    antlr4::tree::TerminalNode *TRANSACTION_SYMBOL();
    antlr4::tree::TerminalNode *TRIGGERS_SYMBOL();
    antlr4::tree::TerminalNode *TYPES_SYMBOL();
    antlr4::tree::TerminalNode *TYPE_SYMBOL();
    antlr4::tree::TerminalNode *UNBOUNDED_SYMBOL();
    antlr4::tree::TerminalNode *UNCOMMITTED_SYMBOL();
    antlr4::tree::TerminalNode *UNDEFINED_SYMBOL();
    antlr4::tree::TerminalNode *UNDOFILE_SYMBOL();
    antlr4::tree::TerminalNode *UNDO_BUFFER_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *UNKNOWN_SYMBOL();
    antlr4::tree::TerminalNode *UNTIL_SYMBOL();
    antlr4::tree::TerminalNode *UPGRADE_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    antlr4::tree::TerminalNode *USE_FRM_SYMBOL();
    antlr4::tree::TerminalNode *VALIDATION_SYMBOL();
    antlr4::tree::TerminalNode *VALUE_SYMBOL();
    antlr4::tree::TerminalNode *VARIABLES_SYMBOL();
    antlr4::tree::TerminalNode *VCPU_SYMBOL();
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    antlr4::tree::TerminalNode *VISIBLE_SYMBOL();
    antlr4::tree::TerminalNode *WAIT_SYMBOL();
    antlr4::tree::TerminalNode *WARNINGS_SYMBOL();
    antlr4::tree::TerminalNode *WEEK_SYMBOL();
    antlr4::tree::TerminalNode *WEIGHT_STRING_SYMBOL();
    antlr4::tree::TerminalNode *WITHOUT_SYMBOL();
    antlr4::tree::TerminalNode *WORK_SYMBOL();
    antlr4::tree::TerminalNode *WRAPPER_SYMBOL();
    antlr4::tree::TerminalNode *X509_SYMBOL();
    antlr4::tree::TerminalNode *XID_SYMBOL();
    antlr4::tree::TerminalNode *XML_SYMBOL();
    antlr4::tree::TerminalNode *YEAR_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierKeywordsUnambiguousContext* identifierKeywordsUnambiguous();

  class  RoleKeywordContext : public antlr4::ParserRuleContext {
  public:
    RoleKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoleOrLabelKeywordContext *roleOrLabelKeyword();
    RoleOrIdentifierKeywordContext *roleOrIdentifierKeyword();
    IdentifierKeywordsUnambiguousContext *identifierKeywordsUnambiguous();
    IdentifierKeywordsAmbiguous2LabelsContext *identifierKeywordsAmbiguous2Labels();
    IdentifierKeywordsAmbiguous4SystemVariablesContext *identifierKeywordsAmbiguous4SystemVariables();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleKeywordContext* roleKeyword();

  class  LValueKeywordContext : public antlr4::ParserRuleContext {
  public:
    LValueKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierKeywordsUnambiguousContext *identifierKeywordsUnambiguous();
    IdentifierKeywordsAmbiguous1RolesAndLabelsContext *identifierKeywordsAmbiguous1RolesAndLabels();
    IdentifierKeywordsAmbiguous2LabelsContext *identifierKeywordsAmbiguous2Labels();
    IdentifierKeywordsAmbiguous3RolesContext *identifierKeywordsAmbiguous3Roles();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  LValueKeywordContext* lValueKeyword();

  class  IdentifierKeywordsAmbiguous4SystemVariablesContext : public antlr4::ParserRuleContext {
  public:
    IdentifierKeywordsAmbiguous4SystemVariablesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GLOBAL_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *PERSIST_SYMBOL();
    antlr4::tree::TerminalNode *PERSIST_ONLY_SYMBOL();
    antlr4::tree::TerminalNode *SESSION_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierKeywordsAmbiguous4SystemVariablesContext* identifierKeywordsAmbiguous4SystemVariables();

  class  RoleOrIdentifierKeywordContext : public antlr4::ParserRuleContext {
  public:
    RoleOrIdentifierKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ACCOUNT_SYMBOL();
    antlr4::tree::TerminalNode *ASCII_SYMBOL();
    antlr4::tree::TerminalNode *ALWAYS_SYMBOL();
    antlr4::tree::TerminalNode *BACKUP_SYMBOL();
    antlr4::tree::TerminalNode *BEGIN_SYMBOL();
    antlr4::tree::TerminalNode *BYTE_SYMBOL();
    antlr4::tree::TerminalNode *CACHE_SYMBOL();
    antlr4::tree::TerminalNode *CHARSET_SYMBOL();
    antlr4::tree::TerminalNode *CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *CLONE_SYMBOL();
    antlr4::tree::TerminalNode *CLOSE_SYMBOL();
    antlr4::tree::TerminalNode *COMMENT_SYMBOL();
    antlr4::tree::TerminalNode *COMMIT_SYMBOL();
    antlr4::tree::TerminalNode *CONTAINS_SYMBOL();
    antlr4::tree::TerminalNode *DEALLOCATE_SYMBOL();
    antlr4::tree::TerminalNode *DO_SYMBOL();
    antlr4::tree::TerminalNode *END_SYMBOL();
    antlr4::tree::TerminalNode *FLUSH_SYMBOL();
    antlr4::tree::TerminalNode *FOLLOWS_SYMBOL();
    antlr4::tree::TerminalNode *FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *GROUP_REPLICATION_SYMBOL();
    antlr4::tree::TerminalNode *HANDLER_SYMBOL();
    antlr4::tree::TerminalNode *HELP_SYMBOL();
    antlr4::tree::TerminalNode *HOST_SYMBOL();
    antlr4::tree::TerminalNode *INSTALL_SYMBOL();
    antlr4::tree::TerminalNode *INVISIBLE_SYMBOL();
    antlr4::tree::TerminalNode *LANGUAGE_SYMBOL();
    antlr4::tree::TerminalNode *NO_SYMBOL();
    antlr4::tree::TerminalNode *OPEN_SYMBOL();
    antlr4::tree::TerminalNode *OPTIONS_SYMBOL();
    antlr4::tree::TerminalNode *OWNER_SYMBOL();
    antlr4::tree::TerminalNode *PARSER_SYMBOL();
    antlr4::tree::TerminalNode *PARTITION_SYMBOL();
    antlr4::tree::TerminalNode *PORT_SYMBOL();
    antlr4::tree::TerminalNode *PRECEDES_SYMBOL();
    antlr4::tree::TerminalNode *PREPARE_SYMBOL();
    antlr4::tree::TerminalNode *REMOVE_SYMBOL();
    antlr4::tree::TerminalNode *REPAIR_SYMBOL();
    antlr4::tree::TerminalNode *RESET_SYMBOL();
    antlr4::tree::TerminalNode *RESTORE_SYMBOL();
    antlr4::tree::TerminalNode *ROLE_SYMBOL();
    antlr4::tree::TerminalNode *ROLLBACK_SYMBOL();
    antlr4::tree::TerminalNode *SAVEPOINT_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_LOAD_SYMBOL();
    antlr4::tree::TerminalNode *SECONDARY_UNLOAD_SYMBOL();
    antlr4::tree::TerminalNode *SECURITY_SYMBOL();
    antlr4::tree::TerminalNode *SERVER_SYMBOL();
    antlr4::tree::TerminalNode *SIGNED_SYMBOL();
    antlr4::tree::TerminalNode *SOCKET_SYMBOL();
    antlr4::tree::TerminalNode *SLAVE_SYMBOL();
    antlr4::tree::TerminalNode *SONAME_SYMBOL();
    antlr4::tree::TerminalNode *START_SYMBOL();
    antlr4::tree::TerminalNode *STOP_SYMBOL();
    antlr4::tree::TerminalNode *TRUNCATE_SYMBOL();
    antlr4::tree::TerminalNode *UNICODE_SYMBOL();
    antlr4::tree::TerminalNode *UNINSTALL_SYMBOL();
    antlr4::tree::TerminalNode *UPGRADE_SYMBOL();
    antlr4::tree::TerminalNode *VISIBLE_SYMBOL();
    antlr4::tree::TerminalNode *WRAPPER_SYMBOL();
    antlr4::tree::TerminalNode *XA_SYMBOL();
    antlr4::tree::TerminalNode *SHUTDOWN_SYMBOL();
    antlr4::tree::TerminalNode *IMPORT_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleOrIdentifierKeywordContext* roleOrIdentifierKeyword();

  class  RoleOrLabelKeywordContext : public antlr4::ParserRuleContext {
  public:
    RoleOrLabelKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ACTION_SYMBOL();
    antlr4::tree::TerminalNode *ACTIVE_SYMBOL();
    antlr4::tree::TerminalNode *ADDDATE_SYMBOL();
    antlr4::tree::TerminalNode *AFTER_SYMBOL();
    antlr4::tree::TerminalNode *AGAINST_SYMBOL();
    antlr4::tree::TerminalNode *AGGREGATE_SYMBOL();
    antlr4::tree::TerminalNode *ALGORITHM_SYMBOL();
    antlr4::tree::TerminalNode *ANALYSE_SYMBOL();
    antlr4::tree::TerminalNode *ANY_SYMBOL();
    antlr4::tree::TerminalNode *AT_SYMBOL();
    antlr4::tree::TerminalNode *AUTHORS_SYMBOL();
    antlr4::tree::TerminalNode *AUTO_INCREMENT_SYMBOL();
    antlr4::tree::TerminalNode *AUTOEXTEND_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *AVG_ROW_LENGTH_SYMBOL();
    antlr4::tree::TerminalNode *AVG_SYMBOL();
    antlr4::tree::TerminalNode *BINLOG_SYMBOL();
    antlr4::tree::TerminalNode *BIT_SYMBOL();
    antlr4::tree::TerminalNode *BLOCK_SYMBOL();
    antlr4::tree::TerminalNode *BOOL_SYMBOL();
    antlr4::tree::TerminalNode *BOOLEAN_SYMBOL();
    antlr4::tree::TerminalNode *BTREE_SYMBOL();
    antlr4::tree::TerminalNode *BUCKETS_SYMBOL();
    antlr4::tree::TerminalNode *CASCADED_SYMBOL();
    antlr4::tree::TerminalNode *CATALOG_NAME_SYMBOL();
    antlr4::tree::TerminalNode *CHAIN_SYMBOL();
    antlr4::tree::TerminalNode *CHANGED_SYMBOL();
    antlr4::tree::TerminalNode *CHANNEL_SYMBOL();
    antlr4::tree::TerminalNode *CIPHER_SYMBOL();
    antlr4::tree::TerminalNode *CLIENT_SYMBOL();
    antlr4::tree::TerminalNode *CLASS_ORIGIN_SYMBOL();
    antlr4::tree::TerminalNode *COALESCE_SYMBOL();
    antlr4::tree::TerminalNode *CODE_SYMBOL();
    antlr4::tree::TerminalNode *COLLATION_SYMBOL();
    antlr4::tree::TerminalNode *COLUMN_NAME_SYMBOL();
    antlr4::tree::TerminalNode *COLUMN_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *COLUMNS_SYMBOL();
    antlr4::tree::TerminalNode *COMMITTED_SYMBOL();
    antlr4::tree::TerminalNode *COMPACT_SYMBOL();
    antlr4::tree::TerminalNode *COMPLETION_SYMBOL();
    antlr4::tree::TerminalNode *COMPONENT_SYMBOL();
    antlr4::tree::TerminalNode *COMPRESSED_SYMBOL();
    antlr4::tree::TerminalNode *COMPRESSION_SYMBOL();
    antlr4::tree::TerminalNode *CONCURRENT_SYMBOL();
    antlr4::tree::TerminalNode *CONNECTION_SYMBOL();
    antlr4::tree::TerminalNode *CONSISTENT_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_CATALOG_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_SCHEMA_SYMBOL();
    antlr4::tree::TerminalNode *CONSTRAINT_NAME_SYMBOL();
    antlr4::tree::TerminalNode *CONTEXT_SYMBOL();
    antlr4::tree::TerminalNode *CONTRIBUTORS_SYMBOL();
    antlr4::tree::TerminalNode *CPU_SYMBOL();
    antlr4::tree::TerminalNode *CURRENT_SYMBOL();
    antlr4::tree::TerminalNode *CURSOR_NAME_SYMBOL();
    antlr4::tree::TerminalNode *DATA_SYMBOL();
    antlr4::tree::TerminalNode *DATAFILE_SYMBOL();
    antlr4::tree::TerminalNode *DATETIME_SYMBOL();
    antlr4::tree::TerminalNode *DATE_SYMBOL();
    antlr4::tree::TerminalNode *DAY_SYMBOL();
    antlr4::tree::TerminalNode *DEFAULT_AUTH_SYMBOL();
    antlr4::tree::TerminalNode *DEFINER_SYMBOL();
    antlr4::tree::TerminalNode *DELAY_KEY_WRITE_SYMBOL();
    antlr4::tree::TerminalNode *DES_KEY_FILE_SYMBOL();
    antlr4::tree::TerminalNode *DESCRIPTION_SYMBOL();
    antlr4::tree::TerminalNode *DIAGNOSTICS_SYMBOL();
    antlr4::tree::TerminalNode *DIRECTORY_SYMBOL();
    antlr4::tree::TerminalNode *DISABLE_SYMBOL();
    antlr4::tree::TerminalNode *DISCARD_SYMBOL();
    antlr4::tree::TerminalNode *DISK_SYMBOL();
    antlr4::tree::TerminalNode *DUMPFILE_SYMBOL();
    antlr4::tree::TerminalNode *DUPLICATE_SYMBOL();
    antlr4::tree::TerminalNode *DYNAMIC_SYMBOL();
    antlr4::tree::TerminalNode *ENCRYPTION_SYMBOL();
    antlr4::tree::TerminalNode *ENDS_SYMBOL();
    antlr4::tree::TerminalNode *ENUM_SYMBOL();
    antlr4::tree::TerminalNode *ENGINE_SYMBOL();
    antlr4::tree::TerminalNode *ENGINES_SYMBOL();
    antlr4::tree::TerminalNode *ERROR_SYMBOL();
    antlr4::tree::TerminalNode *ERRORS_SYMBOL();
    antlr4::tree::TerminalNode *ESCAPE_SYMBOL();
    antlr4::tree::TerminalNode *EVENTS_SYMBOL();
    antlr4::tree::TerminalNode *EVERY_SYMBOL();
    antlr4::tree::TerminalNode *EXCLUDE_SYMBOL();
    antlr4::tree::TerminalNode *EXPANSION_SYMBOL();
    antlr4::tree::TerminalNode *EXPORT_SYMBOL();
    antlr4::tree::TerminalNode *EXTENDED_SYMBOL();
    antlr4::tree::TerminalNode *EXTENT_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *FAULTS_SYMBOL();
    antlr4::tree::TerminalNode *FAST_SYMBOL();
    antlr4::tree::TerminalNode *FOLLOWING_SYMBOL();
    antlr4::tree::TerminalNode *FOUND_SYMBOL();
    antlr4::tree::TerminalNode *ENABLE_SYMBOL();
    antlr4::tree::TerminalNode *FULL_SYMBOL();
    antlr4::tree::TerminalNode *FILE_BLOCK_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *FILTER_SYMBOL();
    antlr4::tree::TerminalNode *FIRST_SYMBOL();
    antlr4::tree::TerminalNode *FIXED_SYMBOL();
    antlr4::tree::TerminalNode *GENERAL_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRY_SYMBOL();
    antlr4::tree::TerminalNode *GEOMETRYCOLLECTION_SYMBOL();
    antlr4::tree::TerminalNode *GET_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *GRANTS_SYMBOL();
    antlr4::tree::TerminalNode *GLOBAL_SYMBOL();
    antlr4::tree::TerminalNode *HASH_SYMBOL();
    antlr4::tree::TerminalNode *HISTOGRAM_SYMBOL();
    antlr4::tree::TerminalNode *HISTORY_SYMBOL();
    antlr4::tree::TerminalNode *HOSTS_SYMBOL();
    antlr4::tree::TerminalNode *HOUR_SYMBOL();
    antlr4::tree::TerminalNode *IDENTIFIED_SYMBOL();
    antlr4::tree::TerminalNode *IGNORE_SERVER_IDS_SYMBOL();
    antlr4::tree::TerminalNode *INVOKER_SYMBOL();
    antlr4::tree::TerminalNode *INDEXES_SYMBOL();
    antlr4::tree::TerminalNode *INITIAL_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *INSTANCE_SYMBOL();
    antlr4::tree::TerminalNode *INACTIVE_SYMBOL();
    antlr4::tree::TerminalNode *IO_SYMBOL();
    antlr4::tree::TerminalNode *IPC_SYMBOL();
    antlr4::tree::TerminalNode *ISOLATION_SYMBOL();
    antlr4::tree::TerminalNode *ISSUER_SYMBOL();
    antlr4::tree::TerminalNode *INSERT_METHOD_SYMBOL();
    antlr4::tree::TerminalNode *JSON_SYMBOL();
    antlr4::tree::TerminalNode *KEY_BLOCK_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *LAST_SYMBOL();
    antlr4::tree::TerminalNode *LEAVES_SYMBOL();
    antlr4::tree::TerminalNode *LESS_SYMBOL();
    antlr4::tree::TerminalNode *LEVEL_SYMBOL();
    antlr4::tree::TerminalNode *LINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *LIST_SYMBOL();
    antlr4::tree::TerminalNode *LOCAL_SYMBOL();
    antlr4::tree::TerminalNode *LOCKED_SYMBOL();
    antlr4::tree::TerminalNode *LOCKS_SYMBOL();
    antlr4::tree::TerminalNode *LOGFILE_SYMBOL();
    antlr4::tree::TerminalNode *LOGS_SYMBOL();
    antlr4::tree::TerminalNode *MAX_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_HEARTBEAT_PERIOD_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_HOST_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PORT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_LOG_FILE_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_LOG_POS_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_USER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_PUBLIC_KEY_PATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SERVER_ID_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_CONNECT_RETRY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_RETRY_COUNT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_DELAY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CA_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CAPATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_TLS_VERSION_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CERT_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CIPHER_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CRL_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_CRLPATH_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_SSL_KEY_SYMBOL();
    antlr4::tree::TerminalNode *MASTER_AUTO_POSITION_SYMBOL();
    antlr4::tree::TerminalNode *MAX_CONNECTIONS_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_QUERIES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_STATEMENT_TIME_SYMBOL();
    antlr4::tree::TerminalNode *MAX_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *MAX_UPDATES_PER_HOUR_SYMBOL();
    antlr4::tree::TerminalNode *MAX_USER_CONNECTIONS_SYMBOL();
    antlr4::tree::TerminalNode *MEDIUM_SYMBOL();
    antlr4::tree::TerminalNode *MEMORY_SYMBOL();
    antlr4::tree::TerminalNode *MERGE_SYMBOL();
    antlr4::tree::TerminalNode *MESSAGE_TEXT_SYMBOL();
    antlr4::tree::TerminalNode *MICROSECOND_SYMBOL();
    antlr4::tree::TerminalNode *MIGRATE_SYMBOL();
    antlr4::tree::TerminalNode *MINUTE_SYMBOL();
    antlr4::tree::TerminalNode *MIN_ROWS_SYMBOL();
    antlr4::tree::TerminalNode *MODIFY_SYMBOL();
    antlr4::tree::TerminalNode *MODE_SYMBOL();
    antlr4::tree::TerminalNode *MONTH_SYMBOL();
    antlr4::tree::TerminalNode *MULTILINESTRING_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOINT_SYMBOL();
    antlr4::tree::TerminalNode *MULTIPOLYGON_SYMBOL();
    antlr4::tree::TerminalNode *MUTEX_SYMBOL();
    antlr4::tree::TerminalNode *MYSQL_ERRNO_SYMBOL();
    antlr4::tree::TerminalNode *NAME_SYMBOL();
    antlr4::tree::TerminalNode *NAMES_SYMBOL();
    antlr4::tree::TerminalNode *NATIONAL_SYMBOL();
    antlr4::tree::TerminalNode *NCHAR_SYMBOL();
    antlr4::tree::TerminalNode *NDBCLUSTER_SYMBOL();
    antlr4::tree::TerminalNode *NESTED_SYMBOL();
    antlr4::tree::TerminalNode *NEVER_SYMBOL();
    antlr4::tree::TerminalNode *NEXT_SYMBOL();
    antlr4::tree::TerminalNode *NEW_SYMBOL();
    antlr4::tree::TerminalNode *NO_WAIT_SYMBOL();
    antlr4::tree::TerminalNode *NODEGROUP_SYMBOL();
    antlr4::tree::TerminalNode *NULLS_SYMBOL();
    antlr4::tree::TerminalNode *NOWAIT_SYMBOL();
    antlr4::tree::TerminalNode *NUMBER_SYMBOL();
    antlr4::tree::TerminalNode *NVARCHAR_SYMBOL();
    antlr4::tree::TerminalNode *OFFSET_SYMBOL();
    antlr4::tree::TerminalNode *OLD_SYMBOL();
    antlr4::tree::TerminalNode *OLD_PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *ONE_SYMBOL();
    antlr4::tree::TerminalNode *OPTIONAL_SYMBOL();
    antlr4::tree::TerminalNode *ORDINALITY_SYMBOL();
    antlr4::tree::TerminalNode *ORGANIZATION_SYMBOL();
    antlr4::tree::TerminalNode *OTHERS_SYMBOL();
    antlr4::tree::TerminalNode *PACK_KEYS_SYMBOL();
    antlr4::tree::TerminalNode *PAGE_SYMBOL();
    antlr4::tree::TerminalNode *PARTIAL_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONING_SYMBOL();
    antlr4::tree::TerminalNode *PARTITIONS_SYMBOL();
    antlr4::tree::TerminalNode *PASSWORD_SYMBOL();
    antlr4::tree::TerminalNode *PATH_SYMBOL();
    antlr4::tree::TerminalNode *PHASE_SYMBOL();
    antlr4::tree::TerminalNode *PLUGIN_DIR_SYMBOL();
    antlr4::tree::TerminalNode *PLUGIN_SYMBOL();
    antlr4::tree::TerminalNode *PLUGINS_SYMBOL();
    antlr4::tree::TerminalNode *POINT_SYMBOL();
    antlr4::tree::TerminalNode *POLYGON_SYMBOL();
    antlr4::tree::TerminalNode *PRECEDING_SYMBOL();
    antlr4::tree::TerminalNode *PRESERVE_SYMBOL();
    antlr4::tree::TerminalNode *PREV_SYMBOL();
    antlr4::tree::TerminalNode *THREAD_PRIORITY_SYMBOL();
    antlr4::tree::TerminalNode *PRIVILEGES_SYMBOL();
    antlr4::tree::TerminalNode *PROCESSLIST_SYMBOL();
    antlr4::tree::TerminalNode *PROFILE_SYMBOL();
    antlr4::tree::TerminalNode *PROFILES_SYMBOL();
    antlr4::tree::TerminalNode *QUARTER_SYMBOL();
    antlr4::tree::TerminalNode *QUERY_SYMBOL();
    antlr4::tree::TerminalNode *QUICK_SYMBOL();
    antlr4::tree::TerminalNode *READ_ONLY_SYMBOL();
    antlr4::tree::TerminalNode *REBUILD_SYMBOL();
    antlr4::tree::TerminalNode *RECOVER_SYMBOL();
    antlr4::tree::TerminalNode *REDO_BUFFER_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *REDOFILE_SYMBOL();
    antlr4::tree::TerminalNode *REDUNDANT_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_SYMBOL();
    antlr4::tree::TerminalNode *RELAYLOG_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_LOG_FILE_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_LOG_POS_SYMBOL();
    antlr4::tree::TerminalNode *RELAY_THREAD_SYMBOL();
    antlr4::tree::TerminalNode *REMOTE_SYMBOL();
    antlr4::tree::TerminalNode *REORGANIZE_SYMBOL();
    antlr4::tree::TerminalNode *REPEATABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_DO_DB_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_IGNORE_DB_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_DO_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_IGNORE_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_WILD_DO_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_WILD_IGNORE_TABLE_SYMBOL();
    antlr4::tree::TerminalNode *REPLICATE_REWRITE_DB_SYMBOL();
    antlr4::tree::TerminalNode *USER_RESOURCES_SYMBOL();
    antlr4::tree::TerminalNode *RESPECT_SYMBOL();
    antlr4::tree::TerminalNode *RESUME_SYMBOL();
    antlr4::tree::TerminalNode *RETAIN_SYMBOL();
    antlr4::tree::TerminalNode *RETURNED_SQLSTATE_SYMBOL();
    antlr4::tree::TerminalNode *RETURNS_SYMBOL();
    antlr4::tree::TerminalNode *REUSE_SYMBOL();
    antlr4::tree::TerminalNode *REVERSE_SYMBOL();
    antlr4::tree::TerminalNode *ROLLUP_SYMBOL();
    antlr4::tree::TerminalNode *ROTATE_SYMBOL();
    antlr4::tree::TerminalNode *ROUTINE_SYMBOL();
    antlr4::tree::TerminalNode *ROW_COUNT_SYMBOL();
    antlr4::tree::TerminalNode *ROW_FORMAT_SYMBOL();
    antlr4::tree::TerminalNode *RTREE_SYMBOL();
    antlr4::tree::TerminalNode *SCHEDULE_SYMBOL();
    antlr4::tree::TerminalNode *SCHEMA_NAME_SYMBOL();
    antlr4::tree::TerminalNode *SECOND_SYMBOL();
    antlr4::tree::TerminalNode *SERIAL_SYMBOL();
    antlr4::tree::TerminalNode *SERIALIZABLE_SYMBOL();
    antlr4::tree::TerminalNode *SESSION_SYMBOL();
    antlr4::tree::TerminalNode *SHARE_SYMBOL();
    antlr4::tree::TerminalNode *SIMPLE_SYMBOL();
    antlr4::tree::TerminalNode *SKIP_SYMBOL();
    antlr4::tree::TerminalNode *SLOW_SYMBOL();
    antlr4::tree::TerminalNode *SNAPSHOT_SYMBOL();
    antlr4::tree::TerminalNode *SOUNDS_SYMBOL();
    antlr4::tree::TerminalNode *SOURCE_SYMBOL();
    antlr4::tree::TerminalNode *SQL_AFTER_GTIDS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_AFTER_MTS_GAPS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BEFORE_GTIDS_SYMBOL();
    antlr4::tree::TerminalNode *SQL_CACHE_SYMBOL();
    antlr4::tree::TerminalNode *SQL_BUFFER_RESULT_SYMBOL();
    antlr4::tree::TerminalNode *SQL_NO_CACHE_SYMBOL();
    antlr4::tree::TerminalNode *SQL_THREAD_SYMBOL();
    antlr4::tree::TerminalNode *SRID_SYMBOL();
    antlr4::tree::TerminalNode *STACKED_SYMBOL();
    antlr4::tree::TerminalNode *STARTS_SYMBOL();
    antlr4::tree::TerminalNode *STATS_AUTO_RECALC_SYMBOL();
    antlr4::tree::TerminalNode *STATS_PERSISTENT_SYMBOL();
    antlr4::tree::TerminalNode *STATS_SAMPLE_PAGES_SYMBOL();
    antlr4::tree::TerminalNode *STATUS_SYMBOL();
    antlr4::tree::TerminalNode *STORAGE_SYMBOL();
    antlr4::tree::TerminalNode *STRING_SYMBOL();
    antlr4::tree::TerminalNode *SUBCLASS_ORIGIN_SYMBOL();
    antlr4::tree::TerminalNode *SUBDATE_SYMBOL();
    antlr4::tree::TerminalNode *SUBJECT_SYMBOL();
    antlr4::tree::TerminalNode *SUBPARTITION_SYMBOL();
    antlr4::tree::TerminalNode *SUBPARTITIONS_SYMBOL();
    antlr4::tree::TerminalNode *SUPER_SYMBOL();
    antlr4::tree::TerminalNode *SUSPEND_SYMBOL();
    antlr4::tree::TerminalNode *SWAPS_SYMBOL();
    antlr4::tree::TerminalNode *SWITCHES_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_NAME_SYMBOL();
    antlr4::tree::TerminalNode *TABLES_SYMBOL();
    antlr4::tree::TerminalNode *TABLE_CHECKSUM_SYMBOL();
    antlr4::tree::TerminalNode *TABLESPACE_SYMBOL();
    antlr4::tree::TerminalNode *TEMPORARY_SYMBOL();
    antlr4::tree::TerminalNode *TEMPTABLE_SYMBOL();
    antlr4::tree::TerminalNode *TEXT_SYMBOL();
    antlr4::tree::TerminalNode *THAN_SYMBOL();
    antlr4::tree::TerminalNode *TIES_SYMBOL();
    antlr4::tree::TerminalNode *TRANSACTION_SYMBOL();
    antlr4::tree::TerminalNode *TRIGGERS_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_ADD_SYMBOL();
    antlr4::tree::TerminalNode *TIMESTAMP_DIFF_SYMBOL();
    antlr4::tree::TerminalNode *TIME_SYMBOL();
    antlr4::tree::TerminalNode *TYPES_SYMBOL();
    antlr4::tree::TerminalNode *TYPE_SYMBOL();
    antlr4::tree::TerminalNode *UDF_RETURNS_SYMBOL();
    antlr4::tree::TerminalNode *UNBOUNDED_SYMBOL();
    antlr4::tree::TerminalNode *UNCOMMITTED_SYMBOL();
    antlr4::tree::TerminalNode *UNDEFINED_SYMBOL();
    antlr4::tree::TerminalNode *UNDO_BUFFER_SIZE_SYMBOL();
    antlr4::tree::TerminalNode *UNDOFILE_SYMBOL();
    antlr4::tree::TerminalNode *UNKNOWN_SYMBOL();
    antlr4::tree::TerminalNode *UNTIL_SYMBOL();
    antlr4::tree::TerminalNode *USER_SYMBOL();
    antlr4::tree::TerminalNode *USE_FRM_SYMBOL();
    antlr4::tree::TerminalNode *VARIABLES_SYMBOL();
    antlr4::tree::TerminalNode *VCPU_SYMBOL();
    antlr4::tree::TerminalNode *VIEW_SYMBOL();
    antlr4::tree::TerminalNode *VALUE_SYMBOL();
    antlr4::tree::TerminalNode *WARNINGS_SYMBOL();
    antlr4::tree::TerminalNode *WAIT_SYMBOL();
    antlr4::tree::TerminalNode *WEEK_SYMBOL();
    antlr4::tree::TerminalNode *WORK_SYMBOL();
    antlr4::tree::TerminalNode *WEIGHT_STRING_SYMBOL();
    antlr4::tree::TerminalNode *X509_SYMBOL();
    antlr4::tree::TerminalNode *XID_SYMBOL();
    antlr4::tree::TerminalNode *XML_SYMBOL();
    antlr4::tree::TerminalNode *YEAR_SYMBOL();
    antlr4::tree::TerminalNode *SHUTDOWN_SYMBOL();
    antlr4::tree::TerminalNode *CUBE_SYMBOL();
    antlr4::tree::TerminalNode *IMPORT_SYMBOL();
    antlr4::tree::TerminalNode *FUNCTION_SYMBOL();
    antlr4::tree::TerminalNode *ROWS_SYMBOL();
    antlr4::tree::TerminalNode *ROW_SYMBOL();
    antlr4::tree::TerminalNode *EXCHANGE_SYMBOL();
    antlr4::tree::TerminalNode *EXPIRE_SYMBOL();
    antlr4::tree::TerminalNode *ONLY_SYMBOL();
    antlr4::tree::TerminalNode *VALIDATION_SYMBOL();
    antlr4::tree::TerminalNode *WITHOUT_SYMBOL();
    antlr4::tree::TerminalNode *ADMIN_SYMBOL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RoleOrLabelKeywordContext* roleOrLabelKeyword();


  virtual bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;
  bool simpleStatementSempred(SimpleStatementContext *_localctx, size_t predicateIndex);
  bool alterStatementSempred(AlterStatementContext *_localctx, size_t predicateIndex);
  bool alterDatabaseSempred(AlterDatabaseContext *_localctx, size_t predicateIndex);
  bool alterTableSempred(AlterTableContext *_localctx, size_t predicateIndex);
  bool standaloneAlterCommandsSempred(StandaloneAlterCommandsContext *_localctx, size_t predicateIndex);
  bool alterPartitionSempred(AlterPartitionContext *_localctx, size_t predicateIndex);
  bool alterListItemSempred(AlterListItemContext *_localctx, size_t predicateIndex);
  bool withValidationSempred(WithValidationContext *_localctx, size_t predicateIndex);
  bool alterTablespaceSempred(AlterTablespaceContext *_localctx, size_t predicateIndex);
  bool createStatementSempred(CreateStatementContext *_localctx, size_t predicateIndex);
  bool createDatabaseOptionSempred(CreateDatabaseOptionContext *_localctx, size_t predicateIndex);
  bool createIndexSempred(CreateIndexContext *_localctx, size_t predicateIndex);
  bool tsDataFileNameSempred(TsDataFileNameContext *_localctx, size_t predicateIndex);
  bool tablespaceOptionSempred(TablespaceOptionContext *_localctx, size_t predicateIndex);
  bool triggerFollowsPrecedesClauseSempred(TriggerFollowsPrecedesClauseContext *_localctx, size_t predicateIndex);
  bool dropStatementSempred(DropStatementContext *_localctx, size_t predicateIndex);
  bool deleteStatementSempred(DeleteStatementContext *_localctx, size_t predicateIndex);
  bool partitionDeleteSempred(PartitionDeleteContext *_localctx, size_t predicateIndex);
  bool doStatementSempred(DoStatementContext *_localctx, size_t predicateIndex);
  bool insertStatementSempred(InsertStatementContext *_localctx, size_t predicateIndex);
  bool queryExpressionSempred(QueryExpressionContext *_localctx, size_t predicateIndex);
  bool queryExpressionBodySempred(QueryExpressionBodyContext *_localctx, size_t predicateIndex);
  bool querySpecificationSempred(QuerySpecificationContext *_localctx, size_t predicateIndex);
  bool olapOptionSempred(OlapOptionContext *_localctx, size_t predicateIndex);
  bool selectOptionSempred(SelectOptionContext *_localctx, size_t predicateIndex);
  bool lockingClauseSempred(LockingClauseContext *_localctx, size_t predicateIndex);
  bool lockStrenghSempred(LockStrenghContext *_localctx, size_t predicateIndex);
  bool tableReferenceSempred(TableReferenceContext *_localctx, size_t predicateIndex);
  bool tableFactorSempred(TableFactorContext *_localctx, size_t predicateIndex);
  bool derivedTableSempred(DerivedTableContext *_localctx, size_t predicateIndex);
  bool jtColumnSempred(JtColumnContext *_localctx, size_t predicateIndex);
  bool tableAliasSempred(TableAliasContext *_localctx, size_t predicateIndex);
  bool updateStatementSempred(UpdateStatementContext *_localctx, size_t predicateIndex);
  bool transactionCharacteristicSempred(TransactionCharacteristicContext *_localctx, size_t predicateIndex);
  bool lockStatementSempred(LockStatementContext *_localctx, size_t predicateIndex);
  bool xaConvertSempred(XaConvertContext *_localctx, size_t predicateIndex);
  bool replicationStatementSempred(ReplicationStatementContext *_localctx, size_t predicateIndex);
  bool resetOptionSempred(ResetOptionContext *_localctx, size_t predicateIndex);
  bool masterResetOptionsSempred(MasterResetOptionsContext *_localctx, size_t predicateIndex);
  bool changeReplicationSempred(ChangeReplicationContext *_localctx, size_t predicateIndex);
  bool slaveUntilOptionsSempred(SlaveUntilOptionsContext *_localctx, size_t predicateIndex);
  bool slaveConnectionOptionsSempred(SlaveConnectionOptionsContext *_localctx, size_t predicateIndex);
  bool cloneStatementSempred(CloneStatementContext *_localctx, size_t predicateIndex);
  bool accountManagementStatementSempred(AccountManagementStatementContext *_localctx, size_t predicateIndex);
  bool alterUserSempred(AlterUserContext *_localctx, size_t predicateIndex);
  bool alterUserTailSempred(AlterUserTailContext *_localctx, size_t predicateIndex);
  bool createUserSempred(CreateUserContext *_localctx, size_t predicateIndex);
  bool createUserTailSempred(CreateUserTailContext *_localctx, size_t predicateIndex);
  bool defaultRoleClauseSempred(DefaultRoleClauseContext *_localctx, size_t predicateIndex);
  bool accountLockPasswordExpireOptionsSempred(AccountLockPasswordExpireOptionsContext *_localctx, size_t predicateIndex);
  bool dropUserSempred(DropUserContext *_localctx, size_t predicateIndex);
  bool grantSempred(GrantContext *_localctx, size_t predicateIndex);
  bool grantTargetListSempred(GrantTargetListContext *_localctx, size_t predicateIndex);
  bool grantOptionsSempred(GrantOptionsContext *_localctx, size_t predicateIndex);
  bool versionedRequireClauseSempred(VersionedRequireClauseContext *_localctx, size_t predicateIndex);
  bool revokeSempred(RevokeContext *_localctx, size_t predicateIndex);
  bool onTypeToSempred(OnTypeToContext *_localctx, size_t predicateIndex);
  bool roleOrPrivilegeSempred(RoleOrPrivilegeContext *_localctx, size_t predicateIndex);
  bool grantIdentifierSempred(GrantIdentifierContext *_localctx, size_t predicateIndex);
  bool tableAdministrationStatementSempred(TableAdministrationStatementContext *_localctx, size_t predicateIndex);
  bool startOptionValueListSempred(StartOptionValueListContext *_localctx, size_t predicateIndex);
  bool optionValueNoOptionTypeSempred(OptionValueNoOptionTypeContext *_localctx, size_t predicateIndex);
  bool setExprOrDefaultSempred(SetExprOrDefaultContext *_localctx, size_t predicateIndex);
  bool showStatementSempred(ShowStatementContext *_localctx, size_t predicateIndex);
  bool showCommandTypeSempred(ShowCommandTypeContext *_localctx, size_t predicateIndex);
  bool nonBlockingSempred(NonBlockingContext *_localctx, size_t predicateIndex);
  bool otherAdministrativeStatementSempred(OtherAdministrativeStatementContext *_localctx, size_t predicateIndex);
  bool flushOptionSempred(FlushOptionContext *_localctx, size_t predicateIndex);
  bool flushTablesOptionsSempred(FlushTablesOptionsContext *_localctx, size_t predicateIndex);
  bool utilityStatementSempred(UtilityStatementContext *_localctx, size_t predicateIndex);
  bool explainCommandSempred(ExplainCommandContext *_localctx, size_t predicateIndex);
  bool explainableStatementSempred(ExplainableStatementContext *_localctx, size_t predicateIndex);
  bool exprSempred(ExprContext *_localctx, size_t predicateIndex);
  bool boolPriSempred(BoolPriContext *_localctx, size_t predicateIndex);
  bool predicateSempred(PredicateContext *_localctx, size_t predicateIndex);
  bool bitExprSempred(BitExprContext *_localctx, size_t predicateIndex);
  bool simpleExprSempred(SimpleExprContext *_localctx, size_t predicateIndex);
  bool arrayCastSempred(ArrayCastContext *_localctx, size_t predicateIndex);
  bool jsonOperatorSempred(JsonOperatorContext *_localctx, size_t predicateIndex);
  bool sumExprSempred(SumExprContext *_localctx, size_t predicateIndex);
  bool runtimeFunctionCallSempred(RuntimeFunctionCallContext *_localctx, size_t predicateIndex);
  bool geometryFunctionSempred(GeometryFunctionContext *_localctx, size_t predicateIndex);
  bool fractionalPrecisionSempred(FractionalPrecisionContext *_localctx, size_t predicateIndex);
  bool internalVariableNameSempred(InternalVariableNameContext *_localctx, size_t predicateIndex);
  bool castTypeSempred(CastTypeContext *_localctx, size_t predicateIndex);
  bool channelSempred(ChannelContext *_localctx, size_t predicateIndex);
  bool getDiagnosticsSempred(GetDiagnosticsContext *_localctx, size_t predicateIndex);
  bool checkOrReferencesSempred(CheckOrReferencesContext *_localctx, size_t predicateIndex);
  bool tableConstraintDefSempred(TableConstraintDefContext *_localctx, size_t predicateIndex);
  bool fieldDefinitionSempred(FieldDefinitionContext *_localctx, size_t predicateIndex);
  bool columnAttributeSempred(ColumnAttributeContext *_localctx, size_t predicateIndex);
  bool keyListVariantsSempred(KeyListVariantsContext *_localctx, size_t predicateIndex);
  bool commonIndexOptionSempred(CommonIndexOptionContext *_localctx, size_t predicateIndex);
  bool dataTypeSempred(DataTypeContext *_localctx, size_t predicateIndex);
  bool charsetNameSempred(CharsetNameContext *_localctx, size_t predicateIndex);
  bool collationNameSempred(CollationNameContext *_localctx, size_t predicateIndex);
  bool createTableOptionSempred(CreateTableOptionContext *_localctx, size_t predicateIndex);
  bool partitionKeyAlgorithmSempred(PartitionKeyAlgorithmContext *_localctx, size_t predicateIndex);
  bool createUserEntrySempred(CreateUserEntryContext *_localctx, size_t predicateIndex);
  bool usePartitionSempred(UsePartitionContext *_localctx, size_t predicateIndex);
  bool columnNameSempred(ColumnNameContext *_localctx, size_t predicateIndex);
  bool pureIdentifierSempred(PureIdentifierContext *_localctx, size_t predicateIndex);
  bool simpleIdentifierSempred(SimpleIdentifierContext *_localctx, size_t predicateIndex);
  bool real_ulonglong_numberSempred(Real_ulonglong_numberContext *_localctx, size_t predicateIndex);
  bool textStringLiteralSempred(TextStringLiteralContext *_localctx, size_t predicateIndex);
  bool textStringHashSempred(TextStringHashContext *_localctx, size_t predicateIndex);
  bool identifierKeywordSempred(IdentifierKeywordContext *_localctx, size_t predicateIndex);
  bool labelKeywordSempred(LabelKeywordContext *_localctx, size_t predicateIndex);
  bool roleKeywordSempred(RoleKeywordContext *_localctx, size_t predicateIndex);
  bool roleOrIdentifierKeywordSempred(RoleOrIdentifierKeywordContext *_localctx, size_t predicateIndex);
  bool roleOrLabelKeywordSempred(RoleOrLabelKeywordContext *_localctx, size_t predicateIndex);

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

