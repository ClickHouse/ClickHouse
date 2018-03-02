#pragma once

#include <Core/Defines.h>
#include <Core/Field.h>
#include <Interpreters/SettingsCommon.h>


namespace DB
{

/** Limits during query execution are part of the settings.
  * Used to provide a more safe execution of queries from the user interface.
  * Basically, limits are checked for each block (not every row). That is, the limits can be slightly violated.
  * Almost all limits apply only to SELECTs.
  * Almost all limits apply to each stream individually.
  */
struct Limits
{
#define APPLY_FOR_LIMITS(M) \
    M(SettingUInt64, max_rows_to_read, 0, "Limit on read rows from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.") \
    M(SettingUInt64, max_bytes_to_read, 0, "Limit on read bytes (after decompression) from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.") \
    M(SettingOverflowMode<false>, read_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    M(SettingUInt64, max_rows_to_group_by, 0, "") \
    M(SettingOverflowMode<true>, group_by_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    M(SettingUInt64, max_bytes_before_external_group_by, 0, "") \
    \
    M(SettingUInt64, max_rows_to_sort, 0, "") \
    M(SettingUInt64, max_bytes_to_sort, 0, "") \
    M(SettingOverflowMode<false>, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    M(SettingUInt64, max_bytes_before_external_sort, 0, "") \
    \
    M(SettingUInt64, max_result_rows, 0, "Limit on result size in rows. Also checked for intermediate data sent from remote servers.") \
    M(SettingUInt64, max_result_bytes, 0, "Limit on result size in bytes (uncompressed). Also checked for intermediate data sent from remote servers.") \
    M(SettingOverflowMode<false>, result_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    /* TODO: Check also when merging and finalizing aggregate functions. */ \
    M(SettingSeconds, max_execution_time, 0, "") \
    M(SettingOverflowMode<false>, timeout_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    M(SettingUInt64, min_execution_speed, 0, "In rows per second.") \
    M(SettingSeconds, timeout_before_checking_execution_speed, 0, "Check that the speed is not too low after the specified time has elapsed.") \
    \
    M(SettingUInt64, max_columns_to_read, 0, "") \
    M(SettingUInt64, max_temporary_columns, 0, "") \
    M(SettingUInt64, max_temporary_non_const_columns, 0, "") \
    \
    M(SettingUInt64, max_subquery_depth, 100, "") \
    M(SettingUInt64, max_pipeline_depth, 1000, "") \
    M(SettingUInt64, max_ast_depth, 1000, "Maximum depth of query syntax tree. Checked after parsing.") \
    M(SettingUInt64, max_ast_elements, 50000, "Maximum size of query syntax tree in number of nodes. Checked after parsing.") \
    M(SettingUInt64, max_expanded_ast_elements, 500000, "Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.") \
    \
    M(SettingUInt64, readonly, 0, "0 - everything is allowed. 1 - only read requests. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.") \
    \
    M(SettingUInt64, max_rows_in_set, 0, "Maximum size of the set (in number of elements) resulting from the execution of the IN section.") \
    M(SettingUInt64, max_bytes_in_set, 0, "Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.") \
    M(SettingOverflowMode<false>, set_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    M(SettingUInt64, max_rows_in_join, 0, "Maximum size of the hash table for JOIN (in number of rows).") \
    M(SettingUInt64, max_bytes_in_join, 0, "Maximum size of the hash table for JOIN (in number of bytes in memory).") \
    M(SettingOverflowMode<false>, join_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    M(SettingUInt64, max_rows_to_transfer, 0, "Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.") \
    M(SettingUInt64, max_bytes_to_transfer, 0, "Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.") \
    M(SettingOverflowMode<false>, transfer_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    M(SettingUInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.") \
    M(SettingUInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.") \
    M(SettingOverflowMode<false>, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.") \
    \
    M(SettingUInt64, max_memory_usage, 0, "Maximum memory usage for processing of single query. Zero means unlimited.") \
    M(SettingUInt64, max_memory_usage_for_user, 0, "Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.") \
    M(SettingUInt64, max_memory_usage_for_all_queries, 0, "Maximum memory usage for processing all concurrently running queries on the server. Zero means unlimited.") \
    \
    M(SettingUInt64, max_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second. Zero means unlimited.") \
    M(SettingUInt64, max_network_bytes, 0, "The maximum number of bytes (compressed) to receive or transmit over the network for execution of the query.") \
    M(SettingUInt64, max_network_bandwidth_for_user, 0, "The maximum speed of data exchange over the network in bytes per second for all concurrently running queries for the user. Zero means unlimited.")

#define DECLARE(TYPE, NAME, DEFAULT, DESCRIPTION) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_LIMITS(DECLARE)

#undef DECLARE

    /// Set setting by name.
    bool trySet(const String & name, const Field & value)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
        else if (name == #NAME) NAME.set(value);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

    /// Set the setting by name. Read the binary serialized value from the buffer (for server-to-server interaction).
    bool trySet(const String & name, ReadBuffer & buf)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
        else if (name == #NAME) NAME.set(buf);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

    /// Skip the binary-serialized value from the buffer.
    bool tryIgnore(const String & name, ReadBuffer & buf)
    {
    #define TRY_IGNORE(TYPE, NAME, DEFAULT, DESCRIPTION) \
        else if (name == #NAME) decltype(NAME)(DEFAULT).set(buf);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_IGNORE)
        else
            return false;

        return true;

        #undef TRY_IGNORE
    }

    /** Set the setting by name. Read the value in text form from a string (for example, from a config, or from a URL parameter).
      */
    bool trySet(const String & name, const String & value)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
        else if (name == #NAME) NAME.set(value);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

    bool tryGet(const String & name, String & value) const
    {
    #define TRY_GET(TYPE, NAME, DEFAULT, DESCRIPTION) \
        else if (name == #NAME) { value = NAME.toString(); return true; }

        if (false) {}
        APPLY_FOR_LIMITS(TRY_GET)

        return false;

    #undef TRY_GET
    }

private:
    friend struct Settings;

    /// Write all the settings to the buffer. (Unlike the corresponding method in Settings, the empty line on the end is not written).
    void serialize(WriteBuffer & buf) const
    {
    #define WRITE(TYPE, NAME, DEFAULT, DESCRIPTION) \
        if (NAME.changed) \
        { \
            writeStringBinary(#NAME, buf); \
            NAME.write(buf); \
        }

        APPLY_FOR_LIMITS(WRITE)

    #undef WRITE
    }
};


}
