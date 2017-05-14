#pragma once

#include <Poco/Timespan.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <Interpreters/SettingsCommon.h>


namespace DB
{

/** Limits during query execution are part of the settings.
  * Used to provide a more safe execution of queries from the user interface.
  * Basically, constraints are checked for each block (not every row). That is, the limits can be slightly violated.
  * Almost all limits apply only to SELECTs.
  * Almost all limits apply to each thread individually.
  */
struct Limits
{
    /** Enumeration of limits: type, name, default value.
      * By default: everything is unlimited, except for rather weak restrictions on the depth of recursion and the size of the expressions.
      */

#define APPLY_FOR_LIMITS(M) \
    /** Limits on reading from the most "deep" sources. \
      * That is, only in the deepest subquery. \
      * When reading from a remote server, it is only checked on a remote server. \
      */ \
    M(SettingUInt64, max_rows_to_read, 0) \
    M(SettingUInt64, max_bytes_to_read, 0) \
    M(SettingOverflowMode<false>, read_overflow_mode, OverflowMode::THROW) \
    \
    M(SettingUInt64, max_rows_to_group_by, 0) \
    M(SettingOverflowMode<true>, group_by_overflow_mode, OverflowMode::THROW) \
    M(SettingUInt64, max_bytes_before_external_group_by, 0) \
    \
    M(SettingUInt64, max_rows_to_sort, 0) \
    M(SettingUInt64, max_bytes_to_sort, 0) \
    M(SettingOverflowMode<false>, sort_overflow_mode, OverflowMode::THROW) \
    M(SettingUInt64, max_bytes_before_external_sort, 0) \
    \
    /** Limits on result size. \
      * Are also checked for subqueries and on remote servers. \
      */ \
    M(SettingUInt64, max_result_rows, 0) \
    M(SettingUInt64, max_result_bytes, 0) \
    M(SettingOverflowMode<false>, result_overflow_mode, OverflowMode::THROW) \
    \
    /* TODO: Check also when merging and finalizing aggregate functions. */ \
    M(SettingSeconds, max_execution_time, 0) \
    M(SettingOverflowMode<false>, timeout_overflow_mode, OverflowMode::THROW) \
    \
    /** In rows per second. */ \
    M(SettingUInt64, min_execution_speed, 0) \
    /** Check that the speed is not too low after the specified time has elapsed. */ \
    M(SettingSeconds, timeout_before_checking_execution_speed, 0) \
    \
    M(SettingUInt64, max_columns_to_read, 0) \
    M(SettingUInt64, max_temporary_columns, 0) \
    M(SettingUInt64, max_temporary_non_const_columns, 0) \
    \
    M(SettingUInt64, max_subquery_depth, 100) \
    M(SettingUInt64, max_pipeline_depth, 1000) \
    M(SettingUInt64, max_ast_depth, 1000)        /** Checked not during parsing, */ \
    M(SettingUInt64, max_ast_elements, 50000)    /**  but after parsing the request. */ \
    \
    /** 0 - everything is allowed. 1 - only read requests. 2 - only read requests, as well as changing settings, except for the readonly setting. */ \
    M(SettingUInt64, readonly, 0) \
    \
    /** Limits for the maximum size of the set resulting from the execution of the IN section. */ \
    M(SettingUInt64, max_rows_in_set, 0) \
    M(SettingUInt64, max_bytes_in_set, 0) \
    M(SettingOverflowMode<false>, set_overflow_mode, OverflowMode::THROW) \
    \
    /** Limits for the maximum size of the set obtained by executing the IN section. */ \
    M(SettingUInt64, max_rows_in_join, 0) \
    M(SettingUInt64, max_bytes_in_join, 0) \
    M(SettingOverflowMode<false>, join_overflow_mode, OverflowMode::THROW) \
    \
    /** Limits for the maximum size of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed. */ \
    M(SettingUInt64, max_rows_to_transfer, 0) \
    M(SettingUInt64, max_bytes_to_transfer, 0) \
    M(SettingOverflowMode<false>, transfer_overflow_mode, OverflowMode::THROW) \
    \
    /** Limits for the maximum size of the stored state when executing DISTINCT. */ \
    M(SettingUInt64, max_rows_in_distinct, 0) \
    M(SettingUInt64, max_bytes_in_distinct, 0) \
    M(SettingOverflowMode<false>, distinct_overflow_mode, OverflowMode::THROW) \
    \
    /** Maximum memory usage when processing a request. 0 - not bounded. */ \
    M(SettingUInt64, max_memory_usage, 0) /* For one query */ \
    /* Totally for concurrently running queries of one user */ \
    M(SettingUInt64, max_memory_usage_for_user, 0) \
    /* Totally for all concurrent queries */ \
    M(SettingUInt64, max_memory_usage_for_all_queries, 0) \
    \
    /** The maximum speed of data exchange over the network in bytes per second. 0 - not bounded. */ \
    M(SettingUInt64, max_network_bandwidth, 0) \
    /** The maximum number of bytes to receive or transmit over the network, as part of the query. */ \
    M(SettingUInt64, max_network_bytes, 0) \

#define DECLARE(TYPE, NAME, DEFAULT) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_LIMITS(DECLARE)

#undef DECLARE

    /// Set setting by name.
    bool trySet(const String & name, const Field & value)
    {
    #define TRY_SET(TYPE, NAME, DEFAULT) \
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
    #define TRY_SET(TYPE, NAME, DEFAULT) \
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
    #define TRY_IGNORE(TYPE, NAME, DEFAULT) \
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
    #define TRY_SET(TYPE, NAME, DEFAULT) \
        else if (name == #NAME) NAME.set(value);

        if (false) {}
        APPLY_FOR_LIMITS(TRY_SET)
        else
            return false;

        return true;

    #undef TRY_SET
    }

private:
    friend struct Settings;

    /// Write all the settings to the buffer. (Unlike the corresponding method in Settings, the empty line on the end is not written).
    void serialize(WriteBuffer & buf) const
    {
    #define WRITE(TYPE, NAME, DEFAULT) \
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
