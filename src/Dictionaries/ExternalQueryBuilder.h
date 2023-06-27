#pragma once

#include <string>
#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Formats/FormatSettings.h>
#include <Parsers/IdentifierQuotingStyle.h>


namespace DB
{
struct DictionaryStructure;
class WriteBuffer;


/** Builds a query to load data from external database.
  */
struct ExternalQueryBuilder
{
    const DictionaryStructure dict_struct;
    const std::string db;
    const std::string schema;
    const std::string table;
    const std::string query;
    const std::string where;

    IdentifierQuotingStyle quoting_style;


    ExternalQueryBuilder(
        const DictionaryStructure & dict_struct_,
        const std::string & db_,
        const std::string & schema_,
        const std::string & table_,
        const std::string & query_,
        const std::string & where_,
        IdentifierQuotingStyle quoting_style_);

    /** Generate a query to load all data. */
    std::string composeLoadAllQuery() const;

    /** Generate a query to load data after certain time point */
    std::string composeUpdateQuery(const std::string & update_field, const std::string & time_point) const;

    /** Generate a query to load data by set of UInt64 keys. */
    std::string composeLoadIdsQuery(const std::vector<UInt64> & ids) const;

    /** Generate a query to load data by set of composite keys.
      * There are three methods of specification of composite keys in WHERE:
      * 1. (x = c11 AND y = c12) OR (x = c21 AND y = c22) ...
      * 2. (x, y) IN ((c11, c12), (c21, c22), ...)
      * 3. (x = c1 AND (y, z) IN ((c2, c3), ...))
      */
    enum LoadKeysMethod
    {
        AND_OR_CHAIN,
        IN_WITH_TUPLES,
        CASSANDRA_SEPARATE_PARTITION_KEY,
    };

    std::string composeLoadKeysQuery(const Columns & key_columns, const std::vector<size_t> & requested_rows, LoadKeysMethod method, size_t partition_key_prefix = 0) const;


private:
    const FormatSettings format_settings;

    void composeLoadAllQuery(WriteBuffer & out) const;

    /// In the following methods `beg` and `end` specifies which columns to write in expression

    /// Expression in form (x = c1 AND y = c2 ...)
    void composeKeyCondition(const Columns & key_columns, size_t row, WriteBuffer & out, size_t beg, size_t end) const;

    /// Expression in form (x, y, ...) IN ((c1, c2, ...), ...)
    void composeInWithTuples(const Columns & key_columns, const std::vector<size_t> & requested_rows, WriteBuffer & out, size_t beg, size_t end) const;

    /// Expression in form (x, y, ...)
    void composeKeyTupleDefinition(WriteBuffer & out, size_t beg, size_t end) const;

    /// Expression in form (c1, c2, ...)
    void composeKeyTuple(const Columns & key_columns, size_t row, WriteBuffer & out, size_t beg, size_t end) const;

    /// Compose update condition
    static void composeUpdateCondition(const std::string & update_field, const std::string & time_point, WriteBuffer & out);

    /// Compose ids condition
    void composeIdsCondition(const std::vector<UInt64> & ids, WriteBuffer & out) const;

    /// Compose keys condition
    void composeKeysCondition(const Columns & key_columns, const std::vector<size_t> & requested_rows, LoadKeysMethod method, size_t partition_key_prefix, WriteBuffer & out) const;

    /// Write string with specified quoting style.
    void writeQuoted(const std::string & s, WriteBuffer & out) const;
};

}
