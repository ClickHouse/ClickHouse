#pragma once

#include <string>
#include <Columns/IColumn.h>


namespace DB
{

struct DictionaryStructure;
class WriteBuffer;


/** Builds a query to load data from external database.
  */
struct ExternalQueryBuilder
{
    const DictionaryStructure & dict_struct;
    const std::string & db;
    const std::string & table;
    const std::string & where;

    /// Method to quote identifiers.
    /// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
    enum QuotingStyle
    {
        None,            /// Write as-is, without quotes.
        Backticks,        /// `mysql` style
        DoubleQuotes    /// "postgres" style
    };

    QuotingStyle quoting_style;


    ExternalQueryBuilder(
        const DictionaryStructure & dict_struct,
        const std::string & db,
        const std::string & table,
        const std::string & where,
        QuotingStyle quoting_style);

    /** Generate a query to load all data. */
    std::string composeLoadAllQuery() const;

    /** Generate a query to load data after certain time point*/
    std::string composeUpdateQuery(const std::string &update_field, const std::string &time_point) const;

    /** Generate a query to load data by set of UInt64 keys. */
    std::string composeLoadIdsQuery(const std::vector<UInt64> & ids);

    /** Generate a query to load data by set of composite keys.
      * There are two methods of specification of composite keys in WHERE:
      * 1. (x = c11 AND y = c12) OR (x = c21 AND y = c22) ...
      * 2. (x, y) IN ((c11, c12), (c21, c22), ...)
      */
    enum LoadKeysMethod
    {
        AND_OR_CHAIN,
        IN_WITH_TUPLES,
    };

    std::string composeLoadKeysQuery(
        const Columns & key_columns,
        const std::vector<size_t> & requested_rows,
        LoadKeysMethod method);


private:
    /// Expression in form (x = c1 AND y = c2 ...)
    void composeKeyCondition(const Columns & key_columns, const size_t row, WriteBuffer & out) const;

    /// Expression in form (x, y, ...)
    std::string composeKeyTupleDefinition() const;

    /// Expression in form (c1, c2, ...)
    void composeKeyTuple(const Columns & key_columns, const size_t row, WriteBuffer & out) const;

    /// Write string with specified quoting style.
    void writeQuoted(const std::string & s, WriteBuffer & out) const;
};

}
