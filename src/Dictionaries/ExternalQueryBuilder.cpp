#include "ExternalQueryBuilder.h"

#include <boost/range/join.hpp>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryStructure.h>


namespace DB
{

static inline void writeParenthesisedString(const String & s, WriteBuffer & buf)
{
    writeChar('(', buf);
    writeString(s, buf);
    writeChar(')', buf);
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

static constexpr std::string_view CONDITION_PLACEHOLDER_TO_REPLACE_VALUE = "{condition}";

ExternalQueryBuilder::ExternalQueryBuilder(
    const DictionaryStructure & dict_struct_,
    const std::string & db_,
    const std::string & schema_,
    const std::string & table_,
    const std::string & query_,
    const std::string & where_,
    IdentifierQuotingStyle quoting_style_)
    : dict_struct(dict_struct_)
    , db(db_)
    , schema(schema_)
    , table(table_)
    , query(query_)
    , where(where_)
    , quoting_style(quoting_style_)
{
    if (table.empty() && query.empty())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Setting `table` or `query` must be non empty");

    if (!query.empty() && (!table.empty() || !where.empty()))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Setting `table` or `where` cannot be used with `query` parameter");
}


void ExternalQueryBuilder::writeQuoted(const std::string & s, WriteBuffer & out) const
{
    switch (quoting_style)
    {
        case IdentifierQuotingStyle::None:
            writeString(s, out);
            break;

        case IdentifierQuotingStyle::Backticks:
            writeBackQuotedString(s, out);
            break;

        case IdentifierQuotingStyle::DoubleQuotes:
            writeDoubleQuotedString(s, out);
            break;

        case IdentifierQuotingStyle::BackticksMySQL:
            writeBackQuotedStringMySQL(s, out);
            break;
    }
}


std::string ExternalQueryBuilder::composeLoadAllQuery() const
{
    if (query.empty())
    {
        WriteBufferFromOwnString out;
        composeLoadAllQuery(out);
        writeChar(';', out);
        return out.str();
    }
    else
    {
        /** In case UPDATE_FIELD is specified in {condition} for dictionary that must load all data.
          * Replace {condition} with true_condition for initial dictionary load.
          * For next dictionary loads {condition} will be updated with UPDATE_FIELD.
          */
        static constexpr auto true_condition = "(1 = 1)";
        auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
        if (condition_position != std::string::npos)
        {
            auto query_copy = query;
            query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), true_condition);
            return query_copy;
        }

        return query;
    }
}

void ExternalQueryBuilder::composeLoadAllQuery(WriteBuffer & out) const
{
    writeString("SELECT ", out);

    if (dict_struct.id)
    {
        if (!dict_struct.id->expression.empty())
        {
            writeParenthesisedString(dict_struct.id->expression, out);
            writeString(" AS ", out);
        }

        writeQuoted(dict_struct.id->name, out);

        if (dict_struct.range_min && dict_struct.range_max)
        {
            writeString(", ", out);

            if (!dict_struct.range_min->expression.empty())
            {
                writeParenthesisedString(dict_struct.range_min->expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(dict_struct.range_min->name, out);

            writeString(", ", out);

            if (!dict_struct.range_max->expression.empty())
            {
                writeParenthesisedString(dict_struct.range_max->expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(dict_struct.range_max->name, out);
        }
    }
    else if (dict_struct.key)
    {
        auto first = true;
        for (const auto & key : *dict_struct.key)
        {
            if (!first)
                writeString(", ", out);

            first = false;

            if (!key.expression.empty())
            {
                writeParenthesisedString(key.expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(key.name, out);
        }

        if (dict_struct.range_min && dict_struct.range_max)
        {
            writeString(", ", out);

            if (!dict_struct.range_min->expression.empty())
            {
                writeParenthesisedString(dict_struct.range_min->expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(dict_struct.range_min->name, out);

            writeString(", ", out);

            if (!dict_struct.range_max->expression.empty())
            {
                writeParenthesisedString(dict_struct.range_max->expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(dict_struct.range_max->name, out);
        }
    }

    for (const auto & attr : dict_struct.attributes)
    {
        writeString(", ", out);

        if (!attr.expression.empty())
        {
            writeParenthesisedString(attr.expression, out);
            writeString(" AS ", out);
        }

        writeQuoted(attr.name, out);
    }

    writeString(" FROM ", out);
    if (!db.empty())
    {
        writeQuoted(db, out);
        writeChar('.', out);
    }
    if (!schema.empty())
    {
        writeQuoted(schema, out);
        writeChar('.', out);
    }
    writeQuoted(table, out);

    if (!where.empty())
    {
        writeString(" WHERE ", out);
        writeString(where, out);
    }
}


std::string ExternalQueryBuilder::composeUpdateQuery(const std::string & update_field, const std::string & time_point) const
{
    WriteBufferFromOwnString out;

    if (query.empty())
    {
        composeLoadAllQuery(out);

        if (!where.empty())
            writeString(" AND ", out);
        else
            writeString(" WHERE ", out);

        composeUpdateCondition(update_field, time_point, out);

        writeChar(';', out);

        return out.str();
    }
    else
    {
        writeString(query, out);

        auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
        if (condition_position == std::string::npos)
        {
            writeString(" WHERE ", out);
            composeUpdateCondition(update_field, time_point, out);
            writeString(";", out);

            return out.str();
        }

        WriteBufferFromOwnString condition_value_buffer;
        composeUpdateCondition(update_field, time_point, condition_value_buffer);
        const auto & condition_value = condition_value_buffer.str();

        auto query_copy = query;
        query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), condition_value);

        return query_copy;
    }
}


std::string ExternalQueryBuilder::composeLoadIdsQuery(const std::vector<UInt64> & ids) const
{
    if (!dict_struct.id)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Simple key required for method");

    WriteBufferFromOwnString out;

    if (query.empty())
    {
        writeString("SELECT ", out);

        if (!dict_struct.id->expression.empty())
        {
            writeParenthesisedString(dict_struct.id->expression, out);
            writeString(" AS ", out);
        }

        writeQuoted(dict_struct.id->name, out);

        for (const auto & attr : dict_struct.attributes)
        {
            writeString(", ", out);

            if (!attr.expression.empty())
            {
                writeParenthesisedString(attr.expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(attr.name, out);
        }

        writeString(" FROM ", out);
        if (!db.empty())
        {
            writeQuoted(db, out);
            writeChar('.', out);
        }
        if (!schema.empty())
        {
            writeQuoted(schema, out);
            writeChar('.', out);
        }

        writeQuoted(table, out);

        writeString(" WHERE ", out);

        if (!where.empty())
        {
            writeString(where, out);
            writeString(" AND ", out);
        }

        composeIdsCondition(ids, out);
        writeString(";", out);

        return out.str();
    }
    else
    {
        writeString(query, out);

        auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
        if (condition_position == std::string::npos)
        {
            writeString(" WHERE ", out);
            composeIdsCondition(ids, out);
            writeString(";", out);

            return out.str();
        }

        WriteBufferFromOwnString condition_value_buffer;
        composeIdsCondition(ids, condition_value_buffer);
        const auto & condition_value = condition_value_buffer.str();

        auto query_copy = query;
        query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), condition_value);

        return query_copy;
    }
}


std::string ExternalQueryBuilder::composeLoadKeysQuery(
    const Columns & key_columns, const std::vector<size_t> & requested_rows, LoadKeysMethod method, size_t partition_key_prefix) const
{
    if (!dict_struct.key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Composite key required for method");

    if (key_columns.size() != dict_struct.key->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of key_columns does not equal to the size of dictionary key");

    WriteBufferFromOwnString out;

    if (query.empty())
    {
        writeString("SELECT ", out);

        auto first = true;
        for (const auto & key_or_attribute : boost::join(*dict_struct.key, dict_struct.attributes))
        {
            if (!first)
                writeString(", ", out);

            first = false;

            if (!key_or_attribute.expression.empty())
            {
                writeParenthesisedString(key_or_attribute.expression, out);
                writeString(" AS ", out);
            }

            writeQuoted(key_or_attribute.name, out);
        }

        writeString(" FROM ", out);
        if (!db.empty())
        {
            writeQuoted(db, out);
            writeChar('.', out);
        }
        if (!schema.empty())
        {
            writeQuoted(schema, out);
            writeChar('.', out);
        }

        writeQuoted(table, out);

        writeString(" WHERE ", out);

        if (!where.empty())
        {
            if (method != CASSANDRA_SEPARATE_PARTITION_KEY)
                writeString("(", out);
            writeString(where, out);
            if (method != CASSANDRA_SEPARATE_PARTITION_KEY)
                writeString(") AND (", out);
            else
                writeString(" AND ", out);
        }

        composeKeysCondition(key_columns, requested_rows, method, partition_key_prefix, out);

        writeString(";", out);

        return out.str();
    }
    else
    {
        writeString(query, out);

        auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
        if (condition_position == std::string::npos)
        {
            writeString(" WHERE ", out);
            composeKeysCondition(key_columns, requested_rows, method, partition_key_prefix, out);
            writeString(";", out);

            return out.str();
        }

        WriteBufferFromOwnString condition_value_buffer;
        composeKeysCondition(key_columns, requested_rows, method, partition_key_prefix, condition_value_buffer);
        const auto & condition_value = condition_value_buffer.str();

        auto query_copy = query;
        query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), condition_value);

        return query_copy;
    }
}


void ExternalQueryBuilder::composeKeyCondition(const Columns & key_columns, size_t row, WriteBuffer & out,
                                               size_t beg, size_t end) const
{
    auto first = true;
    for (size_t i = beg; i < end; ++i)
    {
        if (!first)
            writeString(" AND ", out);

        first = false;

        const auto & key_description = (*dict_struct.key)[i];

        /// key_i=value_i
        writeQuoted(key_description.name, out);
        writeString("=", out);
        key_description.type_serialization->serializeTextQuoted(*key_columns[i], row, out, format_settings);
    }
}


void ExternalQueryBuilder::composeInWithTuples(const Columns & key_columns, const std::vector<size_t> & requested_rows,
                                               WriteBuffer & out, size_t beg, size_t end) const
{
    composeKeyTupleDefinition(out, beg, end);
    writeString(" IN (", out);

    bool first = true;
    for (const auto row : requested_rows)
    {
        if (!first)
            writeString(", ", out);

        first = false;
        composeKeyTuple(key_columns, row, out, beg, end);
    }

    writeString(")", out);
}


void ExternalQueryBuilder::composeKeyTupleDefinition(WriteBuffer & out, size_t beg, size_t end) const
{
    if (!dict_struct.key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Composite key required for method");

    writeChar('(', out);

    auto first = true;
    for (size_t i = beg; i < end; ++i)
    {
        if (!first)
            writeString(", ", out);

        first = false;
        writeQuoted((*dict_struct.key)[i].name, out);
    }

    writeChar(')', out);
}


void ExternalQueryBuilder::composeKeyTuple(const Columns & key_columns, size_t row, WriteBuffer & out, size_t beg, size_t end) const
{
    writeString("(", out);

    auto first = true;
    for (size_t i = beg; i < end; ++i)
    {
        if (!first)
            writeString(", ", out);

        first = false;
        auto serialization = (*dict_struct.key)[i].type_serialization;
        serialization->serializeTextQuoted(*key_columns[i], row, out, format_settings);
    }

    writeString(")", out);
}

void ExternalQueryBuilder::composeUpdateCondition(const std::string & update_field, const std::string & time_point, WriteBuffer & out)
{
    writeString(update_field, out);
    writeString(" >= '", out);
    writeString(time_point, out);
    writeChar('\'', out);
}

void ExternalQueryBuilder::composeIdsCondition(const std::vector<UInt64> & ids, WriteBuffer & out) const
{
    writeQuoted(dict_struct.id->name, out);
    writeString(" IN (", out);

    auto first = true;
    for (const auto id : ids)
    {
        if (!first)
            writeString(", ", out);

        first = false;
        writeString(DB::toString(id), out);
    }

    writeString(")", out);
}

void ExternalQueryBuilder::composeKeysCondition(const Columns & key_columns, const std::vector<size_t> & requested_rows, LoadKeysMethod method, size_t partition_key_prefix, WriteBuffer & out) const
{
    bool first = true;

    if (method == AND_OR_CHAIN)
    {
        first = true;
        for (const auto row : requested_rows)
        {
            if (!first)
                writeString(" OR ", out);

            first = false;

            writeString("(", out);
            composeKeyCondition(key_columns, row, out, 0, key_columns.size());
            writeString(")", out);
        }
    }
    else if (method == IN_WITH_TUPLES)
    {
        composeInWithTuples(key_columns, requested_rows, out, 0, key_columns.size());
    }
    else /* if (method == CASSANDRA_SEPARATE_PARTITION_KEY) */
    {
        /// CQL does not allow using OR conditions
        /// and does not allow using multi-column IN expressions with partition key columns.
        /// So we have to use multiple queries with conditions like
        /// (partition_key_1 = val1 AND partition_key_2 = val2 ...) AND (clustering_key_1, ...) IN ((val3, ...), ...)
        /// for each partition key.
        /// `partition_key_prefix` is a number of columns from partition key.
        /// All `requested_rows` must have the same values of partition key.
        composeKeyCondition(key_columns, requested_rows.at(0), out, 0, partition_key_prefix);
        if (partition_key_prefix && partition_key_prefix < key_columns.size())
            writeString(" AND ", out);
        if (partition_key_prefix < key_columns.size())
            composeInWithTuples(key_columns, requested_rows, out, partition_key_prefix, key_columns.size());
    }

    if (!where.empty() && method != CASSANDRA_SEPARATE_PARTITION_KEY)
    {
        writeString(")", out);
    }
}

}
