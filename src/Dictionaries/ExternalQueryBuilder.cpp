#include "ExternalQueryBuilder.h"
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <boost/range/join.hpp>
#include <ext/range.h>
#include "DictionaryStructure.h"
#include "writeParenthesisedString.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}


ExternalQueryBuilder::ExternalQueryBuilder(
    const DictionaryStructure & dict_struct_,
    const std::string & db_,
    const std::string & table_,
    const std::string & where_,
    IdentifierQuotingStyle quoting_style_)
    : dict_struct(dict_struct_), db(db_), where(where_), quoting_style(quoting_style_)
{
    if (auto pos = table_.find('.'); pos != std::string::npos)
    {
        schema = table_.substr(0, pos);
        table = table_.substr(pos + 1);
    }
    else
    {
        schema = "";
        table = table_;
    }
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
    WriteBufferFromOwnString out;
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

    writeChar(';', out);

    return out.str();
}


std::string ExternalQueryBuilder::composeUpdateQuery(const std::string & update_field, const std::string & time_point) const
{
    std::string out = composeLoadAllQuery();
    std::string update_query;

    if (!where.empty())
        update_query = " AND " + update_field + " >= '" + time_point + "'";
    else
        update_query = " WHERE " + update_field + " >= '" + time_point + "'";

    return out.insert(out.size() - 1, update_query); /// This is done to insert "update_query" before "out"'s semicolon
}


std::string ExternalQueryBuilder::composeLoadIdsQuery(const std::vector<UInt64> & ids)
{
    if (!dict_struct.id)
        throw Exception{"Simple key required for method", ErrorCodes::UNSUPPORTED_METHOD};

    WriteBufferFromOwnString out;
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

    writeString(");", out);

    return out.str();
}


std::string
ExternalQueryBuilder::composeLoadKeysQuery(const Columns & key_columns, const std::vector<size_t> & requested_rows, LoadKeysMethod method)
{
    if (!dict_struct.key)
        throw Exception{"Composite key required for method", ErrorCodes::UNSUPPORTED_METHOD};

    WriteBufferFromOwnString out;
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
        writeString("(", out);
        writeString(where, out);
        writeString(") AND (", out);
    }

    if (method == AND_OR_CHAIN)
    {
        first = true;
        for (const auto row : requested_rows)
        {
            if (!first)
                writeString(" OR ", out);

            first = false;
            composeKeyCondition(key_columns, row, out);
        }
    }
    else /* if (method == IN_WITH_TUPLES) */
    {
        writeString(composeKeyTupleDefinition(), out);
        writeString(" IN (", out);

        first = true;
        for (const auto row : requested_rows)
        {
            if (!first)
                writeString(", ", out);

            first = false;
            composeKeyTuple(key_columns, row, out);
        }

        writeString(")", out);
    }

    if (!where.empty())
    {
        writeString(")", out);
    }

    writeString(";", out);

    return out.str();
}


void ExternalQueryBuilder::composeKeyCondition(const Columns & key_columns, const size_t row, WriteBuffer & out) const
{
    writeString("(", out);

    const auto keys_size = key_columns.size();
    auto first = true;
    for (const auto i : ext::range(0, keys_size))
    {
        if (!first)
            writeString(" AND ", out);

        first = false;

        const auto & key_description = (*dict_struct.key)[i];

        /// key_i=value_i
        writeString(key_description.name, out);
        writeString("=", out);
        key_description.type->serializeAsTextQuoted(*key_columns[i], row, out, format_settings);
    }

    writeString(")", out);
}


std::string ExternalQueryBuilder::composeKeyTupleDefinition() const
{
    if (!dict_struct.key)
        throw Exception{"Composite key required for method", ErrorCodes::UNSUPPORTED_METHOD};

    std::string result{"("};

    auto first = true;
    for (const auto & key : *dict_struct.key)
    {
        if (!first)
            result += ", ";

        first = false;
        result += key.name;
    }

    result += ")";

    return result;
}


void ExternalQueryBuilder::composeKeyTuple(const Columns & key_columns, const size_t row, WriteBuffer & out) const
{
    writeString("(", out);

    const auto keys_size = key_columns.size();
    auto first = true;
    for (const auto i : ext::range(0, keys_size))
    {
        if (!first)
            writeString(", ", out);

        first = false;
        (*dict_struct.key)[i].type->serializeAsTextQuoted(*key_columns[i], row, out, format_settings);
    }

    writeString(")", out);
}


}
