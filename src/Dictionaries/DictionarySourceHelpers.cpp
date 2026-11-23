#include <Dictionaries/DictionarySourceHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int INCORRECT_QUERY;
}

void checkDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message)
{
    const char * query_begin = query.data();
    const char * query_end = query.data() + query.size();
    ParserQuery parser(query_end);
    ASTPtr ast = parseQuery(parser, query_begin, query_end, description, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    if (!ast || ast->getQueryKind() != IAST::QueryKind::Select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);

    /// `INTO OUTFILE` is a part of a `SELECT` for ClickHouse, but an external database (MySQL) writes the file on its host.
    if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()); query_with_output && query_with_output->out_file)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{}", error_message);
}

void checkExternalDictionarySourceQueries(
    const ExternalQueryBuilder & query_builder, const String & update_field, const String & invalidate_query, const String & source_name)
{
    static constexpr auto source_error = "Only SELECT query can be used as a dictionary source";
    checkDictionaryQueryIsSelect(query_builder.composeLoadAllQuery(), "Query for " + source_name + " dictionary", source_error);

    /// The value of `update_field` is inserted into the query verbatim.
    if (!update_field.empty())
        checkDictionaryQueryIsSelect(
            query_builder.composeUpdateQuery(update_field, "1970-01-01 00:00:00"),
            "Update query for " + source_name + " dictionary",
            source_error);

    if (!invalidate_query.empty())
        checkDictionaryQueryIsSelect(
            invalidate_query,
            "Invalidate query for " + source_name + " dictionary",
            "Only SELECT query can be used as a dictionary invalidate query");
}

/// For simple key

Block blockForIds(
    const DictionaryStructure & dict_struct,
    const VectorWithMemoryTracking<UInt64> & ids)
{
    auto column = ColumnUInt64::create(ids.size());
    memcpy(column->getData().data(), ids.data(), ids.size() * sizeof(ids.front()));

    Block block{{std::move(column), std::make_shared<DataTypeUInt64>(), (*dict_struct.id).name}};

    return block;
}

/// For composite key

Block blockForKeys(
    const DictionaryStructure & dict_struct,
    const Columns & key_columns,
    const VectorWithMemoryTracking<size_t> & requested_rows)
{
    Block block;

    for (size_t i = 0, size = key_columns.size(); i < size; ++i)
    {
        const ColumnPtr & source_column = key_columns[i];
        size_t column_rows_size = source_column->size();

        PaddedPODArray<UInt8> filter(column_rows_size, false);

        for (size_t idx : requested_rows)
            filter[idx] = true;

        auto filtered_column = source_column->filter(filter, requested_rows.size());

        block.insert({filtered_column, (*dict_struct.key)[i].type, (*dict_struct.key)[i].name});
    }

    return block;
}


SettingsChanges readSettingsFromDictionaryConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".settings"))
        return {};

    const auto prefix = config_prefix + ".settings";

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(prefix, config_keys);

    SettingsChanges changes;

    for (const std::string & key : config_keys)
    {
        const auto value = config.getString(prefix + "." + key);
        changes.emplace_back(key, value);
    }

    return changes;
}


ContextMutablePtr copyContextAndApplySettingsFromDictionaryConfig(
    const ContextPtr & context, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    auto context_copy = Context::createCopy(context);
    auto changes = readSettingsFromDictionaryConfig(config, config_prefix);
    context_copy->applySettingsChanges(changes);
    return context_copy;
}

static Block transformHeader(Block header, Block block_to_add)
{
    for (Int64 i = static_cast<Int64>(block_to_add.columns() - 1); i >= 0; --i)
        header.insert(0, block_to_add.getByPosition(i).cloneEmpty());

    return header;
}

TransformWithAdditionalColumns::TransformWithAdditionalColumns(
    SharedHeader block_to_add_, SharedHeader header)
    : ISimpleTransform(header, std::make_shared<const Block>(transformHeader(*header, *block_to_add_)), true)
    , block_to_add(std::move(block_to_add_))
{
}

void TransformWithAdditionalColumns::transform(Chunk & chunk)
{
    if (chunk)
    {
        auto num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        auto cut_block = block_to_add->cloneWithCutColumns(current_range_index, num_rows);

        if (cut_block.rows() != num_rows)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Number of rows in block to add after cut must equal to number of rows in block from inner stream");

        for (Int64 i = static_cast<Int64>(cut_block.columns() - 1); i >= 0; --i)
            columns.insert(columns.begin(), cut_block.getByPosition(i).column);

        current_range_index += num_rows;
        chunk.setColumns(std::move(columns), num_rows);
    }
}

String TransformWithAdditionalColumns::getName() const
{
    return "TransformWithAdditionalColumns";
}

}
