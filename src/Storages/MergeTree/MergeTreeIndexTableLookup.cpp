#include <Storages/MergeTree/MergeTreeIndexTableLookup.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/hasNullable.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_INDEX;
    extern const int INCORRECT_QUERY;
}

bool isLookupIndexType(std::string_view type)
{
    return type == TABLE_SET_INDEX_TYPE || type == TABLE_JOIN_INDEX_TYPE;
}

namespace
{

void validateTableLookupKeys(const IndexDescription & index, std::string_view index_type)
{
    if (index.arguments && !index.arguments->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index of type '{}' does not accept arguments", index_type);

    if (!index.expression_list_ast || index.expression_list_ast->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index '{}' must have at least one key column", index.name);

    std::unordered_set<String> unique_columns;
    for (const auto & child : index.expression_list_ast->children)
    {
        const auto * identifier = child->as<ASTIdentifier>();
        if (!identifier)
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Index of type '{}' supports only plain column names in expression, got '{}'",
                index_type,
                child->formatForErrorMessage());
        }

        if (!unique_columns.emplace(identifier->name()).second)
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Index of type '{}' contains duplicate key column '{}'",
                index_type,
                identifier->name());
        }
    }
}

IndicesDescription buildLookupIndices(const ASTs & lookup_index_asts, const ColumnsDescription & columns, ContextPtr context)
{
    IndicesDescription result;

    for (const auto & lookup_index_ast : lookup_index_asts)
    {
        IndexDescription index = IndexDescription::getIndexFromAST(
            lookup_index_ast->clone(),
            columns,
            /* is_implicitly_created */ false,
            /* escape_filenames */ true,
            context);

        if (result.has(index.name))
            throw Exception(ErrorCodes::ILLEGAL_INDEX, "Duplicated lookup index name {} is not allowed. Please use a different index name", backQuoteIfNeed(index.name));

        validateLookupIndex(index);

        if (auto * index_ast = typeid_cast<ASTIndexDeclaration *>(index.definition_ast.get()))
        {
            index_ast->granularity = 0;
            index_ast->is_lookup_index = true;
        }

        result.push_back(std::move(index));
    }

    return result;
}

}

void validateLookupIndex(const IndexDescription & index)
{
    if (index.type == TABLE_SET_INDEX_TYPE)
    {
        validateTableLookupKeys(index, TABLE_SET_INDEX_TYPE);
        return;
    }

    if (index.type == TABLE_JOIN_INDEX_TYPE)
    {
        validateTableLookupKeys(index, TABLE_JOIN_INDEX_TYPE);

        for (const auto & type : index.data_types)
        {
            if (isNullableOrLowCardinalityNullable(type))
            {
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "Index of type '{}' does not support nullable key columns",
                    TABLE_JOIN_INDEX_TYPE);
            }
        }

        return;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "`LOOKUP INDEX` supports only '{}' and '{}', got '{}'",
        TABLE_SET_INDEX_TYPE,
        TABLE_JOIN_INDEX_TYPE,
        index.type);
}

IndicesDescription getLookupIndicesFromAST(const ASTExpressionList * lookup_indices_ast, const ColumnsDescription & columns, ContextPtr context)
{
    if (!lookup_indices_ast)
        return {};

    return buildLookupIndices(lookup_indices_ast->children, columns, context);
}

IndicesDescription parseLookupIndices(const String & str, const ColumnsDescription & columns, ContextPtr context)
{
    if (str.empty())
        return {};

    ParserList parser(
        std::make_unique<ParserIndexDeclaration>(false),
        std::make_unique<ParserToken>(TokenType::Comma),
        false);
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return buildLookupIndices(list->children, columns, context);
}

}
