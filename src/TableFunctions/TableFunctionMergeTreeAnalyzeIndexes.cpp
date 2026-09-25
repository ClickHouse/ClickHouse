#include <DataTypes/DataTypeArray.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <Core/NamesAndTypes.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/StorageMergeTreeAnalyzeIndexes.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <fmt/ranges.h>
#include <Storages/MergeTree/VectorSearchUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <array>
#include <optional>
#include <string_view>

namespace
{

const char * mergeTreeAnalyzeIndexFunctionName(bool resolve_by_uuid)
{
    if (resolve_by_uuid)
        return "mergeTreeAnalyzeIndexesUUID";
    else
        return "mergeTreeAnalyzeIndexes";
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

/// An element of another type leaves the argument malformed, which the caller reports as
/// `BAD_ARGUMENTS` instead of letting it escape as an internal `BAD_GET` from `Field::safeGet`.
static std::optional<Strings> tryExtractStrings(const Array & elements)
{
    Strings result;
    result.reserve(elements.size());
    for (const auto & element : elements)
    {
        if (element.getType() != Field::Types::String)
            return {};
        result.push_back(element.safeGet<String>());
    }
    return result;
}

/// Both `['a', 'b']` and `array('a', 'b')` are parsed as `_CAST(['a', 'b'], 'Array(String)')` with analyzer.
/// While for non-analyzer there is no _CAST
static Strings extractParts(const ASTPtr & argument, const ContextPtr & context)
{
    ASTPtr array = argument;
    if (const auto * func = array->as<ASTFunction>())
    {
        if (func->name == "_CAST" && func->arguments) /// _CAST([], 'Array(String)')
            array = func->arguments->children.at(0);
        else if (func->name == "array") /// array(ExpressionList)
            array = func->arguments;
        else
            array = ASTPtr();
    }

    if (array)
    {
        if (const auto * literal = array->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::Array)
        {
            if (auto parts = tryExtractStrings(literal->value.safeGet<Array>()))
                return std::move(*parts);
        }

        if (const auto * expr_list = array->as<ASTExpressionList>())
        {
            Array elements;
            elements.reserve(expr_list->children.size());
            for (const auto & element : expr_list->children)
                elements.push_back(evaluateConstantExpressionAsLiteral(element, context)->as<ASTLiteral &>().value);

            if (auto parts = tryExtractStrings(elements))
                return std::move(*parts);
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parts must be an array of strings, got: {}", argument->formatForLogging());
}

/// The arguments of an optimization, in the same shapes as `extractParts` above accepts: an array
/// literal wrapped in a `_CAST`, or an `array(...)` call. An argument list of mixed types - the shape
/// `buildAnalyzeIndexQuery` sends - is an `array(...)` call, either bare or, with `use_variant_as_common_type`,
/// wrapped in a `_CAST` to an array of `Variant`. Every element is evaluated on its own, so each keeps its own type.
static Array extractOptimizationArguments(const ASTPtr & argument, const ContextPtr & context)
{
    ASTPtr array = argument;
    if (const auto * func = array->as<ASTFunction>())
    {
        if (func->name == "_CAST" && func->arguments && !func->arguments->children.empty()) /// _CAST([...], 'Array(String)')
            array = func->arguments->children.at(0);

        if (const auto * inner = array->as<ASTFunction>())
        {
            if (inner->name == "array" && inner->arguments) /// array(ExpressionList)
                array = inner->arguments;
            else
                array = ASTPtr();
        }
    }

    if (array)
    {
        if (const auto * literal = array->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::Array)
            return literal->value.safeGet<Array>();

        if (const auto * expr_list = array->as<ASTExpressionList>())
        {
            Array result;
            for (const auto & element : expr_list->children)
                result.push_back(evaluateConstantExpressionAsLiteral(element, context)->as<ASTLiteral &>().value);
            return result;
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Arguments of an optimization must be an array of its parameters, got: {}", argument->formatForLogging());
}

/// The six parameters of the `vector_search_index_analysis` optimization, in the order `buildAnalyzeIndexQuery`
/// sends them. Every slot is checked explicitly, so that a malformed list is reported as `BAD_ARGUMENTS` that
/// names the offending parameter instead of escaping as an internal `BAD_GET` from `Field::safeGet`.
static constexpr std::array<std::string_view, 6> vector_search_parameter_names
    = {"column", "distance function", "limit", "search vector", "additional filters present", "return distances"};

[[noreturn]] static void throwBadVectorSearchArgument(const Field & field, size_t index, std::string_view expected)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Parameter #{} ({}) of the 'vector_search_index_analysis' optimization must be {}, got {}: {}",
        index + 1, vector_search_parameter_names[index], expected, field.getTypeName(), applyVisitor(FieldVisitorToString(), field));
}

static const String & getVectorSearchStringArgument(const Array & args, size_t index)
{
    const Field & field = args[index];
    if (field.getType() != Field::Types::String)
        throwBadVectorSearchArgument(field, index, "a string");
    return field.safeGet<String>();
}

/// A signed literal such as `toInt64(3)` is accepted as long as it is non-negative.
static UInt64 getVectorSearchUnsignedArgument(const Array & args, size_t index)
{
    const Field & field = args[index];
    if (field.getType() == Field::Types::UInt64)
        return field.safeGet<UInt64>();
    if (field.getType() == Field::Types::Int64)
    {
        Int64 value = field.safeGet<Int64>();
        if (value >= 0)
            return static_cast<UInt64>(value);
    }
    throwBadVectorSearchArgument(field, index, "a non-negative integer");
}

/// `buildAnalyzeIndexQuery` formats the flags as `true` / `false`, a hand-written list is likely to use `1` / `0`.
static bool getVectorSearchBoolArgument(const Array & args, size_t index)
{
    const Field & field = args[index];
    switch (field.getType())
    {
        case Field::Types::Bool:
            return field.safeGet<bool>();
        case Field::Types::UInt64:
        case Field::Types::Int64:
        {
            Int64 value = field.safeGet<Int64>();
            if (value == 0 || value == 1)
                return value == 1;
            break;
        }
        default:
            break;
    }
    throwBadVectorSearchArgument(field, index, "a boolean or 0/1");
}

/// The search vector is sent as an array of `Float64`, a hand-written list may contain integer literals.
static VectorWithMemoryTracking<Float64> getVectorSearchReferenceVector(const Array & args, size_t index)
{
    const Field & field = args[index];
    if (field.getType() != Field::Types::Array)
        throwBadVectorSearchArgument(field, index, "an array of numbers");

    VectorWithMemoryTracking<Float64> result;
    for (const auto & element : field.safeGet<Array>())
    {
        switch (element.getType())
        {
            case Field::Types::Float64:
                result.push_back(element.safeGet<Float64>());
                break;
            case Field::Types::UInt64:
                result.push_back(static_cast<Float64>(element.safeGet<UInt64>()));
                break;
            case Field::Types::Int64:
                result.push_back(static_cast<Float64>(element.safeGet<Int64>()));
                break;
            default:
                throwBadVectorSearchArgument(field, index, "an array of numbers");
        }
    }
    return result;
}

class TableFunctionMergeTreeAnalyzeIndexes : public ITableFunction
{
public:
    explicit TableFunctionMergeTreeAnalyzeIndexes(bool resolve_by_uuid_)
        : resolve_by_uuid(resolve_by_uuid_)
    {}

    std::string getName() const override { return mergeTreeAnalyzeIndexFunctionName(resolve_by_uuid); }

    /// The returned storage holds its source table's storage object, so a persisted table would keep the source undroppable.
    bool canBeUsedToCreateTable() const override { return false; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// Technically it's MergeTreeAnalyzeIndexes but it doesn't register itself
        return "";
    }

    void parseArgumentsUUID(const ASTs & args_func, ContextPtr context);
    void parseArgumentsDatabaseTable(const ASTs & args_func, ContextPtr context);

    /// 2 features will benefit from distributed index load + analysis:
    /// a) vector search with large vector indexes
    /// b) top-k using only minmax index (e.g SELECT * FROM youtube ORDER BY dislike_count LIMIT 10)
    /// These 2 cannot be packaged in the 'predicate'
    void parseArgumentsForOptimizations(const ASTs & args, ContextPtr context, size_t start_index);

    const bool resolve_by_uuid;
    StorageID source_table_id{StorageID::createEmpty()};
    Strings parts;
    ASTPtr predicate;
    OptionalVectorSearchParameters vector_search_parameters;
};

VectorWithMemoryTracking<size_t> TableFunctionMergeTreeAnalyzeIndexes::skipAnalysisForArguments(const QueryTreeNodePtr & /* query_node_table_function */, ContextPtr /* context */) const
{
    /// Filter should not be analyzed
    if (resolve_by_uuid)
        return {1};
    else
        return {2};
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments.", quoteString(getName()));

    if (resolve_by_uuid)
        parseArgumentsUUID(args_func, context);
    else
        parseArgumentsDatabaseTable(args_func, context);
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArgumentsUUID(const ASTs & args_func, ContextPtr context)
{
    ASTs & args = args_func.at(0)->children;
    /// clang-tidy suggest to use args.empty() over args.size() < 1, which looks wrong here, but OK, let's use empty()
    if (args.empty() || args.size() > 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have from 1 to 3 or 5 arguments (UUID, condition[, parts_array], [, optimization, args_array]), got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionAsLiteral(args[0], context);
    auto uuid = parseFromString<UUID>(checkAndGetLiteralArgument<String>(args[0], "UUID"));

    if (args.size() > 1)
        predicate = args[1]->clone();

    if (args.size() > 2)
        parts = extractParts(args[2], context);

    if (args.size() > 3)
    {
        if (args.size() < 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Not enough arguments: no args_array for optimization");
        parseArgumentsForOptimizations(args, context, 3);
    }

    source_table_id = StorageID{/*database=*/ "", /*table=*/ "", uuid};
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArgumentsDatabaseTable(const ASTs & args_func, ContextPtr context)
{
    ASTs & args = args_func.at(0)->children;
    if (args.size() < 2 || args.size() > 6)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have from 2 to 4 or 6 arguments (database, table, condition[, parts_array], [, optimization, args_array]), got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    auto database = checkAndGetLiteralArgument<String>(args[0], "database");

    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    auto table = checkAndGetLiteralArgument<String>(args[1], "table");

    if (args.size() > 2)
        predicate = args[2]->clone();

    if (args.size() > 3)
        parts = extractParts(args[3], context);

    if (args.size() > 4)
    {
        if (args.size() < 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Not enough arguments: no args_array for optimization");
        parseArgumentsForOptimizations(args, context, 4);
    }

    source_table_id = StorageID{database, table};
}

void TableFunctionMergeTreeAnalyzeIndexes::parseArgumentsForOptimizations(const ASTs & args, ContextPtr context, size_t start_index)
{
    auto optimization = checkAndGetLiteralArgument<String>(args[start_index++], "extra_optimization");
    if (optimization == "vector_search_index_analysis")
    {
        auto vector_search_args = extractOptimizationArguments(args[start_index++], context);
        if (vector_search_args.size() != vector_search_parameter_names.size())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "The 'vector_search_index_analysis' optimization requires {} parameters ({}), got {}",
                vector_search_parameter_names.size(), fmt::join(vector_search_parameter_names, ", "), vector_search_args.size());

        vector_search_parameters = VectorSearchParameters{
            getVectorSearchStringArgument(vector_search_args, 0),
            getVectorSearchStringArgument(vector_search_args, 1),
            getVectorSearchUnsignedArgument(vector_search_args, 2),
            getVectorSearchReferenceVector(vector_search_args, 3),
            getVectorSearchBoolArgument(vector_search_args, 4),
            getVectorSearchBoolArgument(vector_search_args, 5)};
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown optimization {}, the only supported one is 'vector_search_index_analysis'", quoteString(optimization));
    }
}

ColumnsDescription TableFunctionMergeTreeAnalyzeIndexes::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription(NamesAndTypesList({
        {"part_name", std::make_shared<DataTypeString>()},
        {"ranges", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeUInt64>(), // begin
            std::make_shared<DataTypeUInt64>(), // end
        }))},
    }));
}

StoragePtr TableFunctionMergeTreeAnalyzeIndexes::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    StoragePtr source_table;
    if (source_table_id.hasUUID())
    {
        /// Note, there is no getByUUID() at the time of writing, hence using try*() methods.
        auto database_and_table = DatabaseCatalog::instance().tryGetByUUID(source_table_id.uuid);
        source_table = DatabaseCatalog::instance().tryGetByUUID(source_table_id.uuid).second;
        if (!source_table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table with UUID {} does not exist", source_table_id.uuid);
    }
    else
        source_table = DatabaseCatalog::instance().getTable(source_table_id, context);

    auto columns = getActualTableStructure(context, is_insert_query);
    StorageID storage_id(getDatabaseName(), table_name);

    auto res = std::make_shared<StorageMergeTreeAnalyzeIndexes>(
        std::move(storage_id),
        std::move(source_table),
        std::move(columns),
        parts,
        predicate,
        vector_search_parameters);
    res->startup();
    return res;
}

void registerTableFunctionMergeTreeAnalyzeIndexes(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeAnalyzeIndexes(TableFunctionFactory & factory)
{
    factory.registerFunction(mergeTreeAnalyzeIndexFunctionName(/*resolve_by_uuid=*/ false), TableFunctionFactoryData{
        []() { return std::make_shared<TableFunctionMergeTreeAnalyzeIndexes>(/* resolve_by_uuid_= */ false); },
        {
            .description = "Internal function for index analysis",
            .syntax = "mergeTreeAnalyzeIndexes(currentDatabase(), mt_table, predicate[, ['part1', 'part2']])",
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = true}
    });

    factory.registerFunction(mergeTreeAnalyzeIndexFunctionName(/*resolve_by_uuid=*/ true), TableFunctionFactoryData{
        []() { return std::make_shared<TableFunctionMergeTreeAnalyzeIndexes>(/* resolve_by_uuid_= */ true); },
        {
            .description = "Internal function for index analysis",
            .syntax = "mergeTreeAnalyzeIndexesUUID('table_uuid', predicate[, ['part1', 'part2']])",
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = true}
    });
}

}
