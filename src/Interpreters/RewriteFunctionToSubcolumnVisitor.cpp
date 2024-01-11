#include <Interpreters/RewriteFunctionToSubcolumnVisitor.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace
{

ASTPtr transformToSubcolumn(const String & name_in_storage, const String & subcolumn_name)
{
    return std::make_shared<ASTIdentifier>(Nested::concatenateName(name_in_storage, subcolumn_name));
}

ASTPtr transformEmptyToSubcolumn(const String & name_in_storage, const String & subcolumn_name)
{
    auto ast = transformToSubcolumn(name_in_storage, subcolumn_name);
    return makeASTFunction("equals", ast, std::make_shared<ASTLiteral>(0u));
}

ASTPtr transformNotEmptyToSubcolumn(const String & name_in_storage, const String & subcolumn_name)
{
    auto ast = transformToSubcolumn(name_in_storage, subcolumn_name);
    return makeASTFunction("notEquals", ast, std::make_shared<ASTLiteral>(0u));
}

ASTPtr transformIsNotNullToSubcolumn(const String & name_in_storage, const String & subcolumn_name)
{
    auto ast = transformToSubcolumn(name_in_storage, subcolumn_name);
    return makeASTFunction("not", ast);
}

ASTPtr transformCountNullableToSubcolumn(const String & name_in_storage, const String & subcolumn_name)
{
    auto ast = transformToSubcolumn(name_in_storage, subcolumn_name);
    return makeASTFunction("sum", makeASTFunction("not", ast));
}

ASTPtr transformMapContainsToSubcolumn(const String & name_in_storage, const String & subcolumn_name, const ASTPtr & arg)
{
    auto ast = transformToSubcolumn(name_in_storage, subcolumn_name);
    return makeASTFunction("has", ast, arg);
}

const std::unordered_map<String, std::tuple<TypeIndex, String, decltype(&transformToSubcolumn)>> unary_function_to_subcolumn =
{
    {"length",    {TypeIndex::Array, "size0", transformToSubcolumn}},
    {"empty",     {TypeIndex::Array, "size0", transformEmptyToSubcolumn}},
    {"notEmpty",  {TypeIndex::Array, "size0", transformNotEmptyToSubcolumn}},
    {"isNull",    {TypeIndex::Nullable, "null", transformToSubcolumn}},
    {"isNotNull", {TypeIndex::Nullable, "null", transformIsNotNullToSubcolumn}},
    {"count",     {TypeIndex::Nullable, "null", transformCountNullableToSubcolumn}},
    {"mapKeys",   {TypeIndex::Map, "keys", transformToSubcolumn}},
    {"mapValues", {TypeIndex::Map, "values", transformToSubcolumn}},
};

const std::unordered_map<String, std::tuple<TypeIndex, String, decltype(&transformMapContainsToSubcolumn)>> binary_function_to_subcolumn
{
    {"mapContains", {TypeIndex::Map, "keys", transformMapContainsToSubcolumn}},
};

std::optional<NameAndTypePair> getColumnFromArgumentsToOptimize(
    const ASTs & arguments,
    const StorageMetadataPtr & metadata_snapshot)
{
    if (arguments.empty() || arguments.size() > 2)
        return {};

    const auto * identifier = arguments[0]->as<ASTIdentifier>();
    if (!identifier)
        return {};

    const auto & columns = metadata_snapshot->getColumns();
    const auto & name_in_storage = identifier->name();

    if (!columns.has(name_in_storage))
        return {};

    const auto & column_type = columns.get(name_in_storage).type;
    return NameAndTypePair{name_in_storage, column_type};
}

}

void RewriteFunctionToSubcolumnFirstPassMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        ++data.indentifiers_count[identifier->name()];
        return;
    }

    if (const auto * function = ast->as<ASTFunction>())
    {
        visit(*function, data);
        return;
    }
}

void RewriteFunctionToSubcolumnFirstPassMatcher::visit(const ASTFunction & function, Data & data)
{
    const auto & arguments = function.arguments->children;
    auto column = getColumnFromArgumentsToOptimize(arguments, data.metadata_snapshot);
    if (!column)
        return;

    auto column_type_id = column->type->getTypeId();

    if (arguments.size() == 1)
    {
        auto it = unary_function_to_subcolumn.find(function.name);
        if (it != unary_function_to_subcolumn.end() && std::get<0>(it->second) == column_type_id)
            ++data.optimized_identifiers_count[column->name];
    }
    else
    {
        if (function.name == "tupleElement" && column_type_id == TypeIndex::Tuple)
        {
            const auto * literal = arguments[1]->as<ASTLiteral>();
            if (!literal)
                return;

            auto value_type = literal->value.getType();
            if (value_type == Field::Types::UInt64 || value_type == Field::Types::String)
                ++data.optimized_identifiers_count[column->name];
        }
        else
        {
            auto it = binary_function_to_subcolumn.find(function.name);
            if (it != binary_function_to_subcolumn.end() && std::get<0>(it->second) == column_type_id)
                ++data.optimized_identifiers_count[column->name];
        }
    }
}

void RewriteFunctionToSubcolumnSecondPassData::visit(ASTFunction & function, ASTPtr & ast) const
{
    const auto & arguments = function.arguments->children;
    auto column = getColumnFromArgumentsToOptimize(arguments, metadata_snapshot);
    if (!column)
        return;

    auto column_type_id = column->type->getTypeId();
    const auto & alias = function.tryGetAlias();

    if (arguments.size() == 1)
    {
        auto it = unary_function_to_subcolumn.find(function.name);
        if (it == unary_function_to_subcolumn.end())
            return;

        const auto & [expected_type_id, subcolumn_name, transformer] = it->second;
        if (column_type_id != expected_type_id)
            return;

        ast = transformer(column->name, subcolumn_name);
        ast->setAlias(alias);
    }
    else if (arguments.size() == 2)
    {
        if (function.name == "tupleElement" && column_type_id == TypeIndex::Tuple)
        {
            const auto * literal = arguments[1]->as<ASTLiteral>();
            if (!literal)
                return;

            String subcolumn_name;
            auto value_type = literal->value.getType();
            if (value_type == Field::Types::UInt64)
            {
                const auto & type_tuple = assert_cast<const DataTypeTuple &>(*column->type);
                auto index = literal->value.get<UInt64>();
                subcolumn_name = type_tuple.getNameByPosition(index);
            }
            else if (value_type == Field::Types::String)
            {
                subcolumn_name = literal->value.get<const String &>();
            }
            else
            {
                return;
            }

            ast = transformToSubcolumn(column->name, subcolumn_name);
            ast->setAlias(alias);
        }
        else
        {
            auto it = binary_function_to_subcolumn.find(function.name);
            if (it == binary_function_to_subcolumn.end())
                return;

            const auto & [expected_type_id, subcolumn_name, transformer] = it->second;
            if (column_type_id != expected_type_id)
                return;

            ast = transformer(column->name, subcolumn_name, arguments[1]);
            ast->setAlias(alias);
        }
    }
}

}
