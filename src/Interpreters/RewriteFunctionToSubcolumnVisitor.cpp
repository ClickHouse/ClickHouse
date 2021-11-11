#include <Interpreters/RewriteFunctionToSubcolumnVisitor.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

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

}

void RewriteFunctionToSubcolumnData::visit(ASTFunction & function, ASTPtr & ast) const
{
    const auto & arguments = function.arguments->children;
    if (arguments.empty() || arguments.size() > 2)
        return;

    const auto * identifier = arguments[0]->as<ASTIdentifier>();
    if (!identifier)
        return;

    const auto & columns = metadata_snapshot->getColumns();
    const auto & name_in_storage = identifier->name();

    if (!columns.has(name_in_storage))
        return;

    TypeIndex column_type_id = columns.get(name_in_storage).type->getTypeId();

    if (arguments.size() == 1)
    {
        auto it = unary_function_to_subcolumn.find(function.name);
        if (it != unary_function_to_subcolumn.end())
        {
            const auto & [type_id, subcolumn_name, transformer] = it->second;
            if (column_type_id == type_id)
                ast = transformer(name_in_storage, subcolumn_name);
        }
    }
    else
    {
        auto it = binary_function_to_subcolumn.find(function.name);
        if (it != binary_function_to_subcolumn.end())
        {
            const auto & [type_id, subcolumn_name, transformer] = it->second;
            if (column_type_id == type_id)
                ast = transformer(name_in_storage, subcolumn_name, arguments[1]);
        }
    }
}

}
