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

const std::unordered_map<String, std::tuple<TypeIndex, String, decltype(&transformToSubcolumn)>> function_to_subcolumn =
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

}

void RewriteFunctionToSubcolumnData::visit(ASTFunction & function, ASTPtr & ast) const
{
    const auto & arguments = function.arguments->children;
    if (arguments.size() != 1)
        return;

    const auto * identifier = arguments[0]->as<ASTIdentifier>();
    if (!identifier)
        return;

    auto it = function_to_subcolumn.find(function.name);
    if (it == function_to_subcolumn.end())
        return;

    const auto & [type_id, subcolumn_name, transformer] = it->second;
    const auto & columns = metadata_snapshot->getColumns();
    const auto & name_in_storage = identifier->name();

    if (columns.has(name_in_storage)
        && columns.get(name_in_storage).type->getTypeId() == type_id)
    {
        ast = transformer(name_in_storage, subcolumn_name);
    }
}

}
