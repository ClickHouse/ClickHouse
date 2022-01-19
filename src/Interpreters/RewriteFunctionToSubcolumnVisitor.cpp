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

}

void FindIdentifiersForbiddenToReplaceToSubcolumnsMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (const auto * function = ast->as<ASTFunction>())
    {
        bool is_probably_good =
            unary_function_to_subcolumn.contains(function->name)
            || binary_function_to_subcolumn.contains(function->name)
            || function->name == "tupleElement";

        for (const auto & child : function->arguments->children)
        {
            if (!child->as<ASTIdentifier>() || !is_probably_good)
                visit(child, data);
        }
    }
    else if (ast->as<ASTIdentifier>())
    {
        data.forbidden_identifiers.insert(ast->getColumnName());
    }
}

bool FindIdentifiersForbiddenToReplaceToSubcolumnsMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    /// Will handle ASTFunction customly.
    return !node->as<ASTFunction>();
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

    if (!columns.has(name_in_storage) || forbidden_identifiers.contains(name_in_storage))
        return;

    const auto & column_type = columns.get(name_in_storage).type;
    TypeIndex column_type_id = column_type->getTypeId();
    ASTPtr transformed_ast;

    if (arguments.size() == 1)
    {
        auto it = unary_function_to_subcolumn.find(function.name);
        if (it != unary_function_to_subcolumn.end())
        {
            const auto & [type_id, subcolumn_name, transformer] = it->second;
            if (column_type_id == type_id)
                transformed_ast = transformer(name_in_storage, subcolumn_name);
        }
    }
    else
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
                const auto & type_tuple = assert_cast<const DataTypeTuple &>(*column_type);
                auto index = get<UInt64>(literal->value);
                transformed_ast = transformToSubcolumn(name_in_storage, type_tuple.getNameByPosition(index));
            }
            else if (value_type == Field::Types::String)
            {
                transformed_ast = transformToSubcolumn(name_in_storage, get<const String &>(literal->value));
            }
        }
        else
        {
            auto it = binary_function_to_subcolumn.find(function.name);
            if (it != binary_function_to_subcolumn.end())
            {
                const auto & [type_id, subcolumn_name, transformer] = it->second;
                if (column_type_id == type_id)
                    transformed_ast = transformer(name_in_storage, subcolumn_name, arguments[1]);
            }
        }
    }

    if (transformed_ast)
    {
        transformed_ast->setAlias(ast->tryGetAlias());
        ast = transformed_ast;
    }
}

}
