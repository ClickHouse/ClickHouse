#include <Interpreters/RewriteFunctionToSubcolumnVisitor.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace
{

ASTPtr transformToSubcolumn(const ASTIdentifier & identifier, const String & subcolumn_name)
{
    return std::make_shared<ASTIdentifier>(Nested::concatenateName(identifier.name(), subcolumn_name));
}

ASTPtr transformIsNotNullToSubcolumn(const ASTIdentifier & identifier, const String & subcolumn_name)
{
    auto ast = transformToSubcolumn(identifier, subcolumn_name);
    ast = makeASTFunction("NOT", ast);
    return ast;
}

const std::unordered_map<String, std::pair<String, decltype(&transformToSubcolumn)>> function_to_subcolumn =
{
    {"length",    {"size0", transformToSubcolumn}},
    {"isNull",    {"null", transformToSubcolumn}},
    {"isNotNull", {"null", transformIsNotNullToSubcolumn}},
    {"mapKeys",   {"keys", transformToSubcolumn}},
    {"mapValues", {"values", transformToSubcolumn}}
};

}

void RewriteFunctionToSubcolumnData::visit(ASTFunction & function, ASTPtr & ast)
{
    const auto & arguments = function.arguments->children;
    if (arguments.size() != 1)
        return;

    const auto * identifier = arguments[0]->as<ASTIdentifier>();
    if (!identifier || !columns_to_rewrite.count(identifier->name()))
        return;

    auto it = function_to_subcolumn.find(function.name);
    if (it == function_to_subcolumn.end())
        return;

    const auto & [subcolumn_name, transformer] = it->second;
    ast = transformer(*identifier, subcolumn_name);
}

}
