#include <Storages/IndicesDescription.h>

#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Core/Defines.h>


namespace DB
{

bool IndicesDescription::empty() const
{
    return indices.empty();
}

bool IndicesDescription::has(const String & name) const
{
    return std::cend(indices) != std::find_if(
            std::cbegin(indices), std::cend(indices),
            [&name](const auto & index)
            {
                return index->name == name;
            });
}

String IndicesDescription::toString() const
{
    if (indices.empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : indices)
        list.children.push_back(index);

    return serializeAST(list, true);
}

IndicesDescription IndicesDescription::parse(const String & str)
{
    if (str.empty())
        return {};

    IndicesDescription res;
    ParserIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & index : list->children)
        res.indices.push_back(std::dynamic_pointer_cast<ASTIndexDeclaration>(index));

    return res;
}

}
