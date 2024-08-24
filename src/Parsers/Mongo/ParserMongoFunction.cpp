#include "ParserMongoFunction.h"

#include <Core/Field.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace Mongo
{

bool MongoIdentityFunction::parseImpl(ASTPtr & node)
{
    if (data.IsInt())
    {
        auto identifier = std::make_shared<ASTIdentifier>(edge_name);
        auto literal = std::make_shared<ASTLiteral>(Field(data.GetInt()));
        auto where_condition = makeASTFunction("equals", identifier, literal);
        node = where_condition;
        return true;
    }
    if (data.IsString())
    {
        auto identifier = std::make_shared<ASTIdentifier>(edge_name);
        auto literal = std::make_shared<ASTLiteral>(Field(data.GetString()));
        auto where_condition = makeASTFunction("equals", identifier, literal);
        node = where_condition;
        return true;
    }
    if (data.IsObject())
    {
        auto parser = createInversedParser(std::move(data), metadata, edge_name);
        if (!parser->parseImpl(node))
        {
            return false;
        }
        return true;
    }
    return false;
}

bool MongoLiteralFunction::parseImpl(ASTPtr & node)
{
    if (data.IsString())
    {
        auto literal = std::make_shared<ASTIdentifier>(data.GetString());
        node = literal;
        return true;
    }
    if (data.IsObject())
    {
        if (data.Size() != 1)
        {
            return false;
        }

        auto it = data.MemberBegin();

        const char * name = it->name.GetString();
        auto parser = createParser(copyValue(it->value), metadata, name);
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        node = child_node;
        return true;
    }
    return false;
}


bool MongoOrFunction::parseImpl(ASTPtr & node)
{
    if (!data.IsArray())
    {
        return false;
    }

    std::vector<ASTPtr> child_trees;
    for (unsigned int i = 0; i < data.Size(); ++i)
    {
        auto parser = createParser(copyValue(data[i]), metadata, "");
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        child_trees.push_back(child_node);
    }

    if (child_trees.empty())
    {
        return false;
    }

    if (child_trees.size() == 1)
    {
        node = child_trees[0];
        return true;
    }

    auto result = makeASTFunction("or");
    for (const auto & elem : child_trees)
    {
        result->arguments->children.push_back(elem);
    }
    node = result;
    return true;
}


bool IMongoArithmeticFunction::parseImpl(ASTPtr & node)
{
    if (data.Size() < 2)
    {
        return false;
    }

    std::vector<ASTPtr> children;
    for (unsigned int i = 0; i < data.Size(); ++i)
    {
        auto parser = createParser(copyValue(data[i]), metadata, "$arithmetic_function_element");
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        children.push_back(std::move(child_node));
    }

    /// Wrap function as tree of binary operators like
    ///
    ///      +
    ///     / \
    ///    c0  +
    ///       / \
    ///      c1  c2
    ///
    auto function = makeASTFunction(getFunctionAlias(), children[0], children[1]);
    for (size_t i = 2; i < children.size(); ++i)
    {
        function = makeASTFunction(getFunctionAlias(), function, children[i]);
    }
    node = function;

    return true;
}

bool MongoArithmeticFunctionElement::parseImpl(ASTPtr & node)
{
    if (data.IsInt())
    {
        auto literal = std::make_shared<ASTLiteral>(Field(data.GetInt()));
        node = literal;
        return true;
    }
    if (data.IsString())
    {
        auto identifier = std::make_shared<ASTIdentifier>(data.GetString());
        node = identifier;
        return true;
    }
    if (data.IsObject())
    {
        auto parser = createParser(std::move(data), metadata, "");
        if (!parser->parseImpl(node))
        {
            return false;
        }
        return true;
    }
    return false;
}


}

}
