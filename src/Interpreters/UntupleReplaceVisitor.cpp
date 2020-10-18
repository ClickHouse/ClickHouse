#include <Interpreters/UntupleReplaceVisitor.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    void checkNestedUntupleFunction(ASTPtr node)
    {
        for (const auto & child : node->children)
        {
            if (const auto * function = child->as<ASTFunction>())
            {
                if (function->name == "untuple")
                    throw Exception("Cannot use untuple functions inside another untuple function", ErrorCodes::NOT_IMPLEMENTED);
            }
            checkNestedUntupleFunction(child);
        }
    }
}

/// Replace untuple with a list of columns.
void UntupleReplaceVisitorData::visit(ASTExpressionList & node, ASTPtr &)
{
    ASTs old_children;
    bool has_untuple = false;
    for (const auto & child : node.children)
    {
        if (const auto * function = child->as<ASTFunction>())
        {
            if (function->name == "untuple")
            {
                has_untuple = true;
                checkNestedUntupleFunction(child);
            }
        }
    }

    if (has_untuple)
    {
        old_children.swap(node.children);
        node.children.reserve(old_children.size());
    }

    for (const auto & child : old_children)
    {
        ASTs columns;
        if (const auto * function = child->as<ASTFunction>())
        {
            if (function->name == "untuple")
            {
                std::vector<String> names;
                ASTPtr first_tuple = child;
                while (!first_tuple->children.empty())
                {
                    first_tuple = first_tuple->children.at(0);
                    if (const auto * func = first_tuple->as<ASTFunction>())
                    {
                        if (func->name == "tuple")
                        {
                            for (auto & column : func->arguments->children)
                                names.emplace_back(column->getAliasOrColumnName());
                            break;
                        }
                    }
                }

                if (names.empty())
                    throw Exception("Incorrect use of untuple", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                String prefix = "t_";
                if (function->arguments->children.size() == 2)
                {
                    const auto * as_literal = function->arguments->children.at(1)->as<ASTLiteral>();
                    if (!as_literal || !as_literal->value.tryGet<String>(prefix))
                        throw Exception(
                            "Second argument of function untuple must be a constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }

                size_t tid = 0;
                for (auto & name : names)
                {
                    auto func = makeASTFunction(
                        "tupleElement",
                        function->arguments->as<ASTExpressionList &>().children[0]->clone(),
                        std::make_shared<ASTLiteral>(UInt64(++tid)));
                    func->setAlias(prefix + name);
                    columns.push_back(std::move(func));
                }
            }
        }
        else
            columns.emplace_back(child);
        node.children.insert(node.children.end(), std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()));
    }
}

}
