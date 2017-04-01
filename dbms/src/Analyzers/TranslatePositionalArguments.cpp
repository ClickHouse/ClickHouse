#include <Analyzers/TranslatePositionalArguments.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE;
}


static void processElement(ASTPtr & element, const ASTPtr & select_expression_list, const char * description)
{
    if (ASTLiteral * literal = typeid_cast<ASTLiteral *>(element.get()))
    {
        if (literal->value.getType() == Field::Types::UInt64)
        {
            UInt64 position = literal->value.get<UInt64>();

            if (!literal->alias.empty())
                throw Exception("Unsigned numeric literal " + toString(position) + " in " + toString(description)
                    + " section is interpreted as positional argument, "
                    "but it has alias " + backQuoteIfNeed(literal->alias) + " that is not expected", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            if (position == 0)
                throw Exception("Unsigned numeric literal 0 in " + toString(description) + " section is interpreted as positional argument, "
                    "but positional arguments are 1-based", ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE);

            if (position > select_expression_list->children.size())
                throw Exception("Unsigned numeric literal " + toString(position) + " in " + String(description)
                    + " section is interpreted as positional argument, "
                    "but it is greater than number of expressions in SELECT section ("
                    + toString(select_expression_list->children.size()) + ")", ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE);

            element = select_expression_list->children[position - 1]->clone();
        }
    }
}


static void processClause(ASTPtr & ast, const ASTPtr & select_expression_list, const char * description, bool is_order_by)
{
    if (!ast)
        return;

    for (auto & child : ast->children)
    {
        if (is_order_by)
        {
            if (!typeid_cast<ASTOrderByElement *>(child.get()))
                throw Exception("Child of ORDER BY clause is not an ASTOrderByElement", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            /// It has ASC|DESC and COLLATE inplace, and expression as its only child.
            if (child->children.empty())
                throw Exception("ORDER BY element has no children", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            processElement(child->children[0], select_expression_list, description);
        }
        else
            processElement(child, select_expression_list, description);
    }
}


void TranslatePositionalArguments::process(ASTPtr & ast)
{
    ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get());
    if (!select)
        throw Exception("TranslatePositionalArguments::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    if (!select->select_expression_list)
        throw Exception("SELECT query doesn't have select_expression_list", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    processClause(select->group_expression_list, select->select_expression_list, "GROUP BY", false);
    processClause(select->order_expression_list, select->select_expression_list, "ORDER BY", true);
    processClause(select->limit_by_expression_list, select->select_expression_list, "LIMIT BY", false);
}

}
