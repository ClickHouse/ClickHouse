#include <Interpreters/ExplainTextRewrite.h>

#include <Common/Exception.h>
#include <Core/Field.h>
#include <Parsers/ASTExplainTextAction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <base/arithmeticOverflow.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
ASTSelectQuery & getSingleSelectQuery(const ASTPtr & query, ASTExplainTextAction::Kind action_kind)
{
    /// parsed SELECT sources normally use an ASTSelectWithUnionQuery wrapper
    /// programmatically deserialized ASTs may contain ASTSelectQuery directly.
    if (auto * select_query = query->as<ASTSelectQuery>())
        return *select_query;

    const auto * select_with_union = query->as<ASTSelectWithUnionQuery>();
    if (!select_with_union)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} can only be applied to a SELECT query", ASTExplainTextAction::toString(action_kind));

    if (!select_with_union->list_of_selects || select_with_union->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires exactly one SELECT branch", ASTExplainTextAction::toString(action_kind));

    auto * select_query = select_with_union->list_of_selects->children.front()->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires exactly one plain SELECT query", ASTExplainTextAction::toString(action_kind));

    return *select_query;
}

void setSelectExpression(ASTSelectQuery & select_query, ASTSelectQuery::Expression expression, ASTPtr value)
{
    select_query.setExpression(expression, std::move(value));
    select_query.normalizeChildrenOrder();
}

void applyPage(ASTPtr & query, const ASTExplainTextAction & action)
{
    auto & select_query = getSingleSelectQuery(query, action.getKind());

    const ASTPtr limit = select_query.limitLength();
    if (!limit)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PAGE requires an existing LIMIT");

    const auto & page_literal = action.getOperand()->as<const ASTLiteral &>();
    const UInt64 page = page_literal.value.safeGet<UInt64>();
    if (page == 1)
    {
        setSelectExpression(select_query, ASTSelectQuery::Expression::LIMIT_OFFSET, ASTPtr{});
        return;
    }
    if (page == 2)
    {
        setSelectExpression(select_query, ASTSelectQuery::Expression::LIMIT_OFFSET, limit->clone());
        return;
    }

    ASTPtr offset;
    if (const auto * limit_literal = limit->as<ASTLiteral>();
        limit_literal && limit_literal->value.getType() == Field::Types::UInt64)
    {
        const UInt64 limit_value = limit_literal->value.safeGet<UInt64>();
        UInt64 offset_value{};
        if (common::mulOverflow(limit_value, page - 1, offset_value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PAGE {} with LIMIT {} produces an offset greater than UInt64",
                            page, limit_value);
        offset = make_intrusive<ASTLiteral>(offset_value);
    }
    else
    {
        offset = makeASTFunction("multiply", limit->clone(), make_intrusive<ASTLiteral>(page - 1));
    }

    setSelectExpression(select_query, ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(offset));
}

void applyModifyFormat(ASTPtr & query, const ASTExplainTextAction & action)
{
    auto * query_with_output = dynamic_cast<ASTQueryWithOutput *>(query.get());
    if (!query_with_output)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MODIFY FORMAT requires a query that supports output options");

    ASTPtr format = action.getOperand()->clone();
    setIdentifierSpecial(format);

    query_with_output->setOrReplace(query_with_output->format_ast, std::move(format));
    query_with_output->normalizeOutputOptions();
}
}

ExplainTextRewriteResult rewriteExplainTextQuery(const ASTPtr & query, const ASTPtr & actions)
{
    if (!query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "EXPLAIN TEXT requires an explained query");

    ExplainTextRewriteResult result;
    result.query = query->clone();

    if (!actions)
        return result;

    const auto * action_list = actions->as<ASTExpressionList>();
    if (!action_list || action_list->getSeparator() != ',' || action_list->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "EXPLAIN TEXT requires a non-empty comma-separated action list");

    for (const auto & action_node : action_list->children)
    {
        const auto * action = action_node->as<ASTExplainTextAction>();
        if (!action)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "EXPLAIN TEXT action list contains an invalid node");

        action->validateShape();

        switch (action->getKind())
        {
            case ASTExplainTextAction::Kind::Oneline:
                result.one_line = true;
                break;

            case ASTExplainTextAction::Kind::Multiline:
                result.one_line = false;
                break;

            case ASTExplainTextAction::Kind::ModifyLimit:
            {
                auto & select_query = getSingleSelectQuery(result.query, action->getKind());
                setSelectExpression(select_query, ASTSelectQuery::Expression::LIMIT_LENGTH, action->getOperand()->clone());
                break;
            }

            case ASTExplainTextAction::Kind::ModifyOffset:
            {
                auto & select_query = getSingleSelectQuery(result.query, action->getKind());
                setSelectExpression(select_query, ASTSelectQuery::Expression::LIMIT_OFFSET, action->getOperand()->clone());
                break;
            }

            case ASTExplainTextAction::Kind::Page:
                applyPage(result.query, *action);
                break;

            case ASTExplainTextAction::Kind::ModifyFormat:
                applyModifyFormat(result.query, *action);
                break;
        }
    }
    return result;
}
}
