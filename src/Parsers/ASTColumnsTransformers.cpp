#include <map>
#include "ASTColumnsTransformers.h"
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <re2/re2.h>
#include <stack>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_COMPILE_REGEXP;
}

void IASTColumnsTransformer::transform(const ASTPtr & transformer, ASTs & nodes)
{
    if (const auto * apply = transformer->as<ASTColumnsApplyTransformer>())
    {
        apply->transform(nodes);
    }
    else if (const auto * except = transformer->as<ASTColumnsExceptTransformer>())
    {
        except->transform(nodes);
    }
    else if (const auto * replace = transformer->as<ASTColumnsReplaceTransformer>())
    {
        replace->transform(nodes);
    }
}

void ASTColumnsApplyTransformer::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "APPLY" << (settings.hilite ? hilite_none : "") << " ";

    if (!column_name_prefix.empty())
        settings.ostr << "(";

    if (lambda)
    {
        lambda->formatImpl(settings, state, frame);
    }
    else
    {
        settings.ostr << func_name;

        if (parameters)
            parameters->formatImpl(settings, state, frame);
    }

    if (!column_name_prefix.empty())
        settings.ostr << ", '" << column_name_prefix << "')";
}

void ASTColumnsApplyTransformer::transform(ASTs & nodes) const
{
    for (auto & column : nodes)
    {
        String name;
        auto alias = column->tryGetAlias();
        if (!alias.empty())
            name = alias;
        else
        {
            if (const auto * id = column->as<ASTIdentifier>())
                name = id->shortName();
            else
                name = column->getColumnName();
        }
        if (lambda)
        {
            auto body = lambda->as<const ASTFunction &>().arguments->children.at(1)->clone();
            std::stack<ASTPtr> stack;
            stack.push(body);
            while (!stack.empty())
            {
                auto ast = stack.top();
                stack.pop();
                for (auto & child : ast->children)
                {
                    if (auto arg_name = tryGetIdentifierName(child); arg_name && arg_name == lambda_arg)
                    {
                        child = column->clone();
                        continue;
                    }
                    stack.push(child);
                }
            }
            column = body;
        }
        else
        {
            auto function = makeASTFunction(func_name, column);
            function->parameters = parameters;
            column = function;
        }
        if (!column_name_prefix.empty())
            column->setAlias(column_name_prefix + name);
    }
}

void ASTColumnsExceptTransformer::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "EXCEPT" << (is_strict ? " STRICT " : " ") << (settings.hilite ? hilite_none : "");

    if (children.size() > 1)
        settings.ostr << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            settings.ostr << ", ";
        }
        (*it)->formatImpl(settings, state, frame);
    }

    if (!original_pattern.empty())
        settings.ostr << quoteString(original_pattern);

    if (children.size() > 1)
        settings.ostr << ")";
}

void ASTColumnsExceptTransformer::transform(ASTs & nodes) const
{
    std::set<String> expected_columns;
    if (original_pattern.empty())
    {
        for (const auto & child : children)
            expected_columns.insert(child->as<const ASTIdentifier &>().name());

        for (auto it = nodes.begin(); it != nodes.end();)
        {
            if (const auto * id = it->get()->as<ASTIdentifier>())
            {
                auto expected_column = expected_columns.find(id->shortName());
                if (expected_column != expected_columns.end())
                {
                    expected_columns.erase(expected_column);
                    it = nodes.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }
    else
    {
        for (auto it = nodes.begin(); it != nodes.end();)
        {
            if (const auto * id = it->get()->as<ASTIdentifier>())
            {
                if (isColumnMatching(id->shortName()))
                {
                    it = nodes.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    if (is_strict && !expected_columns.empty())
    {
        String expected_columns_str;
        std::for_each(expected_columns.begin(), expected_columns.end(),
            [&](String x) { expected_columns_str += (" " + x) ; });

        throw Exception(
            "Columns transformer EXCEPT expects following column(s) :" + expected_columns_str,
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }
}

void ASTColumnsExceptTransformer::setPattern(String pattern)
{
    original_pattern = std::move(pattern);
    column_matcher = std::make_shared<RE2>(original_pattern, RE2::Quiet);
    if (!column_matcher->ok())
        throw DB::Exception(
            "COLUMNS pattern " + original_pattern + " cannot be compiled: " + column_matcher->error(),
            DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
}

bool ASTColumnsExceptTransformer::isColumnMatching(const String & column_name) const
{
    return RE2::PartialMatch(column_name, *column_matcher);
}

void ASTColumnsReplaceTransformer::Replacement::formatImpl(
    const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    expr->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(name);
}

void ASTColumnsReplaceTransformer::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "REPLACE" << (is_strict ? " STRICT " : " ") << (settings.hilite ? hilite_none : "");

    if (children.size() > 1)
        settings.ostr << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            settings.ostr << ", ";
        }
        (*it)->formatImpl(settings, state, frame);
    }

    if (children.size() > 1)
        settings.ostr << ")";
}

void ASTColumnsReplaceTransformer::replaceChildren(ASTPtr & node, const ASTPtr & replacement, const String & name)
{
    for (auto & child : node->children)
    {
        if (const auto * id = child->as<ASTIdentifier>())
        {
            if (id->shortName() == name)
                child = replacement->clone();
        }
        else
            replaceChildren(child, replacement, name);
    }
}

void ASTColumnsReplaceTransformer::transform(ASTs & nodes) const
{
    std::map<String, ASTPtr> replace_map;
    for (const auto & replace_child : children)
    {
        auto & replacement = replace_child->as<Replacement &>();
        if (replace_map.find(replacement.name) != replace_map.end())
            throw Exception(
                "Expressions in columns transformer REPLACE should not contain the same replacement more than once",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        replace_map.emplace(replacement.name, replacement.expr);
    }

    for (auto & column : nodes)
    {
        if (const auto * id = column->as<ASTIdentifier>())
        {
            auto replace_it = replace_map.find(id->shortName());
            if (replace_it != replace_map.end())
            {
                column = replace_it->second;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
        else if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(column.get()))
        {
            auto replace_it = replace_map.find(ast_with_alias->alias);
            if (replace_it != replace_map.end())
            {
                auto new_ast = replace_it->second->clone();
                ast_with_alias->alias = ""; // remove the old alias as it's useless after replace transformation
                replaceChildren(new_ast, column, replace_it->first);
                column = new_ast;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
    }

    if (is_strict && !replace_map.empty())
    {
        String expected_columns;
        for (auto & elem: replace_map)
        {
            if (!expected_columns.empty())
                expected_columns += ", ";
            expected_columns += elem.first;
        }
        throw Exception(
            "Columns transformer REPLACE expects following column(s) : " + expected_columns,
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }

}

}
