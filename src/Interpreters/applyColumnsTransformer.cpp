#include <Interpreters/applyColumnsTransformer.h>

#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/Exception.h>
#include <Common/re2.h>

#include <algorithm>
#include <map>
#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_COMPILE_REGEXP;
}

std::shared_ptr<re2::RE2> getColumnsExceptMatcher(const ASTColumnsExceptTransformer & transformer)
{
    if (!transformer.getPattern())
        return {};

    auto regexp = std::make_shared<re2::RE2>(*transformer.getPattern(), re2::RE2::Quiet);
    if (!regexp->ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
            "COLUMNS pattern {} cannot be compiled: {}", *transformer.getPattern(), regexp->error());
    return regexp;
}

namespace
{

bool lambdaArgumentShadowsName(const ASTFunction & lambda, const String & name)
{
    return std::ranges::contains(getASTLambdaArgumentNames(lambda), name);
}

void replaceLambdaArgument(ASTPtr & ast, const ASTPtr & replacement, const String & lambda_arg, bool is_masked = false)
{
    if (!ast)
        return;

    if (!is_masked)
    {
        if (auto arg_name = tryGetIdentifierName(ast); arg_name && *arg_name == lambda_arg)
        {
            ast = replacement->clone();
            return;
        }
    }

    if (auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
    {
        if (!function->arguments || function->arguments->children.size() != 2)
            return;

        replaceLambdaArgument(
            function->arguments->children[1],
            replacement,
            lambda_arg,
            is_masked || lambdaArgumentShadowsName(*function, lambda_arg));
        return;
    }

    for (auto & child : ast->children)
        replaceLambdaArgument(child, replacement, lambda_arg, is_masked);
}

void applyColumnsApplyTransformer(const ASTColumnsApplyTransformer & transformer, ASTs & nodes)
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
        if (transformer.lambda)
        {
            auto body = transformer.lambda->as<const ASTFunction &>().arguments->children.at(1)->clone();
            replaceLambdaArgument(body, column, transformer.lambda_arg);
            column = body;
        }
        else
        {
            auto function = makeASTFunction(transformer.func_name, column);
            function->parameters = transformer.parameters;
            column = function;
        }
        if (!transformer.column_name_prefix.empty())
            column->setAlias(transformer.column_name_prefix + name);
    }
}


void applyColumnsExceptTransformer(const ASTColumnsExceptTransformer & transformer, ASTs & nodes)
{
    std::set<String> expected_columns;
    if (!transformer.getPattern())
    {
        for (const auto & child : transformer.children)
        {
            if (const auto * identifier = child->as<ASTIdentifier>())
                expected_columns.insert(identifier->name());
            else
                expected_columns.insert(child->getAliasOrColumnName());
        }

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
        auto regexp = getColumnsExceptMatcher(transformer);

        for (auto it = nodes.begin(); it != nodes.end();)
        {
            if (auto * id = it->get()->as<ASTIdentifier>())
            {
                if (RE2::PartialMatch(id->shortName(), *regexp))
                {
                    it = nodes.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    if (transformer.is_strict && !expected_columns.empty())
    {
        String expected_columns_str;
        std::for_each(expected_columns.begin(), expected_columns.end(),
            [&](String x) { expected_columns_str += (" " + x) ; });

        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Columns transformer EXCEPT expects following column(s) :{}",
            expected_columns_str);
    }
}


/// Substitutes the pre-`REPLACE` expression of a column for the references to that column inside
/// the replacement expression. Like `replaceLambdaArgument` above, the walk is lambda-scope-aware:
/// an identifier bound by an enclosing lambda parameter of the same name is a different variable
/// and must be left intact, and lambda parameter lists are never rewritten.
void replaceColumnReferences(ASTPtr & ast, const ASTPtr & replacement, const String & name, bool is_masked = false)
{
    if (!ast)
        return;

    if (!is_masked)
    {
        if (const auto * id = ast->as<ASTIdentifier>(); id && id->shortName() == name)
        {
            ast = replacement->clone();
            return;
        }
    }

    if (auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
    {
        if (!function->arguments || function->arguments->children.size() != 2)
            return;

        replaceColumnReferences(
            function->arguments->children[1],
            replacement,
            name,
            is_masked || lambdaArgumentShadowsName(*function, name));
        return;
    }

    for (auto & child : ast->children)
        replaceColumnReferences(child, replacement, name, is_masked);
}


void applyColumnsReplaceTransformer(const ASTColumnsReplaceTransformer & transformer, ASTs & nodes)
{
    std::map<String, ASTPtr> replace_map;
    for (const auto & replace_child : transformer.children)
    {
        auto & replacement = replace_child->as<ASTColumnsReplaceTransformer::Replacement &>();
        if (replace_map.contains(replacement.name))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expressions in columns transformer REPLACE should not contain the same replacement more than once");
        replace_map.emplace(replacement.name, replacement.children[0]);
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
                replaceColumnReferences(new_ast, column, replace_it->first);
                column = new_ast;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
    }

    if (transformer.is_strict && !replace_map.empty())
    {
        String expected_columns;
        for (auto & elem: replace_map)
        {
            if (!expected_columns.empty())
                expected_columns += ", ";
            expected_columns += elem.first;
        }
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Columns transformer REPLACE expects following column(s) : {}",
            expected_columns);
    }

}


}

void applyColumnsTransformer(const ASTPtr & transformer, ASTs & nodes)
{
    if (const auto * apply = transformer->as<ASTColumnsApplyTransformer>())
        applyColumnsApplyTransformer(*apply, nodes);
    else if (const auto * except = transformer->as<ASTColumnsExceptTransformer>())
        applyColumnsExceptTransformer(*except, nodes);
    else if (const auto * replace = transformer->as<ASTColumnsReplaceTransformer>())
        applyColumnsReplaceTransformer(*replace, nodes);
}

}
