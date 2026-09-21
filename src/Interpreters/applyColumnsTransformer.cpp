#include <Interpreters/applyColumnsTransformer.h>

#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/Exception.h>
#include <Common/re2.h>

#include <map>
#include <set>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int BAD_ARGUMENTS;
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

struct ColumnsTransformerState
{
    ASTs nodes;
    std::vector<String> root_names;
};

String getColumnRootName(const ASTPtr & column)
{
    auto alias = column->tryGetAlias();
    if (!alias.empty())
        return alias;

    if (const auto * id = column->as<ASTIdentifier>())
        return id->shortName();

    return column->getColumnName();
}

/// Column transformers (APPLY with a name prefix, REPLACE) assign an alias to the
/// resulting node. Only ASTWithAlias nodes (identifiers, functions, literals, subqueries)
/// support aliases. When a transformer expression expands to an asterisk / qualified
/// asterisk / COLUMNS matcher (e.g. `* APPLY (x -> t.*, 'p_')`), the target does not
/// support aliases and the default IAST::setAlias would throw a LOGICAL_ERROR (server
/// abort in debug/sanitizer builds). Reject such queries with a clear user error instead.
void setTransformerAlias(ASTPtr & node, const String & alias)
{
    if (!dynamic_cast<const ASTWithAlias *>(node.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot set alias '{}' on '{}': the column transformer expression expands to an asterisk or "
            "COLUMNS matcher, which does not support aliases", alias, node->formatForErrorMessage());
    node->setAlias(alias);
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
            std::stack<ASTPtr> stack;
            stack.push(body);
            while (!stack.empty())
            {
                auto ast = stack.top();
                stack.pop();
                for (auto & child : ast->children)
                {
                    if (auto arg_name = tryGetIdentifierName(child); arg_name && arg_name == transformer.lambda_arg)
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
            auto function = makeASTFunction(transformer.func_name, column);
            function->parameters = transformer.parameters;
            column = function;
        }
        if (!transformer.column_name_prefix.empty())
            setTransformerAlias(column, transformer.column_name_prefix + name);
    }
}


void applyColumnsExceptTransformer(const ASTColumnsExceptTransformer & transformer, ColumnsTransformerState & state)
{
    auto & nodes = state.nodes;
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
                    auto index = static_cast<size_t>(it - nodes.begin());
                    it = nodes.erase(it);
                    state.root_names.erase(state.root_names.begin() + index);
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
                    auto index = static_cast<size_t>(it - nodes.begin());
                    it = nodes.erase(it);
                    state.root_names.erase(state.root_names.begin() + index);
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


void replaceChildren(ASTPtr & node, const ASTPtr & replacement, const String & name)
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
                setTransformerAlias(column, replace_it->first);
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
                setTransformerAlias(column, replace_it->first);
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


void applyColumnsRenameTransformer(const ASTColumnsRenameTransformer & transformer, ColumnsTransformerState & state)
{
    std::map<String, String> rename_map;
    std::set<String> target_names;
    for (const auto & rename_child : transformer.children)
    {
        const auto & rename = rename_child->as<const ASTColumnsRenameTransformer::Rename &>();
        if (!rename_map.emplace(rename.source_name, rename.target_name).second)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Columns transformer RENAME should not contain the same source column more than once");
        if (!target_names.emplace(rename.target_name).second)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Columns transformer RENAME should not contain the same target column more than once");
    }

    std::set<String> matched_columns;
    for (size_t i = 0; i < state.nodes.size(); ++i)
    {
        auto rename_it = rename_map.find(state.root_names[i]);
        if (rename_it != rename_map.end())
        {
            if (!matched_columns.emplace(rename_it->first).second)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Columns transformer RENAME source column '{}' matches more than one column. Qualify the matcher to disambiguate",
                    rename_it->first);

            state.nodes[i]->setAlias(rename_it->second);
        }
    }

    if (matched_columns.size() != rename_map.size())
    {
        String expected_columns_str;
        for (const auto & [column_name, _] : rename_map)
        {
            if (matched_columns.contains(column_name))
                continue;
            if (!expected_columns_str.empty())
                expected_columns_str += ", ";
            expected_columns_str += column_name;
        }

        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "Columns transformer RENAME expects following column(s): {}", expected_columns_str);
    }
}

void applyColumnsTransformerImpl(const ASTPtr & transformer, ColumnsTransformerState & state)
{
    if (const auto * apply = transformer->as<ASTColumnsApplyTransformer>())
        applyColumnsApplyTransformer(*apply, state.nodes);
    else if (const auto * except = transformer->as<ASTColumnsExceptTransformer>())
        applyColumnsExceptTransformer(*except, state);
    else if (const auto * replace = transformer->as<ASTColumnsReplaceTransformer>())
        applyColumnsReplaceTransformer(*replace, state.nodes);
    else if (const auto * rename = transformer->as<ASTColumnsRenameTransformer>())
        applyColumnsRenameTransformer(*rename, state);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported columns transformer");
}


}

void applyColumnsTransformer(const ASTPtr & transformer, ASTs & nodes)
{
    ColumnsTransformerState state{nodes, {}};
    state.root_names.reserve(nodes.size());
    for (const auto & node : nodes)
        state.root_names.push_back(getColumnRootName(node));

    applyColumnsTransformerImpl(transformer, state);
    nodes = std::move(state.nodes);
}

void applyColumnsTransformers(const ASTs & transformers, ASTs & nodes)
{
    ColumnsTransformerState state{nodes, {}};
    state.root_names.reserve(nodes.size());
    for (const auto & node : nodes)
        state.root_names.push_back(getColumnRootName(node));

    bool rename_seen = false;
    for (const auto & transformer : transformers)
    {
        if (rename_seen)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "RENAME must be the last column transformer");

        applyColumnsTransformerImpl(transformer, state);
        rename_seen = transformer->as<ASTColumnsRenameTransformer>() != nullptr;
    }

    nodes = std::move(state.nodes);
}

}
