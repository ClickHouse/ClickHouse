#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTTLElement.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <ranges>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

const ASTFunction * asLambda(const ASTPtr & ast)
{
    const auto * function = ast->as<ASTFunction>();
    if (function && function->name == "lambda" && function->arguments && function->arguments->children.size() == 2)
        return function;
    return nullptr;
}

Names getLambdaParameters(const ASTFunction & lambda)
{
    Names names;
    const auto * tuple = lambda.arguments->children[0]->as<ASTFunction>();
    if (tuple && tuple->name == "tuple" && tuple->arguments)
        for (const auto & child : tuple->arguments->children)
            if (const auto * identifier = child->as<ASTIdentifier>())
                names.push_back(identifier->name());
    return names;
}

const String * findBindingParameter(const ASTIdentifier & identifier, const Names & parameters, bool new_analyzer_binding)
{
    for (const auto & parameter : parameters)
        if (parameter == identifier.name() || (new_analyzer_binding && parameter == identifier.name_parts.front()))
            return &parameter;
    return nullptr;
}

bool isRenamedReference(const IAST & ast, const RenameColumnData & data)
{
    const auto * identifier = ast.as<ASTIdentifier>();
    return identifier && IdentifierSemantic::getColumnName(*identifier) == data.column_name
        && !findBindingParameter(*identifier, data.lambda_parameters, data.new_analyzer_binding);
}

/// Checks one analysis scope: the expressions whose aliases are visible to each other.
class ScopeChecker
{
public:
    explicit ScopeChecker(RenameColumnData data_) : data(std::move(data_)) {}

    void collect(const ASTPtr & ast, const String & where)
    {
        checkStackSize();
        const auto * lambda = asLambda(ast);
        /// For the analyzer an alias on a lambda names the lambda, not an expression.
        if (String alias = ast->tryGetAlias(); !alias.empty() && !(lambda && data.new_analyzer_binding))
        {
            const IAST * scope = data.new_analyzer_binding && !lambdas.empty() ? lambdas.back().node : nullptr;
            aliases.push_back({alias, ast.get(), where, scope, isRenamedReference(*ast, data)});
        }

        const auto * identifier = ast->as<ASTIdentifier>();
        if (data.new_analyzer_binding && identifier && identifier->compound()
            && (identifier->name() == data.column_name || identifier->name() == data.rename_to))
        {
            for (const auto & frame : lambdas | std::views::reverse)
            {
                if (const auto * parameter = findBindingParameter(*identifier, frame.parameters, true))
                {
                    lambda_fields.try_emplace(frame.node, identifier->name(), *parameter);
                    break;
                }
            }
        }

        if (lambda)
            inLambda(*lambda, [&](const ASTPtr & body) { collect(body, where); });
        else if (!ast->as<ASTSubquery>())
            for (const auto & child : ast->children)
                collect(child, where);
    }

    /// Returns the first conflict, or an empty string.
    String check(const ASTPtr & ast, const String & where)
    {
        checkStackSize();
        const bool has_alias = !ast->tryGetAlias().empty();
        if (has_alias)
            aliased_ancestors.push_back(ast.get());

        String conflict;
        if (isRenamedReference(*ast, data))
            conflict = checkReference(ast->as<const ASTIdentifier &>(), where);

        if (const auto * lambda = asLambda(ast))
            inLambda(*lambda, [&](const ASTPtr & body) { if (conflict.empty()) conflict = check(body, where); });
        else if (!ast->as<ASTSubquery>())
            for (const auto & child : ast->children)
                if (conflict.empty())
                    conflict = check(child, where);

        if (has_alias)
            aliased_ancestors.pop_back();
        return conflict;
    }

private:
    struct Alias
    {
        String name;
        const IAST * node;
        String where;
        /// The lambda the alias is visible in, nullptr for the whole scope.
        const IAST * lambda;
        /// Put directly on a renamed reference, so it keeps aliasing the column.
        bool exempt;
    };

    struct Lambda
    {
        const IAST * node;
        Names parameters;
    };

    template <typename F>
    void inLambda(const ASTFunction & lambda, F && visit_body)
    {
        Names parameters = getLambdaParameters(lambda);
        data.lambda_parameters.insert(data.lambda_parameters.end(), parameters.begin(), parameters.end());
        lambdas.push_back({&lambda, std::move(parameters)});
        visit_body(lambda.arguments->children[1]);
        data.lambda_parameters.resize(data.lambda_parameters.size() - lambdas.back().parameters.size());
        lambdas.pop_back();
    }

    bool isVisible(const Alias & alias) const
    {
        return !alias.lambda || std::ranges::any_of(lambdas, [&](const Lambda & lambda) { return lambda.node == alias.lambda; });
    }

    /// An alias does not bind inside its own definition: for the analyzer inside any aliased ancestor,
    /// for `TreeRewriter` inside the nearest one.
    bool isInFlight(const Alias & alias) const
    {
        if (data.new_analyzer_binding)
            return std::ranges::find(aliased_ancestors, alias.node) != aliased_ancestors.end();
        return !aliased_ancestors.empty() && aliased_ancestors.back() == alias.node;
    }

    String checkReference(const ASTIdentifier & reference, const String & where) const
    {
        if (std::ranges::find(data.lambda_parameters, data.rename_to) != data.lambda_parameters.end())
            return fmt::format("{} is a lambda parameter in {}", backQuote(data.rename_to), where);

        for (const auto & alias : aliases)
        {
            if (alias.exempt || !isVisible(alias) || isInFlight(alias))
                continue;
            if (alias.name == data.rename_to)
                return fmt::format("{} is already an alias in {}", backQuote(alias.name), alias.where);
            /// The reference meant the alias, or the alias's field for the analyzer.
            if (alias.name == data.column_name
                || (data.new_analyzer_binding && reference.compound() && alias.name == reference.name_parts.front()))
                return fmt::format("{} is also an alias in {}", backQuote(alias.name), alias.where);
        }

        /// The analyzer resolves `x.y` and `` `x.y` `` in one lambda with the parameter `x` to the same thing.
        for (const auto & lambda : lambdas)
            if (auto it = lambda_fields.find(lambda.node); it != lambda_fields.end())
                return fmt::format("{} is also a field of the lambda parameter {} in {}",
                    backQuote(it->second.first), backQuote(it->second.second), where);

        return {};
    }

    RenameColumnData data;
    std::vector<Alias> aliases;
    std::vector<Lambda> lambdas;
    std::vector<const IAST *> aliased_ancestors;
    /// Lambdas whose parameter binds a compound identifier named like the column before or after the rename.
    std::unordered_map<const IAST *, std::pair<String, String>> lambda_fields;
};

}

bool RenameColumnMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &, const Data &)
{
    /// A subquery resolves its identifiers against its own tables.
    return !asLambda(node) && !node->as<ASTSubquery>();
}

void RenameColumnMatcher::visit(ASTPtr & ast, Data & data)
{
    if (const auto * lambda = asLambda(ast))
    {
        Data lambda_data = data;
        std::ranges::move(getLambdaParameters(*lambda), std::back_inserter(lambda_data.lambda_parameters));
        RenameColumnVisitor{lambda_data}.visit(lambda->arguments->children[1]);
    }
    // TODO(ilezhankin): make proper rename
    else if (isRenamedReference(*ast, data))
        ast->as<ASTIdentifier &>().setShortName(data.rename_to);
}

RenameColumnConflicts::RenameColumnConflicts(const RenameColumnData & data_)
    : column_name(data_.column_name)
    , rename_to(data_.rename_to)
{
}

void RenameColumnConflicts::addColumnDefaults(const std::vector<std::pair<ASTPtr, String>> & defaults)
{
    addScope(defaults, /*new_analyzer_binding=*/ true);
}

void RenameColumnConflicts::add(const ASTPtr & ast, const String & where)
{
    if (ast->as<ASTTTLElement>() || (ast->as<ASTExpressionList>() && !ast->children.empty() && ast->children.front()->as<ASTTTLElement>()))
    {
        for (const auto & child : ast->children)
            add(child, where);
        return;
    }
    addScope({{ast, where}}, /*new_analyzer_binding=*/ false);
}

void RenameColumnConflicts::addScope(const std::vector<std::pair<ASTPtr, String>> & expressions, bool new_analyzer_binding)
{
    if (!conflict.empty())
        return;

    ScopeChecker checker(RenameColumnData{column_name, rename_to, new_analyzer_binding, {}});
    for (const auto & [ast, where] : expressions)
        checker.collect(ast, where);
    for (const auto & [ast, where] : expressions)
        if (conflict = checker.check(ast, where); !conflict.empty())
            return;
}

void RenameColumnConflicts::throwIfAny() const
{
    if (!conflict.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot rename column {} to {}: {}. Choose another name or change the expression first",
            backQuote(column_name), backQuote(rename_to), conflict);
}

}
