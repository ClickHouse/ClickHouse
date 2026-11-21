#include <Common/StackWithMemoryTracking.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <stack>
#include <unordered_set>

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTTTLElement.h>
#include <Core/Settings.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>

#include <Common/StringUtils.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool skip_redundant_aliases_in_udf;
    extern const SettingsUUIDTypeVersion uuid_type_version;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Whether `name` occurs as a whole word (a maximal run of the characters of a bare identifier) in `text`.
bool containsWord(std::string_view text, std::string_view name)
{
    size_t pos = 0;
    while ((pos = text.find(name, pos)) != std::string_view::npos)
    {
        const bool starts_word = pos == 0 || !isWordCharASCII(text[pos - 1]);
        const bool ends_word = pos + name.size() == text.size() || !isWordCharASCII(text[pos + name.size()]);
        if (starts_word && ends_word)
            return true;
        ++pos;
    }
    return false;
}

/// The argument of the lambda `APPLY(x -> f(x))` that replaces `APPLY f` must not capture a free identifier of the
/// body of `f`, as `x` in `y -> y + x`. `expansion` is the expansion of `f` called with a literal argument, so the
/// identifiers in it are exactly the ones the argument of the lambda would meet. A word of its text that is not an
/// identifier (as in a string literal) only makes the chosen name longer.
String makeFreshLambdaArgument(const IAST & expansion, const auto & reserved_names)
{
    const String text = expansion.formatWithSecretsOneLine();
    String lambda_argument = "x";
    while (containsWord(text, lambda_argument) || reserved_names.contains(lambda_argument))
        lambda_argument += "_";
    return lambda_argument;
}

/// The names of the parameters of `ast` if it is a lambda, as `a` and `b` for `lambda(tuple(a, b), a + b)`.
VectorWithMemoryTracking<String> getLambdaParameterNames(const IAST & ast)
{
    VectorWithMemoryTracking<String> names;
    const auto * function = ast.as<ASTFunction>();
    if (!function || function->name != "lambda" || !function->arguments || function->arguments->children.empty())
        return names;
    const auto * parameters = function->arguments->children[0]->as<ASTFunction>();
    if (!parameters || !parameters->arguments)
        return names;
    for (const auto & parameter : parameters->arguments->children)
        if (const auto * parameter_identifier = parameter->as<ASTIdentifier>())
            names.push_back(parameter_identifier->name());
    return names;
}

}

void UserDefinedSQLFunctionVisitor::visit(ASTPtr & ast, ContextPtr context_)
{
    chassert(ast);

    std::unordered_set<String> lambda_names; /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    collectLambdaNames(*ast, lambda_names);
    visitImpl(ast, context_, lambda_names, /*inside_apply=*/false);
}

void UserDefinedSQLFunctionVisitor::visitImpl(
    ASTPtr & ast, ContextPtr context_, const std::unordered_set<String> & lambda_names, bool inside_apply) /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
{
    chassert(ast);

    for (auto & child : ast->children)
    {
        if (!child)
            return;

        auto * old_ptr = child.get();
        visitImpl(child, context_, lambda_names, inside_apply);
        auto * new_ptr = child.get();

        /// Some AST classes have naked pointers to children elements as members.
        /// We have to replace them if the child was replaced.
        if (new_ptr != old_ptr)
            ast->updatePointerToChild(old_ptr, new_ptr);
    }

    /// `GROUP BY` keys and `SET` assignments of a `TTL` rule are not among its children.
    if (auto * ttl_element = ast->template as<ASTTTLElement>())
    {
        for (auto & key : ttl_element->group_by_key)
            visitImpl(key, context_, lambda_names, inside_apply);
        for (auto & assignment : ttl_element->group_by_assignments)
            visitImpl(assignment, context_, lambda_names, inside_apply);
    }

    /// The function of an `APPLY` transformer is not among its children either.
    if (auto * apply_transformer = ast->template as<ASTColumnsApplyTransformer>())
    {
        /// The analyzer expands the SQL UDFs of `APPLY` by itself and names the resulting columns after the original
        /// expression (`f(a)` for `APPLY f`), while an expansion here makes it name them after the expanded body.
        /// So the expressions of `APPLY` are expanded here only when it is needed to materialize `uuid_type_version`
        /// into the bodies of the UDFs (a bare `UUID` in the body of a UDF means `UUID2` with version 2): then
        /// `APPLY f` with a SQL UDF `f` is rewritten to `APPLY(x -> f(x))`, so that the body of `f` can be expanded.
        /// Not if a lambda named `f` is declared in the query: it takes precedence over the UDF in the analyzer.
        if (context_->getSettingsRef()[Setting::uuid_type_version] == 2)
        {
            ASTPtr expanded_lambda;
            String expanded_lambda_arg;
            if (apply_transformer->lambda)
            {
                expanded_lambda = apply_transformer->lambda->clone();
                expanded_lambda_arg = apply_transformer->lambda_arg;
            }
            else if (!apply_transformer->parameters && !lambda_names.contains(apply_transformer->func_name))
            {
                const auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(apply_transformer->func_name);
                if (user_defined_function && user_defined_function->as<ASTCreateSQLFunctionQuery>())
                {
                    ASTPtr probe = makeASTFunction(apply_transformer->func_name, make_intrusive<ASTLiteral>(Field()));
                    visitImpl(probe, context_, lambda_names, /*inside_apply=*/true);
                    const String lambda_argument = makeFreshLambdaArgument(*probe, lambda_names);
                    expanded_lambda = makeASTFunction("lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>(lambda_argument)),
                        makeASTFunction(apply_transformer->func_name, make_intrusive<ASTIdentifier>(lambda_argument)));
                    expanded_lambda_arg = lambda_argument;
                }
            }

            if (expanded_lambda)
            {
                visitImpl(expanded_lambda, context_, lambda_names, /*inside_apply=*/true);
                /// The lambdas declared in the rest of the query shadow the functions of the same name in the body.
                if (applyUUIDTypeVersionInPlace(*expanded_lambda->clone(), 2, lambda_names))
                {
                    apply_transformer->lambda = expanded_lambda;
                    apply_transformer->lambda_arg = expanded_lambda_arg;
                    apply_transformer->func_name.clear();
                }
            }

            /// The parameters of an aggregate function, as in `APPLY(quantile(f()))`, likewise: their expansion changes
            /// the names of the resulting columns too (`quantile(0.5)(a)` instead of `quantile(f())(a)`).
            if (apply_transformer->parameters)
            {
                ASTPtr expanded_parameters = apply_transformer->parameters->clone();
                visitImpl(expanded_parameters, context_, lambda_names, /*inside_apply=*/true);
                if (applyUUIDTypeVersionInPlace(*expanded_parameters->clone(), 2, lambda_names))
                    apply_transformer->parameters = expanded_parameters;
            }
        }
    }

    if (const auto * function = ast->template as<ASTFunction>())
    {
        /// The expressions of `APPLY` used not to be expanded here, so the analyzer resolved a lambda of the same name
        /// before the UDF. Keep that precedence for them. Elsewhere, the expansion is unchanged.
        if (inside_apply && lambda_names.contains(function->name))
            return;

        UnorderedSetWithMemoryTracking<std::string> udf_in_replace_process;
        auto replace_result = tryToReplaceFunction(
            *function, udf_in_replace_process, context_, lambda_names, inside_apply ? &lambda_names : nullptr);
        if (replace_result)
            ast = replace_result;
    }
}

namespace
{
bool isVariadic(const ASTPtr & arg)
{
    return arg->as<ASTAsterisk>() || arg->as<ASTQualifiedAsterisk>() || arg->as<ASTColumnsRegexpMatcher>()
        || arg->as<ASTColumnsListMatcher>() || arg->as<ASTQualifiedColumnsRegexpMatcher>() || arg->as<ASTQualifiedColumnsListMatcher>();
}
}

ASTPtr UserDefinedSQLFunctionVisitor::tryToReplaceFunction(
    const ASTFunction & function,
    UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
    ContextPtr context_,
    const std::unordered_set<String> & query_lambda_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    const std::unordered_set<String> * shadowed_names) /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
{
    if (udf_in_replace_process.contains(function.name))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive function call detected during function call {}",
            function.name);

    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function.name);
    if (!user_defined_function)
        return nullptr;

    const auto & function_arguments_list = function.children.at(0)->as<ASTExpressionList>();
    auto & function_arguments = function_arguments_list->children;

    auto * create_function_query = user_defined_function->as<ASTCreateSQLFunctionQuery>();

    if (!create_function_query && user_defined_function->as<ASTCreateWasmFunctionQuery>())
        return nullptr;

    /// Driver-created executable functions are resolved through `UserDefinedExecutableFunctionFactory`.
    if (!create_function_query && user_defined_function->as<ASTCreateFunctionWithDriverQuery>())
        return nullptr;

    if (!create_function_query)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "The function '{}' is not a SQL defined function", function.formatForErrorMessage());

    auto & function_core_expression = create_function_query->function_core->children.at(0);

    const auto & identifiers_expression_list = function_core_expression->children.at(0)->children.at(0)->as<ASTExpressionList>();
    const auto & identifiers_raw = identifiers_expression_list->children;

    if (function_arguments.size() != identifiers_raw.size())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Function {} expects {} arguments actual arguments {}",
            create_function_query->getFunctionName(),
            identifiers_raw.size(),
            function_arguments.size());

    for (const auto & arg : function_arguments)
    {
        if (isVariadic(arg))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "It is not possible to replace a variadic argument '{}' in UDF {}",
                arg->getColumnName(),
                function.name);
    }

    if (isVariadic(function_core_expression->children.at(1)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "It is not possible to replace a variadic argument '{}' in UDF {}",
            function_core_expression->children.at(1)->getColumnName(),
            function.name);

    ArgumentSubstitutions identifier_name_to_function_argument;

    for (size_t parameter_index = 0; parameter_index < identifiers_raw.size(); ++parameter_index)
    {
        const auto & identifier = identifiers_raw[parameter_index]->as<ASTIdentifier>();
        const auto & function_argument = function_arguments[parameter_index];
        const auto & identifier_name = identifier->name();

        identifier_name_to_function_argument.emplace(identifier_name, function_argument);
    }

    auto [it, _] = udf_in_replace_process.emplace(function.name);

    auto function_body_to_update = function_core_expression->children.at(1)->clone();

    if (context_->getSettingsRef()[Setting::skip_redundant_aliases_in_udf])
    {
        Aliases aliases;
        QueryAliasesVisitor(aliases).visit(function_body_to_update);

        /// Mark table ASTIdentifiers with not a column marker
        MarkTableIdentifiersVisitor::Data identifiers_data{aliases};
        MarkTableIdentifiersVisitor(identifiers_data).visit(function_body_to_update);

        /// Common subexpression elimination. Rewrite rules.
        /// `source_columns` must be a named local: `QueryNormalizer::Data` stores `source_columns_set`
        /// by reference, so a `{}` temporary would dangle once this statement ends.
        NameSet source_columns;
        QueryNormalizer::Data normalizer_data(aliases, source_columns, true, QueryNormalizer::ExtractedSettings(context_->getSettingsRef()), true, false);
        QueryNormalizer(normalizer_data).visit(function_body_to_update);
    }

    /// The lambdas declared in the body itself, as in `x -> (WITH (y -> y * 3) AS g SELECT g(x))`, shadow the UDFs
    /// of the same name in the body as well.
    std::unordered_set<String> body_shadowed_names; /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    if (shadowed_names)
    {
        body_shadowed_names = *shadowed_names;
        collectLambdaNames(*function_body_to_update, body_shadowed_names);
        shadowed_names = &body_shadowed_names;
    }

    /// The expressions of `APPLY` in the body are expanded like the ones in the query (see `visitImpl`): a lambda
    /// declared in the query or in the body takes precedence over a UDF of the same name there.
    std::unordered_set<String> apply_shadowed_names; /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    if (shadowed_names)
    {
        apply_shadowed_names = *shadowed_names;
    }
    else
    {
        apply_shadowed_names = query_lambda_names;
        collectLambdaNames(*function_body_to_update, apply_shadowed_names);
    }

    auto expression_list = make_intrusive<ASTExpressionList>();
    expression_list->children.emplace_back(std::move(function_body_to_update));

    expandFunctionBody(
        expression_list, identifier_name_to_function_argument, udf_in_replace_process, context_,
        query_lambda_names, shadowed_names, apply_shadowed_names);

    udf_in_replace_process.erase(it);

    function_body_to_update = expression_list->children[0];

    auto function_alias = function.tryGetAlias();

    if (!function_alias.empty())
        function_body_to_update->setAlias(function_alias);

    return function_body_to_update;
}


void UserDefinedSQLFunctionVisitor::expandFunctionBody(
    const ASTPtr & expression_list,
    const ArgumentSubstitutions & substitutions,
    UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
    ContextPtr context_,
    const std::unordered_set<String> & query_lambda_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    const std::unordered_set<String> * shadowed_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    const std::unordered_set<String> & apply_shadowed_names) /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
{
    StackWithMemoryTracking<ASTPtr> ast_nodes_to_update;
    ast_nodes_to_update.push(expression_list);

    while (!ast_nodes_to_update.empty())
    {
        auto ast_node_to_update = ast_nodes_to_update.top();
        ast_nodes_to_update.pop();

        if (auto * apply_transformer = ast_node_to_update->as<ASTColumnsApplyTransformer>())
            expandApplyTransformerInFunctionBody(
                *apply_transformer, substitutions, udf_in_replace_process, context_, query_lambda_names, apply_shadowed_names);

        for (auto & child : ast_node_to_update->children)
        {
            /// The analyzer resolves the functions in the body of a UDF in the scope of the query, so a lambda declared
            /// there shadows a UDF of the same name called from the body too.
            if (auto * inner_function = child->as<ASTFunction>(); inner_function && !(shadowed_names && shadowed_names->contains(inner_function->name)))
            {
                auto replace_result = tryToReplaceFunction(
                    *inner_function, udf_in_replace_process, context_, query_lambda_names, shadowed_names);
                if (replace_result)
                    child = replace_result;
            }

            auto identifier_name_opt = tryGetIdentifierName(child);
            if (identifier_name_opt)
            {
                auto function_argument_it = substitutions.find(*identifier_name_opt);

                if (function_argument_it == substitutions.end())
                    continue;

                auto child_alias = child->tryGetAlias();
                child = function_argument_it->second->clone();

                if (!child_alias.empty())
                    child->setAlias(child_alias);

                continue;
            }

            /// The parameters of a nested lambda, as `x` in `x -> arrayMap(x -> x + 1, [x])`, shadow the parameters
            /// of the UDF of the same name: only the free identifiers of the lambda are substituted.
            if (const auto lambda_parameters = getLambdaParameterNames(*child); !lambda_parameters.empty())
            {
                ArgumentSubstitutions lambda_substitutions = substitutions;
                bool shadows = false;
                for (const auto & parameter : lambda_parameters)
                    shadows |= lambda_substitutions.erase(parameter) > 0;
                if (shadows)
                {
                    expandFunctionBody(
                        child, lambda_substitutions, udf_in_replace_process, context_,
                        query_lambda_names, shadowed_names, apply_shadowed_names);
                    continue;
                }
            }

            ast_nodes_to_update.push(child);
        }
    }
}

void UserDefinedSQLFunctionVisitor::expandApplyTransformerInFunctionBody(
    ASTColumnsApplyTransformer & apply_transformer,
    const ArgumentSubstitutions & substitutions,
    UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
    ContextPtr context_,
    const std::unordered_set<String> & query_lambda_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    const std::unordered_set<String> & apply_shadowed_names) /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
{
    /// Only to materialize `uuid_type_version` into the bodies of the UDFs, as in `visitImpl`.
    if (context_->getSettingsRef()[Setting::uuid_type_version] != 2)
        return;

    /// Expands the UDFs in a copy of `expression` with the same recursion detection and the same substitution of the
    /// arguments of the enclosing UDF, except the ones shadowed by `shadowed_argument` (the argument of the lambda).
    const auto expand = [&](const ASTPtr & expression, const String & shadowed_argument)
    {
        ArgumentSubstitutions expression_substitutions = substitutions;
        if (!shadowed_argument.empty())
            expression_substitutions.erase(shadowed_argument);

        auto wrapper = make_intrusive<ASTExpressionList>();
        wrapper->children.push_back(expression->clone());
        expandFunctionBody(
            wrapper, expression_substitutions, udf_in_replace_process, context_,
            query_lambda_names, &apply_shadowed_names, apply_shadowed_names);
        return wrapper->children[0];
    };

    ASTPtr lambda;
    String lambda_argument;
    if (apply_transformer.lambda)
    {
        lambda = apply_transformer.lambda;
        lambda_argument = apply_transformer.lambda_arg;
    }
    else if (!apply_transformer.parameters && !apply_shadowed_names.contains(apply_transformer.func_name))
    {
        const auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(apply_transformer.func_name);
        if (user_defined_function && user_defined_function->as<ASTCreateSQLFunctionQuery>())
        {
            /// The argument must not be the name of a parameter of the enclosing UDF either: it would be substituted then.
            const ASTPtr probe = expand(
                makeASTFunction(apply_transformer.func_name, make_intrusive<ASTLiteral>(Field())), {});
            lambda_argument = makeFreshLambdaArgument(*probe, substitutions);
            lambda = makeASTFunction("lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(lambda_argument)),
                makeASTFunction(apply_transformer.func_name, make_intrusive<ASTIdentifier>(lambda_argument)));
        }
    }

    if (lambda)
    {
        ASTPtr expanded_lambda = expand(lambda, lambda_argument);
        if (applyUUIDTypeVersionInPlace(*expanded_lambda->clone(), 2, apply_shadowed_names))
        {
            apply_transformer.lambda = expanded_lambda;
            apply_transformer.lambda_arg = lambda_argument;
            apply_transformer.func_name.clear();
        }
    }

    if (apply_transformer.parameters)
    {
        ASTPtr expanded_parameters = expand(apply_transformer.parameters, {});
        if (applyUUIDTypeVersionInPlace(*expanded_parameters->clone(), 2, apply_shadowed_names))
            apply_transformer.parameters = expanded_parameters;
    }
}

}
