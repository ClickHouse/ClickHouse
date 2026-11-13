#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Interpreters/Context_fwd.h>

#include <unordered_set>

namespace DB
{

class ASTFunction;
class ASTColumnsApplyTransformer;

/** Visits ASTFunction nodes and if it is user-defined function replace it with function body.
  * Example:
  *
  * CREATE FUNCTION test_function AS a -> a + 1;
  *
  * Before applying visitor:
  * SELECT test_function(number) FROM system.numbers LIMIT 10;
  *
  * After applying visitor:
  * SELECT number + 1 FROM system.numbers LIMIT 10;
  */
class UserDefinedSQLFunctionVisitor
{
public:
    static void visit(ASTPtr & ast, ContextPtr context_);

private:
    /// `lambda_names` are the names of the lambdas declared in the whole query (see `collectLambdaNames`).
    /// `inside_apply` tells whether `ast` is in the expression of an `APPLY` transformer.
    static void visitImpl(ASTPtr & ast, ContextPtr context_, const std::unordered_set<String> & lambda_names, bool inside_apply); /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
    /// `shadowed_names`, if not null, are the names of the functions that are not expanded in the bodies of UDFs,
    /// because a lambda of the same name declared in the query takes precedence over them (inside `APPLY`).
    /// `query_lambda_names` are the names of the lambdas declared in the whole query (see `collectLambdaNames`).
    static ASTPtr tryToReplaceFunction(
        const ASTFunction & function,
        UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
        ContextPtr context_,
        const std::unordered_set<String> & query_lambda_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
        const std::unordered_set<String> * shadowed_names); /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`

    using ArgumentSubstitutions = UnorderedMapWithMemoryTracking<std::string, ASTPtr>;

    /// Expands the SQL UDFs in the expressions under `expression_list` (the body of a UDF being expanded)
    /// and substitutes the arguments of that UDF for the identifiers of its parameters.
    /// `apply_shadowed_names` are the names of the functions that are not expanded in the expressions of `APPLY`
    /// transformers in the body, because a lambda of the same name takes precedence over them.
    static void expandFunctionBody(
        const ASTPtr & expression_list,
        const ArgumentSubstitutions & substitutions,
        UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
        ContextPtr context_,
        const std::unordered_set<String> & query_lambda_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
        const std::unordered_set<String> * shadowed_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
        const std::unordered_set<String> & apply_shadowed_names); /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`

    /// The expressions of an `APPLY` transformer in the body of a UDF are not among the children of the transformer,
    /// so `expandFunctionBody` handles them here, like `visitImpl` does for the query itself.
    static void expandApplyTransformerInFunctionBody(
        ASTColumnsApplyTransformer & apply_transformer,
        const ArgumentSubstitutions & substitutions,
        UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
        ContextPtr context_,
        const std::unordered_set<String> & query_lambda_names, /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`
        const std::unordered_set<String> & apply_shadowed_names); /// STYLE_CHECK_ALLOW_STD_CONTAINERS: the type of `collectLambdaNames`

};

}
