#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <unordered_set>

namespace DB
{

class ASTFunction;

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
    static void visitImpl(ASTPtr & ast, ContextPtr context_, const std::unordered_set<String> & lambda_names, bool inside_apply);
    static ASTPtr tryToReplaceFunction(const ASTFunction & function, UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process, ContextPtr context_);

};

}
