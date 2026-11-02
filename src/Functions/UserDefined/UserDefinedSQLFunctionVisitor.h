#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

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

    /// `visit` does not reach the functions applied by the `APPLY` transformers of column matchers: the lambda of
    /// `APPLY (x -> ...)` is not among the children of the transformer, and `APPLY f` keeps only the name of `f`.
    /// In the column and index declarations of `ast`, inlines the user-defined functions of these transformers
    /// too, turning `APPLY f` with a user-defined `f` into the lambda form. Column default and index expressions
    /// are stored with the user-defined functions inlined, so that their column matchers are expanded against
    /// the schema of the table. Other `APPLY` transformers, like those of `SELECT` queries, are left alone,
    /// because inlining `APPLY f` would change the names of the result columns.
    static void visitApplyTransformersInDefinitions(ASTPtr & ast, ContextPtr context_);

private:
    static ASTPtr tryToReplaceFunction(const ASTFunction & function, UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process, ContextPtr context_);

};

}
