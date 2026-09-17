#pragma once

#include <cstddef>

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
    /// Read-only pre-dispatch audit used before a CREATE-family distributed
    /// handoff can enqueue the still-unexpanded query. Exact replacement
    /// repeats the same check below to close a concurrent UDF-definition race.
    static void assertNoStoredUDTSyntaxInFunctionBodiesToReplace(const ASTPtr & ast, ContextPtr context_);

    /// Analyzer-side counterpart for the exact SQL UDF definition image it is
    /// about to lower. This closes paths which resolve a UDF after the legacy
    /// AST substitution pass or reuse an analyzer-local lambda cache.
    static void assertNoStoredUDTSyntaxInFunctionDefinition(const ASTPtr & create_function_ast, size_t & remaining_inspection_nodes);

    /// Stored-object DDL may substitute a SQL UDF only when the definition
    /// itself cannot introduce a context-bearing UDT/type-string sink. The
    /// query's own arguments remain ordinary children and are checked by their
    /// owning boundary; global SQL UDF bodies are not a durable UDT authority.
    static void visit(ASTPtr & ast, ContextPtr context_, bool reject_stored_udt_syntax_in_function_bodies = false);

private:
    static void visitImpl(
        ASTPtr & ast,
        ContextPtr context_,
        bool reject_stored_udt_syntax_in_function_bodies,
        size_t * remaining_inspection_nodes);

    static ASTPtr tryToReplaceFunction(
        const ASTFunction & function,
        UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
        ContextPtr context_,
        bool reject_stored_udt_syntax_in_function_bodies,
        size_t * remaining_inspection_nodes,
        size_t udf_expansion_depth);
};

}
