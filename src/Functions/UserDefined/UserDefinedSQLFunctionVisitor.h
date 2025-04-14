#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/** Visits ASTFunction nodes and if it is used defined function replace it with function body.
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
    static void visit(IAST *, ContextPtr context_);
    static ASTPtr tryToReplaceFunction(const ASTFunction & function, std::unordered_set<std::string> & udf_in_replace_process, ContextPtr context_);

};

}
