#pragma once

#include <Parsers/IAST.h>
#include <vector>


namespace DB
{

class WriteBuffer;


/** For every lambda expression, rename its parameters to '_lambda0_arg0' form.
  * Check correctness of lambda expressions.
  * Find functions, that have lambda expressions as arguments (they are called "higher order" functions).
  *
  * This should be done before CollectAliases.
  */
struct AnalyzeLambdas
{
    void process(ASTPtr & ast);

    /// Parameters of lambda expressions.
    using LambdaParameters = std::vector<String>;
    static LambdaParameters extractLambdaParameters(ASTPtr & ast);


    using HigherOrderFunctions = std::vector<ASTPtr>;
    HigherOrderFunctions higher_order_functions;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
