#pragma once

#include <Parsers/IAST.h>
#include <DataTypes/IDataType.h>
#include <Common/UInt128.h>
#include <unordered_map>


namespace DB
{

class Context;
class WriteBuffer;
struct CollectAliases;
struct AnalyzeColumns;
struct AnalyzeLambdas;
struct ExecuteTableFunctions;
class IFunctionBase;
class IAggregateFunction;


/** For every expression, deduce its type,
  *  and if it is a constant expression, calculate its value.
  *
  * Types and constants inference goes together,
  *  because sometimes resulting type of a function depend on value of constant expression.
  * Notable examples: tupleElement(tuple, N) and toFixedString(s, N) functions.
  *
  * Also creates and stores function objects.
  * TODO (?) Also calculate ids for expressions, that will identify common subexpressions.
  */
struct TypeAndConstantInference
{
    void process(ASTPtr & ast, const Context & context,
        CollectAliases & aliases,
        const AnalyzeColumns & columns,
        const AnalyzeLambdas & analyze_lambdas,
        ExecuteTableFunctions & table_functions);

    struct ExpressionInfo
    {
        /// Must identify identical expressions.
        /// For example following three expressions in query are the same: SELECT sum(x) AS a, SUM(t.x) AS b, a FROM t
        UInt128 id {};
        ASTPtr node;
        DataTypePtr data_type;
        bool is_constant_expression = false;
        Field value;    /// Has meaning if is_constant_expression == true.
        std::shared_ptr<IFunctionBase> function;
        std::shared_ptr<IAggregateFunction> aggregate_function;
    };

    /// Key is getColumnName of AST node.
    using Info = std::unordered_map<String, ExpressionInfo>;
    Info info;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
