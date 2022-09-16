#include "Common/Exception.h"

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>

#include "Functions/FunctionHelpers.h"
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include "Interpreters/Context.h"
#include "Interpreters/Context_fwd.h"
#include <Interpreters/InterpreterSelectQuery.h>
#include "Interpreters/ReplaceQueryParameterVisitor.h"

#include "Parsers/ParserSelectQuery.h"
#include "Parsers/formatAST.h"
#include "Parsers/parseQuery.h"

#include "Processors/Executors/PullingPipelineExecutor.h"




namespace DB 
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace 
{
class FunctionEval final : public IFunction
{
public:
    static constexpr auto name = "eval";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionEval>(context_); }

    explicit FunctionEval(ContextPtr context_) : context(context_) {}

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, [[maybe_unused]] size_t input_rows_count) const override
    {
        // Check and get query
        const ColumnConst * query_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!query_column) 
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be string",
                arguments[0].type->getName(),
                1,
                getName());

        String query = query_column->getValue<String>();

        // parsing query
        ParserSelectQuery parser;
        ASTPtr parsed_query = parseQuery(parser, query, 1000, 1000);
        
        // transfering parameters from arguments to context
        ContextMutablePtr new_context;
        new_context = Context::createCopy(context);

        // TODO new_context->setQueryParameters(const NameToNameMap &parameters) 

        //replacing parameters in parsed query
        ReplaceQueryParameterVisitor visitor(new_context->getQueryParameters());
        visitor.visit(parsed_query);

        // interpreting query
        InterpreterSelectQuery interpreter(parsed_query, new_context, SelectQueryOptions());
        auto io = interpreter.execute();
        PullingPipelineExecutor executor(io.pipeline);

        Block block;
        while (executor.pull(block)) {}
        
        if (block.rows() != 1)
            throw Exception("correlated subquery returned " + toString(block.rows()) + " rows, not 1",
                ErrorCodes::LOGICAL_ERROR);

        if (block.columns() != 1)
            throw Exception("correlated subquery returned " + toString(block.columns()) + " columns, not 1",
                ErrorCodes::LOGICAL_ERROR);                
        
        
    }

private:
    ContextPtr context;

};


}

void registerFunctionEval(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEval>(FunctionFactory::CaseInsensitive);
}

}
