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

#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTQueryParameter.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/ParserSelectQuery.h"
#include "Parsers/formatAST.h"
#include "Parsers/parseQuery.h"

#include "Processors/Executors/PullingPipelineExecutor.h"
#include "base/types.h"




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

    void getParameters(ASTPtr & ast, std::vector<String> & names) const {
        if (ast->as<ASTQueryParameter>())
        {   
            const auto & ast_param = ast->as<ASTQueryParameter &>();
            names.emplace_back(ast_param.name);
        }
        else if (ast->as<ASTIdentifier>() || ast->as<ASTTableIdentifier>())
        {
            return;
            // TODO: cant get private members of ASTIdentifier

            // auto ast_identifier = dynamic_pointer_cast<ASTIdentifier>(ast);
            // if (ast_identifier->children.empty())
            //     return;

            // auto & name_parts = ast_identifier->name_parts;
            // for (size_t i = 0, j = 0, size = name_parts.size(); i < size; ++i)
            // {
            //     if (name_parts[i].empty())
            //     {
            //         const auto & ast_param = ast_identifier->children[j++]->as<ASTQueryParameter &>();
            //         name_parts[i] = getParamValue(ast_param.name);
            //     }
            // }   
        }
        else
        {
            for (auto & child : ast->children)
                getParameters(child, names);
        }
    }

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

        // set qery params
        std::vector<String> names;
        getParameters(parsed_query, names);

        for (size_t i = 1; i != arguments.size(); ++i) {
            if (i >= names.size())
                throw Exception("Not enough parameters",
                    ErrorCodes::LOGICAL_ERROR);

            const ColumnConst * argument_column = checkAndGetColumnConst<ColumnString>(arguments[i].column.get());
            String param = argument_column->getValue<String>();

            new_context->setQueryParameter(names[i - 1], param);
        }

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
