#include "Functions/FunctionHelpers.h"
#include "Interpreters/Context_fwd.h"
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>

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

    explicit FunctionEval(ContextMutablePtr context_) : context(context_) {
        context_->setQueryParameter(parameter_name, value);
    }

    static FunctionPtr create(ContextMutablePtr context) { return std::make_shared<FunctionEval>(context); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    // String getParamType(const String query) const {
    //     auto start = query.find(':');
    //     auto end = query.find('}');
    //     return query.substr(start + 1, end - start - 1);
    // }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, [[maybe_unused]] size_t input_rows_count) const override
    {
        const ColumnConst * query_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!query_column) 
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be string",
                arguments[0].type->getName(),
                1,
                getName());

        String query = query_column->getValue<String>();

        auto start = query.find('{');

        // String param_type = getParamType(query);
        
    }

private:
    ContextMutablePtr context;

};


}

void registerFunctionEval(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEval>(FunctionFactory::CaseInsensitive);
}

}
