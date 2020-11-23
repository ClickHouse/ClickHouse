#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    class FunctionLogTrace : public IFunction
    {
    public:
        static constexpr auto name = "logTrace";
        static FunctionPtr create(const Context &) { return std::make_shared<FunctionLogTrace>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isString(arguments[0]))
                throw Exception(
                    "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeUInt8>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            String message;
            if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
                message = col->getDataAt(0).data;
            else
                throw Exception(
                    "First argument for function " + getName() + " must be Constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            static auto * log = &Poco::Logger::get("FunctionLogTrace");
            LOG_TRACE(log, message);

            return DataTypeUInt8().createColumnConst(input_rows_count, 0);
        }
    };

}

void registerFunctionLogTrace(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLogTrace>();
}

}
