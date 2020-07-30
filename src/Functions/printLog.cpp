#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionPrintLog : public IFunction
{
public:
    static constexpr auto name = "printLog";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPrintLog>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        String message;
        UInt64 quantity;

        if (const ColumnConst * col_message = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get()))
            message = col_message->getDataAt(0).data;
        else
            throw Exception("First argument for function " + getName() + " must be Constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_quantity = checkAndGetColumnConst<ColumnUInt64>(block.getByPosition(arguments[1]).column.get()))
            quantity = col_quantity->getUInt(0);
        else
            throw Exception("Second argument for function " + getName() + " must be Constant UInt64 number", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (size_t i = 0; i < quantity; ++i)
            LOG_INFO(&Poco::Logger::get("printLog"), message);

        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, static_cast<String>("Ok."));
    }
};

void registerFunctionPrintLog(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPrintLog>();
}

}
