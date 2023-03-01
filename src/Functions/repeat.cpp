#include <Functions/repeat.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
class FunctionRepeat : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64>(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = "repeat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRepeat>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());
        if (!isUnsignedInteger(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[1]->getName(), getName());
        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & strcolumn = arguments[0].column;
        const auto & numcolumn = arguments[1].column;
        ColumnPtr res;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(strcolumn.get()))
        {
            if (const ColumnConst * scale_column_num = checkAndGetColumn<ColumnConst>(numcolumn.get()))
            {
                UInt64 repeat_time = scale_column_num->getValue<UInt64>();
                auto col_res = ColumnString::create();
                RepeatImpl::vectorStrConstRepeat(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), repeat_time);
                return col_res;
            }
            else if (castType(arguments[1].type.get(), [&](const auto & type)
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;
                    const ColumnVector<T> * colnum = checkAndGetColumn<ColumnVector<T>>(numcolumn.get());
                    auto col_res = ColumnString::create();
                    RepeatImpl::vectorStrVectorRepeat(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), colnum->getData());
                    res = std::move(col_res);
                    return true;
                }))
            {
                return res;
            }
        }
        else if (const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(strcolumn.get()))
        {
            /// Note that const-const case is handled by useDefaultImplementationForConstants.

            std::string_view copy_str = col_const->getDataColumn().getDataAt(0).toView();

            if (castType(arguments[1].type.get(), [&](const auto & type)
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;
                    const ColumnVector<T> * colnum = checkAndGetColumn<ColumnVector<T>>(numcolumn.get());
                    auto col_res = ColumnString::create();
                    RepeatImpl::constStrVectorRepeat(copy_str, col_res->getChars(), col_res->getOffsets(), colnum->getData());
                    res = std::move(col_res);
                    return true;
                }))
            {
                return res;
            }
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
            arguments[0].column->getName(), getName());
    }
};

}

REGISTER_FUNCTION(Repeat)
{
    factory.registerFunction<FunctionRepeat>({}, FunctionFactory::CaseInsensitive);
}

}
