#include <Functions/repeat.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionSpace : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64>(type, std::forward<F>(f));
    }

    public:
        static constexpr auto space_str = " ";
        static constexpr auto name = "space";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSpace>(); }

        String getName() const override { return name; }
        size_t getNumberOfArguments() const override { return 1; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 1 || !isUnsignedInteger(arguments[0]))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Function {} must have one argument with unsigned integer type",
                                name);
            }
            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
        {
            const ColumnPtr & numColumn = arguments[0].column;
            ColumnPtr res;
            if (castType(arguments[0].type.get(), [&](const auto & type)
            {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;
                    const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(numColumn.get());
                    auto col_res = ColumnString::create();
                    RepeatImpl::constStrVectorRepeat(space_str, col_res->getChars(), col_res->getOffsets(), column->getData());
                    res = std::move(col_res);
                    return true;
            }))
            {
                return res;
            }
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
            arguments[0].column->getName(), getName());
        }
};
}

REGISTER_FUNCTION(Space)
{
    factory.registerFunction<FunctionSpace>({
        "Repeats a space as many times as specified and concatenates the replicated values as a single string."
    }, FunctionFactory::CaseInsensitive);
}

}
