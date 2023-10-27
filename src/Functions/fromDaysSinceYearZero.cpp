#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/DateTimeTransforms.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>

#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>

#include <array>
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

// TODO: Move to a common header (used in makeDate)
class FunctionWithNumericParamsBase : public IFunction
{
public:
    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return false; /// invalid argument values and timestamps that are out of supported range are converted into a default value
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

protected:
    template <class DataType = DataTypeFloat32, class ArgumentNames>
    Columns convertMandatoryArguments(const ColumnsWithTypeAndName & arguments, const ArgumentNames & argument_names) const
    {
        Columns converted_arguments;
        const DataTypePtr converted_argument_type = std::make_shared<DataType>();
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            ColumnPtr argument_column = castColumn(arguments[i], converted_argument_type);
            argument_column = argument_column->convertToFullColumnIfConst();
            converted_arguments.push_back(argument_column);
        }
        return converted_arguments;
    }
};


struct DateTraits
{
    static constexpr auto name = "fromDaysSinceYearZero";
    using ReturnDataType = DataTypeDate;
};

struct DateTraits32
{
    static constexpr auto name = "fromDaysSinceYearZero32";
    using ReturnDataType = DataTypeDate32;
};

template <typename Traits>
class FunctionFromDaysSinceYearZero : public FunctionWithNumericParamsBase
{
private:
    static constexpr std::array mandatory_argument_names = {"daysSinceYearZero"};

public:
    static constexpr auto name = Traits::name;
    using RawReturnType = typename Traits::ReturnDataType::FieldType;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromDaysSinceYearZero>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {mandatory_argument_names[0], &isNumber<IDataType>, nullptr, "Number"}
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        using namespace std::chrono;
        Columns converted_arguments;
        converted_arguments = convertMandatoryArguments(arguments, mandatory_argument_names);

        auto res_column = Traits::ReturnDataType::ColumnType::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & days_data  = typeid_cast<const ColumnFloat32 &>(*converted_arguments[0]).getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto result = static_cast<Int32>(days_data[i]) - DAYS_BETWEEN_YEARS_0_AND_1970;
            // TODO: Complain if out of range rather than clamp
            //       Or provide two flavours - one that clamps and one that complains
            if (result < 0)
                result = 0;
            if (result > std::numeric_limits<RawReturnType>::max())
                result = std::numeric_limits<RawReturnType>::max();
            result_data[i] = result;
        }

        return res_column;
    }
};


}

REGISTER_FUNCTION(FromDaysSinceYearZero)
{
    // TODO: Docs
    factory.registerFunction<FunctionFromDaysSinceYearZero<DateTraits>>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionFromDaysSinceYearZero<DateTraits32>>({}, FunctionFactory::CaseInsensitive);
    // Also could do to pretty string for out of range dates (as per civil_from_days)
}

}
