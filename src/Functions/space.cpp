#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

/// Prints whitespace n-times. Actually, space() could also be pushed down to repeat(). Chose a standalone-implementation because
/// we can do memset() whereas repeat() does memcpy().
class FunctionSpace : public IFunction
{
private:
    static constexpr auto space = ' ';

    /// Safety threshold against DoS.
    static inline void checkRepeatTime(size_t repeat_time)
    {
        static constexpr auto max_repeat_times = 1'000'000uz;
        if (repeat_time > max_repeat_times)
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too many times to repeat ({}), maximum is: {}", repeat_time, max_repeat_times);
    }

public:
    static constexpr auto name = "space";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSpace>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"n", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"}
        };

        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }


    template <typename DataType>
    bool executeConstant(ColumnPtr col_times, ColumnString::Offsets & res_offsets, ColumnString::Chars & res_chars) const
    {
        const ColumnConst * col_times_const = checkAndGetColumn<ColumnConst>(col_times.get());

        const ColumnPtr & col_times_const_internal = col_times_const->getDataColumnPtr();
        if (!checkAndGetColumn<typename DataType::ColumnType>(col_times_const_internal.get()))
            return false;

        using T = typename DataType::FieldType;
        T times = col_times_const->getValue<T>();

        if (times < 1)
            times = 0;

        checkRepeatTime(times);

        res_offsets.resize(col_times->size());
        res_chars.resize(col_times->size() * (times + 1));

        size_t pos = 0;

        for (size_t i = 0; i < col_times->size(); ++i)
        {
            memset(res_chars.begin() + pos, space, times);
            pos += times;

            *(res_chars.begin() + pos) = '\0';
            pos += 1;

            res_offsets[i] = pos;
        }

        return true;
    }


    template <typename DataType>
    bool executeVector(ColumnPtr col_times_, ColumnString::Offsets & res_offsets, ColumnString::Chars & res_chars) const
    {
        auto * col_times = checkAndGetColumn<typename DataType::ColumnType>(col_times_.get());
        if (!col_times)
            return false;

        res_offsets.resize(col_times->size());
        res_chars.resize(col_times->size() * 10); /// heuristic

        const PaddedPODArray<typename DataType::FieldType> & times_data = col_times->getData();

        size_t pos = 0;

        for (size_t i = 0; i < col_times->size(); ++i)
        {
            typename DataType::FieldType times = times_data[i];

            if (times < 1)
                times = 0;

            checkRepeatTime(times);

            if (pos + times + 1 > res_chars.size())
                res_chars.resize(std::max(2 * res_chars.size(), static_cast<size_t>(pos + times + 1)));

            memset(res_chars.begin() + pos, space, times);
            pos += times;

            *(res_chars.begin() + pos) = '\0';
            pos += 1;

            res_offsets[i] = pos;
        }

        res_chars.resize(pos);

        return true;
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & col_num = arguments[0].column;

        auto col_res = ColumnString::create();

        ColumnString::Offsets & res_offsets = col_res->getOffsets();
        ColumnString::Chars & res_chars = col_res->getChars();

        if (const ColumnConst * col_num_const = checkAndGetColumn<ColumnConst>(col_num.get()))
        {
            if ((executeConstant<DataTypeUInt8>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeUInt16>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeUInt32>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeUInt64>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeInt8>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeInt16>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeInt32>(col_num, res_offsets, res_chars))
                || (executeConstant<DataTypeInt64>(col_num, res_offsets, res_chars)))
                return col_res;
        }
        else
        {
            if ((executeVector<DataTypeUInt8>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeUInt16>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeUInt32>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeUInt64>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeInt8>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeInt16>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeInt32>(col_num, res_offsets, res_chars))
                || (executeVector<DataTypeInt64>(col_num, res_offsets, res_chars)))
                return col_res;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};
}

REGISTER_FUNCTION(Space)
{
    factory.registerFunction<FunctionSpace>({}, FunctionFactory::CaseInsensitive);
}

}
