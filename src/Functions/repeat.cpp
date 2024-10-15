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
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

struct RepeatImpl
{
    /// Safety threshold against DoS.
    static void checkRepeatTime(UInt64 repeat_time)
    {
        static constexpr UInt64 max_repeat_times = 1'000'000;
        if (repeat_time > max_repeat_times)
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too many times to repeat ({}), maximum is: {}", repeat_time, max_repeat_times);
    }

    static void checkStringSize(UInt64 size)
    {
        static constexpr UInt64 max_string_size = 1 << 30;
        if (size > max_string_size)
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size ({}) in function repeat, maximum is: {}", size, max_string_size);
    }

    template <typename T>
    static void vectorStrConstRepeat(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        T repeat_time)
    {
        repeat_time = repeat_time < 0 ? static_cast<T>(0) : repeat_time;
        checkRepeatTime(repeat_time);

        UInt64 data_size = 0;
        res_offsets.assign(offsets);
        for (UInt64 i = 0; i < offsets.size(); ++i)
        {
            /// Note that accessing -1th element is valid for PaddedPODArray.
            size_t repeated_size = (offsets[i] - offsets[i - 1] - 1) * repeat_time + 1;
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
        for (UInt64 i = 0; i < res_offsets.size(); ++i)
        {
            process(data.data() + offsets[i - 1], res_data.data() + res_offsets[i - 1], offsets[i] - offsets[i - 1], repeat_time);
        }
    }

    template <typename T>
    static void vectorStrVectorRepeat(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        const PaddedPODArray<T> & col_num)
    {
        UInt64 data_size = 0;
        res_offsets.assign(offsets);
        for (UInt64 i = 0; i < col_num.size(); ++i)
        {
            T repeat_time = col_num[i] < 0 ? static_cast<T>(0) : col_num[i];
            size_t repeated_size = (offsets[i] - offsets[i - 1] - 1) * repeat_time + 1;
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);

        for (UInt64 i = 0; i < col_num.size(); ++i)
        {
            T repeat_time = col_num[i] < 0 ? static_cast<T>(0) : col_num[i];
            checkRepeatTime(repeat_time);
            process(data.data() + offsets[i - 1], res_data.data() + res_offsets[i - 1], offsets[i] - offsets[i - 1], repeat_time);
        }
    }

    template <typename T>
    static void constStrVectorRepeat(
        std::string_view copy_str,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        const PaddedPODArray<T> & col_num)
    {
        UInt64 data_size = 0;
        res_offsets.resize(col_num.size());
        UInt64 str_size = copy_str.size();
        UInt64 col_size = col_num.size();
        for (UInt64 i = 0; i < col_size; ++i)
        {
            T repeat_time = col_num[i] < 0 ? static_cast<T>(0) : col_num[i];
            size_t repeated_size = str_size * repeat_time + 1;
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
        for (UInt64 i = 0; i < col_size; ++i)
        {
            T repeat_time = col_num[i] < 0 ? static_cast<T>(0) : col_num[i];
            checkRepeatTime(repeat_time);
            process(
                reinterpret_cast<UInt8 *>(const_cast<char *>(copy_str.data())),
                res_data.data() + res_offsets[i - 1],
                str_size + 1,
                repeat_time);
        }
    }

private:
    // A very fast repeat implementation, only invoke memcpy for O(log(n)) times.
    // as the calling times decreases, more data will be copied for each memcpy, thus
    // SIMD optimization will be more efficient.
    static void process(const UInt8 * src, UInt8 * dst, UInt64 size, UInt64 repeat_time)
    {
        if (unlikely(repeat_time <= 0))
        {
            *dst = 0;
            return;
        }

        size -= 1;
        UInt64 k = 0;
        UInt64 last_bit = repeat_time & 1;
        repeat_time >>= 1;

        const UInt8 * dst_hdr = dst;
        memcpy(dst, src, size);
        dst += size;

        while (repeat_time > 0)
        {
            UInt64 cpy_size = size * (1ULL << k);
            memcpy(dst, dst_hdr, cpy_size);
            dst += cpy_size;
            if (last_bit)
            {
                memcpy(dst, dst_hdr, cpy_size);
                dst += cpy_size;
            }
            k += 1;
            last_bit = repeat_time & 1;
            repeat_time >>= 1;
        }
        *dst = 0;
    }
};


class FunctionRepeat : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeInt128,
            DataTypeInt256,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeUInt128,
            DataTypeUInt256>(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = "repeat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRepeat>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"s", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"n", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"},
        };

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const auto & col_str = arguments[0].column;
        const auto & col_num = arguments[1].column;
        ColumnPtr res;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(col_str.get()))
        {
            if (const ColumnConst * col_num_const = checkAndGetColumn<ColumnConst>(col_num.get()))
            {
                auto col_res = ColumnString::create();
                auto success = castType(arguments[1].type.get(), [&](const auto & type)
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;
                    T times = col_num_const->getValue<T>();
                    RepeatImpl::vectorStrConstRepeat(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), times);
                    return true;
                });

                if (!success)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type {} of 'n' of function {}",
                        arguments[1].column->getName(), getName());

                return col_res;
            }
            if (castType(
                    arguments[1].type.get(),
                    [&](const auto & type)
                    {
                        using DataType = std::decay_t<decltype(type)>;
                        using T = typename DataType::FieldType;
                        const ColumnVector<T> & column = checkAndGetColumn<ColumnVector<T>>(*col_num);
                        auto col_res = ColumnString::create();
                        RepeatImpl::vectorStrVectorRepeat(
                            col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), column.getData());
                        res = std::move(col_res);
                        return true;
                    }))
            {
                return res;
            }
        }
        else if (const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(col_str.get()))
        {
            /// Note that const-const case is handled by useDefaultImplementationForConstants.

            std::string_view copy_str = col_const->getDataColumn().getDataAt(0).toView();

            if (castType(arguments[1].type.get(), [&](const auto & type)
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;
                    const ColumnVector<T> & column = checkAndGetColumn<ColumnVector<T>>(*col_num);
                    auto col_res = ColumnString::create();
                    RepeatImpl::constStrVectorRepeat(copy_str, col_res->getChars(), col_res->getOffsets(), column.getData());
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
    factory.registerFunction<FunctionRepeat>({}, FunctionFactory::Case::Insensitive);
}

}
