#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/castTypeToEither.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

struct RepeatImpl
{
    /// Safety threshold against DoS.
    static inline void checkRepeatTime(UInt64 repeat_time)
    {
        static constexpr UInt64 max_repeat_times = 1000000;
        if (repeat_time > max_repeat_times)
            throw Exception("Too many times to repeat (" + std::to_string(repeat_time) + "), maximum is: " + std::to_string(max_repeat_times),
                ErrorCodes::TOO_LARGE_STRING_SIZE);
    }

    static inline void checkStringSize(UInt64 size)
    {
        static constexpr UInt64 max_string_size = 1 << 30;
        if (size > max_string_size)
            throw Exception("Too large string size (" + std::to_string(size) + ") in function repeat, maximum is: " + std::to_string(max_string_size),
                ErrorCodes::TOO_LARGE_STRING_SIZE);
    }

    static void vectorStrConstRepeat(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        UInt64 repeat_time)
    {
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
            size_t repeated_size = (offsets[i] - offsets[i - 1] - 1) * col_num[i] + 1;
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);

        for (UInt64 i = 0; i < col_num.size(); ++i)
        {
            T repeat_time = col_num[i];
            checkRepeatTime(repeat_time);
            process(data.data() + offsets[i - 1], res_data.data() + res_offsets[i - 1], offsets[i] - offsets[i - 1], repeat_time);
        }
    }

    template <typename T>
    static void constStrVectorRepeat(
        const StringRef & copy_str,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        const PaddedPODArray<T> & col_num)
    {
        UInt64 data_size = 0;
        res_offsets.resize(col_num.size());
        UInt64 str_size = copy_str.size;
        UInt64 col_size = col_num.size();
        for (UInt64 i = 0; i < col_size; ++i)
        {
            size_t repeated_size = str_size * col_num[i] + 1;
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
        for (UInt64 i = 0; i < col_size; ++i)
        {
            T repeat_time = col_num[i];
            checkRepeatTime(repeat_time);
            process(
                reinterpret_cast<UInt8 *>(const_cast<char *>(copy_str.data)),
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
        return castTypeToEither<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64>(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = "repeat";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRepeat>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!isUnsignedInteger(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

            StringRef copy_str = col_const->getDataColumn().getDataAt(0);

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

        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

void registerFunctionRepeat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRepeat>();
}

}
