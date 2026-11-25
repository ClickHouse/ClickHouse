#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>


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
    /// A 128- or 256-bit count is compared in `T` before any narrowing, so a count above `UINT64_MAX` is not
    /// truncated into a small one. A narrower count is widened instead, because the threshold does not fit
    /// into an 8- or 16-bit `T`. `repeat_time` must already be non-negative.
    template <typename T>
    static void checkRepeatTime(T repeat_time)
    {
        static constexpr UInt64 max_repeat_times = 1'000'000;
        bool too_many = false;
        if constexpr (sizeof(T) > sizeof(UInt64))
            too_many = repeat_time > static_cast<T>(max_repeat_times);
        else
            too_many = static_cast<UInt64>(repeat_time) > max_repeat_times;
        if (too_many)
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too many times to repeat ({}), maximum is: {}", toString(repeat_time), max_repeat_times);
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
            size_t repeated_size = static_cast<size_t>((offsets[i] - offsets[static_cast<ssize_t>(i) - 1]) * repeat_time);
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
        if (repeat_time == 0)
            return;
        if (repeat_time == 1)
        {
            if (data_size != 0)
                memcpy(res_data.data(), data.data(), data_size);
            return;
        }
#if defined(__x86_64__)
        const size_t rows = offsets.size();
        /// Both strategies support every row width; the average only selects the copy strategy.
        if (rows != 0 && data.size() / rows > 16)
        {
            for (UInt64 i = 0; i < rows; ++i)
            {
                processImpl<false>(
                    data.data() + offsets[static_cast<ssize_t>(i) - 1],
                    res_data.data() + res_offsets[static_cast<ssize_t>(i) - 1],
                    offsets[i] - offsets[static_cast<ssize_t>(i) - 1],
                    static_cast<UInt64>(repeat_time));
            }
            return;
        }
#endif
        for (UInt64 i = 0; i < res_offsets.size(); ++i)
        {
            process(
                data.data() + offsets[static_cast<ssize_t>(i) - 1],
                res_data.data() + res_offsets[static_cast<ssize_t>(i) - 1],
                offsets[i] - offsets[static_cast<ssize_t>(i) - 1],
                static_cast<UInt64>(repeat_time));
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
            /// Bound the multiplier before multiplying: an unbounded one overflows the product and
            /// slips past `checkStringSize`, which would then under-size `res_data`.
            checkRepeatTime(repeat_time);
            size_t repeated_size = static_cast<size_t>((offsets[i] - offsets[static_cast<ssize_t>(i) - 1]) * repeat_time);
            checkStringSize(repeated_size);
            data_size += repeated_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);

        for (UInt64 i = 0; i < col_num.size(); ++i)
        {
            T repeat_time = col_num[i] < 0 ? static_cast<T>(0) : col_num[i];
            checkRepeatTime(repeat_time);
            process(
                data.data() + offsets[static_cast<ssize_t>(i) - 1],
                res_data.data() + res_offsets[static_cast<ssize_t>(i) - 1],
                offsets[i] - offsets[static_cast<ssize_t>(i) - 1],
                static_cast<UInt64>(repeat_time));
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
            /// Bound the multiplier before multiplying: an unbounded one overflows the product and
            /// slips past `checkStringSize`, which would then under-size `res_data`.
            checkRepeatTime(repeat_time);
            size_t repeated_size = static_cast<size_t>(str_size * repeat_time);
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
                res_data.data() + res_offsets[static_cast<ssize_t>(i) - 1],
                str_size,
                static_cast<UInt64>(repeat_time));
        }
    }

private:
    template <bool short_seed>
    static ALWAYS_INLINE void processImpl(const UInt8 * src, UInt8 * dst, UInt64 size, UInt64 repeat_time)
    {
        auto copy = [](UInt8 * to, const UInt8 * from, UInt64 length)
        {
            if constexpr (short_seed)
            {
                if (length <= 16)
                {
                    /// `ColumnString` padding permits the extra bytes; the rounded ranges can overlap.
                    __builtin_memmove(to, from, 16);
                    return;
                }
                if (length <= 32)
                {
                    __builtin_memcpy(to, from, 16);
                    __builtin_memcpy(to + length - 16, from + length - 16, 16);
                    return;
                }
                if (length <= 64)
                {
                    __builtin_memcpy(to, from, 32);
                    __builtin_memcpy(to + length - 32, from + length - 32, 32);
                    return;
                }
            }
            memcpy(to, from, length);
        };
        UInt64 k = 0;
        UInt64 last_bit = repeat_time & 1;
        repeat_time >>= 1;

        const UInt8 * dst_hdr = dst;
        copy(dst, src, size);
        dst += size;

        while (repeat_time > 0)
        {
            UInt64 copy_size = size * (1ULL << k);
            copy(dst, dst_hdr, copy_size);
            dst += copy_size;
            if (last_bit)
            {
                copy(dst, dst_hdr, copy_size);
                dst += copy_size;
            }
            k += 1;
            last_bit = repeat_time & 1;
            repeat_time >>= 1;
        }
    }

    static ALWAYS_INLINE void process(const UInt8 * src, UInt8 * dst, UInt64 size, UInt64 repeat_time)
    {
        if (unlikely(repeat_time == 0))
            return;
#if defined(__x86_64__)
        if (size <= 16)
        {
            if (size == 0)
                return;
            if (size == 1)
            {
                if (repeat_time == 1)
                    *dst = *src;
                else
                    memset(dst, *src, repeat_time);
                return;
            }
            processImpl<true>(src, dst, size, repeat_time);
            return;
        }
#endif
        processImpl<false>(src, dst, size, repeat_time);
    }
};


class FunctionRepeat final : public IFunction
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

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(String, Integer) -> String";
    }

    /// `repeat` throws `TOO_LARGE_STRING_SIZE` when the number of repetitions or the size of the
    /// resulting string exceeds the limit, and both of them depend on the data of a single row.
    [[nodiscard]] std::optional<bool> canThrowIfKnown(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
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

            std::string_view copy_str = col_const->getDataColumn().getDataAt(0);

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
    FunctionDocumentation::Description description = R"(
Concatenates a string as many times with itself as specified.
    )";
    FunctionDocumentation::Syntax syntax = "repeat(s, n)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to repeat.", {"String"}},
        {"n", "The number of times to repeat the string.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A string containing string `s` repeated `n` times. If `n` is negative, the function returns the empty string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT repeat('abc', 10)",
        R"(
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
    )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRepeat>(documentation, FunctionFactory::Case::Insensitive);
}

}
