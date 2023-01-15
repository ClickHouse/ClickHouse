#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <pcg_random.hpp>
#include <Common/assert_cast.h>
#include <Common/randomSeed.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Shuffle array elements
 * arrayShuffle(arr)
 * arrayShuffle(arr, seed)
 */
class FunctionArrayShuffle : public IFunction
{
public:
    static constexpr auto name = "arrayShuffle";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShuffle>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 2 || arguments.empty())
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs 1..2 arguments; passed {}.", getName(), arguments.size());
        }

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            WhichDataType which(arguments[1]);
            if (!which.isUInt() && !which.isInt())
                throw Exception(
                    "Illegal type " + arguments[1]->getName() + " of argument of function " + getName() + " (must be UInt or Int)",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override;

private:
    template <typename T>
    static bool executeNumber(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, pcg64_fast &);
    static bool executeFixedString(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, pcg64_fast & rng);
    static bool executeString(const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets, IColumn & res_data, pcg64_fast & rng);
    static bool executeGeneric(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, pcg64_fast &);
};

ColumnPtr FunctionArrayShuffle::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!array)
        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

    auto res_ptr = array->cloneEmpty();
    ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);
    res.getOffsetsPtr() = array->getOffsetsPtr();

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & offsets = array->getOffsets();

    IColumn & res_data = res.getData();

    const ColumnNullable * src_nullable_col = typeid_cast<const ColumnNullable *>(&src_data);
    ColumnNullable * res_nullable_col = typeid_cast<ColumnNullable *>(&res_data);

    const IColumn * src_inner_col = src_nullable_col ? &src_nullable_col->getNestedColumn() : &src_data;
    IColumn * res_inner_col = res_nullable_col ? &res_nullable_col->getNestedColumn() : &res_data;

    const auto seed = [&]() -> uint64_t
    {
        if (arguments.size() == 1)
            return randomSeed();
        const auto * val = arguments[1].column.get();
        return val->getUInt(0);
    }();
    pcg64_fast rng(seed);

    false // NOLINT
        || executeNumber<UInt8>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<UInt16>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<UInt32>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<UInt64>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<Int8>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<Int16>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<Int32>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<Int64>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<Float32>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeNumber<Float64>(*src_inner_col, offsets, *res_inner_col, rng)
        || executeString(*src_inner_col, offsets, *res_inner_col, rng)
        || executeFixedString(*src_inner_col, offsets, *res_inner_col, rng)
        || executeGeneric(*src_inner_col, offsets, *res_inner_col, rng);

    if (src_nullable_col)
    {
        rng.seed(seed);
        if (!executeNumber<UInt8>(src_nullable_col->getNullMapColumn(), offsets, res_nullable_col->getNullMapColumn(), rng))
            throw Exception(
                "Illegal column " + src_nullable_col->getNullMapColumn().getName() + " of null map of the first argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    return res_ptr;
}

bool FunctionArrayShuffle::executeGeneric(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, pcg64_fast & rng)
{
    size_t size = src_offsets.size();
    res_data.reserve(size);

    IColumn::Permutation permutation;
    ColumnArray::Offset prev_off = 0;
    for (auto off: src_offsets)
    {
        size_t count = off - prev_off;

        permutation.resize(count);
        for (size_t idx = 0; idx < count; ++idx)
            permutation[idx] = idx;
        std::shuffle(std::begin(permutation), std::end(permutation), rng);
        for (size_t unshuffled_idx = 0; unshuffled_idx != count; ++unshuffled_idx)
        {
            auto shuffled_idx = permutation[unshuffled_idx];
            res_data.insertFrom(src_data, shuffled_idx);
        }
        prev_off = off;
    }

    return true;
}

template <typename T>
bool FunctionArrayShuffle::executeNumber(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, pcg64_fast & rng)
{
    if (const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data))
    {
        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();
        PaddedPODArray<T> & res_vec = typeid_cast<ColumnVector<T> &>(res_data).getData();
        res_vec.resize(src_data.size());

        ColumnArray::Offset prev_off = 0;
        for (auto off: src_offsets)
        {
            const auto * src = &src_vec[prev_off];
            const auto * src_end = &src_vec[off];
            if (src == src_end)
                continue;
            auto * dst = &res_vec[prev_off];
            size_t count = off - prev_off;
            memcpy(dst, src, count * sizeof(T));

            std::shuffle(dst, dst + count, rng);

            prev_off = off;
        }
        return true;
    }
    else
        return false;
}

bool FunctionArrayShuffle::executeFixedString(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, pcg64_fast & rng)
{
    if (const ColumnFixedString * src_data_concrete = checkAndGetColumn<ColumnFixedString>(&src_data))
    {
        const size_t n = src_data_concrete->getN();
        const ColumnFixedString::Chars & src_data_chars = src_data_concrete->getChars();
        ColumnFixedString::Chars & res_chars = typeid_cast<ColumnFixedString &>(res_data).getChars();
        res_chars.resize(src_data_chars.size());

        IColumn::Permutation permutation;
        ColumnArray::Offset prev_off = 0;
        for (auto off: src_offsets)
        {
            const UInt8 * src = &src_data_chars[prev_off * n];
            size_t count = off - prev_off;

            if (count == 0)
                continue;

            UInt8 * dst = &res_chars[prev_off * n];

            permutation.resize(count);
            for (size_t idx = 0; idx < count; ++idx)
                permutation[idx] = idx;
            std::shuffle(std::begin(permutation), std::end(permutation), rng);

            for (size_t unshuffled_idx = 0; unshuffled_idx != count; ++unshuffled_idx)
            {
                auto shuffled_idx = permutation[unshuffled_idx];
                memcpy(dst + unshuffled_idx * n, src + shuffled_idx * n, n);
            }
            prev_off = off;
        }
        return true;
    }
    else
        return false;
}

bool FunctionArrayShuffle::executeString(const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets, IColumn & res_data, pcg64_fast & rng)
{
    if (const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data))
    {
        const ColumnString::Offsets & src_string_offsets = src_data_concrete->getOffsets();
        ColumnString::Offsets & res_string_offsets = typeid_cast<ColumnString &>(res_data).getOffsets();

        const ColumnString::Chars & src_data_chars = src_data_concrete->getChars();
        ColumnString::Chars & res_chars = typeid_cast<ColumnString &>(res_data).getChars();

        res_string_offsets.resize(src_string_offsets.size());
        res_chars.resize(src_data_chars.size());

        IColumn::Permutation permutation;
        ColumnArray::Offset arr_prev_off = 0;
        ColumnString::Offset string_prev_off = 0;
        for (auto arr_off: src_array_offsets)
        {
            if (arr_off != arr_prev_off)
            {
                size_t string_count = arr_off - arr_prev_off;

                permutation.resize(string_count);
                for (size_t idx = 0; idx < string_count; ++idx)
                    permutation[idx] = idx;
                std::shuffle(std::begin(permutation), std::end(permutation), rng);

                for (size_t unshuffled_idx = 0; unshuffled_idx < string_count; ++unshuffled_idx)
                {
                    auto shuffled_idx = permutation[unshuffled_idx];
                    auto src_pos = src_string_offsets[arr_prev_off + shuffled_idx - 1];
                    size_t string_size = src_string_offsets[arr_prev_off + shuffled_idx] - src_pos;
                    memcpySmallAllowReadWriteOverflow15(&res_chars[string_prev_off], &src_data_chars[src_pos], string_size);

                    string_prev_off += string_size;
                    res_string_offsets[arr_prev_off + unshuffled_idx] = string_prev_off;
                }
            }
            arr_prev_off = arr_off;
        }
        return true;
    }
    else
        return false;
}

REGISTER_FUNCTION(ArrayShuffle)
{
    factory.registerFunction<FunctionArrayShuffle>();
}

}
