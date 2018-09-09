#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// TODO Add generic implementation.

class FunctionArrayReverse : public IFunction
{
public:
    static constexpr auto name = "arrayReverse";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayReverse>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result,
                          size_t input_rows_count);

    template <typename T>
    bool executeNumber(
        const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        const ColumnNullable * nullable_col,
        ColumnNullable * nullable_res_col);

    bool executeFixedString(
        const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        const ColumnNullable * nullable_col,
        ColumnNullable * nullable_res_col);

    bool executeString(
        const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
        IColumn & res_data_col,
        const ColumnNullable * nullable_col,
        ColumnNullable * nullable_res_col);
};


void FunctionArrayReverse::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    if (executeConst(block, arguments, result, input_rows_count))
        return;

    const ColumnArray * array = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
    if (!array)
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    auto res_ptr = array->cloneEmpty();
    ColumnArray & res = static_cast<ColumnArray &>(*res_ptr);
    res.getOffsetsPtr() = array->getOffsetsPtr();

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & offsets = array->getOffsets();
    IColumn & res_data = res.getData();

    const ColumnNullable * nullable_col = nullptr;
    ColumnNullable * nullable_res_col = nullptr;

    const IColumn * inner_col;
    IColumn * inner_res_col;

    if (src_data.isColumnNullable())
    {
        nullable_col = static_cast<const ColumnNullable *>(&src_data);
        inner_col = &nullable_col->getNestedColumn();

        nullable_res_col = static_cast<ColumnNullable *>(&res_data);
        inner_res_col = &nullable_res_col->getNestedColumn();
    }
    else
    {
        inner_col = &src_data;
        inner_res_col = &res_data;
    }

    if (!( executeNumber<UInt8>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<UInt16>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<UInt32>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<UInt64>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int8>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int16>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int32>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Int64>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Float32>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeNumber<Float64>(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeString(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)
        || executeFixedString(*inner_col, offsets, *inner_res_col, nullable_col, nullable_res_col)))
        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    block.getByPosition(result).column = std::move(res_ptr);
}

bool FunctionArrayReverse::executeConst(Block & block, const ColumnNumbers & arguments, size_t result,
                                        size_t input_rows_count)
{
    if (const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[0]).column.get()))
    {
        Array arr = const_array->getValue<Array>();

        size_t size = arr.size();
        Array res(size);

        for (size_t i = 0; i < size; ++i)
            res[i] = arr[size - i - 1];

        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, res);

        return true;
    }
    else
        return false;
}

template <typename T>
bool FunctionArrayReverse::executeNumber(
    const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    const ColumnNullable * nullable_col,
    ColumnNullable * nullable_res_col)
{
    auto do_reverse = [](const auto & src_data, const auto & src_offsets, auto & res_data)
    {
        size_t size = src_offsets.size();
        ColumnArray::Offset src_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const auto * src = &src_data[src_prev_offset];
            const auto * src_end = &src_data[src_offsets[i]];

            if (src == src_end)
                continue;

            auto dst = &res_data[src_offsets[i] - 1];

            while (src < src_end)
            {
                *dst = *src;
                ++src;
                --dst;
            }

            src_prev_offset = src_offsets[i];
        }
    };

    if (const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data))
    {
        const PaddedPODArray<T> & src_data = src_data_concrete->getData();
        PaddedPODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(res_data_col).getData();
        res_data.resize(src_data.size());
        do_reverse(src_data, src_offsets, res_data);

        if ((nullable_col) && (nullable_res_col))
        {
            /// Make a reverted null map.
            const auto & src_null_map = static_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData();
            auto & res_null_map = static_cast<ColumnUInt8 &>(nullable_res_col->getNullMapColumn()).getData();
            res_null_map.resize(src_data.size());
            do_reverse(src_null_map, src_offsets, res_null_map);
        }

        return true;
    }
    else
        return false;
}

bool FunctionArrayReverse::executeFixedString(
    const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    const ColumnNullable * nullable_col,
    ColumnNullable * nullable_res_col)
{
    if (const ColumnFixedString * src_data_concrete = checkAndGetColumn<ColumnFixedString>(&src_data))
    {
        const size_t n = src_data_concrete->getN();
        const ColumnFixedString::Chars_t & src_data = src_data_concrete->getChars();
        ColumnFixedString::Chars_t & res_data = typeid_cast<ColumnFixedString &>(res_data_col).getChars();
        size_t size = src_offsets.size();
        res_data.resize(src_data.size());

        ColumnArray::Offset src_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * src = &src_data[src_prev_offset * n];
            const UInt8 * src_end = &src_data[src_offsets[i] * n];

            if (src == src_end)
                continue;

            UInt8 * dst = &res_data[src_offsets[i] * n - n];

            while (src < src_end)
            {
                /// NOTE: memcpySmallAllowReadWriteOverflow15 doesn't work correctly here.
                memcpy(dst, src, n);
                src += n;
                dst -= n;
            }

            src_prev_offset = src_offsets[i];
        }

        if ((nullable_col) && (nullable_res_col))
        {
            /// Make a reverted null map.
            const auto & src_null_map = static_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData();
            auto & res_null_map = static_cast<ColumnUInt8 &>(nullable_res_col->getNullMapColumn()).getData();
            res_null_map.resize(src_null_map.size());

            ColumnArray::Offset src_prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const UInt8 * src = &src_null_map[src_prev_offset];
                const UInt8 * src_end = &src_null_map[src_offsets[i]];

                if (src == src_end)
                    continue;

                UInt8 * dst = &res_null_map[src_offsets[i] - 1];

                while (src < src_end)
                {
                    *dst = *src;
                    ++src;
                    --dst;
                }

                src_prev_offset = src_offsets[i];
            }
        }

        return true;
    }
    else
        return false;
}

bool FunctionArrayReverse::executeString(
    const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
    IColumn & res_data_col,
    const ColumnNullable * nullable_col,
    ColumnNullable * nullable_res_col)
{
    if (const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data))
    {
        const ColumnString::Offsets & src_string_offsets = src_data_concrete->getOffsets();
        ColumnString::Offsets & res_string_offsets = typeid_cast<ColumnString &>(res_data_col).getOffsets();

        const ColumnString::Chars_t & src_data = src_data_concrete->getChars();
        ColumnString::Chars_t & res_data = typeid_cast<ColumnString &>(res_data_col).getChars();

        size_t size = src_array_offsets.size();
        res_string_offsets.resize(src_string_offsets.size());
        res_data.resize(src_data.size());

        ColumnArray::Offset src_array_prev_offset = 0;
        ColumnString::Offset res_string_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            if (src_array_offsets[i] != src_array_prev_offset)
            {
                size_t array_size = src_array_offsets[i] - src_array_prev_offset;

                for (size_t j = 0; j < array_size; ++j)
                {
                    size_t j_reversed = array_size - j - 1;

                    auto src_pos = src_array_prev_offset + j_reversed == 0 ? 0 : src_string_offsets[src_array_prev_offset + j_reversed - 1];
                    size_t string_size = src_string_offsets[src_array_prev_offset + j_reversed] - src_pos;

                    memcpySmallAllowReadWriteOverflow15(&res_data[res_string_prev_offset], &src_data[src_pos], string_size);

                    res_string_prev_offset += string_size;
                    res_string_offsets[src_array_prev_offset + j] = res_string_prev_offset;
                }
            }

            src_array_prev_offset = src_array_offsets[i];
        }

        if ((nullable_col) && (nullable_res_col))
        {
            /// Make a reverted null map.
            const auto & src_null_map = static_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData();
            auto & res_null_map = static_cast<ColumnUInt8 &>(nullable_res_col->getNullMapColumn()).getData();
            res_null_map.resize(src_string_offsets.size());

            size_t size = src_string_offsets.size();
            ColumnArray::Offset src_prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const auto * src = &src_null_map[src_prev_offset];
                const auto * src_end = &src_null_map[src_array_offsets[i]];

                if (src == src_end)
                    continue;

                auto dst = &res_null_map[src_array_offsets[i] - 1];

                while (src < src_end)
                {
                    *dst = *src;
                    ++src;
                    --dst;
                }

                src_prev_offset = src_array_offsets[i];
            }
        }

        return true;
    }
    else
        return false;
}


void registerFunctionArrayReverse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReverse>();
}

}
