#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Common/HashTable/HashTable.h>
#include <Functions/array/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
/// arrayCompact(['a', 'a', 'b', 'b', 'a']) = ['a', 'b', 'a'] - compact arrays
namespace ErrorCodes
{
}

struct ArrayCompactImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & nested_type, const DataTypePtr &)
    {
        return std::make_shared<DataTypeArray>(nested_type);
    }

    template <typename T>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

        const ColVecType * src_values_column = checkAndGetColumn<ColVecType>(mapped.get());

        if (!src_values_column)
            return false;

        const IColumn::Offsets & src_offsets = array.getOffsets();
        const typename ColVecType::Container & src_values = src_values_column->getData();

        typename ColVecType::MutablePtr res_values_column;
        if constexpr (IsDecimalNumber<T>)
            res_values_column = ColVecType::create(src_values.size(), src_values.getScale());
        else
            res_values_column = ColVecType::create(src_values.size());

        typename ColVecType::Container & res_values = res_values_column->getData();
        size_t src_offsets_size = src_offsets.size();
        auto res_offsets_column = ColumnArray::ColumnOffsets::create(src_offsets_size);
        IColumn::Offsets & res_offsets = res_offsets_column->getData();

        size_t res_pos = 0;
        size_t src_pos = 0;

        for (size_t i = 0; i < src_offsets_size; ++i)
        {
            auto src_offset = src_offsets[i];

            /// If array is not empty.
            if (src_pos < src_offset)
            {
                /// Insert first element unconditionally.
                res_values[res_pos] = src_values[src_pos];

                /// For the rest of elements, insert if the element is different from the previous.
                ++src_pos;
                ++res_pos;
                for (; src_pos < src_offset; ++src_pos)
                {
                    if (!bitEquals(src_values[src_pos], src_values[src_pos - 1]))
                    {
                        res_values[res_pos] = src_values[src_pos];
                        ++res_pos;
                    }
                }
            }
            res_offsets[i] = res_pos;
        }
        res_values.resize(res_pos);

        res_ptr = ColumnArray::create(std::move(res_values_column), std::move(res_offsets_column));
        return true;
    }

    static void executeGeneric(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        const IColumn::Offsets & src_offsets = array.getOffsets();

        auto res_values_column = mapped->cloneEmpty();
        res_values_column->reserve(mapped->size());

        size_t src_offsets_size = src_offsets.size();
        auto res_offsets_column = ColumnArray::ColumnOffsets::create(src_offsets_size);
        IColumn::Offsets & res_offsets = res_offsets_column->getData();

        size_t res_pos = 0;
        size_t src_pos = 0;

        for (size_t i = 0; i < src_offsets_size; ++i)
        {
            auto src_offset = src_offsets[i];

            /// If array is not empty.
            if (src_pos < src_offset)
            {
                /// Insert first element unconditionally.
                res_values_column->insertFrom(*mapped, src_pos);

                /// For the rest of elements, insert if the element is different from the previous.
                ++src_pos;
                ++res_pos;
                for (; src_pos < src_offset; ++src_pos)
                {
                    if (mapped->compareAt(src_pos - 1, src_pos, *mapped, 1))
                    {
                        res_values_column->insertFrom(*mapped, src_pos);
                        ++res_pos;
                    }
                }
            }
            res_offsets[i] = res_pos;
        }

        res_ptr = ColumnArray::create(std::move(res_values_column), std::move(res_offsets_column));
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        ColumnPtr res;

        mapped = mapped->convertToFullColumnIfConst();
        if (!(executeType< UInt8 >(mapped, array, res) ||
            executeType< UInt16>(mapped, array, res) ||
            executeType< UInt32>(mapped, array, res) ||
            executeType< UInt64>(mapped, array, res) ||
            executeType< Int8  >(mapped, array, res) ||
            executeType< Int16 >(mapped, array, res) ||
            executeType< Int32 >(mapped, array, res) ||
            executeType< Int64 >(mapped, array, res) ||
            executeType<Float32>(mapped, array, res) ||
            executeType<Float64>(mapped, array, res)) ||
            executeType<Decimal32>(mapped, array, res) ||
            executeType<Decimal64>(mapped, array, res) ||
            executeType<Decimal128>(mapped, array, res))
        {
            executeGeneric(mapped, array, res);
        }
        return res;
    }
};

struct NameArrayCompact { static constexpr auto name = "arrayCompact"; };
using FunctionArrayCompact = FunctionArrayMapped<ArrayCompactImpl, NameArrayCompact>;

void registerFunctionArrayCompact(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayCompact>();
}

}

