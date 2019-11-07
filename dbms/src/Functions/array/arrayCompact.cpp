#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/array/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
/// arrayCompact(['a', 'a', 'b', 'b', 'a']) = ['a', 'b', 'a'] - compact arrays
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

struct ArrayCompactImpl
{
    static bool useDefaultImplementationForConstants() { return true; }
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
        const ColumnVector<T> * src_values_column = checkAndGetColumn<ColumnVector<T>>(mapped.get());

        if (!src_values_column)
            return false;

        const IColumn::Offsets & src_offsets = array.getOffsets();
        const typename ColumnVector<T>::Container & src_values = src_values_column->getData();

        auto res_values_column = ColumnVector<T>::create(src_values.size());
        typename ColumnVector<T>::Container & res_values = res_values_column->getData();
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
                    if (src_values[src_pos] != src_values[src_pos - 1])
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

        if (!(executeType< UInt8 >(mapped, array, res) ||
            executeType< UInt16>(mapped, array, res) ||
            executeType< UInt32>(mapped, array, res) ||
            executeType< UInt64>(mapped, array, res) ||
            executeType< Int8  >(mapped, array, res) ||
            executeType< Int16 >(mapped, array, res) ||
            executeType< Int32 >(mapped, array, res) ||
            executeType< Int64 >(mapped, array, res) ||
            executeType<Float32>(mapped, array, res) ||
            executeType<Float64>(mapped, array, res)))
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

