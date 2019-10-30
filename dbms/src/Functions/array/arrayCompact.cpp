#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include "FunctionArrayMapped.h"
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

        static DataTypePtr getReturnType(const DataTypePtr & nested_type, const DataTypePtr & /*nested_type*/)
        {
            return std::make_shared<DataTypeArray>(nested_type);
        }

        template <typename T>
        static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
        {
            const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(&*mapped);

            if (!column)
                return false;

            const IColumn::Offsets & offsets = array.getOffsets();
            const typename ColumnVector<T>::Container & data = column->getData();
            auto column_data = ColumnVector<T>::create(data.size());
            typename ColumnVector<T>::Container & res_values = column_data->getData();
            auto column_offsets = ColumnArray::ColumnOffsets::create(offsets.size());
            IColumn::Offsets & res_offsets = column_offsets->getData();

            size_t res_pos = 0;
            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                if (pos < offsets[i])
                {
                    res_values[res_pos] = data[pos];
                    for (++pos, ++res_pos; pos < offsets[i]; ++pos)
                    {
                        if (data[pos] != data[pos - 1])
                        {
                            res_values[res_pos++] = data[pos];
                        }
                    }
                }
                res_offsets[i] = res_pos;
            }
            for (size_t i = 0; i < data.size() - res_pos; ++i)
            {
                res_values.pop_back();
            }
            res_ptr = ColumnArray::create(std::move(column_data), std::move(column_offsets));
            return true;
        }

        static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
        {
            ColumnPtr res;

            if (executeType< UInt8 >(mapped, array, res) ||
                executeType< UInt16>(mapped, array, res) ||
                executeType< UInt32>(mapped, array, res) ||
                executeType< UInt64>(mapped, array, res) ||
                executeType< Int8  >(mapped, array, res) ||
                executeType< Int16 >(mapped, array, res) ||
                executeType< Int32 >(mapped, array, res) ||
                executeType< Int64 >(mapped, array, res) ||
                executeType<Float32>(mapped, array, res) ||
                executeType<Float64>(mapped, array, res))
                return res;
            else
                throw Exception("Unexpected column for arrayCompact: " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

    };

    struct NameArrayCompact { static constexpr auto name = "arrayCompact"; };
    using FunctionArrayCompact = FunctionArrayMapped<ArrayCompactImpl, NameArrayCompact>;

    void registerFunctionArrayCompact(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionArrayCompact>();
    }

}

