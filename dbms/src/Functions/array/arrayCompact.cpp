#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
    }

    struct ArrayCompactImpl
    {
        static bool needBoolean() { return false; }
        static bool needExpression() { return false; }
        static bool needOneArray() { return false; }

        static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
        {
            WhichDataType which(expression_return);

            if (which.isNativeUInt())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());

            if (which.isNativeInt())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());

            if (which.isFloat())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

            throw Exception("arrayCompact cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }


        template <typename Element, typename Result>
        static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
        {
            const ColumnVector<Element> * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);

            if (!column)
            {
                const ColumnConst * column_const = checkAndGetColumnConst<ColumnVector<Element>>(&*mapped);

                if (!column_const)
                    return false;

                const Element x = column_const->template getValue<Element>();
                const IColumn::Offsets & offsets = array.getOffsets();
                auto column_data = ColumnVector<Result>::create(column_const->size());
                typename ColumnVector<Result>::Container & res_values = column_data->getData();
                auto column_offsets = ColumnArray::ColumnOffsets::create(offsets.size());
                IColumn::Offsets & res_offsets = column_offsets->getData();

                size_t res_pos = 0;
                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    if (pos < offsets[i])
                    {
                        res_values[res_pos] = x;
                        for (++pos, ++res_pos; pos < offsets[i]; ++pos)
                        {
                            res_values[res_pos++] = x;
                        }
                    }
                    res_offsets[i] = res_pos;
                }
                for(size_t i = 0; i < column_data->size() - res_pos; ++i)
                {
                    res_values.pop_back();
                }
                res_ptr = ColumnArray::create(std::move(column_data), std::move(column_offsets));
                return true;
            }

            const IColumn::Offsets & offsets = array.getOffsets();
            const typename ColumnVector<Element>::Container & data = column->getData();
            auto column_data = ColumnVector<Result>::create(data.size());
            typename ColumnVector<Result>::Container & res_values = column_data->getData();
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
                        if(data[pos] != data[pos - 1])
                        {
                            res_values[res_pos++] = data[pos];
                        }
                    }
                }
                res_offsets[i] = res_pos;
            }
            for(size_t i = 0; i < data.size() - res_pos; ++i)
            {
                res_values.pop_back();
            }
            res_ptr = ColumnArray::create(std::move(column_data), std::move(column_offsets));
            return true;
        }

        static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
        {
            ColumnPtr res;

            if (executeType< UInt8 , UInt64>(mapped, array, res) ||
                executeType< UInt16, UInt64>(mapped, array, res) ||
                executeType< UInt32, UInt64>(mapped, array, res) ||
                executeType< UInt64, UInt64>(mapped, array, res) ||
                executeType<  Int8 ,  Int64>(mapped, array, res) ||
                executeType<  Int16,  Int64>(mapped, array, res) ||
                executeType<  Int32,  Int64>(mapped, array, res) ||
                executeType<  Int64,  Int64>(mapped, array, res) ||
                executeType<Float32,Float64>(mapped, array, res) ||
                executeType<Float64,Float64>(mapped, array, res))
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

