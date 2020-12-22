#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** emptyArrayToSingle(arr) - replace empty arrays with arrays of one element with a default value.
  */
class FunctionEmptyArrayToSingle : public IFunction
{
public:
    static constexpr auto name = "emptyArrayToSingle";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionEmptyArrayToSingle>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};


namespace
{
    namespace FunctionEmptyArrayToSingleImpl
    {
        ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
        {
            if (const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get()))
            {
                if (const_array->getValue<Array>().empty())
                {
                    auto nested_type = typeid_cast<const DataTypeArray &>(*arguments[0].type).getNestedType();

                    return result_type->createColumnConst(
                        input_rows_count,
                        Array{nested_type->getDefault()});
                }
                else
                    return arguments[0].column;
            }
            else
                return nullptr;
        }

        template <typename T, bool nullable>
        bool executeNumber(
            const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data))
            {
                const PaddedPODArray<T> & src_data_vec = src_data_concrete->getData();
                PaddedPODArray<T> & res_data = assert_cast<ColumnVector<T> &>(res_data_col).getData();

                size_t size = src_offsets.size();
                res_offsets.resize(size);
                res_data.reserve(src_data_vec.size());

                if (nullable)
                    res_null_map->reserve(src_null_map->size());

                ColumnArray::Offset src_prev_offset = 0;
                ColumnArray::Offset res_prev_offset = 0;

                for (size_t i = 0; i < size; ++i)
                {
                    if (src_offsets[i] != src_prev_offset)
                    {
                        size_t size_to_write = src_offsets[i] - src_prev_offset;
                        res_data.resize(res_prev_offset + size_to_write);
                        memcpy(&res_data[res_prev_offset], &src_data_vec[src_prev_offset], size_to_write * sizeof(T));

                        if (nullable)
                        {
                            res_null_map->resize(res_prev_offset + size_to_write);
                            memcpy(&(*res_null_map)[res_prev_offset], &(*src_null_map)[src_prev_offset], size_to_write);
                        }

                        res_prev_offset += size_to_write;
                        res_offsets[i] = res_prev_offset;
                    }
                    else
                    {
                        res_data.push_back(T());
                        ++res_prev_offset;
                        res_offsets[i] = res_prev_offset;

                        if (nullable)
                            res_null_map->push_back(1); /// Push NULL.
                    }

                    src_prev_offset = src_offsets[i];
                }

                return true;
            }
            else
                return false;
        }


        template <bool nullable>
        bool executeFixedString(
            const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (const ColumnFixedString * src_data_concrete = checkAndGetColumn<ColumnFixedString>(&src_data))
            {
                const size_t n = src_data_concrete->getN();
                const ColumnFixedString::Chars & src_data_vec = src_data_concrete->getChars();

                auto * concrete_res_data = typeid_cast<ColumnFixedString *>(&res_data_col);
                if (!concrete_res_data)
                    throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

                ColumnFixedString::Chars & res_data = concrete_res_data->getChars();
                size_t size = src_offsets.size();
                res_offsets.resize(size);
                res_data.reserve(src_data_vec.size());

                if (nullable)
                    res_null_map->reserve(src_null_map->size());

                ColumnArray::Offset src_prev_offset = 0;
                ColumnArray::Offset res_prev_offset = 0;

                for (size_t i = 0; i < size; ++i)
                {
                    if (src_offsets[i] != src_prev_offset)
                    {
                        size_t size_to_write = src_offsets[i] - src_prev_offset;
                        size_t prev_res_data_size = res_data.size();
                        res_data.resize(prev_res_data_size + size_to_write * n);
                        memcpy(&res_data[prev_res_data_size], &src_data_vec[src_prev_offset * n], size_to_write * n);

                        if (nullable)
                        {
                            res_null_map->resize(res_prev_offset + size_to_write);
                            memcpy(&(*res_null_map)[res_prev_offset], &(*src_null_map)[src_prev_offset], size_to_write);
                        }

                        res_prev_offset += size_to_write;
                        res_offsets[i] = res_prev_offset;
                    }
                    else
                    {
                        size_t prev_res_data_size = res_data.size();
                        res_data.resize(prev_res_data_size + n);
                        memset(&res_data[prev_res_data_size], 0, n);
                        ++res_prev_offset;
                        res_offsets[i] = res_prev_offset;

                        if (nullable)
                            res_null_map->push_back(1);
                    }

                    src_prev_offset = src_offsets[i];
                }

                return true;
            }
            else
                return false;
        }


        template <bool nullable>
        bool executeString(
            const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_array_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data))
            {
                const ColumnString::Offsets & src_string_offsets = src_data_concrete->getOffsets();

                auto * concrete_res_string_offsets = typeid_cast<ColumnString *>(&res_data_col);
                if (!concrete_res_string_offsets)
                    throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
                ColumnString::Offsets & res_string_offsets = concrete_res_string_offsets->getOffsets();

                const ColumnString::Chars & src_data_vec = src_data_concrete->getChars();

                auto * concrete_res_data = typeid_cast<ColumnString *>(&res_data_col);
                if (!concrete_res_data)
                    throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
                ColumnString::Chars & res_data = concrete_res_data->getChars();

                size_t size = src_array_offsets.size();
                res_array_offsets.resize(size);
                res_string_offsets.reserve(src_string_offsets.size());
                res_data.reserve(src_data_vec.size());

                if (nullable)
                    res_null_map->reserve(src_null_map->size());

                ColumnArray::Offset src_array_prev_offset = 0;
                ColumnArray::Offset res_array_prev_offset = 0;

                ColumnString::Offset src_string_prev_offset = 0;
                ColumnString::Offset res_string_prev_offset = 0;

                for (size_t i = 0; i < size; ++i)
                {
                    if (src_array_offsets[i] != src_array_prev_offset)
                    {
                        size_t array_size = src_array_offsets[i] - src_array_prev_offset;

                        size_t bytes_to_copy = 0;
                        size_t from_string_prev_offset_local = src_string_prev_offset;
                        for (size_t j = 0; j < array_size; ++j)
                        {
                            size_t string_size = src_string_offsets[src_array_prev_offset + j] - from_string_prev_offset_local;

                            res_string_prev_offset += string_size;
                            res_string_offsets.push_back(res_string_prev_offset);

                            from_string_prev_offset_local += string_size;
                            bytes_to_copy += string_size;
                        }

                        size_t res_data_old_size = res_data.size();
                        res_data.resize(res_data_old_size + bytes_to_copy);
                        memcpy(&res_data[res_data_old_size], &src_data_vec[src_string_prev_offset], bytes_to_copy);

                        if (nullable)
                        {
                            res_null_map->resize(res_array_prev_offset + array_size);
                            memcpy(&(*res_null_map)[res_array_prev_offset], &(*src_null_map)[src_array_prev_offset], array_size);
                        }

                        res_array_prev_offset += array_size;
                        res_array_offsets[i] = res_array_prev_offset;
                    }
                    else
                    {
                        res_data.push_back(0);  /// An empty string, including zero at the end.

                        if (nullable)
                            res_null_map->push_back(1);

                        ++res_string_prev_offset;
                        res_string_offsets.push_back(res_string_prev_offset);

                        ++res_array_prev_offset;
                        res_array_offsets[i] = res_array_prev_offset;
                    }

                    src_array_prev_offset = src_array_offsets[i];

                    if (src_array_prev_offset)
                        src_string_prev_offset = src_string_offsets[src_array_prev_offset - 1];
                }

                return true;
            }
            else
                return false;
        }


        template <bool nullable>
        void executeGeneric(
            const IColumn & src_data, const ColumnArray::Offsets & src_offsets,
            IColumn & res_data, ColumnArray::Offsets & res_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            size_t size = src_offsets.size();
            res_offsets.resize(size);
            res_data.reserve(src_data.size());

            if (nullable)
                res_null_map->reserve(src_null_map->size());

            ColumnArray::Offset src_prev_offset = 0;
            ColumnArray::Offset res_prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                if (src_offsets[i] != src_prev_offset)
                {
                    size_t size_to_write = src_offsets[i] - src_prev_offset;
                    res_data.insertRangeFrom(src_data, src_prev_offset, size_to_write);

                    if (nullable)
                    {
                        res_null_map->resize(res_prev_offset + size_to_write);
                        memcpy(&(*res_null_map)[res_prev_offset], &(*src_null_map)[src_prev_offset], size_to_write);
                    }

                    res_prev_offset += size_to_write;
                    res_offsets[i] = res_prev_offset;
                }
                else
                {
                    res_data.insertDefault();
                    ++res_prev_offset;
                    res_offsets[i] = res_prev_offset;

                    if (nullable)
                        res_null_map->push_back(1);
                }

                src_prev_offset = src_offsets[i];
            }
        }


        template <bool nullable>
        void executeDispatch(
            const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets,
            IColumn & res_data_col, ColumnArray::Offsets & res_array_offsets,
            const NullMap * src_null_map,
            NullMap * res_null_map)
        {
            if (!(executeNumber<UInt8, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<UInt16, nullable> (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<UInt32, nullable> (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<UInt64, nullable> (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int8, nullable>   (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int16, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int32, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Int64, nullable>  (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Float32, nullable>(src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeNumber<Float64, nullable>(src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeString<nullable>         (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)
                || executeFixedString<nullable>    (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map)))
                executeGeneric<nullable>           (src_data, src_array_offsets, res_data_col, res_array_offsets, src_null_map, res_null_map);
        }
    }
}


ColumnPtr FunctionEmptyArrayToSingle::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    if (auto res = FunctionEmptyArrayToSingleImpl::executeConst(arguments, result_type, input_rows_count))
        return res;

    const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!array)
        throw Exception("Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    MutableColumnPtr res_ptr = array->cloneEmpty();
    ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & src_offsets = array->getOffsets();
    IColumn & res_data = res.getData();
    ColumnArray::Offsets & res_offsets = res.getOffsets();

    const NullMap * src_null_map = nullptr;
    NullMap * res_null_map = nullptr;

    const IColumn * inner_col;
    IColumn * inner_res_col;

    const auto * nullable_col = checkAndGetColumn<ColumnNullable>(src_data);
    if (nullable_col)
    {
        inner_col = &nullable_col->getNestedColumn();
        src_null_map = &nullable_col->getNullMapData();

        auto & nullable_res_col = assert_cast<ColumnNullable &>(res_data);
        inner_res_col = &nullable_res_col.getNestedColumn();
        res_null_map = &nullable_res_col.getNullMapData();
    }
    else
    {
        inner_col = &src_data;
        inner_res_col = &res_data;
    }

    if (nullable_col)
        FunctionEmptyArrayToSingleImpl::executeDispatch<true>(*inner_col, src_offsets, *inner_res_col, res_offsets, src_null_map, res_null_map);
    else
        FunctionEmptyArrayToSingleImpl::executeDispatch<false>(*inner_col, src_offsets, *inner_res_col, res_offsets, src_null_map, res_null_map);

    return res_ptr;
}


void registerFunctionEmptyArrayToSingle(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayToSingle>();
}

}
