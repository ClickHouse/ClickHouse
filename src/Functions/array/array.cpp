#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_variant_type;
    extern const SettingsBool use_variant_as_common_type;
}

/// array(c1, c2, ...) - create an array.
class FunctionArray : public IFunction
{
public:
    static constexpr auto name = "array";

    explicit FunctionArray(bool use_variant_as_common_type_ = false) : use_variant_as_common_type(use_variant_as_common_type_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionArray>(context->getSettingsRef()[Setting::allow_experimental_variant_type] && context->getSettingsRef()[Setting::use_variant_as_common_type]);
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// array(..., Nothing, ...) -> Array(..., Nothing, ...)
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (use_variant_as_common_type)
            return std::make_shared<DataTypeArray>(getLeastSupertypeOrVariant(arguments));

        return std::make_shared<DataTypeArray>(getLeastSupertype(arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const size_t num_elements = arguments.size();

        if (num_elements == 0)
        {
            /// We should return constant empty array.
            return result_type->createColumnConstWithDefaultValue(input_rows_count);
        }

        const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();

        /** If part of columns have not same type as common type of all elements of array,
            *  then convert them to common type.
            * If part of columns are constants,
            *  then convert them to full columns.
            */

        Columns columns_holder(num_elements);
        ColumnRawPtrs column_ptrs(num_elements);
        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = arguments[i];

            ColumnPtr preprocessed_column = arg.column;

            if (!arg.type->equals(*elem_type))
                preprocessed_column = castColumn(arg, elem_type);

            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();

            columns_holder[i] = std::move(preprocessed_column);
            column_ptrs[i] = columns_holder[i].get();
        }

        /// Create and fill the result array.
        auto out = ColumnArray::create(elem_type->createColumn());
        IColumn & out_data = out->getData();
        IColumn::Offsets & out_offsets = out->getOffsets();

        /// Fill out_offsets
        out_offsets.resize_exact(input_rows_count);
        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            current_offset += num_elements;
            out_offsets[i] = current_offset;
        }

        /// Fill out_data
        out_data.reserve(input_rows_count * num_elements);
        if (num_elements == 1)
            out_data.insertRangeFrom(*column_ptrs[0], 0, input_rows_count);
        else
            execute(column_ptrs, out_data, input_rows_count);
        return out;
    }

private:
    bool execute(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        return executeNumber<UInt8>(columns, out_data, input_rows_count) || executeNumber<UInt16>(columns, out_data, input_rows_count)
            || executeNumber<UInt32>(columns, out_data, input_rows_count) || executeNumber<UInt64>(columns, out_data, input_rows_count)
            || executeNumber<UInt128>(columns, out_data, input_rows_count) || executeNumber<UInt256>(columns, out_data, input_rows_count)
            || executeNumber<Int8>(columns, out_data, input_rows_count) || executeNumber<Int16>(columns, out_data, input_rows_count)
            || executeNumber<Int32>(columns, out_data, input_rows_count) || executeNumber<Int64>(columns, out_data, input_rows_count)
            || executeNumber<Int128>(columns, out_data, input_rows_count) || executeNumber<Int256>(columns, out_data, input_rows_count)
            || executeNumber<Float32>(columns, out_data, input_rows_count) || executeNumber<Float64>(columns, out_data, input_rows_count)
            || executeNumber<Decimal32>(columns, out_data, input_rows_count)
            || executeNumber<Decimal64>(columns, out_data, input_rows_count)
            || executeNumber<Decimal128>(columns, out_data, input_rows_count)
            || executeNumber<Decimal256>(columns, out_data, input_rows_count)
            || executeNumber<DateTime64>(columns, out_data, input_rows_count) || executeString(columns, out_data, input_rows_count)
            || executeNullable(columns, out_data, input_rows_count) || executeTuple(columns, out_data, input_rows_count)
            || executeFixedString(columns, out_data, input_rows_count) || executeGeneric(columns, out_data, input_rows_count);
    }

    template <typename T>
    bool executeNumber(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        using Container = ColumnVectorOrDecimal<T>::Container;
        std::vector<const Container *> containers(columns.size(), nullptr);
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const ColumnVectorOrDecimal<T> * concrete_column = checkAndGetColumn<ColumnVectorOrDecimal<T>>(columns[i]);
            if (!concrete_column)
                return false;

            containers[i] = &concrete_column->getData();
        }

        ColumnVectorOrDecimal<T> & concrete_out_data = assert_cast<ColumnVectorOrDecimal<T> &>(out_data);
        Container & out_container = concrete_out_data.getData();
        out_container.resize_exact(columns.size() * input_rows_count);

        for (size_t row_i = 0; row_i < input_rows_count; ++row_i)
        {
            const size_t base = row_i * columns.size();
            for (size_t col_i = 0; col_i < columns.size(); ++col_i)
                out_container[base + col_i] = (*containers[col_i])[row_i];
        }
        return true;
    }

    bool executeString(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        size_t total_bytes = 0;
        std::vector<const ColumnString *> concrete_columns(columns.size(), nullptr);
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const ColumnString * concrete_column = checkAndGetColumn<ColumnString>(columns[i]);
            if (!concrete_column)
                return false;

            total_bytes += concrete_column->getChars().size();
            concrete_columns[i] = concrete_column;
        }

        ColumnString & concrete_out_data = assert_cast<ColumnString &>(out_data);
        auto & out_chars = concrete_out_data.getChars();
        auto & out_offsets = concrete_out_data.getOffsets();
        out_chars.resize_exact(total_bytes);
        out_offsets.resize_exact(input_rows_count * columns.size());

        size_t cur_out_offset = 0;
        for (size_t row_i = 0; row_i < input_rows_count; ++row_i)
        {
            const size_t base = row_i * columns.size();
            for (size_t col_i = 0; col_i < columns.size(); ++col_i)
            {
                StringRef ref = concrete_columns[col_i]->getDataAt(row_i);
                memcpySmallAllowReadWriteOverflow15(&out_chars[cur_out_offset], ref.data, ref.size);
                out_chars[cur_out_offset + ref.size] = 0;

                cur_out_offset += ref.size + 1;
                out_offsets[base + col_i] = cur_out_offset;
            }
        }
        return true;
    }

    bool executeFixedString(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        std::vector<const ColumnFixedString *> concrete_columns(columns.size(), nullptr);
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const ColumnFixedString * concrete_column = checkAndGetColumn<ColumnFixedString>(columns[i]);
            if (!concrete_column)
                return false;

            concrete_columns[i] = concrete_column;
        }

        ColumnFixedString & concrete_out_data = assert_cast<ColumnFixedString &>(out_data);
        auto & out_chars = concrete_out_data.getChars();

        const size_t n = concrete_out_data.getN();
        size_t total_bytes = n * columns.size() * input_rows_count;
        out_chars.resize_exact(total_bytes);

        size_t curr_out_offset = 0;
        for (size_t row_i = 0; row_i < input_rows_count; ++row_i)
        {
            for (size_t col_i = 0; col_i < columns.size(); ++col_i)
            {
                StringRef ref = concrete_columns[col_i]->getDataAt(row_i);
                memcpySmallAllowReadWriteOverflow15(&out_chars[curr_out_offset], ref.data, n);
                curr_out_offset += n;
            }
        }
        return true;
    }

    bool executeNullable(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        ColumnRawPtrs null_maps(columns.size(), nullptr);
        ColumnRawPtrs nested_columns(columns.size(), nullptr);
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const ColumnNullable * concrete_column = checkAndGetColumn<ColumnNullable>(columns[i]);
            if (!concrete_column)
                return false;

            null_maps[i] = &concrete_column->getNullMapColumn();
            nested_columns[i] = &concrete_column->getNestedColumn();
        }

        ColumnNullable & concrete_out_data = assert_cast<ColumnNullable &>(out_data);
        auto & out_null_map = concrete_out_data.getNullMapColumn();
        auto & out_nested_column = concrete_out_data.getNestedColumn();
        execute(null_maps, out_null_map, input_rows_count);
        execute(nested_columns, out_nested_column, input_rows_count);
        return true;
    }

    bool executeTuple(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        ColumnTuple * concrete_out_data = typeid_cast<ColumnTuple *>(&out_data);
        if (!concrete_out_data)
            return false;

        const size_t tuple_size = concrete_out_data->tupleSize();
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnRawPtrs elem_columns(columns.size(), nullptr);
            for (size_t j = 0; j < columns.size(); ++j)
            {
                const ColumnTuple * concrete_column = assert_cast<const ColumnTuple *>(columns[j]);
                elem_columns[j] = &concrete_column->getColumn(i);
            }
            execute(elem_columns, concrete_out_data->getColumn(i), input_rows_count);
        }
        return true;
    }

    bool executeGeneric(const ColumnRawPtrs & columns, IColumn & out_data, size_t input_rows_count) const
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            for (const auto * column : columns)
                out_data.insertFrom(*column, i);
        return true;
    }


    String getName() const override
    {
        return name;
    }

    bool use_variant_as_common_type = false;
};


REGISTER_FUNCTION(Array)
{
    factory.registerFunction<FunctionArray>();
}

}
