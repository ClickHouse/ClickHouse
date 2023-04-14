#include <boost/algorithm/string.hpp>

#include <base/find_symbols.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>  
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

class FunctionMapFromString : public IFunction
{
public:
    static constexpr auto name = "mapFromString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapFromString>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"str_expr", &isStringOrFixedString<IDataType>, nullptr, "String or Fixed String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"pair_delim", &isString<IDataType>, isColumnConst, "const String"},
            {"key_value_delim", &isString<IDataType>, isColumnConst, "const String"}
        };

        validateFunctionArgumentTypes(*this, arguments, args, optional_args);

        DataTypePtr string_type = std::make_shared<DataTypeString>();
        DataTypePtr string_type_nullable = std::make_shared<DataTypeNullable>(string_type);
        DataTypes key_value_type{string_type, string_type_nullable};

        return std::make_shared<DataTypeMap>(key_value_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    static void executeString(const ColumnString * col_str_in, ColumnPtr & col_res, size_t input_rows_count, char pair_delim, char key_value_delim)
    {
        String pair_delim_str(1, pair_delim);
        
        const ColumnString::Chars & in_vec = col_str_in->getChars();
        const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();
        size_t size = in_offsets.size();

        MutableColumnPtr keys_data = DataTypeString().createColumn();
        MutableColumnPtr values_data = DataTypeString().createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        keys_data->reserve(input_rows_count);
        values_data->reserve(input_rows_count);
        offsets->reserve(input_rows_count);

        ColumnUInt8::MutablePtr values_null_map = ColumnUInt8::create();

        ColumnString::Offset curr_res_offset = 0, curr_in_offset = 0;

        std::vector<String> key_value_pairs;

        for (size_t i = 0; i < size; ++i)
        {
            key_value_pairs.clear();
            const char * pos_in = reinterpret_cast<const char *>(&in_vec[curr_in_offset]);
            size_t curr_size = in_offsets[i] - curr_in_offset - 1;
            // splitting the string into key-value pairs
            boost::split(key_value_pairs, std::string(pos_in, curr_size), boost::is_any_of(pair_delim_str));
            // preparing the key, value columns for the map
            for (size_t j = 0; j < key_value_pairs.size(); j++)
            {
                const std::string_view str_view = key_value_pairs[j];
                size_t delim_pos = str_view.find(key_value_delim);
                if (delim_pos != str_view.npos)
                {
                    keys_data->insert(str_view.substr(0, delim_pos));
                    values_data->insert(str_view.substr(delim_pos + 1));
                    values_null_map->insert(false);
                }
                else
                {
                    keys_data->insert(str_view);
                    values_data->insert(str_view.substr(0, 0));
                    values_null_map->insert(true);
                }
            }
            curr_in_offset = in_offsets[i];
            curr_res_offset += key_value_pairs.size();
            offsets->insert(curr_res_offset);
        }

        auto values_data_nullable = ColumnNullable::create(
            std::move(values_data), std::move(values_null_map));

        auto nested_column = ColumnArray::create(
        ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data_nullable)}),
        std::move(offsets));

        col_res = ColumnMap::create(nested_column);            
    }

    static void executeFixedString(const ColumnFixedString * col_fstr_in, ColumnPtr & col_res, size_t input_rows_count, char pair_delim, char key_value_delim)
    {
        String pair_delim_str(1, pair_delim);

        const ColumnFixedString::Chars & in_vec = col_fstr_in->getChars();
        size_t size = col_fstr_in->size();

        MutableColumnPtr keys_data = DataTypeString().createColumn();
        MutableColumnPtr values_data = DataTypeString().createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        keys_data->reserve(input_rows_count);
        values_data->reserve(input_rows_count);
        offsets->reserve(input_rows_count);

        ColumnUInt8::MutablePtr values_null_map = ColumnUInt8::create();

        ColumnString::Offset curr_res_offset = 0;
        const char * pos_in = reinterpret_cast<const char *>(in_vec.data());
        size_t n = col_fstr_in->getN();

        std::vector<String> key_value_pairs;

        for (size_t i = 0; i < size; ++i)
        {
            key_value_pairs.clear();
            size_t curr_size = strnlen(pos_in, n);
            
            // splitting the string into key-value pairs
            boost::split(key_value_pairs, std::string(pos_in, curr_size), boost::is_any_of(pair_delim_str));
            // preparing the key, value columns for the map
            for (size_t j = 0; j < key_value_pairs.size(); j++)
            {
                const std::string_view str_view = key_value_pairs[j];
                size_t delim_pos = str_view.find(key_value_delim);
                if (delim_pos != str_view.npos)
                {
                    keys_data->insert(str_view.substr(0, delim_pos));
                    values_data->insert(str_view.substr(delim_pos + 1));
                    values_null_map->insert(false);
                }
                else
                {
                    keys_data->insert(str_view);
                    values_data->insert(str_view.substr(0, 0));
                    values_null_map->insert(true);
                }
            }
            curr_res_offset += key_value_pairs.size();
            offsets->insert(curr_res_offset);
            pos_in += n;
        }

        auto values_data_nullable = ColumnNullable::create(
            std::move(values_data), std::move(values_null_map));

        auto nested_column = ColumnArray::create(
        ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data_nullable)}),
        std::move(offsets));

        col_res = ColumnMap::create(nested_column);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t input_rows_count) const override
    {
        const IColumn * column = arguments[0].column.get();
        ColumnPtr res_column;
        size_t num_arguments = arguments.size();

        char pair_delim = ',';
        char key_value_delim = ':';

        if (num_arguments >= 2)
        {
            auto pair_delim_value = arguments[1].column->getDataAt(0);

            if (pair_delim_value.size != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Illegal pair delimiter for function {}. Must be exactly one byte.", getName());

            pair_delim = pair_delim_value.data[0];
        }

        if (num_arguments == 3)
        {
            auto key_value_delim_value = arguments[2].column->getDataAt(0);

            if (key_value_delim_value.size != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Illegal key-value delimiter for function {}. Must be exactly one byte.", getName());

            key_value_delim = key_value_delim_value.data[0];
        }
        
        if (const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(column))
            executeString(col_str_in, res_column, input_rows_count, pair_delim, key_value_delim);
        else if (const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(column))
            executeFixedString(col_fstr_in, res_column, input_rows_count, pair_delim, key_value_delim);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[0].column->getName(), getName());

        return res_column;
    }
};


REGISTER_FUNCTION(MapFromString)
{
    factory.registerFunction<FunctionMapFromString>();

    // For compatibility with Databricks and Spark
    factory.registerAlias("str_to_map", "mapFromString", FunctionFactory::CaseInsensitive);
}

}
