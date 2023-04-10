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
#include <Functions/IFunction.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

class FunctionStringToMap : public IFunction
{
public:
    static constexpr auto name = "stringToMap";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringToMap>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t num_arguments = arguments.size();

        if (num_arguments < 1 || num_arguments > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Number of arguments for function {} doesn't match: "
                            "passed {}, should be 1, 2 or 3", getName(), num_arguments);
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[0]->getName(), getName());
        if (num_arguments >= 2 && !isStringOrFixedString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[1]->getName(), getName());
        if (num_arguments == 3 && !isStringOrFixedString(arguments[2]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[2]->getName(), getName());

        DataTypePtr string_type = std::make_shared<DataTypeString>();
        DataTypePtr string_type_nullable = std::make_shared<DataTypeNullable>(string_type);
        DataTypes key_value_type{string_type, string_type_nullable};

        return std::make_shared<DataTypeMap>(key_value_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    static bool tryExecuteString(const IColumn * col, ColumnPtr & col_res, char pair_delim, char key_value_delim)
    {
        const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            const ColumnString::Chars & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();

            MutableColumnPtr keys_data = DataTypeString().createColumn();
            MutableColumnPtr values_data = DataTypeString().createColumn();
            MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

            ColumnUInt8::MutablePtr values_null_map = ColumnUInt8::create();

            ColumnString::Offset curr_res_offset = 0, curr_in_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const char * pos_in = reinterpret_cast<const char *>(&in_vec[curr_in_offset]);
                size_t curr_size = strlen(pos_in);
                const char * pos_in_end = pos_in + curr_size;
                const char * pos_delim = nullptr;
                std::vector<String> key_value_pairs;
                // splitting the string into key-value pairs
                while (pos_delim != pos_in_end)
                {
                    pos_delim = reinterpret_cast<const char *>(strchr(pos_in, pair_delim));
                    if (!pos_delim)
                        pos_delim = pos_in_end;

                    key_value_pairs.emplace_back(pos_in, pos_delim);
                    pos_in = pos_delim + 1;
                }
                // preparing the key, value columns for the map
                for (size_t pair = 0; pair < key_value_pairs.size(); pair++)
                {
                    const char * begin = key_value_pairs[pair].c_str();
                    const char * end = begin + key_value_pairs[pair].size();
                    const char * delim = reinterpret_cast<const char *>(strchr(begin, key_value_delim));
                    if (delim)
                    {
                        keys_data->insertData(begin, delim - begin);
                        values_data->insertData(delim + 1, end - delim - 1);
                        values_null_map->insert(false);
                    }
                    else
                    {
                        keys_data->insertData(begin, end - begin);
                        values_data->insertData(begin, 1);
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
            return true;
        }
        else
        {
            return false;
        }
    }

    static bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res, char pair_delim, char key_value_delim)
    {
        const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            const ColumnString::Chars & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            MutableColumnPtr keys_data = DataTypeString().createColumn();
            MutableColumnPtr values_data = DataTypeString().createColumn();
            MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

            ColumnUInt8::MutablePtr values_null_map = ColumnUInt8::create();

            ColumnString::Offset curr_res_offset = 0;
            const char * pos_in = reinterpret_cast<const char *>(in_vec.data());
            size_t n = col_fstr_in->getN();
            for (size_t i = 0; i < size; ++i)
            {
                size_t curr_size = strnlen(pos_in, n);
                const char * pos_in_end = pos_in + curr_size;
                const char * pos_delim = nullptr;
                std::vector<String> key_value_pairs;
                const char * curr_pos = pos_in;
                // splitting the string into key-value pairs
                while (pos_delim != pos_in_end)
                {
                    pos_delim = reinterpret_cast<const char *>(strchr(curr_pos, pair_delim));
                    if (!pos_delim)
                        pos_delim = pos_in_end;

                    key_value_pairs.emplace_back(curr_pos, pos_delim);
                    curr_pos = pos_delim + 1;
                }
                // preparing the key, value columns for the map
                for (size_t pair = 0; pair < key_value_pairs.size(); pair++)
                {
                    const char * begin = key_value_pairs[pair].c_str();
                    const char * end = begin + key_value_pairs[pair].size();
                    const char * delim = reinterpret_cast<const char *>(strchr(begin, key_value_delim));
                    if (delim)
                    {
                        keys_data->insertData(begin, delim - begin);
                        values_data->insertData(delim + 1, end - delim - 1);
                        values_null_map->insert(false);
                    }
                    else
                    {
                        keys_data->insertData(begin, end - begin);
                        values_data->insertData(begin, 1);
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
            return true;
        }
        else
        {
            return false;
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t /*input_rows_count*/) const override
    {
        const IColumn * column = arguments[0].column.get();
        ColumnPtr res_column;
        size_t num_arguments = arguments.size();

        char pair_delim = ',',
             key_value_delim = ':';

        if (num_arguments >= 2)
        {
            const auto * column_const_pair_delim = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
            if(!column_const_pair_delim)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Pair delimiter needs to be a constant in function {}", getName());

            auto pair_delim_value = column_const_pair_delim->getValue<String>();

            if(pair_delim_value.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Illegal pair delimiter for function {}. Must be exactly one byte.", getName());

            pair_delim = pair_delim_value[0];
        }

        if (num_arguments == 3)
        {
            const auto * column_const_key_value_delim = checkAndGetColumn<ColumnConst>(arguments[2].column.get());
            if(!column_const_key_value_delim)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Key-Value delimiter needs to be a constant in function {}", getName());

            auto key_value_delim_value = column_const_key_value_delim->getValue<String>();

            if(key_value_delim_value.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Illegal key-value delimiter for function {}. Must be exactly one byte.", getName());

            key_value_delim = key_value_delim_value[0];
        }

        if (tryExecuteFixedString(column, res_column, pair_delim, key_value_delim) || tryExecuteString(column, res_column, pair_delim, key_value_delim))
            return res_column;

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[0].column->getName(), getName());
    }
};


REGISTER_FUNCTION(StringToMap)
{
    factory.registerFunction<FunctionStringToMap>();
}

}
