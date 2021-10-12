#include <cassert>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include "Core/ColumnWithTypeAndName.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct TupArg
{
    const IColumn & key_column;
    const IColumn & val_column;
    const IColumn::Offsets & key_offsets;
    const IColumn::Offsets & val_offsets;
    bool is_const;
};
using TupleMaps = std::vector<struct TupArg>;

namespace OpTypes
{
    extern const int ADD = 0;
    extern const int SUBTRACT = 1;
}

template <int op_type>
class FunctionMapOp : public IFunction
{
public:
    static constexpr auto name = (op_type == OpTypes::ADD) ? "mapAdd" : "mapSubtract";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMapOp>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        bool is_float = false;
        DataTypePtr key_type, val_type, res;

        if (arguments.size() < 2)
            throw Exception{getName() + " accepts at least two map tuples", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        for (const auto & tup_arg : arguments)
        {
            const DataTypeTuple * tup = checkAndGetDataType<DataTypeTuple>(tup_arg.get());
            if (!tup)
                throw Exception{getName() + " accepts at least two map tuples", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

            auto elems = tup->getElements();
            if (elems.size() != 2)
                throw Exception(
                    "Each tuple in " + getName() + " arguments should consist of two arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypeArray * k = checkAndGetDataType<DataTypeArray>(elems[0].get());
            const DataTypeArray * v = checkAndGetDataType<DataTypeArray>(elems[1].get());

            if (!k || !v)
                throw Exception(
                    "Each tuple in " + getName() + " arguments should consist of two arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto result_type = v->getNestedType();
            if (!result_type->canBePromoted())
                throw Exception{"Values to be summed are expected to be Numeric, Float or Decimal.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            WhichDataType which_val(result_type);

            auto promoted_type = result_type->promoteNumericType();
            if (!key_type)
            {
                key_type = k->getNestedType();
                val_type = promoted_type;
                is_float = which_val.isFloat();
            }
            else
            {
                if (!(k->getNestedType()->equals(*key_type)))
                    throw Exception(
                        "All key types in " + getName() + " should be same: " + key_type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                if (is_float != which_val.isFloat())
                    throw Exception(
                        "All value types in " + getName() + " should be or float or integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                if (!(promoted_type->equals(*val_type)))
                {
                    throw Exception(
                        "All value types in " + getName() + " should be promotable to " + val_type->getName() + ", got "
                            + promoted_type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }

            if (!res)
            {
                res = std::make_shared<DataTypeTuple>(
                    DataTypes{std::make_shared<DataTypeArray>(k->getNestedType()), std::make_shared<DataTypeArray>(promoted_type)});
            }
        }

        return res;
    }

    template <typename KeyType, bool is_str_key, typename ValType>
    ColumnPtr execute2(size_t row_count, TupleMaps & args, const DataTypeTuple & res_type) const
    {
        MutableColumnPtr res_tuple = res_type.createColumn();

        auto * to_tuple = assert_cast<ColumnTuple *>(res_tuple.get());
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple->getColumn(0));
        auto & to_keys_data = to_keys_arr.getData();
        auto & to_keys_offset = to_keys_arr.getOffsets();

        auto & to_vals_arr = assert_cast<ColumnArray &>(to_tuple->getColumn(1));
        auto & to_vals_data = to_vals_arr.getData();

        size_t res_offset = 0;
        std::map<KeyType, ValType> summing_map;

        for (size_t i = 0; i < row_count; i++)
        {
            [[maybe_unused]] bool first = true;
            for (auto & arg : args)
            {
                size_t offset = 0, len = arg.key_offsets[0];

                if (!arg.is_const)
                {
                    offset = i > 0 ? arg.key_offsets[i - 1] : 0;
                    len = arg.key_offsets[i] - offset;

                    if (arg.val_offsets[i] != arg.key_offsets[i])
                        throw Exception(
                            "Key and value array should have same amount of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }

                for (size_t j = 0; j < len; j++)
                {
                    KeyType key;
                    if constexpr (is_str_key)
                    {
                        // have to use Field structs to get strings
                        key = arg.key_column.operator[](offset + j).get<KeyType>();
                    }
                    else
                    {
                        key = assert_cast<const ColumnVector<KeyType> &>(arg.key_column).getData()[offset + j];
                    }

                    auto value = arg.val_column.operator[](offset + j).get<ValType>();

                    if constexpr (op_type == OpTypes::ADD)
                    {
                        const auto [it, inserted] = summing_map.insert({key, value});
                        if (!inserted)
                            it->second += value;
                    }
                    else
                    {
                        static_assert(op_type == OpTypes::SUBTRACT);
                        const auto [it, inserted] = summing_map.insert({key, first ? value : -value});
                        if (!inserted)
                            it->second -= value;
                    }
                }

                first = false;
            }

            for (const auto & elem : summing_map)
            {
                res_offset++;
                to_keys_data.insert(elem.first);
                to_vals_data.insert(elem.second);
            }
            to_keys_offset.push_back(res_offset);
            summing_map.clear();
        }

        // same offsets as in keys
        to_vals_arr.getOffsets().insert(to_keys_offset.begin(), to_keys_offset.end());

        return res_tuple;
    }

    template <typename KeyType, bool is_str_key>
    ColumnPtr execute1(size_t row_count, const DataTypeTuple & res_type, TupleMaps & args) const
    {
        const auto & promoted_type = (assert_cast<const DataTypeArray *>(res_type.getElements()[1].get()))->getNestedType();
#define MATCH_EXECUTE(is_str) \
        switch (promoted_type->getTypeId()) { \
            case TypeIndex::Int64: return execute2<KeyType, is_str, Int64>(row_count, args, res_type); \
            case TypeIndex::UInt64: return execute2<KeyType, is_str, UInt64>(row_count, args, res_type); \
            case TypeIndex::Float64: return execute2<KeyType, is_str, Float64>(row_count, args, res_type); \
            default: \
                throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN}; \
        }

        if constexpr (is_str_key)
        {
            MATCH_EXECUTE(true)
        }
        else
        {
            MATCH_EXECUTE(false)
        }
#undef MATCH_EXECUTE
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const DataTypeTuple * tup_type = checkAndGetDataType<DataTypeTuple>((arguments[0]).type.get());
        const DataTypeArray * key_array_type = checkAndGetDataType<DataTypeArray>(tup_type->getElements()[0].get());
        const DataTypeArray * val_array_type = checkAndGetDataType<DataTypeArray>(tup_type->getElements()[1].get());

        /* determine output type */
        const DataTypeTuple & res_type
            = DataTypeTuple(DataTypes{std::make_shared<DataTypeArray>(key_array_type->getNestedType()),
                                      std::make_shared<DataTypeArray>(val_array_type->getNestedType()->promoteNumericType())});

        TupleMaps args{};
        args.reserve(arguments.size());

        //prepare columns, extract data columns for direct access and put them to the vector
        for (const auto & col : arguments)
        {
            const ColumnTuple * tup;
            bool is_const = isColumnConst(*col.column);
            if (is_const)
            {
                const auto * c = assert_cast<const ColumnConst *>(col.column.get());
                tup = assert_cast<const ColumnTuple *>(c->getDataColumnPtr().get());
            }
            else
                tup = assert_cast<const ColumnTuple *>(col.column.get());

            const auto & arr1 = assert_cast<const ColumnArray &>(tup->getColumn(0));
            const auto & arr2 = assert_cast<const ColumnArray &>(tup->getColumn(1));

            const auto & key_offsets = arr1.getOffsets();
            const auto & key_column = arr1.getData();

            const auto & val_offsets = arr2.getOffsets();
            const auto & val_column = arr2.getData();

            // we can check const columns before any processing
            if (is_const)
            {
                if (val_offsets[0] != key_offsets[0])
                    throw Exception(
                        "Key and value array should have same amount of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            args.push_back({key_column, val_column, key_offsets, val_offsets, is_const});
        }

        size_t row_count = arguments[0].column->size();
        auto key_type_id = key_array_type->getNestedType()->getTypeId();

        switch (key_type_id)
        {
            case TypeIndex::Enum8:
            case TypeIndex::Int8:
                return execute1<Int8, false>(row_count, res_type, args);
            case TypeIndex::Enum16:
            case TypeIndex::Int16:
                return execute1<Int16, false>(row_count, res_type, args);
            case TypeIndex::Int32:
                return execute1<Int32, false>(row_count, res_type, args);
            case TypeIndex::Int64:
                return execute1<Int64, false>(row_count, res_type, args);
            case TypeIndex::UInt8:
                return execute1<UInt8, false>(row_count, res_type, args);
            case TypeIndex::Date:
            case TypeIndex::UInt16:
                return execute1<UInt16, false>(row_count, res_type, args);
            case TypeIndex::DateTime:
            case TypeIndex::UInt32:
                return execute1<UInt32, false>(row_count, res_type, args);
            case TypeIndex::UInt64:
                return execute1<UInt64, false>(row_count, res_type, args);
            case TypeIndex::UUID:
                return execute1<UInt128, false>(row_count, res_type, args);
            case TypeIndex::FixedString:
            case TypeIndex::String:
                return execute1<String, true>(row_count, res_type, args);
            default:
                throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};

}

void registerFunctionMapOp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapOp<OpTypes::ADD>>();
    factory.registerFunction<FunctionMapOp<OpTypes::SUBTRACT>>();
}

}
