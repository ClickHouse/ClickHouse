#include <cassert>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <common/arithmeticOverflow.h>
#include "Columns/ColumnMap.h"
#include "DataTypes/DataTypeMap.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct TupArg
{
    const ColumnPtr & key_column;
    const ColumnPtr & val_column;
    const IColumn::Offsets & key_offsets;
    const IColumn::Offsets & val_offsets;
    bool is_const;
};
using TupleMaps = std::vector<TupArg>;

enum class OpTypes
{
    ADD = 0,
    SUBTRACT = 1
};

template <OpTypes op_type>
class FunctionMapOp : public IFunction
{
public:
    static constexpr auto name = (op_type == OpTypes::ADD) ? "mapAdd" : "mapSubtract";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapOp>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void checkTypes(
        DataTypePtr & key_type, DataTypePtr & promoted_val_type, const DataTypePtr & check_key_type, DataTypePtr & check_val_type) const
    {
        if (!(check_key_type->equals(*key_type)))
            throw Exception(
                "Expected same " + key_type->getName() + " type for all keys in " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        WhichDataType which_val(promoted_val_type);
        WhichDataType which_ch_val(check_val_type);

        if (which_ch_val.isFloat() != which_val.isFloat())
            throw Exception(
                "All value types in " + getName() + " should be either or float or integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!(check_val_type->equals(*promoted_val_type)))
        {
            throw Exception(
                "All value types in " + getName() + " should be promotable to " + promoted_val_type->getName() + ", got "
                    + check_val_type->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    DataTypePtr getReturnTypeForTuples(const DataTypes & arguments) const
    {
        DataTypePtr key_type, val_type, res;

        for (const auto & arg : arguments)
        {
            const DataTypeArray * k;
            const DataTypeArray * v;

            const DataTypeTuple * tup = checkAndGetDataType<DataTypeTuple>(arg.get());
            if (!tup)
                throw Exception(getName() + " accepts at least two map tuples", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            auto elems = tup->getElements();
            if (elems.size() != 2)
                throw Exception(
                    "Each tuple in " + getName() + " arguments should consist of two arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            k = checkAndGetDataType<DataTypeArray>(elems[0].get());
            v = checkAndGetDataType<DataTypeArray>(elems[1].get());

            if (!k || !v)
                throw Exception(
                    "Each tuple in " + getName() + " arguments should consist of two arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto result_type = v->getNestedType();
            if (!result_type->canBePromoted())
                throw Exception(
                    "Values to be summed are expected to be Numeric, Float or Decimal.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto promoted_val_type = result_type->promoteNumericType();
            if (!key_type)
            {
                key_type = k->getNestedType();
                val_type = promoted_val_type;
                res = std::make_shared<DataTypeTuple>(
                    DataTypes{std::make_shared<DataTypeArray>(k->getNestedType()), std::make_shared<DataTypeArray>(promoted_val_type)});
            }
            else
                checkTypes(key_type, val_type, k->getNestedType(), promoted_val_type);
        }

        return res;
    }

    DataTypePtr getReturnTypeForMaps(const DataTypes & arguments) const
    {
        DataTypePtr key_type, val_type, res;

        for (const auto & arg : arguments)
        {
            const auto * map = checkAndGetDataType<DataTypeMap>(arg.get());
            if (!map)
                throw Exception(getName() + " accepts at least two maps", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            const auto & v = map->getValueType();

            if (!v->canBePromoted())
                throw Exception(
                    "Values to be summed are expected to be Numeric, Float or Decimal.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto promoted_val_type = v->promoteNumericType();
            if (!key_type)
            {
                key_type = map->getKeyType();
                val_type = promoted_val_type;
                res = std::make_shared<DataTypeMap>(DataTypes({key_type, promoted_val_type}));
            }
            else
                checkTypes(key_type, val_type, map->getKeyType(), promoted_val_type);
        }

        return res;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(getName() + " accepts at least two maps or map tuples", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getTypeId() == TypeIndex::Tuple)
            return getReturnTypeForTuples(arguments);
        else if (arguments[0]->getTypeId() == TypeIndex::Map)
            return getReturnTypeForMaps(arguments);
        else
            throw Exception(getName() + " only accepts maps", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <typename KeyType, typename ValType>
    ColumnPtr execute2(size_t row_count, TupleMaps & args, const DataTypePtr res_type) const
    {
        MutableColumnPtr res_column = res_type->createColumn();
        IColumn *to_keys_data, *to_vals_data;
        ColumnArray::Offsets * to_keys_offset;
        ColumnArray::Offsets * to_vals_offset = nullptr;

        // prepare output destinations
        if (res_type->getTypeId() == TypeIndex::Tuple)
        {
            auto * to_tuple = assert_cast<ColumnTuple *>(res_column.get());
            auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple->getColumn(0));
            to_keys_data = &to_keys_arr.getData();
            to_keys_offset = &to_keys_arr.getOffsets();

            auto & to_vals_arr = assert_cast<ColumnArray &>(to_tuple->getColumn(1));
            to_vals_data = &to_vals_arr.getData();
            to_vals_offset = &to_vals_arr.getOffsets();
        }
        else
        {
            assert(res_type->getTypeId() == TypeIndex::Map);

            auto * to_map = assert_cast<ColumnMap *>(res_column.get());
            auto & to_wrapper_arr = to_map->getNestedColumn();
            to_keys_offset = &to_wrapper_arr.getOffsets();

            auto & to_map_tuple = to_map->getNestedData();
            to_keys_data = &to_map_tuple.getColumn(0);
            to_vals_data = &to_map_tuple.getColumn(1);
        }

        std::map<KeyType, ValType> summing_map;

        for (size_t i = 0; i < row_count; i++)
        {
            [[maybe_unused]] bool first = true;
            for (auto & arg : args)
            {
                size_t offset = 0, len = arg.key_offsets[0];

                if (!arg.is_const)
                {
                    offset = arg.key_offsets[i - 1];
                    len = arg.key_offsets[i] - offset;

                    if (arg.val_offsets[i] != arg.key_offsets[i])
                        throw Exception(
                            "Key and value array should have same amount of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }

                Field temp_val;
                for (size_t j = 0; j < len; j++)
                {
                    KeyType key;
                    if constexpr (std::is_same<KeyType, String>::value)
                    {
                        if (const auto * col_fixed = checkAndGetColumn<ColumnFixedString>(arg.key_column.get()))
                            key = col_fixed->getDataAt(offset + j).toString();
                        else if (const auto * col_str = checkAndGetColumn<ColumnString>(arg.key_column.get()))
                            key = col_str->getDataAt(offset + j).toString();
                        else
                            // should not happen
                            throw Exception(
                                "Expected String or FixedString, got " + std::string(getTypeName(arg.key_column->getDataType()))
                                    + " in " + getName(),
                                ErrorCodes::LOGICAL_ERROR);
                    }
                    else
                    {
                        key = assert_cast<const ColumnVector<KeyType> *>(arg.key_column.get())->getData()[offset + j];
                    }

                    arg.val_column->get(offset + j, temp_val);
                    ValType value = temp_val.get<ValType>();

                    if constexpr (op_type == OpTypes::ADD)
                    {
                        const auto [it, inserted] = summing_map.insert({key, value});
                        if (!inserted)
                            it->second = common::addIgnoreOverflow(it->second, value);
                    }
                    else
                    {
                        static_assert(op_type == OpTypes::SUBTRACT);
                        const auto [it, inserted] = summing_map.insert({key, first ? value : common::negateIgnoreOverflow(value)});
                        if (!inserted)
                            it->second = common::subIgnoreOverflow(it->second, value);
                    }
                }

                first = false;
            }

            for (const auto & elem : summing_map)
            {
                to_keys_data->insert(elem.first);
                to_vals_data->insert(elem.second);
            }
            to_keys_offset->push_back(to_keys_data->size());
            summing_map.clear();
        }

        if (to_vals_offset)
        {
            // same offsets as in keys
            to_vals_offset->insert(to_keys_offset->begin(), to_keys_offset->end());
        }

        return res_column;
    }

    template <typename KeyType>
    ColumnPtr execute1(size_t row_count, const DataTypePtr res_type, const DataTypePtr res_value_type, TupleMaps & args) const
    {
        switch (res_value_type->getTypeId())
        {
            case TypeIndex::Int64:
                return execute2<KeyType, Int64>(row_count, args, res_type);
            case TypeIndex::Int128:
                return execute2<KeyType, Int128>(row_count, args, res_type);
            case TypeIndex::Int256:
                return execute2<KeyType, Int256>(row_count, args, res_type);
            case TypeIndex::UInt64:
                return execute2<KeyType, UInt64>(row_count, args, res_type);
            case TypeIndex::UInt128:
                return execute2<KeyType, UInt128>(row_count, args, res_type);
            case TypeIndex::UInt256:
                return execute2<KeyType, UInt256>(row_count, args, res_type);
            case TypeIndex::Float64:
                return execute2<KeyType, Float64>(row_count, args, res_type);
            default:
                throw Exception(
                    "Illegal column type " + res_value_type->getName() + " for values in arguments of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        DataTypePtr key_type;
        size_t row_count;
        const DataTypeTuple * tup_type = checkAndGetDataType<DataTypeTuple>((arguments[0]).type.get());
        DataTypePtr res_type;
        DataTypePtr res_value_type;
        TupleMaps args{};
        args.reserve(arguments.size());

        //prepare columns, extract data columns for direct access and put them to the vector
        if (tup_type)
        {
            const DataTypeArray * key_array_type = checkAndGetDataType<DataTypeArray>(tup_type->getElements()[0].get());
            const DataTypeArray * val_array_type = checkAndGetDataType<DataTypeArray>(tup_type->getElements()[1].get());

            /* determine output type */
            res_value_type = val_array_type->getNestedType()->promoteNumericType();
            res_type = std::make_shared<DataTypeTuple>(DataTypes{
                std::make_shared<DataTypeArray>(key_array_type->getNestedType()), std::make_shared<DataTypeArray>(res_value_type)});

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
                const auto & key_column = arr1.getDataPtr();

                const auto & val_offsets = arr2.getOffsets();
                const auto & val_column = arr2.getDataPtr();

                args.push_back({key_column, val_column, key_offsets, val_offsets, is_const});
            }

            key_type = key_array_type->getNestedType();
        }
        else
        {
            const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>((arguments[0]).type.get());
            if (map_type)
            {
                key_type = map_type->getKeyType();
                res_value_type = map_type->getValueType()->promoteNumericType();
                res_type = std::make_shared<DataTypeMap>(DataTypes{map_type->getKeyType(), res_value_type});

                for (const auto & col : arguments)
                {
                    const ColumnMap * map;
                    bool is_const = isColumnConst(*col.column);
                    if (is_const)
                    {
                        const auto * c = assert_cast<const ColumnConst *>(col.column.get());
                        map = assert_cast<const ColumnMap *>(c->getDataColumnPtr().get());
                    }
                    else
                        map = assert_cast<const ColumnMap *>(col.column.get());

                    const auto & map_arr = map->getNestedColumn();
                    const auto & key_offsets = map_arr.getOffsets();
                    const auto & val_offsets = key_offsets;

                    const auto & map_tup = map->getNestedData();
                    const auto & key_column = map_tup.getColumnPtr(0);
                    const auto & val_column = map_tup.getColumnPtr(1);

                    args.push_back({key_column, val_column, key_offsets, val_offsets, is_const});
                }
            }
            else
                throw Exception(
                    "Illegal column type " + arguments[0].type->getName() + " in arguments of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        // we can check const columns before any processing
        for (auto & arg : args)
        {
            if (arg.is_const)
            {
                if (arg.val_offsets[0] != arg.key_offsets[0])
                    throw Exception(
                        "Key and value array should have same amount of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
        }

        row_count = arguments[0].column->size();
        switch (key_type->getTypeId())
        {
            case TypeIndex::Enum8:
            case TypeIndex::Int8:
                return execute1<Int8>(row_count, res_type, res_value_type, args);
            case TypeIndex::Enum16:
            case TypeIndex::Int16:
                return execute1<Int16>(row_count, res_type, res_value_type, args);
            case TypeIndex::Int32:
                return execute1<Int32>(row_count, res_type, res_value_type, args);
            case TypeIndex::Int64:
                return execute1<Int64>(row_count, res_type, res_value_type, args);
            case TypeIndex::Int128:
                return execute1<Int128>(row_count, res_type, res_value_type, args);
            case TypeIndex::Int256:
                return execute1<Int256>(row_count, res_type, res_value_type, args);
            case TypeIndex::UInt8:
                return execute1<UInt8>(row_count, res_type, res_value_type, args);
            case TypeIndex::Date:
            case TypeIndex::UInt16:
                return execute1<UInt16>(row_count, res_type, res_value_type, args);
            case TypeIndex::DateTime:
            case TypeIndex::UInt32:
                return execute1<UInt32>(row_count, res_type, res_value_type, args);
            case TypeIndex::UInt64:
                return execute1<UInt64>(row_count, res_type, res_value_type, args);
            case TypeIndex::UInt128:
                return execute1<UInt128>(row_count, res_type, res_value_type, args);
            case TypeIndex::UInt256:
                return execute1<UInt256>(row_count, res_type, res_value_type, args);
            case TypeIndex::UUID:
                return execute1<UUID>(row_count, res_type, res_value_type, args);
            case TypeIndex::FixedString:
            case TypeIndex::String:
                return execute1<String>(row_count, res_type, res_value_type, args);
            default:
                throw Exception(
                    "Illegal column type " + key_type->getName() + " for keys in arguments of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
