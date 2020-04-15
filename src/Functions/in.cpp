#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/Set.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/** in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  */

template <bool negative, bool global, bool null_is_skipped>
struct FunctionInName;

template <>
struct FunctionInName<false, false, true>
{
    static constexpr auto name = "in";
};

template <>
struct FunctionInName<false, true, true>
{
    static constexpr auto name = "globalIn";
};

template <>
struct FunctionInName<true, false, true>
{
    static constexpr auto name = "notIn";
};

template <>
struct FunctionInName<true, true, true>
{
    static constexpr auto name = "globalNotIn";
};

template <>
struct FunctionInName<false, false, false>
{
    static constexpr auto name = "nullIn";
};

template <>
struct FunctionInName<false, true, false>
{
    static constexpr auto name = "globalNullIn";
};

template <>
struct FunctionInName<true, false, false>
{
    static constexpr auto name = "notNullIn";
};

template <>
struct FunctionInName<true, true, false>
{
    static constexpr auto name = "globalNotNullIn";
};

template <bool negative, bool global, bool null_is_skipped>
class FunctionIn : public IFunction
{
public:
    static constexpr auto name = FunctionInName<negative, global, null_is_skipped>::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIn>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return null_is_skipped; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        /// NOTE: after updating this code, check that FunctionIgnoreExceptNull returns the same type of column.

        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
        const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            column_set = checkAndGetColumn<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        Block block_of_key_columns;

        /// First argument may be a tuple or a single column.
        const ColumnWithTypeAndName & left_arg = block.getByPosition(arguments[0]);
        const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(left_arg.column.get());
        const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        ColumnPtr materialized_tuple;
        if (const_tuple)
        {
            materialized_tuple = const_tuple->convertToFullColumn();
            tuple = typeid_cast<const ColumnTuple *>(materialized_tuple.get());
        }

        auto set = column_set->getData();
        auto set_types = set->getDataTypes();
        if (tuple && (set_types.size() != 1 || !set_types[0]->equals(*type_tuple)))
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                block_of_key_columns.insert({ tuple_columns[i], tuple_types[i], "" });
        }
        else
            block_of_key_columns.insert(left_arg);

        block.getByPosition(result).column = set->execute(block_of_key_columns, negative);
    }
};


void registerFunctionsIn(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIn<false, false, true>>();
    factory.registerFunction<FunctionIn<false, true, true>>();
    factory.registerFunction<FunctionIn<true, false, true>>();
    factory.registerFunction<FunctionIn<true, true, true>>();
    factory.registerFunction<FunctionIn<false, false, false>>();
    factory.registerFunction<FunctionIn<false, true, false>>();
    factory.registerFunction<FunctionIn<true, false, false>>();
    factory.registerFunction<FunctionIn<true, true, false>>();
}

}
