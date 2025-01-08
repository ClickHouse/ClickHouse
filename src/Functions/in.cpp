#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/Set.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{

/** in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  */

template <bool negative, bool global, bool null_is_skipped, bool ignore_set>
struct FunctionInName;

template <> struct FunctionInName<false, false, true, false> { static constexpr auto name = "in"; };
template <> struct FunctionInName<false, true, true, false> { static constexpr auto name = "globalIn"; };
template <> struct FunctionInName<true, false, true, false> { static constexpr auto name = "notIn"; };
template <> struct FunctionInName<true, true, true, false> { static constexpr auto name = "globalNotIn"; };
template <> struct FunctionInName<false, false, false, false> { static constexpr auto name = "nullIn"; };
template <> struct FunctionInName<false, true, false, false> { static constexpr auto name = "globalNullIn"; };
template <> struct FunctionInName<true, false, false, false> { static constexpr auto name = "notNullIn"; };
template <> struct FunctionInName<true, true, false, false> { static constexpr auto name = "globalNotNullIn"; };
template <> struct FunctionInName<false, false, true, true> { static constexpr auto name = "inIgnoreSet"; };
template <> struct FunctionInName<false, true, true, true> { static constexpr auto name = "globalInIgnoreSet"; };
template <> struct FunctionInName<true, false, true, true> { static constexpr auto name = "notInIgnoreSet"; };
template <> struct FunctionInName<true, true, true, true> { static constexpr auto name = "globalNotInIgnoreSet"; };
template <> struct FunctionInName<false, false, false, true> { static constexpr auto name = "nullInIgnoreSet"; };
template <> struct FunctionInName<false, true, false, true> { static constexpr auto name = "globalNullInIgnoreSet"; };
template <> struct FunctionInName<true, false, false, true> { static constexpr auto name = "notNullInIgnoreSet"; };
template <> struct FunctionInName<true, true, false, true> { static constexpr auto name = "globalNotNullInIgnoreSet"; };

template <bool negative, bool global, bool null_is_skipped, bool ignore_set>
class FunctionIn : public IFunction
{
public:
    /// ignore_set flag means that we don't use set from the second argument, just return zero column.
    /// It is needed to perform type analysis without creation of set.
    static constexpr auto name = FunctionInName<negative, global, null_is_skipped, ignore_set>::name;

    static FunctionPtr create(ContextPtr)
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

    bool useDefaultImplementationForConstants() const override
    {
        /// Never return constant for -IgnoreSet functions to avoid constant folding.
        return !ignore_set;
    }

    bool useDefaultImplementationForNulls() const override { return null_is_skipped; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, [[maybe_unused]] size_t input_rows_count) const override
    {
        if constexpr (ignore_set)
            return ColumnUInt8::create(input_rows_count, 0u);
        if (input_rows_count == 0)
            return ColumnUInt8::create();

        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = arguments[1].column;
        const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            column_set = checkAndGetColumn<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function '{}' must be Set; found {}",
                getName(), column_set_ptr->getName());

        ColumnsWithTypeAndName columns_of_key_columns;

        /// First argument may be a tuple or a single column.
        const ColumnWithTypeAndName & left_arg = arguments[0];
        const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(left_arg.column.get());
        const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        ColumnPtr materialized_tuple;
        if (const_tuple)
        {
            materialized_tuple = const_tuple->convertToFullColumn();
            tuple = typeid_cast<const ColumnTuple *>(materialized_tuple.get());
        }

        auto future_set = column_set->getData();
        if (!future_set)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No Set is passed as the second argument for function '{}'", getName());

        auto set = future_set->get();
        if (!set)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not-ready Set is passed as the second argument for function '{}'", getName());

        auto set_types = set->getDataTypes();

        if (tuple && set_types.size() != 1 && set_types.size() == tuple->tupleSize())
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                columns_of_key_columns.emplace_back(tuple_columns[i], tuple_types[i], "_" + toString(i));
        }
        else
            columns_of_key_columns.emplace_back(left_arg);

        bool is_const = false;
        if (columns_of_key_columns.size() == 1)
        {
            auto & arg = columns_of_key_columns.at(0);
            const auto * col = arg.column.get();
            if (const auto * const_col = typeid_cast<const ColumnConst *>(col))
            {
                col = &const_col->getDataColumn();
                is_const = true;
            }
        }

        auto res = set->execute(columns_of_key_columns, negative);

        if (is_const)
            res = ColumnUInt8::create(input_rows_count, res->getUInt(0));

        if (res->size() != input_rows_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output size is different from input size, expect {}, get {}", input_rows_count, res->size());

        return res;
    }
};

template<bool ignore_set>
void registerFunctionsInImpl(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIn<false, false, true, ignore_set>>();
    factory.registerFunction<FunctionIn<false, true, true, ignore_set>>();
    factory.registerFunction<FunctionIn<true, false, true, ignore_set>>();
    factory.registerFunction<FunctionIn<true, true, true, ignore_set>>();
    factory.registerFunction<FunctionIn<false, false, false, ignore_set>>();
    factory.registerFunction<FunctionIn<false, true, false, ignore_set>>();
    factory.registerFunction<FunctionIn<true, false, false, ignore_set>>();
    factory.registerFunction<FunctionIn<true, true, false, ignore_set>>();
}

}

REGISTER_FUNCTION(In)
{
    registerFunctionsInImpl<false>(factory);
    registerFunctionsInImpl<true>(factory);
}

}
