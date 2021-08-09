#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnLowCardinality.h>
#include <Interpreters/Set.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
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

    /// Do not use default implementation for LowCardinality.
    /// For now, Set may be const or non const column, depending on how it was created.
    /// But we will return UInt8 for any case.
    /// TODO: we could use special implementation later.
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, [[maybe_unused]] size_t input_rows_count) const override
    {
        if constexpr (ignore_set)
            return ColumnUInt8::create(input_rows_count, 0u);

        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = arguments[1].column;
        const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            column_set = checkAndGetColumn<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        DB::Block columns_of_key_columns;

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

        auto set = column_set->getData();
        auto set_types = set->getDataTypes();
        if (tuple && (set_types.size() != 1 || !set_types[0]->equals(*type_tuple)))
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                columns_of_key_columns.insert({ tuple_columns[i], tuple_types[i], "" });
        }
        else
            columns_of_key_columns.insert(left_arg);

        /// Replace single LowCardinality column to it's dictionary if possible.
        ColumnPtr lc_indexes = nullptr;
        if (columns_of_key_columns.columns() == 1)
        {
            auto & arg = columns_of_key_columns.safeGetByPosition(0);
            const auto * col = arg.column.get();
            if (const auto * const_col = typeid_cast<const ColumnConst *>(col))
                col = &const_col->getDataColumn();

            if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(col))
            {
                lc_indexes = lc->getIndexesPtr();
                arg.column = lc->getDictionary().getNestedColumn();
                arg.type = removeLowCardinality(arg.type);
            }
        }

        auto res = set->execute(columns_of_key_columns, negative);

        if (lc_indexes)
            return res->index(*lc_indexes, 0);

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

void registerFunctionsIn(FunctionFactory & factory)
{
    registerFunctionsInImpl<false>(factory);
    registerFunctionsInImpl<true>(factory);
}

}
