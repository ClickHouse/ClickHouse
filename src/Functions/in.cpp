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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  */

class FunctionIn : public IFunction
{
public:
    FunctionIn(String name_, bool negative_, bool null_is_skipped_, bool ignore_set_)
        : function_name(std::move(name_)), negative(negative_), null_is_skipped(null_is_skipped_), ignore_set(ignore_set_) {}

    /// ignore_set flag means that we don't use set from the second argument, just return zero column.
    /// It is needed to perform type analysis without creation of set.

    static FunctionPtr create(String name, bool negative, bool null_is_skipped, bool ignore_set)
    {
        return std::make_shared<FunctionIn>(std::move(name), negative, null_is_skipped, ignore_set);
    }

    String getName() const override
    {
        return function_name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->hasDynamicSubcolumns())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        /// Never return constant for -IgnoreSet functions to avoid constant folding.
        return !ignore_set;
    }

    bool useDefaultImplementationForDynamic() const override
    {
        return false;
    }

    bool useDefaultImplementationForNulls() const override { return null_is_skipped; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImpl(arguments, true, input_rows_count);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImpl(arguments, false, input_rows_count);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, bool dry_run, size_t input_rows_count) const
    {
        if (ignore_set)
            return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
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
        {
            if (dry_run)
                return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));

            throw Exception(ErrorCodes::LOGICAL_ERROR, "No Set is passed as the second argument for function '{}'", getName());
        }

        auto set = future_set->get();
        if (!set)
        {
            if (dry_run)
                return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not-ready Set is passed as the second argument for function '{}'", getName());
        }

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
            if (typeid_cast<const ColumnConst *>(arg.column.get()))
                is_const = true;
        }

        auto res = set->execute(columns_of_key_columns, negative);

        if (is_const)
            res = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(res->getUInt(0)));

        if (res->size() != input_rows_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output size is different from input size, expect {}, get {}", input_rows_count, res->size());

        return res;
    }

private:
    String function_name;
    bool negative;
    bool null_is_skipped;
    bool ignore_set;
};

void registerFunctionsInImpl(FunctionFactory & factory, bool ignore_set)
{
    const char * suffix = ignore_set ? "IgnoreSet" : "";
    auto reg = [&](const char * name, bool negative_, bool null_is_skipped_)
    {
        String full_name = String(name) + suffix;
        factory.registerFunction(full_name, [negative_, null_is_skipped_, ignore_set, n = full_name](ContextPtr)
        {
            return FunctionIn::create(n, negative_, null_is_skipped_, ignore_set);
        });
    };

    reg("in", false, true);
    reg("globalIn", false, true);
    reg("notIn", true, true);
    reg("globalNotIn", true, true);
    reg("nullIn", false, false);
    reg("globalNullIn", false, false);
    reg("notNullIn", true, false);
    reg("globalNotNullIn", true, false);
}

}

REGISTER_FUNCTION(In)
{
    registerFunctionsInImpl(factory, false);
    registerFunctionsInImpl(factory, true);
}

}
