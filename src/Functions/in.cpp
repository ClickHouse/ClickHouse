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
    const String suffix = ignore_set ? "IgnoreSet" : "";
    static constexpr auto ignore_set_description_suffix = " This is the IgnoreSet variant used for type analysis without creating the set.";

    /// in
    FunctionDocumentation::Description description_in = R"(
Checks if the left operand is a member of the right operand set. Returns 1 if it is, 0 otherwise. NULL values in the left operand are skipped (treated as not in the set).
    )";
    FunctionDocumentation::Syntax syntax_in = "in(x, set)";
    FunctionDocumentation::Arguments arguments_in = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_in = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_in = {{"Basic usage", "SELECT 1 IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_in = {1, 1};
    FunctionDocumentation::Category category_in = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_in = {description_in, syntax_in, arguments_in, {}, returned_value_in, examples_in, introduced_in_in, category_in};
    if (ignore_set)
        documentation_in.description = String(documentation_in.description) + ignore_set_description_suffix;
    String full_name_in = "in";
    full_name_in += suffix;
    factory.registerFunction(full_name_in, [ignore_set, n = full_name_in](ContextPtr)
    {
        return FunctionIn::create(n, false, true, ignore_set);
    }, std::move(documentation_in));

    /// globalIn
    FunctionDocumentation::Description description_globalIn = R"(
Same as `in`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalIn = "globalIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalIn = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalIn = {{"Basic usage", "SELECT 1 IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalIn = {1, 1};
    FunctionDocumentation::Category category_globalIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalIn = {description_globalIn, syntax_globalIn, arguments_globalIn, {}, returned_value_globalIn, examples_globalIn, introduced_in_globalIn, category_globalIn};
    if (ignore_set)
        documentation_globalIn.description = String(documentation_globalIn.description) + ignore_set_description_suffix;
    String full_name_globalIn = "globalIn";
    full_name_globalIn += suffix;
    factory.registerFunction(full_name_globalIn, [ignore_set, n = full_name_globalIn](ContextPtr)
    {
        return FunctionIn::create(n, false, true, ignore_set);
    }, std::move(documentation_globalIn));

    /// notIn
    FunctionDocumentation::Description description_notIn = R"(
Checks if the left operand is NOT a member of the right operand set. Returns 1 if it is not in the set, 0 otherwise. NULL values in the left operand are skipped.
    )";
    FunctionDocumentation::Syntax syntax_notIn = "notIn(x, set)";
    FunctionDocumentation::Arguments arguments_notIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_notIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_notIn = {{"Basic usage", "SELECT 4 NOT IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_notIn = {1, 1};
    FunctionDocumentation::Category category_notIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_notIn = {description_notIn, syntax_notIn, arguments_notIn, {}, returned_value_notIn, examples_notIn, introduced_in_notIn, category_notIn};
    if (ignore_set)
        documentation_notIn.description = String(documentation_notIn.description) + ignore_set_description_suffix;
    String full_name_notIn = "notIn";
    full_name_notIn += suffix;
    factory.registerFunction(full_name_notIn, [ignore_set, n = full_name_notIn](ContextPtr)
    {
        return FunctionIn::create(n, true, true, ignore_set);
    }, std::move(documentation_notIn));

    /// globalNotIn
    FunctionDocumentation::Description description_globalNotIn = R"(
Same as `notIn`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalNotIn = "globalNotIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalNotIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalNotIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalNotIn = {{"Basic usage", "SELECT 4 NOT IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalNotIn = {1, 1};
    FunctionDocumentation::Category category_globalNotIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalNotIn = {description_globalNotIn, syntax_globalNotIn, arguments_globalNotIn, {}, returned_value_globalNotIn, examples_globalNotIn, introduced_in_globalNotIn, category_globalNotIn};
    if (ignore_set)
        documentation_globalNotIn.description = String(documentation_globalNotIn.description) + ignore_set_description_suffix;
    String full_name_globalNotIn = "globalNotIn";
    full_name_globalNotIn += suffix;
    factory.registerFunction(full_name_globalNotIn, [ignore_set, n = full_name_globalNotIn](ContextPtr)
    {
        return FunctionIn::create(n, true, true, ignore_set);
    }, std::move(documentation_globalNotIn));

    /// nullIn
    FunctionDocumentation::Description description_nullIn = R"(
Checks if the left operand is a member of the right operand set. Unlike `in`, NULL values are not skipped: NULL is compared with set elements, and NULL = NULL evaluates to true.
    )";
    FunctionDocumentation::Syntax syntax_nullIn = "nullIn(x, set)";
    FunctionDocumentation::Arguments arguments_nullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_nullIn = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_nullIn = {{"Basic usage", "SELECT nullIn(NULL, tuple(1, NULL))", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_nullIn = {1, 1};
    FunctionDocumentation::Category category_nullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_nullIn = {description_nullIn, syntax_nullIn, arguments_nullIn, {}, returned_value_nullIn, examples_nullIn, introduced_in_nullIn, category_nullIn};
    if (ignore_set)
        documentation_nullIn.description = String(documentation_nullIn.description) + ignore_set_description_suffix;
    String full_name_nullIn = "nullIn";
    full_name_nullIn += suffix;
    factory.registerFunction(full_name_nullIn, [ignore_set, n = full_name_nullIn](ContextPtr)
    {
        return FunctionIn::create(n, false, false, ignore_set);
    }, std::move(documentation_nullIn));

    /// globalNullIn
    FunctionDocumentation::Description description_globalNullIn = R"(
Same as `nullIn`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalNullIn = "globalNullIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalNullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalNullIn = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalNullIn = {{"Basic usage", "SELECT nullIn(NULL, tuple(1, NULL))", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalNulllIn = {1, 1};
    FunctionDocumentation::Category category_globalNullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalNullIn = {description_globalNullIn, syntax_globalNullIn, arguments_globalNullIn, {}, returned_value_globalNullIn, examples_globalNullIn, introduced_in_globalNulllIn, category_globalNullIn};
    if (ignore_set)
        documentation_globalNullIn.description = String(documentation_globalNullIn.description) + ignore_set_description_suffix;
    String full_name_globalNullIn = "globalNullIn";
    full_name_globalNullIn += suffix;
    factory.registerFunction(full_name_globalNullIn, [ignore_set, n = full_name_globalNullIn](ContextPtr)
    {
        return FunctionIn::create(n, false, false, ignore_set);
    }, std::move(documentation_globalNullIn));

    /// notNullIn
    FunctionDocumentation::Description description_notNullIn = R"(
Checks if the left operand is NOT a member of the right operand set. Unlike `notIn`, NULL values are not skipped: NULL is compared with set elements, and NULL = NULL evaluates to true.
    )";
    FunctionDocumentation::Syntax syntax_notNullIn = "notNullIn(x, set)";
    FunctionDocumentation::Arguments arguments_notNullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_notNullIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_notNullIn = {{"Basic usage", "SELECT notNullIn(NULL, tuple(1, NULL))", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in_notNulllIn = {1, 1};
    FunctionDocumentation::Category category_notNullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_notNullIn = {description_notNullIn, syntax_notNullIn, arguments_notNullIn, {}, returned_value_notNullIn, examples_notNullIn, introduced_in_notNulllIn, category_notNullIn};
    if (ignore_set)
        documentation_notNullIn.description = String(documentation_notNullIn.description) + ignore_set_description_suffix;
    String full_name_notNullIn = "notNullIn";
    full_name_notNullIn += suffix;
    factory.registerFunction(full_name_notNullIn, [ignore_set, n = full_name_notNullIn](ContextPtr)
    {
        return FunctionIn::create(n, true, false, ignore_set);
    }, std::move(documentation_notNullIn));

    /// globalNotNullIn
    FunctionDocumentation::Description description_globalNotNullIn = R"(
Same as `notNullIn`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalNotNullIn = "globalNotNullIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalNotNullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalNotNullIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalNotNullIn = {{"Basic usage", "SELECT notNullIn(NULL, tuple(1, NULL))", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalNotNulllIn = {1, 1};
    FunctionDocumentation::Category category_globalNotNullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalNotNullIn = {description_globalNotNullIn, syntax_globalNotNullIn, arguments_globalNotNullIn, {}, returned_value_globalNotNullIn, examples_globalNotNullIn, introduced_in_globalNotNulllIn, category_globalNotNullIn};
    if (ignore_set)
        documentation_globalNotNullIn.description = String(documentation_globalNotNullIn.description) + ignore_set_description_suffix;
    String full_name_globalNotNullIn = "globalNotNullIn";
    full_name_globalNotNullIn += suffix;
    factory.registerFunction(full_name_globalNotNullIn, [ignore_set, n = full_name_globalNotNullIn](ContextPtr)
    {
        return FunctionIn::create(n, true, false, ignore_set);
    }, std::move(documentation_globalNotNullIn));
}

}

REGISTER_FUNCTION(In)
{
    registerFunctionsInImpl(factory, false);
    registerFunctionsInImpl(factory, true);
}

}
