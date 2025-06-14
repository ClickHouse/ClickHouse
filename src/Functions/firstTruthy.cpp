#include <Columns/IColumn.h>

#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Interpreters/castColumn.h>
#include <Interpreters/Context_fwd.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Implements the function firstTruthy which takes a set of arguments and
/// returns the value of the leftmost non-falsey argument.
/// If all arguments are falsey, returns the last argument.
/// Result type is the supertype of all arguments.
class FunctionFirstTruthy : public IFunction
{
public:
    static constexpr auto name = "firstTruthy";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionFirstTruthy>();
    }

    FunctionFirstTruthy() = default;

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        ColumnNumbers args;
        for (size_t i = 0; i + 1 < number_of_arguments; ++i)
            args.push_back(i);
        return args;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one argument", getName());
        size_t max_args = 1024;
        if (arguments.size() > max_args)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at most {} arguments, got {}", getName(), max_args, arguments.size());

        if (arguments.size() == 1)
            return arguments[0];

        return recursiveRemoveLowCardinality(getLeastSupertype(arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.empty() || result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        if (arguments.size() == 1)
            return castColumn(arguments[0], result_type);

        size_t num_columns = arguments.size();
        auto common_type = removeNullable(result_type);

        /// Cast all arguments to the common_type
        /// Use this columns to insert values into the result column
        std::vector<ColumnPtr> materialized_columns;
        std::vector<ColumnPtr> casted_columns;
        std::vector<const NullMap *> null_maps;

        materialized_columns.reserve(num_columns);
        casted_columns.reserve(num_columns);
        null_maps.reserve(num_columns);

        for (const auto & arg : arguments)
        {
            auto column = arg.column->convertToFullColumnIfConst()->convertToFullColumnIfSparse()->convertToFullColumnIfLowCardinality();

            auto column_to_cast = arg;
            column_to_cast.column = column;
            column_to_cast.type = recursiveRemoveLowCardinality(arg.type);

            const NullMap * null_map = nullptr;
            if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(column.get()))
            {
                null_map = &col_nullable->getNullMapColumn().getData();
                column_to_cast.column = col_nullable->getNestedColumnPtr();
                column_to_cast.type = removeNullable(column_to_cast.type);
            }

            auto casted_column = castColumn(column_to_cast, common_type);

            materialized_columns.push_back(std::move(column));
            casted_columns.push_back(std::move(casted_column));
            null_maps.push_back(null_map);
        }

        auto result_col = common_type->createColumn();
        result_col->reserve(input_rows_count);

        NullMap result_null_map;
        if (result_type->isNullable())
            /// Let's hope it's assign(size, value), not assign(begin, end)
            result_null_map.assign(input_rows_count, UInt8(0));

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            bool found = false;

            /// Check each argument for truthiness
            for (size_t arg_idx = 0; !found && arg_idx < num_columns; ++arg_idx)
            {
                /// A value is considered "falsey" if:
                /// - It's NULL (isNullAt returns true)
                /// - It's a default value (isDefaultAt returns true)
                /// For example:
                /// - for numeric types, the default is 0
                /// - for strings, the default is ''
                /// - for arrays, the default is []
                bool is_null = null_maps[arg_idx] && (*null_maps[arg_idx])[row];
                if (!is_null && !casted_columns[arg_idx]->isDefaultAt(row))
                {
                    /// Found a truthy value, insert it into the result
                    result_col->insertFrom(*casted_columns[arg_idx], row);
                    found = true;
                }
            }

            /// If no truthy value was found, use the last argument
            if (!found)
            {
                result_col->insertDefault();
                if (!result_null_map.empty())
                    result_null_map[row] = 1;
            }
        }

        if (result_type->isNullable())
        {
            auto null_mask_col = ColumnUInt8::create();
            null_mask_col->getData().swap(result_null_map);
            result_col = ColumnNullable::create(std::move(result_col), std::move(null_mask_col));
        }

        return result_col;
    }
};

}

REGISTER_FUNCTION(FirstTruthy)
{
    FunctionDocumentation::Description description = "Returns the first non-falsey value in a list of arguments. "
        "A value is considered 'falsey' if it's NULL or the default value for its type (0, empty string, empty array, etc.). "
        "If all values are falsey, returns the last argument.";
    FunctionDocumentation::Syntax syntax = "firstTruthy(arg1, arg2, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"arg1, arg2, ...", "Two or more arguments of any types with compatible types."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The first non-falsey value encountered when evaluating arguments from left to right. "
        "The result type is the least supertype of all argument types.";
    FunctionDocumentation::Examples examples = {
        {"Basic example", "SELECT firstTruthy(0, NULL, 42, 43)", "42"},
        {"All falsey", "SELECT firstTruthy(0 :: Int32, 0 :: UInt8, NULL, 0 :: UInt32)", "0"},
        {"Arrays", "SELECT firstTruthy([], [NULL], [1, 2, 3])", "[NULL]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = FunctionDocumentation::VERSION_UNKNOWN;
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Conditional;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionFirstTruthy>(documentation, FunctionFactory::Case::Insensitive);
}

}
