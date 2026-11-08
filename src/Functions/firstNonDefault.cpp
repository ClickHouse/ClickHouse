#include <Columns/IColumn.h>

#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Interpreters/castColumn.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Implements the function which takes a set of arguments and
/// returns the value of the leftmost non-falsey argument.
/// If all arguments are falsey, returns the default value for the result type.
/// Result type is the supertype of all arguments.
class FunctionFirstNonDefault : public IFunction
{
public:
    static constexpr auto name = "firstNonDefault";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionFirstNonDefault>();
    }

    FunctionFirstNonDefault() = default;

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

        return getLeastSupertype(arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() == 1)
            return arguments[0].column;

        size_t num_columns = arguments.size();

        auto result_col = result_type->createColumn();
        result_col->reserve(input_rows_count);

        /// Cast all arguments to the result type
        /// Use this columns to insert values into the result column
        std::vector<ColumnPtr> casted_columns;
        casted_columns.reserve(num_columns);
        for (const auto & arg : arguments)
        {
            auto casted_column = castColumn(arg, result_type);
            casted_column = casted_column->convertToFullColumnIfConst();
            casted_column = casted_column->convertToFullColumnIfSparse();

            if (casted_column->getDataType() != result_col->getDataType())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "All arguments must cast to the same type, got {} and {} for result type {}",
                    casted_column->dumpStructure(), result_col->dumpStructure(), result_type->getName());
            }

            casted_columns.push_back(std::move(casted_column));
        }

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            bool found = false;

            /// Check each argument for truthiness
            for (size_t arg_idx = 0; !found && arg_idx < num_columns; ++arg_idx)
            {
                /// A value is considered "falsey" if it's NULL or the default value for its type
                /// For example:
                /// - for numeric types, the default is 0
                /// - for strings, the default is ''
                /// - for arrays, the default is []
                if (!arguments[arg_idx].column->isDefaultAt(row))
                {
                    /// Found a truthy value, insert it into the result
                    result_col->insertFrom(*casted_columns[arg_idx], row);
                    found = true;
                }
            }

            if (!found)
                result_col->insertDefault();
        }
        return result_col;
    }
};

}

REGISTER_FUNCTION(FirstNonDefault)
{
    FunctionDocumentation doc;
    doc.description = "Returns the first non-default value from a set of arguments";
    doc.syntax = "firstNonDefault(arg1[, arg2[ ...]])";
    doc.arguments = {
        {"arg1", "The first argument to check"},
        {"arg2", "The second argument to check"},
        {"...", "Additional arguments to check"},
    };

    doc.returned_value = FunctionDocumentation::ReturnedValue{"Result type is the supertype of all arguments", {}};
    doc.examples = {
        {"integers", "SELECT firstNonDefault(0, 1, 2)", "1"},
        {"strings", "SELECT firstNonDefault('', 'hello', 'world')", "'hello'"},
        {"nulls", "SELECT firstNonDefault(NULL, 0 :: UInt8, 1 :: UInt8)", "1"},
        {"nullable zero", "SELECT firstNonDefault(NULL, 0 :: Nullable(UInt8), 1 :: Nullable(UInt8))", "0"},
    };
    doc.category = {FunctionDocumentation::Category::Null};

    doc.introduced_in = {25, 9};
    factory.registerFunction<FunctionFirstNonDefault>(doc, FunctionFactory::Case::Insensitive);
}

}
