#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Interpreters/castColumn.h>
#include <Interpreters/Context_fwd.h>

#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

void markNonDefaultRows(const IColumn & column, PaddedPODArray<UInt8> & non_default)
{
    /// `ColumnAggregateFunction` lacks `getIndicesOfNonDefaultRows` and has no default rows.
    if (column.getNumberOfDefaultRows() == 0)
    {
        std::fill(non_default.begin(), non_default.end(), 1);
        return;
    }
    IColumn::Offsets rows;
    column.getIndicesOfNonDefaultRows(rows, 0, 0);
    for (auto row : rows)
        non_default[row] = 1;
}

template <typename EnumType>
void markNonDefaultEnumRows(const IColumn & column, const EnumType & type, PaddedPODArray<UInt8> & non_default)
{
    const auto & data = assert_cast<const typename EnumType::ColumnType &>(column).getData();
    const auto first_value = type.getDefaultValue();
    for (size_t row = 0; row < data.size(); ++row)
        non_default[row] |= data[row] != first_value;
}

/// The default of an `Enum` is its first value, not 0; elsewhere `isDefaultAt` decides, as for a whole column.
void markNonDefaultRows(const IColumn & column, const IDataType & type, PaddedPODArray<UInt8> & non_default)
{
    if (type.isDefaultInsertTrivial())
        markNonDefaultRows(column, non_default);
    else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
            markNonDefaultRows(tuple_column.getColumn(i), *tuple_type->getElement(i), non_default);
    }
    else if (const auto * object_type = typeid_cast<const DataTypeObject *>(&type))
    {
        const auto & object_column = assert_cast<const ColumnObject &>(column);
        for (const auto & [path, path_column] : object_column.getTypedPaths())
            markNonDefaultRows(*path_column, *object_type->getTypedPaths().at(path), non_default);
        for (const auto & [path, path_column] : object_column.getDynamicPaths())
            markNonDefaultRows(*path_column, non_default);
        markNonDefaultRows(*object_column.getSharedDataPtr(), non_default);
    }
    else if (const auto * enum8 = typeid_cast<const DataTypeEnum8 *>(&type))
        markNonDefaultEnumRows(column, *enum8, non_default);
    else if (const auto * enum16 = typeid_cast<const DataTypeEnum16 *>(&type))
        markNonDefaultEnumRows(column, *enum16, non_default);
    else
        markNonDefaultRows(column, non_default);
}

/// A column whose default rows are the rows of `argument` holding the default of its type.
ColumnPtr getDefaultRowsProbe(const ColumnWithTypeAndName & argument)
{
    if (argument.type->isDefaultInsertTrivial())
        return argument.column;

    auto column = argument.column->convertToFullIfWrapped();
    auto probe = ColumnUInt8::create(column->size(), static_cast<UInt8>(0));
    markNonDefaultRows(*column, *argument.type, probe->getData());
    return probe;
}

/// Implements the function which takes a set of arguments and
/// returns the value of the leftmost non-falsey argument.
/// If all arguments are falsey, returns the default value for the result type.
/// Result type is the supertype of all arguments.
class FunctionFirstNonDefault final : public IFunction
{
public:
    static constexpr auto name = "firstNonDefault";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionFirstNonDefault>();
    }

    FunctionFirstNonDefault() = default;

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }

    [[nodiscard]] ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        ColumnNumbers args;
        for (size_t i = 0; i + 1 < number_of_arguments; ++i)
            args.push_back(i);
        return args;
    }

    [[nodiscard]] String getSignatureString() const override
    {
        /// Variadic over T1..Tn; the type-function ellipsis needs the variable index to
        /// build the group-to-repeat, hence the explicit `T1` rather than bare `T`.
        return "(T1, ...) -> leastSupertype(T1, ...)";
    }

    /// Cap the arity at 1024 like the legacy `getReturnTypeImpl` did. The DSL
    /// signature has no upper-bound matcher, so enforce it here before
    /// delegating to the DSL for the actual return-type computation.
    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        constexpr size_t max_args = 1024;
        if (arguments.size() > max_args)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at most {} arguments, got {}", getName(), max_args, arguments.size());
        return IFunction::getReturnTypeImpl(arguments);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() == 1)
            return arguments[0].column;

        size_t num_columns = arguments.size();

        auto result_col = result_type->createColumn();

        /// Cast all arguments to the result type
        /// Use this columns to insert values into the result column
        VectorWithMemoryTracking<ColumnPtr> casted_columns;
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

        if (input_rows_count == 0)
            return result_col;

        /// A value is "falsey" if it is the default of the ORIGINAL argument's type (NULL for Nullable).
        VectorWithMemoryTracking<ColumnPtr> default_probes(num_columns);
        auto get_default_probe = [&](size_t arg_idx) -> const IColumn &
        {
            if (!default_probes[arg_idx])
                default_probes[arg_idx] = getDefaultRowsProbe(arguments[arg_idx]);
            return *default_probes[arg_idx];
        };

        size_t first = 0;
        for (; first < num_columns; ++first)
        {
            size_t num_defaults = get_default_probe(first).getNumberOfDefaultRows();
            if (num_defaults == 0)
                return casted_columns[first];
            if (num_defaults != input_rows_count)
                break;
        }

        const bool trivial_result_default = result_type->isDefaultInsertTrivial();
        if (first == num_columns)
        {
            if (trivial_result_default)
                result_col->insertManyDefaults(input_rows_count);
            else
                result_type->insertManyDefaultsInto(*result_col, input_rows_count);
            return result_col;
        }

        /// Only `ColumnAggregateFunction` lacks `getIndicesOfNonDefaultRows`, and it has no default rows.
        IColumn::Offsets non_default_rows;
        get_default_probe(first).getIndicesOfNonDefaultRows(non_default_rows, 0, 0);

        result_col->reserve(input_rows_count);
        VectorWithMemoryTracking<const IColumn *> probe_columns(num_columns, nullptr);
        auto insert_from_next_arguments = [&](size_t row)
        {
            for (size_t arg_idx = first + 1; arg_idx < num_columns; ++arg_idx)
            {
                const IColumn *& probe = probe_columns[arg_idx];
                if (!probe)
                    probe = &get_default_probe(arg_idx);
                if (!probe->isDefaultAt(row))
                {
                    result_col->insertFrom(*casted_columns[arg_idx], row);
                    return;
                }
            }
            if (trivial_result_default)
                result_col->insertDefault();
            else
                result_type->insertDefaultInto(*result_col);
        };

        size_t row = 0;
        for (size_t i = 0; i < non_default_rows.size();)
        {
            size_t run_start = non_default_rows[i];
            size_t run_end = run_start + 1;
            for (++i; i < non_default_rows.size() && non_default_rows[i] == run_end; ++i)
                ++run_end;

            for (; row < run_start; ++row)
                insert_from_next_arguments(row);
            if (run_end - run_start == 1)
                result_col->insertFrom(*casted_columns[first], run_start);
            else
                result_col->insertRangeFrom(*casted_columns[first], run_start, run_end - run_start);
            row = run_end;
        }
        for (; row < input_rows_count; ++row)
            insert_from_next_arguments(row);

        return result_col;
    }
};

}

REGISTER_FUNCTION(FirstNonDefault)
{
    FunctionDocumentation doc{};
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
        {"strings", "SELECT firstNonDefault('', 'hello', 'world')", "hello"},
        {"nulls", "SELECT firstNonDefault(NULL, 0 :: UInt8, 1 :: UInt8)", "1"},
        {"nullable zero", "SELECT firstNonDefault(NULL, 0 :: Nullable(UInt8), 1 :: Nullable(UInt8))", "0"},
        {"enum", "SELECT firstNonDefault('a' :: Enum8('a' = 1, 'b' = 2), 'b' :: Enum8('a' = 1, 'b' = 2))", "b"},
    };
    doc.category = {FunctionDocumentation::Category::Null};

    doc.introduced_in = {25, 9};
    factory.registerFunction<FunctionFirstNonDefault>(doc, FunctionFactory::Case::Insensitive);
}

}
