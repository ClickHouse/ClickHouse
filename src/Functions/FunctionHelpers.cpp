#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column)
{
    if (!isColumnConst(*column))
        return {};

    const ColumnConst * res = assert_cast<const ColumnConst *>(column);

    if (checkColumn<ColumnString>(&res->getDataColumn())
        || checkColumn<ColumnFixedString>(&res->getDataColumn()))
        return res;

    return {};
}


Columns convertConstTupleToConstantElements(const ColumnConst & column)
{
    const ColumnTuple & src_tuple = assert_cast<const ColumnTuple &>(column.getDataColumn());
    const auto & src_tuple_columns = src_tuple.getColumns();
    size_t tuple_size = src_tuple_columns.size();
    size_t rows = column.size();

    Columns res(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        res[i] = ColumnConst::create(src_tuple_columns[i], rows);

    return res;
}

ColumnWithTypeAndName columnGetNested(const ColumnWithTypeAndName & col)
{
    if (col.type->isNullable())
    {
        const DataTypePtr & nested_type = static_cast<const DataTypeNullable &>(*col.type).getNestedType();

        if (!col.column)
        {
            return ColumnWithTypeAndName{nullptr, nested_type, col.name};
        }
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*col.column))
        {
            const auto & nested_col = nullable->getNestedColumnPtr();
            return ColumnWithTypeAndName{nested_col, nested_type, col.name};
        }
        if (const auto * const_column = checkAndGetColumn<ColumnConst>(&*col.column))
        {
            const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&const_column->getDataColumn());

            ColumnPtr nullable_res;
            if (nullable_column)
            {
                const auto & nested_col = nullable_column->getNestedColumnPtr();
                nullable_res = ColumnConst::create(nested_col, col.column->size());
            }
            else
            {
                nullable_res = makeNullable(col.column);
            }
            return ColumnWithTypeAndName{nullable_res, nested_type, col.name};
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} for DataTypeNullable", col.dumpStructure());
    }
    return col;
}

ColumnsWithTypeAndName createBlockWithNestedColumns(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName res;
    for (const auto & col : columns)
        res.emplace_back(columnGetNested(col));

    return res;
}

namespace
{

String withOrdinalEnding(size_t i)
{
    switch (i)
    {
        case 0: return "1st";
        case 1: return "2nd";
        case 2: return "3rd";
        default: return std::to_string(i) + "th";
    }

}

void validateArgumentsImpl(const IFunction & func,
                           const ColumnsWithTypeAndName & arguments,
                           size_t argument_offset,
                           const FunctionArgumentDescriptors & descriptors)
{
    for (size_t i = 0; i < descriptors.size(); ++i)
    {
        const auto argument_index = i + argument_offset;
        if (argument_index >= arguments.size())
            break;

        const auto & arg = arguments[i + argument_offset];
        const auto & descriptor = descriptors[i];
        if (int error_code = descriptor.isValid(arg.type, arg.column); error_code != 0)
            throw Exception(error_code,
                            "A value of illegal type was provided as {} argument '{}' to function '{}'. Expected: {}, got: {}",
                            withOrdinalEnding(argument_offset + i),
                            descriptor.name,
                            func.getName(),
                            descriptor.type_name,
                            arg.type ? arg.type->getName() : "<?>");
    }
}

}

int FunctionArgumentDescriptor::isValid(const DataTypePtr & data_type, const ColumnPtr & column) const
{
    if (name.empty() || type_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "name or type_name are not set");

    if (type_validator && (data_type == nullptr || !type_validator(*data_type)))
        return ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT;

    if (column_validator && (column == nullptr || !column_validator(*column)))
        return ErrorCodes::ILLEGAL_COLUMN;

    return 0;
}

void validateFunctionArguments(const IFunction & func,
                               const ColumnsWithTypeAndName & arguments,
                               const FunctionArgumentDescriptors & mandatory_args,
                               const FunctionArgumentDescriptors & optional_args)
{
    if (arguments.size() < mandatory_args.size() || arguments.size() > mandatory_args.size() + optional_args.size())
    {
        auto argument_singular_or_plural = [](const auto & args) -> std::string_view { return args.size() == 1 ? "argument" : "arguments"; };

        String expected_args_string;
        if (!mandatory_args.empty() && !optional_args.empty())
            expected_args_string = fmt::format("{} mandatory {} and {} optional {}", mandatory_args.size(), argument_singular_or_plural(mandatory_args), optional_args.size(), argument_singular_or_plural(optional_args));
        else if (!mandatory_args.empty() && optional_args.empty())
            expected_args_string = fmt::format("{} {}", mandatory_args.size(), argument_singular_or_plural(mandatory_args)); /// intentionally not "_mandatory_ arguments"
        else if (mandatory_args.empty() && !optional_args.empty())
            expected_args_string = fmt::format("{} optional {}", optional_args.size(), argument_singular_or_plural(optional_args));
        else
            expected_args_string = "0 arguments";

        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "An incorrect number of arguments was specified for function '{}'. Expected {}, got {}",
            func.getName(),
            expected_args_string,
            fmt::format("{} {}", arguments.size(), argument_singular_or_plural(arguments)));
    }

    validateArgumentsImpl(func, arguments, 0, mandatory_args);
    if (!optional_args.empty())
        validateArgumentsImpl(func, arguments, mandatory_args.size(), optional_args);
}

std::pair<std::vector<const IColumn *>, const ColumnArray::Offset *>
checkAndGetNestedArrayOffset(const IColumn ** columns, size_t num_arguments)
{
    assert(num_arguments > 0);
    std::vector<const IColumn *> nested_columns(num_arguments);
    const ColumnArray::Offsets * offsets = nullptr;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnArray::Offsets * offsets_i = nullptr;
        if (const ColumnArray * arr = checkAndGetColumn<const ColumnArray>(columns[i]))
        {
            nested_columns[i] = &arr->getData();
            offsets_i = &arr->getOffsets();
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} as argument of function", columns[i]->getName());
        if (i == 0)
            offsets = offsets_i;
        else if (*offsets_i != *offsets)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Lengths of all arrays passed to aggregate function must be equal.");
    }
    return {nested_columns, offsets->data()};
}

ColumnPtr wrapInNullable(const ColumnPtr & src, const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count)
{
    ColumnPtr result_null_map_column;

    /// If result is already nullable.
    ColumnPtr src_not_nullable = src;

    if (src->onlyNull())
        return src;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*src))
    {
        src_not_nullable = nullable->getNestedColumnPtr();
        result_null_map_column = nullable->getNullMapColumnPtr();
    }

    for (const auto & elem : args)
    {
        if (!elem.type->isNullable())
            continue;

        /// Const Nullable that are NULL.
        if (elem.column->onlyNull())
        {
            assert(result_type->isNullable());
            return result_type->createColumnConstWithDefaultValue(input_rows_count);
        }

        if (isColumnConst(*elem.column))
            continue;

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*elem.column))
        {
            const ColumnPtr & null_map_column = nullable->getNullMapColumnPtr();
            if (!result_null_map_column)
            {
                result_null_map_column = null_map_column;
            }
            else
            {
                MutableColumnPtr mutable_result_null_map_column = IColumn::mutate(std::move(result_null_map_column));

                NullMap & result_null_map = assert_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
                const NullMap & src_null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();

                for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                    result_null_map[i] |= src_null_map[i];

                result_null_map_column = std::move(mutable_result_null_map_column);
            }
        }
    }

    if (!result_null_map_column)
        return makeNullable(src);

    return ColumnNullable::create(src_not_nullable->convertToFullColumnIfConst(), result_null_map_column);
}

NullPresence getNullPresense(const ColumnsWithTypeAndName & args)
{
    NullPresence res;

    for (const auto & elem : args)
    {
        res.has_nullable |= elem.type->isNullable();
        res.has_null_constant |= elem.type->onlyNull();
    }

    return res;
}

bool isDecimalOrNullableDecimal(const DataTypePtr & type)
{
    WhichDataType which(type);
    if (which.isDecimal())
        return true;
    if (!which.isNullable())
        return false;
    return isDecimal(assert_cast<const DataTypeNullable *>(type.get())->getNestedType());
}

/// Note that, for historical reasons, most of the functions use the first argument size to determine which is the
/// size of all the columns. When short circuit optimization was introduced, `input_rows_count` was also added for
/// all functions, but many have not been adjusted
void checkFunctionArgumentSizes(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
{
    for (size_t i = 0; i < arguments.size(); i++)
    {
        if (isColumnConst(*arguments[i].column))
            continue;

        size_t current_size = arguments[i].column->size();

        if (current_size != input_rows_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected the argument №{} ('{}' of type {}) to have {} rows, but it has {}",
                i + 1,
                arguments[i].name,
                arguments[i].type->getName(),
                input_rows_count,
                current_size);
    }
}
}
