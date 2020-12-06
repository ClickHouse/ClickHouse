#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
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


ColumnsWithTypeAndName createBlockWithNestedColumns(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName res;
    for (const auto & col : columns)
    {
        if (col.type->isNullable())
        {
            const DataTypePtr & nested_type = static_cast<const DataTypeNullable &>(*col.type).getNestedType();

            if (!col.column)
            {
                res.emplace_back(ColumnWithTypeAndName{nullptr, nested_type, col.name});
            }
            else if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*col.column))
            {
                const auto & nested_col = nullable->getNestedColumnPtr();
                res.emplace_back(ColumnWithTypeAndName{nested_col, nested_type, col.name});
            }
            else if (const auto * const_column = checkAndGetColumn<ColumnConst>(*col.column))
            {
                const auto & nested_col = checkAndGetColumn<ColumnNullable>(const_column->getDataColumn())->getNestedColumnPtr();
                res.emplace_back(ColumnWithTypeAndName{ ColumnConst::create(nested_col, col.column->size()), nested_type, col.name});
            }
            else
                throw Exception("Illegal column for DataTypeNullable", ErrorCodes::ILLEGAL_COLUMN);
        }
        else
            res.emplace_back(col);
    }

    return res;
}

void validateArgumentType(const IFunction & func, const DataTypes & arguments,
                          size_t argument_index, bool (* validator_func)(const IDataType &),
                          const char * expected_type_description)
{
    if (arguments.size() <= argument_index)
        throw Exception("Incorrect number of arguments of function " + func.getName(),
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto & argument = arguments[argument_index];
    if (!validator_func(*argument))
        throw Exception("Illegal type " + argument->getName() +
                        " of " + std::to_string(argument_index) +
                        " argument of function " + func.getName() +
                        " expected " + expected_type_description,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

namespace
{
void validateArgumentsImpl(const IFunction & func,
                           const ColumnsWithTypeAndName & arguments,
                           size_t argument_offset,
                           const FunctionArgumentDescriptors & descriptors)
{
    for (size_t i = 0; i < descriptors.size(); ++i)
    {
        const auto argument_index = i + argument_offset;
        if (argument_index >= arguments.size())
        {
            break;
        }

        const auto & arg = arguments[i + argument_offset];
        const auto descriptor = descriptors[i];
        if (int error_code = descriptor.isValid(arg.type, arg.column); error_code != 0)
            throw Exception("Illegal type of argument #" + std::to_string(argument_offset + i + 1) // +1 is for human-friendly 1-based indexing
                            + (descriptor.argument_name ? " '" + std::string(descriptor.argument_name) + "'" : String{})
                            + " of function " + func.getName()
                            + (descriptor.expected_type_description ? String(", expected ") + descriptor.expected_type_description : String{})
                            + (arg.type ? ", got " + arg.type->getName() : String{}),
                            error_code);
    }
}

}

int FunctionArgumentDescriptor::isValid(const DataTypePtr & data_type, const ColumnPtr & column) const
{
    if (type_validator_func && (data_type == nullptr || !type_validator_func(*data_type)))
        return ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT;

    if (column_validator_func && (column == nullptr || !column_validator_func(*column)))
        return ErrorCodes::ILLEGAL_COLUMN;

    return 0;
}

void validateFunctionArgumentTypes(const IFunction & func,
                                   const ColumnsWithTypeAndName & arguments,
                                   const FunctionArgumentDescriptors & mandatory_args,
                                   const FunctionArgumentDescriptors & optional_args)
{
    if (arguments.size() < mandatory_args.size() || arguments.size() > mandatory_args.size() + optional_args.size())
    {
        auto join_argument_types = [](const auto & args, const String sep = ", ")
        {
            String result;
            for (const auto & a : args)
            {
                using A = std::decay_t<decltype(a)>;
                if constexpr (std::is_same_v<A, FunctionArgumentDescriptor>)
                {
                    if (a.argument_name)
                        result += "'" + std::string(a.argument_name) + "' : ";
                    if (a.expected_type_description)
                        result += a.expected_type_description;
                }
                else if constexpr (std::is_same_v<A, ColumnWithTypeAndName>)
                    result += a.type->getName();

                result += sep;
            }

            if (!args.empty())
                result.erase(result.end() - sep.length(), result.end());

            return result;
        };

        throw Exception("Incorrect number of arguments for function " + func.getName()
                        + " provided " + std::to_string(arguments.size())
                        + (!arguments.empty() ? " (" + join_argument_types(arguments) + ")" : String{})
                        + ", expected " + std::to_string(mandatory_args.size())
                        + (!optional_args.empty() ? " to " + std::to_string(mandatory_args.size() + optional_args.size()) : "")
                        + " (" + join_argument_types(mandatory_args)
                        + (!optional_args.empty() ? ", [" + join_argument_types(optional_args) + "]" : "")
                        + ")",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
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
            throw Exception("Illegal column " + columns[i]->getName() + " as argument of function", ErrorCodes::ILLEGAL_COLUMN);
        if (i == 0)
            offsets = offsets_i;
        else if (*offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to aggregate function must be equal.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
    }
    return {nested_columns, offsets->data()};
}

}
