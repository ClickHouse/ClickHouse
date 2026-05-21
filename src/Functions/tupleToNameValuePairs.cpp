#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Transform a named tuple into an array of pairs, where the first element
  * of the pair corresponds to the tuple field name and the second one to the
  * tuple value.
  */
class FunctionTupleToNameValuePairs final : public IFunction
{
public:
    static constexpr auto name = "tupleToNameValuePairs";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTupleToNameValuePairs>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const DataTypePtr col = arguments[0].type;
        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(removeNullable(col).get());

        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be a Tuple or a Nullable(Tuple).",
                            getName());

        const auto & element_types = tuple->getElements();

        if (element_types.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument tuple for function {} must not be empty.",
                            getName());

        const auto & first_element_type = element_types[0];

        bool all_value_types_equal = std::all_of(element_types.begin() + 1,
                                                 element_types.end(),
                                                 [&](const auto &other)
                                                 {
                                                     return first_element_type->equals(*other);
                                                 });

        if (!all_value_types_equal)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument tuple for function {} must contain just one type",
                            getName());
        }

        DataTypes item_data_types = {std::make_shared<DataTypeString>(), col->isNullable() ? makeNullableSafe(first_element_type) : first_element_type};
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(item_data_types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const DataTypeTuple * tuple_type = checkAndGetDataType<DataTypeTuple>(removeNullable(arguments[0].type).get());

        const auto * source_nullable_column = checkAndGetColumn<ColumnNullable>(arguments[0].column.get());
        const IColumn * source_tuple_column
            = source_nullable_column ? &source_nullable_column->getNestedColumn() : arguments[0].column.get();
        const auto * source_tuple_column_concrete = assert_cast<const ColumnTuple *>(source_tuple_column);
        const NullMap * source_null_map = source_nullable_column ? &source_nullable_column->getNullMapData() : nullptr;

        const auto & result_tuple_type
            = assert_cast<const DataTypeTuple &>(*assert_cast<const DataTypeArray &>(*result_type).getNestedType());
        auto keys_column = ColumnString::create();
        MutableColumnPtr values_column = result_tuple_type.getElements()[1]->createColumn();
        auto * nullable_values_column = typeid_cast<ColumnNullable *>(values_column.get());

        auto offsets = ColumnVector<UInt64>::create();
        UInt64 current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            bool row_is_null = source_null_map && (*source_null_map)[row];
            for (size_t col = 0; col < source_tuple_column_concrete->tupleSize(); ++col)
            {
                const std::string & key = tuple_type->getElementNames()[col];
                keys_column->insertData(key.data(), key.size());

                if (row_is_null)
                {
                    values_column->insertDefault();
                }
                else
                {
                    const IColumn & source_value_column = source_tuple_column_concrete->getColumn(col);
                    if (nullable_values_column && !source_value_column.isNullable())
                        nullable_values_column->insertFromNotNullable(source_value_column, row);
                    else
                        values_column->insertFrom(source_value_column, row);
                }
            }
            current_offset += source_tuple_column_concrete->tupleSize();
            offsets->insertValue(current_offset);
        }

        std::vector<ColumnPtr> tuple_columns = { std::move(keys_column), std::move(values_column) };
        auto tuple_column = ColumnTuple::create(std::move(tuple_columns));
        return ColumnArray::create(std::move(tuple_column), std::move(offsets));
    }
};

}

REGISTER_FUNCTION(TupleToNameValuePairs)
{
    FunctionDocumentation::Description description = R"(
Converts a tuple to an array of `(name, value)` pairs.
For example, tuple `Tuple(n1 T1, n2 T2, ...)` is converted to `Array(Tuple('n1', T1), Tuple('n2', T2), ...)`.
All values in the tuple must be of the same type.
)";
    FunctionDocumentation::Syntax syntax = "tupleToNameValuePairs(tuple)";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "Named tuple with any types of values.", {"Tuple(n1 T1[, n2 T2, ...])"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array with `(name, value)` pairs.", {"Array(Tuple(String, T))"}};
    FunctionDocumentation::Examples examples = {
        {"Named tuple", "SELECT tupleToNameValuePairs(tuple(1593 AS user_ID, 2502 AS session_ID))", "[('1', 1593), ('2', 2502)]"},
        {"Unnamed tuple", "SELECT tupleToNameValuePairs(tuple(3, 2, 1))", "[('1', 3), ('2', 2), ('3', 1)]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTupleToNameValuePairs>(documentation);
}

}
