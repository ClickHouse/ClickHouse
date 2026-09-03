#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeRow.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// Returns one field of a `Row(...)` column. The constant second argument is
/// either a 1-based field index (emitted by optimizeUseRowWrappers, O(1)) or a
/// field name. Execution is a constant-time pointer pick into the ColumnTuple.
class FunctionRowElement : public IFunction
{
public:
    static constexpr auto name = "__rowElement";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRowElement>(); }
    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * row = getRowType(arguments);
        size_t index = resolveIndex(*row, arguments[1]);
        return row->getElements()[index];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const auto * row = getRowType(arguments);
        size_t index = resolveIndex(*row, arguments[1]);
        const auto & tuple = assert_cast<const ColumnTuple &>(*arguments[0].column);
        return tuple.getColumnPtr(index);
    }

private:
    const DataTypeRow * getRowType(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} expects exactly 2 arguments", getName());

        const auto * row = typeid_cast<const DataTypeRow *>(arguments[0].type.get());
        if (!row)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be a Row(...) type; got {}", getName(), arguments[0].type->getName());
        return row;
    }

    size_t resolveIndex(const DataTypeRow & row, const ColumnWithTypeAndName & arg) const
    {
        const size_t num_fields = row.getElements().size();

        if (WhichDataType(arg.type).isNativeUInt() || WhichDataType(arg.type).isNativeInt())
        {
            UInt64 one_based = arg.column->getUInt(0);
            if (one_based == 0 || one_based > num_fields)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Row field index {} is out of range [1, {}]", one_based, num_fields);
            return static_cast<size_t>(one_based - 1);
        }

        const auto * column_const = checkAndGetColumnConst<ColumnString>(arg.column.get());
        if (!column_const)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a constant String name or constant integer index", getName());

        auto field_name = column_const->getValue<String>();
        const auto & names = row.getElementNames();
        for (size_t i = 0; i < num_fields; ++i)
            if (names[i] == field_name)
                return i;

        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
            "Row({}) has no field named '{}'", row.getName(), field_name);
    }
};


REGISTER_FUNCTION(RowElement)
{
    factory.registerFunction<FunctionRowElement>(FunctionDocumentation{
        .description = "Internal: extract a field from a `Row(...)` column by name. "
                       "Used by the `query_plan_use_row_wrappers` optimizer rule.",
        .syntax = "__rowElement(row_column, 'field_name')",
        .returned_value = {"The value of the requested field for each row.", {"Any"}},
        .category = FunctionDocumentation::Category::Internal,
    });
}

}
