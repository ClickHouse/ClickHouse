#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Dictionaries/IDictionary.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int TYPE_MISMATCH;
extern const int BAD_ARGUMENTS;
}

static inline UInt64 hashAt(const IColumn & column, size_t row)
{
    SipHash h;
    column.updateHashWithValue(row, h);
    return h.get64();
}

static inline bool equalAt(const IColumn & left_column, size_t left_row_id, const IColumn & right_column, size_t right_row_id)
{
    if (const auto * left_nullable = checkAndGetColumn<ColumnNullable>(&left_column))
    {
        if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(&right_column))
        {
            const bool left_is_null = left_nullable->isNullAt(left_row_id);
            const bool right_is_null = right_nullable->isNullAt(right_row_id);
            if (left_is_null | right_is_null)
                return left_is_null & right_is_null;

            /// Both not null
            return left_nullable->getNestedColumn().compareAt(
                       left_row_id, right_row_id, right_nullable->getNestedColumn(), /*nan_direction_hint*/ 1)
                == 0;
        }

        if (left_nullable->isNullAt(left_row_id))
            return false;

        /// Right is not nullable
        return left_nullable->getNestedColumn().compareAt(left_row_id, right_row_id, right_column, /*nan_direction_hint*/ 1) == 0;
    }

    if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(&right_column))
    {
        if (right_nullable->isNullAt(right_row_id))
            return false;

        return left_column.compareAt(left_row_id, right_row_id, right_nullable->getNestedColumn(), /*nan_direction_hint*/ 1) == 0;
    }

    return left_column.compareAt(left_row_id, right_row_id, right_column, /*nan_direction_hint*/ 1) == 0;
}

class FunctionDictGetKeys final : public IFunction
{
public:
    static constexpr auto name = "dictGetKeys";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDictGetKeys>(context); }

    explicit FunctionDictGetKeys(ContextPtr context_)
        : helper(context_)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isVariadic() const override { return false; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const final { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * dict_name_const_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!dict_name_const_col)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String.",
                arguments[0].type->getName(),
                getName());

        const String dictionary_name = dict_name_const_col->getValue<String>();

        const auto * attr_name_const_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!attr_name_const_col)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String.",
                arguments[1].type->getName(),
                getName());

        const String attr_name = attr_name_const_col->getValue<String>();

        auto dict_struct = helper.getDictionaryStructure(dictionary_name);
        if (!dict_struct.hasAttribute(attr_name))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dictionary has no attribute '{}'", attr_name);

        const auto key_types = dict_struct.getKeyTypes();
        if (key_types.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary has no keys");

        if (key_types.size() == 1)
            return std::make_shared<DataTypeArray>(key_types[0]);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(key_types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConstWithDefaultValue(input_rows_count);
    }

private:
    mutable FunctionDictHelper helper;
};


REGISTER_FUNCTION(DictGetKeys)
{
    FunctionDocumentation::Description description = "Inverse dictionary lookup: return keys where attribute equals the given value.";
    FunctionDocumentation::Syntax syntax = "dictGetKeys('dict_name', 'attr_name', value_expr)";
    FunctionDocumentation::Arguments arguments
        = {{"dict_name", "Name of the dictionary.", {"String"}},
           {"attr_name", "Attribute to match.", {"String"}},
           {"value_expr", "Value to match. Vector or constant. Casts to attribute type.", {}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Array of keys. Element is UInt64 for simple key or Tuple(...) for complex key.", {}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Dictionary;
    FunctionDocumentation docs{description, syntax, arguments, returned_value, {}, introduced_in, category};

    factory.registerFunction<FunctionDictGetKeys>(docs);
}

}
