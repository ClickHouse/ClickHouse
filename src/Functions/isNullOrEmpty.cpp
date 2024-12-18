#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnArray.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace
{

/// Implements the function isNullOrEmpty which returns true (UInt8=1) if:
///   - The argument is NULL; or
///   - The argument is an array whose size is 0 (empty).
/// Returns false (UInt8=0) otherwise.
class FunctionIsNullOrEmpty : public IFunction
{
public:
    static constexpr auto name = "isNullOrEmpty";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIsNullOrEmpty>(
            context->getSettingsRef()[Setting::allow_experimental_analyzer]
        );
    }

    explicit FunctionIsNullOrEmpty(bool use_analyzer_) 
        : use_analyzer(use_analyzer_) 
    {}

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override 
    { 
        return 1; 
    }

    bool useDefaultImplementationForNulls() const override 
    { 
        return false; 
    }

    bool useDefaultImplementationForLowCardinalityColumns() const override 
    { 
        return false; 
    }

    bool useDefaultImplementationForConstants() const override 
    { 
        return true; 
    }

    bool isSuitableForShortCircuitArgumentsExecution(
        const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(
        size_t /*number_of_arguments*/) const override 
    { 
        return {0}; 
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*args*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr getConstantResultForNonConstArguments(
        const ColumnsWithTypeAndName & arguments, 
        const DataTypePtr & result_type
    ) const override
    {
        if (!use_analyzer)
            return nullptr;

        const ColumnWithTypeAndName & elem = arguments[0];

        if (elem.type->onlyNull())
            return result_type->createColumnConst(1, UInt8(1));

        if (canContainNull(*elem.type) || isArray(elem.type))
            return nullptr;

        return result_type->createColumnConst(1, UInt8(0));
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, 
        const DataTypePtr &, 
        size_t /*input_rows_count*/
    ) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];

        if (!elem.type->isNullable() && !isArray(elem.type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} expects an Array or Nullable type as input, but got {}", getName(), elem.type->getName());

        //
        // 1) If the column is a low-cardinality nullable type
        //
        if (elem.type->isLowCardinalityNullable())
        {
            const auto & low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(*elem.column);
            size_t null_index = low_cardinality_column.getDictionary().getNullValueIndex();

            auto res = DataTypeUInt8().createColumn();
            auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
            data.reserve(low_cardinality_column.size());

            for (size_t i = 0; i != low_cardinality_column.size(); ++i)
            {
                bool is_null = (low_cardinality_column.getIndexAt(i) == null_index);
                if (is_null)
                {
                    data.push_back(1);
                }
                else
                {
                    /// If not null, see if it’s an array and check emptiness.
                    data.push_back(0);
                }
            }
            return res;
        }

        //
        // 2) If it’s a straightforward Nullable column
        //
        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(elem.column.get()))
        {
            const auto & nested_col = nullable_col->getNestedColumn();
            const auto & null_map = nullable_col->getNullMapData();

            if (const auto * arr_col = checkAndGetColumn<ColumnArray>(&nested_col))
            {
                const auto & offsets = arr_col->getOffsets();
                auto res = DataTypeUInt8().createColumn();
                auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
                data.resize(offsets.size());

                size_t prev_offset = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    bool is_null = (null_map[i] == 1);
                    if (is_null)
                        data[i] = 1;
                    else
                    {
                        size_t array_size = offsets[i] - prev_offset;
                        data[i] = (array_size == 0) ? 1 : 0;
                    }
                    prev_offset = offsets[i];
                }
                return res;
            }
            else
            {
                /// Not an array => "empty" concept doesn't apply, so it's basically just `isNull`.
                /// Return the embedded null map as 1 for null, 0 for not-null.
                return nullable_col->getNullMapColumnPtr();
            }
        }

        //
        // 3) Non-nullable array
        //
        if (const auto * arr_col = checkAndGetColumn<ColumnArray>(elem.column.get()))
        {
            const auto & offsets = arr_col->getOffsets();
            auto res = DataTypeUInt8().createColumn();
            auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
            data.resize(offsets.size());

            size_t prev_offset = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                size_t array_size = offsets[i] - prev_offset;
                data[i] = (array_size == 0) ? 1 : 0;
                prev_offset = offsets[i];
            }
            return res;
        }

        //
        // 4) If it’s not an array and not nullable => "empty" doesn’t apply; check if it’s "onlyNull" or not. 
        //    For typical columns (e.g., Int32, String, etc.), there's no "empty" concept. So the result is always 0.
        //
        return DataTypeUInt8().createColumnConst(elem.column->size(), 0u);
    }

private:
    bool use_analyzer;
};

}

REGISTER_FUNCTION(IsNullOrEmpty)
{
    factory.registerFunction<FunctionIsNullOrEmpty>
    (FunctionDocumentation{
         .description=R"(
Returns 1 (true) if the argument is NULL or an empty array, and 0 (false) otherwise.

Syntax:
isNullOrEmpty(expression)

Arguments:
expression — The input value to evaluate.
The argument must be of type Array or Nullable(Array). If the argument is of any other type, the function will throw an exception.

Returned Value
- 1 (true) if:
- The argument is NULL, or
- The argument is an empty array ([]).
- 0 (false) otherwise.

Example:
[example:typical]
)",
        .examples{
            {"typical", R"(
Query:
```
SELECT isNullOrEmpty([]);
```
Result:
```
┌─isNullOrEmpty([])─┐
│                 1 │
└───────────────────┘
```
)", ""}},
        .categories{"Array"}}, FunctionFactory::Case::Insensitive);
}

}
