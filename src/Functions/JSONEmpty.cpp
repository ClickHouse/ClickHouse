#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


namespace
{

/// Implements the function JSONEmpty which returns true if provided JSON object is empty and false otherwise.
class FunctionJSONEmpty : public IFunction
{
public:
    static constexpr auto name = "JSONEmpty";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONEmpty>(); }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & data_types) const override
    {
        if (data_types.size() != 1 || data_types[0]->getTypeId() != TypeIndex::Object)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires single argument with type JSON", getName());
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        const auto * object_column = typeid_cast<const ColumnObject *>(elem.column.get());
        if (!object_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column type in function {}. Expected Object column, got {}", getName(), elem.column->getName());

        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        const auto & typed_paths = object_column->getTypedPaths();
        size_t size = object_column->size();
        /// If object column has at least 1 typed path, it will never be empty, because these paths always have values.
        if (!typed_paths.empty())
        {
            data.resize_fill(size, 0);
            return res;
        }

        const auto & dynamic_paths = object_column->getDynamicPaths();
        const auto & shared_data = object_column->getSharedDataPtr();
        data.reserve(size);
        for (size_t i = 0; i != size; ++i)
        {
            bool empty = true;
            /// Check if there is no paths in shared data.
            if (!shared_data->isDefaultAt(i))
            {
                empty = false;
            }
            /// Check that all dynamic paths have NULL value in this row.
            else
            {
                for (const auto & [path, column] : dynamic_paths)
                {
                    if (!column->isNullAt(i))
                    {
                        empty = false;
                        break;
                    }
                }
            }

            data.push_back(empty);
        }

        return res;
    }
};

}

REGISTER_FUNCTION(JSONEmpty)
{
    factory.registerFunction<FunctionJSONEmpty>(FunctionDocumentation{
        .description = R"(
Checks whether the input JSON object is empty.
)",
        .syntax = {"JSONEmpty(json)"},
        .arguments = {{"json", "JSON column"}},
        .examples = {{{
            "Example",
            R"(
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {}}, {"json" : {"b" : "Hello"}}, {"json" : {}}
SELECT json, JSONEmpty(json) FROM test;
)",
            R"(
┌─json──────────┬─JSONEmpty(json)─┐
│ {"a":"42"}    │               0 │
│ {}            │               1 │
│ {"b":"Hello"} │               0 │
│ {}            │               1 │
└───────────────┴─────────────────┘

)"}}},
        .categories{"JSON"},
    });
}

}
