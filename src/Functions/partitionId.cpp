#include <memory>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Storages/MergeTree/MergeTreePartition.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** partitionId(x, y, ...) is a function that computes partition ids of arguments.
  * The function is slow and should not be called for large amount of rows.
  */
class FunctionPartitionId : public IFunction
{
public:
    static constexpr auto name = "partitionId";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPartitionId>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Block sample_block(arguments);
        size_t size = arguments.size();

        auto result_column = ColumnString::create();
        for (size_t j = 0; j < input_rows_count; ++j)
        {
            Row row(size);
            for (size_t i = 0; i < size; ++i)
                arguments[i].column->get(j, row[i]);
            MergeTreePartition partition(std::move(row));
            result_column->insert(partition.getID(sample_block));
        }
        return result_column;
    }
};

REGISTER_FUNCTION(PartitionId)
{
    FunctionDocumentation::Description description = R"(
Computes the [partition ID](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).

:::note
This function is slow and should not be called for large numbers of rows.
:::
)";
    FunctionDocumentation::Syntax syntax = "partitionId(column1[, column2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"column1, column2, ...", "Column for which to return the partition ID."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the partition ID that the row belongs to.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
  i int,
  j int
)
ENGINE = MergeTree
PARTITION BY i
ORDER BY tuple();

INSERT INTO tab VALUES (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6);

SELECT i, j, partitionId(i), _partition_id FROM tab ORDER BY i, j;
        )",
        R"(
┌─i─┬─j─┬─partitionId(i)─┬─_partition_id─┐
│ 1 │ 1 │ 1              │ 1             │
│ 1 │ 2 │ 1              │ 1             │
│ 1 │ 3 │ 1              │ 1             │
│ 2 │ 4 │ 2              │ 2             │
│ 2 │ 5 │ 2              │ 2             │
│ 2 │ 6 │ 2              │ 2             │
└───┴───┴────────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPartitionId>(documentation);
    factory.registerAlias("partitionID", "partitionId");
}

}
