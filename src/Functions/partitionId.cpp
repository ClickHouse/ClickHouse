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
            throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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
    factory.registerFunction<FunctionPartitionId>();
}

}
