#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>


namespace DB
{
namespace
{

/// Implements the function isNull which returns true if a value
/// is null, false otherwise.
class FunctionIsNull : public IFunction
{
public:
    static constexpr auto name = "isNull";

    [[maybe_unused]] static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        if (elem.type->isLowCardinalityNullable())
        {
            const auto * low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(*elem.column);
            size_t null_index = low_cardinality_column->getDictionary().getNullValueIndex();
            auto res = DataTypeUInt8().createColumn();
            auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
            data.reserve(low_cardinality_column->size());
            for (size_t i = 0; i != low_cardinality_column->size(); ++i)
                data.push_back(low_cardinality_column->getIndexAt(i) == null_index);
            return res;
        }

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*elem.column))
        {
            /// Merely return the embedded null map.
            return nullable->getNullMapColumnPtr();
        }
        else
        {
            /// Since no element is nullable, return a zero-constant column representing
            /// a zero-filled null map.
            return DataTypeUInt8().createColumnConst(elem.column->size(), 0u);
        }
    }
};

}

}
