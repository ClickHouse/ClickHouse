#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
/// Function may accept more then two arguments. If so, the first array with non-empty offsets is chosen.
class FunctionReplicate final : public IFunction
{
public:
    static constexpr auto name = "replicate";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReplicate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    /// The first argument is the value, it keeps its type even if it is `FixedArray`.
    /// The other arguments only provide the sizes, and `FixedArray` is processed as `Array` there.
    bool useDefaultImplementationForFixedArray() const override { return false; }

    String getSignatureString() const override
    {
        return "(T, Array, ...) -> Array(T)";
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override;
};

}
