#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

#include <pcg_random.hpp>

namespace DB
{

class FunctionGenerateRandomStructure final : public IFunction
{
public:
    static constexpr auto name = "generateRandomStructure";

    explicit FunctionGenerateRandomStructure(bool allow_suspicious_lc_types_) : allow_suspicious_lc_types(allow_suspicious_lc_types_)
    {
    }

    static FunctionPtr create(ContextPtr context);

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0, 1}; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

    static String generateRandomStructure(size_t seed, const ContextPtr & context);
    static String generateRandomDataType(pcg64 & rng, bool allow_suspicious_lc_types, bool allow_complex_types);

    /// Test-only seam: the depth-limit fallback in writeRandomType is unreachable from SQL at the
    /// shipped MAX_DEPTH (a 36M-seed sweep maxed out at depth 16), so this lets a unit test drive it
    /// deterministically with a reduced limit. Returns one random type for the given seed and depth cap.
    static String generateRandomTypeForTest(size_t seed, bool allow_suspicious_lc_types, size_t max_depth);

private:
    bool allow_suspicious_lc_types;
};

}
