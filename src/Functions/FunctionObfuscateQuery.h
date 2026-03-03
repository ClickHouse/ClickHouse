#pragma once

#include <Functions/IFunction.h>
#include <Parsers/obfuscateQueries.h>
#include <string_view>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <IO/WriteBuffer.h>
#include <Common/SipHash.h>
#include <Common/randomSeed.h>

namespace DB
{

class FunctionObfuscateQuery : public IFunction
{
public:
    enum class Mode
    {
        WithRandomSeed,
        WithTag,
        WithProvidedSeed
    };

    explicit FunctionObfuscateQuery(String name_, Mode mode_) : name(name_), mode(mode_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override
    {
        // For functions with explicit seed we can use constant folding
        return mode == Mode::WithProvidedSeed;
    }

    bool isDeterministic() const override
    {
        return mode == Mode::WithProvidedSeed;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        // WithTag mode is deterministic within the same call but different across calls
        return mode == Mode::WithProvidedSeed;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    String name;
    Mode mode;

    static UInt64 hashStringToUInt64(std::string_view s);
};

}
