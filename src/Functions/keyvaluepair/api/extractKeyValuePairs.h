#pragma once

#include <unordered_set>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/keyvaluepair/src/KeyValuePairExtractor.h>
#include "ArgumentExtractor.h"

namespace DB
{

class ExtractKeyValuePairs : public IFunction
{
public:
    ExtractKeyValuePairs();

    static constexpr auto name = "extractKeyValuePairs";

    static FunctionPtr create(ContextPtr);

    String getName() const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override;

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override;

    bool isVariadic() const override;

    size_t getNumberOfArguments() const override;

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override;

private:
    DataTypePtr return_type;

    static auto getExtractor(const ArgumentExtractor::ParsedArguments & parsed_arguments);

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override;
};

}
