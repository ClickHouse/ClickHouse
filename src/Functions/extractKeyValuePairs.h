#pragma once

#include <unordered_set>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/keyvaluepair/src/KeyValuePairExtractor.h>

namespace DB
{

class ExtractKeyValuePairs : public IFunction
{
public:
    using CharArgument = std::optional<char>;
    using SetArgument = std::unordered_set<char>;
    using BoolArgument = std::optional<bool>;

    struct ParsedArguments
    {
        ColumnPtr data_column;
        CharArgument key_value_pair_delimiter = {};
        SetArgument pair_delimiters = {};
        CharArgument quoting_character = {};
        BoolArgument with_escaping = {};
    };

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

    static ParsedArguments parseArguments(const ColumnsWithTypeAndName & arguments);

    static CharArgument extractControlCharacter(ColumnPtr column);

    static auto getExtractor(const ParsedArguments & parsed_arguments);

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override;
};

}
