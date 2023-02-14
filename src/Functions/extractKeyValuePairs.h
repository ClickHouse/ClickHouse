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

    struct ParsedArguments
    {
        ColumnPtr data_column;
        CharArgument escape_character;
        CharArgument key_value_pair_delimiter;
        CharArgument item_delimiter;
        CharArgument enclosing_character;
        std::unordered_set<char> value_special_characters_allow_list;
    };

    ExtractKeyValuePairs();

    static constexpr auto name = "extractKeyValuePairs";

    static FunctionPtr create(ContextPtr) { return std::make_shared<ExtractKeyValuePairs>(); }

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

    static auto getExtractor(CharArgument escape_character, CharArgument key_value_pair_delimiter,
                             CharArgument item_delimiter, CharArgument enclosing_character);

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override;
};

}
