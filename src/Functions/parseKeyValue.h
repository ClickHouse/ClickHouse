#pragma once

#include <unordered_set>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Functions/keyvaluepair/KeyValuePairExtractor.h>

namespace DB {

class ParseKeyValue : public IFunction
{

    using CharArgument = std::optional<char>;
    using SetArgument = std::unordered_set<char>;

    struct ParsedArguments {
        ColumnPtr data_column;
        CharArgument escape_character;
        CharArgument key_value_pair_delimiter;
        CharArgument item_delimiter;
        CharArgument enclosing_character;
        std::unordered_set<char> value_special_characters_allow_list;
    };

public:
    ParseKeyValue();

    static constexpr auto name = "parseKeyValue";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ParseKeyValue>();
    }

    /// Get the main function name.
    String getName() const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override;

    bool isVariadic() const override;

    size_t getNumberOfArguments() const override;

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override;

private:
    DataTypePtr return_type;

    ParsedArguments parseArguments(const ColumnsWithTypeAndName & arguments) const;

    std::shared_ptr<KeyValuePairExtractor> getExtractor(CharArgument escape_character, CharArgument key_value_pair_delimiter,
                                                        CharArgument item_delimiter, CharArgument enclosing_character,
                                                        SetArgument value_special_characters_allow_list) const;

//    ColumnPtr build

};

}
