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
        bool ch_inline = false;
    };

    struct RawColumns
    {
        ColumnString::Ptr keys;
        ColumnString::Ptr values;
        ColumnUInt64::Ptr offsets;
    };

public:
    using EscapingProcessorOutput = std::unordered_map<std::string, std::string>;

    ExtractKeyValuePairs();

    static constexpr auto name = "extractKeyValuePairs";

    static FunctionPtr create(ContextPtr) { return std::make_shared<ExtractKeyValuePairs>(); }

    /// Get the main function name.
    String getName() const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override;

    bool isVariadic() const override;

    size_t getNumberOfArguments() const override;

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override;

private:
    DataTypePtr return_type;

    static ParsedArguments parseArguments(const ColumnsWithTypeAndName & arguments);

    static char extractControlCharacter(ColumnPtr column);

    static std::shared_ptr<KeyValuePairExtractor<EscapingProcessorOutput>> getExtractor(
        CharArgument escape_character,
        CharArgument key_value_pair_delimiter,
        CharArgument item_delimiter,
        CharArgument enclosing_character);

    static std::shared_ptr<KeyValuePairExtractor<std::unordered_map<std::string, std::string>>> getExtractor2(
        CharArgument escape_character,
        CharArgument key_value_pair_delimiter,
        CharArgument item_delimiter,
        CharArgument enclosing_character,
        SetArgument value_special_characters_allow_list);

    static RawColumns extract(std::shared_ptr<KeyValuePairExtractor<EscapingProcessorOutput>> extractor, ColumnPtr data_column);

    static ColumnPtr escape(RawColumns & raw_columns, char escape_character);

    static ColumnPtr chInline(
        ColumnPtr data_column,
        CharArgument escape_character,
        CharArgument key_value_pair_delimiter,
        CharArgument item_delimiter,
        CharArgument enclosing_character,
        SetArgument value_special_characters_allow_list) ;

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override;
};

}
