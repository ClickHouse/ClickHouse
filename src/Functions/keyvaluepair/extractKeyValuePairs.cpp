#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Core/Settings.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>

#include <Interpreters/Context.h>

#include <Functions/keyvaluepair/impl/KeyValuePairExtractor.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/ArgumentExtractor.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 extract_key_value_pairs_max_pairs_per_row;
}

template <typename Name, bool WITH_ESCAPING>
class ExtractKeyValuePairs : public IFunction
{
    auto getExtractor(const ArgumentExtractor::ParsedArguments & parsed_arguments) const
    {
        auto builder = KeyValuePairExtractorBuilder();

        if constexpr (WITH_ESCAPING)
        {
            builder.withEscaping();
        }

        if (parsed_arguments.key_value_delimiter)
        {
            builder.withKeyValueDelimiter(parsed_arguments.key_value_delimiter.value());
        }

        if (!parsed_arguments.pair_delimiters.empty())
        {
            builder.withItemDelimiters(parsed_arguments.pair_delimiters);
        }

        if (parsed_arguments.quoting_character)
        {
            builder.withQuotingCharacter(parsed_arguments.quoting_character.value());
        }

        bool is_number_of_pairs_unlimited = context->getSettingsRef()[Setting::extract_key_value_pairs_max_pairs_per_row] == 0;

        if (!is_number_of_pairs_unlimited)
        {
            builder.withMaxNumberOfPairs(context->getSettingsRef()[Setting::extract_key_value_pairs_max_pairs_per_row]);
        }

        return builder.build();
    }

    ColumnPtr extract(ColumnPtr data_column, std::shared_ptr<KeyValuePairExtractor> extractor, size_t input_rows_count) const
    {
        auto offsets = ColumnUInt64::create();

        auto keys = ColumnString::create();
        auto values = ColumnString::create();

        uint64_t offset = 0u;

        for (auto i = 0u; i < input_rows_count; i++)
        {
            auto row = data_column->getDataAt(i).toView();

            auto pairs_count = extractor->extract(row, keys, values);

            offset += pairs_count;

            offsets->insert(offset);
        }

        keys->validate();
        values->validate();

        ColumnPtr keys_ptr = std::move(keys);

        return ColumnMap::create(keys_ptr, std::move(values), std::move(offsets));
    }

public:
    explicit ExtractKeyValuePairs(ContextPtr context_) : context(context_) {}

    static constexpr auto name = Name::name;

    String getName() const override
    {
        return name;
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<ExtractKeyValuePairs>(context);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto parsed_arguments = ArgumentExtractor::extract(arguments);

        auto extractor = getExtractor(parsed_arguments);

        return extract(parsed_arguments.data_column, extractor, input_rows_count);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    bool isVariadic() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

    std::size_t getNumberOfArguments() const override
    {
        return 0u;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {1, 2, 3, 4};
    }

private:
    ContextPtr context;
};

struct NameExtractKeyValuePairs
{
    static constexpr auto name = "extractKeyValuePairs";
};

struct NameExtractKeyValuePairsWithEscaping
{
    static constexpr auto name = "extractKeyValuePairsWithEscaping";
};

REGISTER_FUNCTION(ExtractKeyValuePairs)
{
    factory.registerFunction<ExtractKeyValuePairs<NameExtractKeyValuePairs, false>>(
        FunctionDocumentation{
            .description=R"(Extracts key-value pairs from any string. The string does not need to be 100% structured in a key value pair format;

            It can contain noise (e.g. log files). The key-value pair format to be interpreted should be specified via function arguments.

            A key-value pair consists of a key followed by a `key_value_delimiter` and a value. Quoted keys and values are also supported. Key value pairs must be separated by pair delimiters.

            **Syntax**
            ``` sql
            extractKeyValuePairs(data, [key_value_delimiter], [pair_delimiter], [quoting_character])
            ```

            **Arguments**
            - `data` - String to extract key-value pairs from. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `key_value_delimiter` - Character to be used as delimiter between the key and the value. Defaults to `:`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `pair_delimiters` - Set of character to be used as delimiters between pairs. Defaults to `\space`, `,` and `;`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `quoting_character` - Character to be used as quoting character. Defaults to `"`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

            **Returned values**
            - The extracted key-value pairs in a Map(String, String).

            **Examples**

            Query:

            **Simple case**
            ``` sql
            arthur :) select extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv

            SELECT extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv

            Query id: f9e0ca6f-3178-4ee2-aa2c-a5517abb9cee

            ┌─kv──────────────────────────────────────────────────────────────────────┐
            │ {'name':'neymar','age':'31','team':'psg','nationality':'brazil'}        │
            └─────────────────────────────────────────────────────────────────────────┘
            ```

            **Single quote as quoting character**
            ``` sql
            arthur :) select extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') as kv

            SELECT extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') as kv

            Query id: 0e22bf6b-9844-414a-99dc-32bf647abd5e

            ┌─kv───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
            │ {'name':'neymar','age':'31','team':'psg','nationality':'brazil','last_key':'last_value'}                                 │
            └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            ```

            **Escape sequences without escape sequences support**
            ``` sql
            arthur :) select extractKeyValuePairs('age:a\\x0A\\n\\0') as kv

            SELECT extractKeyValuePairs('age:a\\x0A\\n\\0') AS kv

            Query id: e9fd26ee-b41f-4a11-b17f-25af6fd5d356

            ┌─kv────────────────────┐
            │ {'age':'a\\x0A\\n\\0'} │
            └───────────────────────┘
            ```)"}
    );

    factory.registerFunction<ExtractKeyValuePairs<NameExtractKeyValuePairsWithEscaping, true>>(
        FunctionDocumentation{
            .description=R"(Same as `extractKeyValuePairs` but with escaping support.

            Escape sequences supported: `\x`, `\N`, `\a`, `\b`, `\e`, `\f`, `\n`, `\r`, `\t`, `\v` and `\0`.
            Non standard escape sequences are returned as it is (including the backslash) unless they are one of the following:
            `\\`, `'`, `"`, `backtick`, `/`, `=` or ASCII control characters (c <= 31).

            This function will satisfy the use case where pre-escaping and post-escaping are not suitable. For instance, consider the following
            input string: `a: "aaaa\"bbb"`. The expected output is: `a: aaaa\"bbbb`.
            - Pre-escaping: Pre-escaping it will output: `a: "aaaa"bbb"` and `extractKeyValuePairs` will then output: `a: aaaa`
            - Post-escaping: `extractKeyValuePairs` will output `a: aaaa\` and post-escaping will keep it as it is.

            Leading escape sequences will be skipped in keys and will be considered invalid for values.

            **Escape sequences with escape sequence support turned on**
            ``` sql
            arthur :) select extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0') as kv

            SELECT extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0') AS kv

            Query id: 44c114f0-5658-4c75-ab87-4574de3a1645

            ┌─kv───────────────┐
            │ {'age':'a\n\n\0'} │
            └──────────────────┘
            ```)"}
    );
    factory.registerAlias("str_to_map", NameExtractKeyValuePairs::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("mapFromString", NameExtractKeyValuePairs::name);
}

}
