#include <Dictionaries/RangeHashedDictionary.h>

#include <Core/Settings.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

template <DictionaryKeyType dictionary_key_type>
static DictionaryPtr createRangeHashedDictionary(const std::string & full_name,
                            const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            ContextPtr global_context,
                            DictionarySourcePtr source_ptr)
{
    static constexpr auto layout_name = dictionary_key_type == DictionaryKeyType::Simple ? "range_hashed" : "complex_key_range_hashed";

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'range_hashed'");
    }
    else
    {
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for dictionary of layout 'complex_key_range_hashed'");
    }

    if (!dict_struct.range_min || !dict_struct.range_max)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' requires .structure.range_min and .structure.range_max",
            full_name,
            layout_name);

    const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

    String dictionary_layout_prefix = config_prefix + ".layout." + layout_name;
    const bool convert_null_range_bound_to_open = config.getBool(dictionary_layout_prefix + ".convert_null_range_bound_to_open", true);
    String range_lookup_strategy = config.getString(dictionary_layout_prefix + ".range_lookup_strategy", "min");
    RangeHashedDictionaryLookupStrategy lookup_strategy = RangeHashedDictionaryLookupStrategy::min;

    if (range_lookup_strategy == "min")
        lookup_strategy = RangeHashedDictionaryLookupStrategy::min;
    else if (range_lookup_strategy == "max")
        lookup_strategy = RangeHashedDictionaryLookupStrategy::max;

    auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
    const auto * clickhouse_source = dynamic_cast<const ClickHouseDictionarySource *>(source_ptr.get());
    bool use_async_executor = clickhouse_source && clickhouse_source->isLocal() && context->getSettingsRef()[Setting::dictionary_use_async_executor];

    RangeHashedDictionaryConfiguration configuration
    {
        .convert_null_range_bound_to_open = convert_null_range_bound_to_open,
        .lookup_strategy = lookup_strategy,
        .require_nonempty = require_nonempty,
        .use_async_executor = use_async_executor,
    };

    DictionaryPtr result = std::make_unique<RangeHashedDictionary<dictionary_key_type>>(
        dict_id,
        dict_struct,
        std::move(source_ptr),
        dict_lifetime,
        configuration);

    return result;
}

void registerDictionaryRangeHashed(DictionaryFactory & factory);
void registerDictionaryRangeHashed(DictionaryFactory & factory)
{
    auto create_layout_simple = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        return createRangeHashedDictionary<DictionaryKeyType::Simple>(full_name, dict_struct, config, config_prefix, global_context, std::move(source_ptr));
    };

    factory.registerLayout("range_hashed", create_layout_simple, false, true, Documentation{
        .description = R"DOCS_MD(
# range_hashed dictionary layout types

## range_hashed {#range_hashed}

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values.

This storage method works the same way as hashed and allows using date/time (arbitrary numeric type) ranges in addition to the key.

Example: The table contains discounts for each advertiser in the format:

```text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

To use a sample for date ranges, define the `range_min` and `range_max` elements in the [structure](/reference/statements/create/dictionary/attributes#composite-key). These elements must contain elements `name` and `type` (if `type` is not specified, the default type will be used - Date). `type` can be any numeric type (Date / DateTime / UInt64 / Int32 / others).

<Note>
Values of `range_min` and `range_max` should fit in `Int64` type.
</Note>

Example:

<Tabs>
<Tab title="DDL">

```sql
CREATE DICTIONARY discounts_dict (
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Date,
    amount Float64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'discounts'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN discount_start_date MAX discount_end_date)
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
    <range_hashed>
        <!-- Strategy for overlapping ranges (min/max). Default: min (return a matching range with the min(range_min -> range_max) value) -->
        <range_lookup_strategy>min</range_lookup_strategy>
    </range_hashed>
</layout>
<structure>
    <id>
        <name>advertiser_id</name>
    </id>
    <range_min>
        <name>discount_start_date</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>discount_end_date</name>
        <type>Date</type>
    </range_max>
    ...
```

</Tab>
</Tabs>
<br/>

To work with these dictionaries, you need to pass an additional argument to the `dictGet` function, for which a range is selected:

```sql
dictGet('dict_name', 'attr_name', id, date)
```
Query example:

```sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

This function returns the value for the specified `id`s and the date range that includes the passed date.

Details of the algorithm:

- If the `id` is not found or a range is not found for the `id`, it returns the default value of the attribute's type.
- If there are overlapping ranges and `range_lookup_strategy=min`, it returns a matching range with minimal `range_min`, if several ranges found, it returns a range with minimal `range_max`, if again several ranges found (several ranges had the same `range_min` and `range_max` it returns a random range of them.
- If there are overlapping ranges and `range_lookup_strategy=max`, it returns a matching range with maximal `range_min`, if several ranges found, it returns a range with maximal `range_max`, if again several ranges found (several ranges had the same `range_min` and `range_max` it returns a random range of them.
- If the `range_max` is `NULL`, the range is open. `NULL` is treated as maximal possible value. For the `range_min` `1970-01-01` or `0` (-MAX_INT) can be used as the open value.

Configuration example:

<Tabs>
<Tab title="DDL">

```sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

</Tab>
<Tab title="Configuration file">

```xml
<clickhouse>
    <dictionary>
        ...

        <layout>
            <range_hashed />
        </layout>

        <structure>
            <id>
                <name>Abcdef</name>
            </id>
            <range_min>
                <name>StartTimeStamp</name>
                <type>UInt64</type>
            </range_min>
            <range_max>
                <name>EndTimeStamp</name>
                <type>UInt64</type>
            </range_max>
            <attribute>
                <name>XXXType</name>
                <type>String</type>
                <null_value />
            </attribute>
        </structure>

    </dictionary>
</clickhouse>
```

</Tab>
</Tabs>
<br/>

Configuration example with overlapping ranges and open ranges:

```sql
CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

SELECT * FROM discounts ORDER BY advertiser_id, discount_start_date;
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│             1 │          2015-01-01 │              ᴺᵁᴸᴸ │    0.1 │
│             1 │          2015-01-15 │              ᴺᵁᴸᴸ │    0.2 │
│             2 │          2015-01-01 │        2015-01-15 │    0.3 │
│             2 │          2015-01-04 │        2015-01-10 │    0.4 │
│             3 │          1970-01-01 │        2015-01-15 │    0.5 │
│             3 │          1970-01-01 │        2015-01-10 │    0.6 │
└───────────────┴─────────────────────┴───────────────────┴────────┘

-- RANGE_LOOKUP_STRATEGY 'max'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.2 │ -- two ranges are matching, range_min 2015-01-15 (0.2) is bigger than 2015-01-01 (0.1)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.4 │ -- two ranges are matching, range_min 2015-01-04 (0.4) is bigger than 2015-01-01 (0.3)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.5 │ -- two ranges are matching, range_min are equal, 2015-01-15 (0.5) is bigger than 2015-01-10 (0.6)
└─────┘

DROP DICTIONARY discounts_dict;

-- RANGE_LOOKUP_STRATEGY 'min'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.1 │ -- two ranges are matching, range_min 2015-01-01 (0.1) is less than 2015-01-15 (0.2)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.3 │ -- two ranges are matching, range_min 2015-01-01 (0.3) is less than 2015-01-04 (0.4)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.6 │ -- two ranges are matching, range_min are equal, 2015-01-10 (0.6) is less than 2015-01-15 (0.5)
└─────┘
```

## complex_key_range_hashed {#complex_key_range_hashed}

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values (see [range_hashed](#range_hashed)). This type of storage is for use with composite [keys](/reference/statements/create/dictionary/attributes#composite-key).

Configuration example:

```sql
CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  CountryKey String,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);
```
)DOCS_MD",
        .syntax = "LAYOUT(RANGE_HASHED())",
        .related = {"complex_key_range_hashed", "hashed"}});

    auto create_layout_complex = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        return createRangeHashedDictionary<DictionaryKeyType::Complex>(full_name, dict_struct, config, config_prefix, global_context, std::move(source_ptr));
    };

    factory.registerLayout("complex_key_range_hashed", create_layout_complex, true, true, Documentation{
        .description = "Like `range_hashed`, but supports composite keys.",
        .syntax = "LAYOUT(COMPLEX_KEY_RANGE_HASHED())",
        .related = {"range_hashed"}});
}

}
