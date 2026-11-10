#include <Core/Settings.h>
#include <Dictionaries/HashedDictionary.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}

void registerDictionaryHashed(DictionaryFactory & factory);
void registerDictionaryHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             DictionaryKeyType dictionary_key_type,
                             bool sparse) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::Simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed dictionary");
        if (dictionary_key_type == DictionaryKeyType::Complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        std::string dictionary_layout_name;

        if (dictionary_key_type == DictionaryKeyType::Simple)
            dictionary_layout_name = sparse ? "sparse_hashed" : "hashed";
        else
            dictionary_layout_name = sparse ? "complex_key_sparse_hashed" : "complex_key_hashed";

        const std::string dictionary_layout_prefix = ".layout." + dictionary_layout_name;
        const bool preallocate = config.getBool(config_prefix + dictionary_layout_prefix + ".preallocate", false);
        if (preallocate)
            LOG_WARNING(getLogger("HashedDictionary"), "'prellocate' attribute is obsolete, consider looking at 'shards'");

        Int64 shards = config.getInt64(config_prefix + dictionary_layout_prefix + ".shards", 1);
        if (shards <= 0 || shards > 128)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARDS parameter should be within [1, 128]", full_name);

        Int64 shard_load_queue_backlog = config.getInt64(config_prefix + dictionary_layout_prefix + ".shard_load_queue_backlog", 10000);
        if (shard_load_queue_backlog <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARD_LOAD_QUEUE_BACKLOG parameter should be greater then zero", full_name);

        float max_load_factor = static_cast<float>(config.getDouble(config_prefix + dictionary_layout_prefix + ".max_load_factor", 0.5));
        if (max_load_factor < 0.5f || max_load_factor > 0.99f)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: max_load_factor parameter should be within [0.5, 0.99], got {}", full_name, max_load_factor);

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
        const auto & settings = context->getSettingsRef();

        const auto * clickhouse_source = dynamic_cast<const ClickHouseDictionarySource *>(source_ptr.get());
        bool use_async_executor = clickhouse_source && clickhouse_source->isLocal() && settings[Setting::dictionary_use_async_executor];

        HashedDictionaryConfiguration configuration{
            static_cast<UInt64>(shards),
            static_cast<UInt64>(shard_load_queue_backlog),
            max_load_factor,
            require_nonempty,
            dict_lifetime,
            use_async_executor,
            std::chrono::seconds(settings[Setting::max_execution_time].totalSeconds()),
        };

        if (source_ptr->hasUpdateField() && shards > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: SHARDS parameter does not supports for updatable source (UPDATE_FIELD)", full_name);

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, true>>(
                    dict_id, dict_struct, std::move(source_ptr), configuration);
            return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, false>>(
                dict_id, dict_struct, std::move(source_ptr), configuration);
        }

        if (sparse)
            return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, true>>(
                dict_id, dict_struct, std::move(source_ptr), configuration);
        return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, false>>(
            dict_id, dict_struct, std::move(source_ptr), configuration);
    };

    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Simple, /* sparse = */ false); }, false, true, Documentation{
        .description = R"DOCS_MD(
# hashed dictionary layout types

## hashed {#hashed}

The dictionary is completely stored in memory in the form of a hash table. The dictionary can contain any number of elements with any identifiers. In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](/reference/data-types/int-uint) type.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

Configuration example:

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(HASHED())
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
  <hashed />
</layout>
```

</Tab>
</Tabs>
<br/>

Configuration example with settings:

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
  <hashed>
    <!-- If shards greater then 1 (default is `1`) the dictionary will load
         data in parallel, useful if you have huge amount of elements in one
         dictionary. -->
    <shards>10</shards>

    <!-- Size of the backlog for blocks in parallel queue.

         Since the bottleneck in parallel loading is rehash, and so to avoid
         stalling because of thread is doing rehash, you need to have some
         backlog.

         10000 is good balance between memory and speed.
         Even for 10e10 elements and can handle all the load without starvation. -->
    <shard_load_queue_backlog>10000</shard_load_queue_backlog>

    <!-- Maximum load factor of the hash table, with greater values, the memory
         is utilized more efficiently (less memory is wasted) but read/performance
         may deteriorate.

         Valid values: [0.5, 0.99]
         Default: 0.5 -->
    <max_load_factor>0.5</max_load_factor>
  </hashed>
</layout>
```

</Tab>
</Tabs>
<br/>

## sparse_hashed {#sparse_hashed}

Similar to `hashed`, but uses less memory in favor more CPU usage.

The dictionary key has the [UInt64](/reference/data-types/int-uint) type.

Configuration example:

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
  <sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </sparse_hashed>
</layout>
```

</Tab>
</Tabs>
<br/>

It is also possible to use `shards` for this type of dictionary, and again it is more important for `sparse_hashed` then for `hashed`, since `sparse_hashed` is slower.

## complex_key_hashed {#complex_key_hashed}

This type of storage is for use with composite [keys](/reference/statements/create/dictionary/attributes#composite-key). Similar to `hashed`.

Configuration example:

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
  <complex_key_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_hashed>
</layout>
```

</Tab>
</Tabs>
<br/>

## complex_key_sparse_hashed {#complex_key_sparse_hashed}

This type of storage is for use with composite [keys](/reference/statements/create/dictionary/attributes#composite-key). Similar to [sparse_hashed](#sparse_hashed).

Configuration example:

<Tabs>
<Tab title="DDL">

```sql
LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

</Tab>
<Tab title="Configuration file">

```xml
<layout>
  <complex_key_sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_sparse_hashed>
</layout>
```

</Tab>
</Tabs>
<br/>
)DOCS_MD",
        .syntax = "LAYOUT(HASHED())",
        .related = {"sparse_hashed", "complex_key_hashed", "hashed_array", "flat"}});
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Simple, /* sparse = */ true); }, false, true, Documentation{
        .description = "Like `hashed`, but uses significantly less memory at the cost of slower lookups.",
        .syntax = "LAYOUT(SPARSE_HASHED())",
        .related = {"hashed"}});
    factory.registerLayout("complex_key_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Complex, /* sparse = */ false); }, true, true, Documentation{
        .description = "Like `hashed`, but supports composite keys (a key consisting of several attributes or of a non-integer type).",
        .syntax = "LAYOUT(COMPLEX_KEY_HASHED())",
        .related = {"hashed"}});
    factory.registerLayout("complex_key_sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Complex, /* sparse = */ true); }, true, true, Documentation{
        .description = "Like `sparse_hashed`, but supports composite keys.",
        .syntax = "LAYOUT(COMPLEX_KEY_SPARSE_HASHED())",
        .related = {"sparse_hashed", "complex_key_hashed"}});

}

}
