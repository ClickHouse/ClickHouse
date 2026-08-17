#pragma once

#include <Storages/IStorage.h>
#include <Storages/StorageConfiguration.h>
#include <Common/randomSeed.h>

#include "config.h"

#if USE_SIMDJSON || USE_RAPIDJSON
namespace DB
{

class NamedCollection;

class StorageFuzzJSON final : public IStorage
{
public:
    struct Configuration : public StatelessTableEngineConfiguration
    {
        // A full N-ary tree may be memory-intensive as it can potentially contain
        // up to (B^(D + 1) - 1) / (B - 1) nodes, where B is the number of branches,
        // and D is the depth of the tree. Therefore, a value number limit is introduced.
        // This limit includes complex values (arrays and nested objects).
        static constexpr UInt64 value_number_limit = 1000;
        static constexpr UInt64 output_length_limit = 1LU << 16;

        String json_str = "{}";
        UInt64 random_seed = randomSeed();
        bool should_reuse_output = false;
        bool should_malform_output = false;
        Float64 probability = 0.25;

        UInt64 max_output_length = 1024;

        // Key parameters
        UInt64 min_key_length = 4;
        UInt64 max_key_length = 20;

        // Value parameters
        // Maximum number of fields (key-value pairs) at each level of a JSON.
        UInt64 max_object_size = 10;
        // Maximum number of elements within a JSON array.
        UInt64 max_array_size = 10;
        // Max depth of nested structures. How deeply objects or arrays can be
        // nested within one another.
        UInt64 max_nesting_level = 5;
        UInt64 max_string_value_length = 32;
    };

    StorageFuzzJSON(
        const StorageID & table_id_, const ColumnsDescription & columns_, const String & comment_, const Configuration & config_);

    std::string getName() const override { return "FuzzJSON"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static void processNamedCollectionResult(Configuration & configuration, const NamedCollection & collection);

    static StorageFuzzJSON::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);

private:
    const Configuration config;
};

}
#endif
