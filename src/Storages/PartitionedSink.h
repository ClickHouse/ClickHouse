#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/Arena.h>
#include <absl/container/flat_hash_map.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class PartitionedSink : public SinkToStorage
{
public:
    static constexpr auto PARTITION_ID_WILDCARD = "{_partition_id}";

    PartitionedSink(const ASTPtr & partition_by, ContextPtr context_, const Block & sample_block_);

    String getName() const override { return "PartitionedSink"; }

    void consume(Chunk chunk) override;

    void onFinish() override;

    virtual SinkPtr createSinkForPartition(const String & partition_id) = 0;

    static void validatePartitionKey(const String & str, bool allow_slash);

    static String replaceWildcards(const String & haystack, const String & partition_id);

private:
    ContextPtr context;
    Block sample_block;

    ExpressionActionsPtr partition_by_expr;
    String partition_by_column_name;

    absl::flat_hash_map<StringRef, SinkPtr> partition_id_to_sink;
    HashMapWithSavedHash<StringRef, size_t> partition_id_to_chunk_index;
    IColumn::Selector chunk_row_index_to_partition_index;
    Arena partition_keys_arena;

    SinkPtr getSinkForPartitionKey(StringRef partition_key);

};

}
