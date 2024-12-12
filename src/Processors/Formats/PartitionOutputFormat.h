#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Core/Block.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Port.h>
#include <IO/DynamicWriteBufferManager.h>
#include <Interpreters/ExpressionActions.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

using OutputFormatPtr = std::shared_ptr<IOutputFormat>;
using OutputFormatForPath = std::function<OutputFormatPtr(const String & name)>;

class PartitionOutputFormat : public IOutputFormat
{
public:

    using Key = StringRefs;

    struct KeyHash
    {
        size_t operator()(const StringRefs & key) const
        {
            SipHash hash;
            hash.update(key.size());
            for (const auto & part : key)
                hash.update(part.toView());
            return hash.get64();
        }
    };

    PartitionOutputFormat(
        const OutputFormatForPath & output_format_for_path_,
        DynamicWriteBufferManager & write_buffers_manager_,
        const Block & header_,
        const String & out_file_template_,
        const ASTPtr & partition_by,
        const ContextPtr & context);

    String getName() const override { return "PartitionOutputFormat"; }

protected:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void flushImpl() override;

private:
    OutputFormatPtr getOrCreateOutputFormat(const Key & key);
    Key copyKeyToArena(const Key & key);

    absl::flat_hash_map<Key, OutputFormatPtr, KeyHash> partition_key_to_output_format;
    absl::flat_hash_map<StringRef, size_t> partition_key_name_to_index;
    Arena partition_keys_arena;

    std::vector<ExpressionActionsPtr> partition_by_exprs;
    std::vector<String> partition_by_expr_names;

    Block header;
    String out_file_template;
    OutputFormatForPath output_format_for_path;
    DynamicWriteBufferManager& write_buffers_manager;
};

void throwIfTemplateIsNotValid(const String & out_file_template, const ASTPtr & partition_by);

std::vector<StringRef> copyStringsInArena(Arena & arena, const StringRefs & strings);

};
