#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class IDictionary;

/// Reads a dictionary as a table.
/// `IDictionary::read` is called when the pipeline is built, not when the step is created: it may do work
/// proportional to the size of the dictionary, and a JOIN that looks the keys up drops this step unused.
class ReadFromDictionaryStep final : public ISourceStep
{
public:
    ReadFromDictionaryStep(
        SharedHeader output_header_,
        StoragePtr storage_,
        std::shared_ptr<const IDictionary> dictionary_,
        Names column_names_,
        size_t max_block_size_,
        size_t num_streams_,
        std::shared_ptr<const StorageLimitsList> storage_limits_,
        ContextPtr context_);

    [[nodiscard]] String getName() const override { return "ReadFromDictionary"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    [[nodiscard]] const StoragePtr & getStorage() const { return storage; }

private:
    StoragePtr storage;
    std::shared_ptr<const IDictionary> dictionary;
    Names column_names;
    size_t max_block_size;
    size_t num_streams;
    std::shared_ptr<const StorageLimitsList> storage_limits;
    ContextPtr context;
};

}
