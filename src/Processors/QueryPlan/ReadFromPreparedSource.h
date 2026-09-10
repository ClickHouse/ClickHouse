#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    /// The caller supplies a known input bound; an arbitrary prepared pipe may be unbounded.
    explicit ReadFromPreparedSource(Pipe pipe_, bool is_bounded_ = false);

    bool hasBoundedRead() const { return is_bounded; }
    bool hasTotals() const { return pipe.getTotalsPort() != nullptr; }
    bool hasExtremes() const { return pipe.getExtremesPort() != nullptr; }

    String getName() const override { return "ReadFromPreparedSource"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

protected:
    Pipe pipe;

private:
    const bool is_bounded;
};

class ReadFromStorageStep final : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, StoragePtr storage_, ContextPtr context_, const SelectQueryInfo & query_info_);

    String getName() const override { return "ReadFromStorage"; }

    const StoragePtr & getStorage() const { return storage; }

    void serialize(Serialization & ctx) const override;
    /// serialize is implemented only for StorageSystemOne.
    bool isSerializable() const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    StoragePtr storage;

    ContextPtr context;
    SelectQueryInfo query_info;
};

}
