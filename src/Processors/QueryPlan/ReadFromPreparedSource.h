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
    explicit ReadFromPreparedSource(Pipe pipe_);

    String getName() const override { return "ReadFromPreparedSource"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

protected:
    Pipe pipe;
};

struct ReadFromStorageWire;

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

    /// The framed format: the wire struct is what the manifest in `ReadFromPreparedSource.cpp` declares.
    ReadFromStorageWire toWire() const;
    static QueryPlanStepPtr fromWire(ReadFromStorageWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    StoragePtr storage;

    ContextPtr context;
    SelectQueryInfo query_info;
};

/// What `ReadFromStorageStep` puts on the wire in the framed format. Only `system.one` is serialized.
struct ReadFromStorageWire
{
    String storage_name;
};

}
