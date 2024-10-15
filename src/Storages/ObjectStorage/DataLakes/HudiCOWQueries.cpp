#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Common/Exception.h>
#include <DataStreams/BlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

class HudiCOWQueries
{
public:
    void executeSnapshot(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto snapshot = loadSnapshot(metadata);
            processSnapshot(snapshot, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiCOWQueries"), "Error executing COW Snapshot Query: {}", e.what());
            throw;
        }
    }

    void executeIncremental(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto incremental_changes = loadIncrementalChanges(metadata);
            processIncrementalChanges(incremental_changes, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiCOWQueries"), "Error executing COW Incremental Query: {}", e.what());
            throw;
        }
    }

    void executeCDC(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto cdc_changes = loadCDCChanges(metadata);
            applyCDCChanges(cdc_changes, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiCOWQueries"), "Error executing COW CDC Query: {}", e.what());
            throw;
        }
    }

    void executeBootstrap(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto bootstrap_data = loadBootstrapData(metadata);
            processBootstrapData(bootstrap_data, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiCOWQueries"), "Error executing COW Bootstrap Query: {}", e.what());
            throw;
        }
    }

private:
    SnapshotPtr loadSnapshot(const HudiMetadata & metadata)
    {
        SnapshotPtr snapshot = std::make_shared<Snapshot>(metadata);
        if (!snapshot->isValid())
        {
            throw Exception("Invalid COW snapshot data", ErrorCodes::LOGICAL_ERROR);
        }
        return snapshot;
    }

    IncrementalChangesPtr loadIncrementalChanges(const HudiMetadata & metadata)
    {
        IncrementalChangesPtr changes = std::make_shared<IncrementalChanges>(metadata);
        if (!changes->isValid())
        {
            throw Exception("Invalid COW incremental changes data", ErrorCodes::LOGICAL_ERROR);
        }
        return changes;
    }

    CDCChangesPtr loadCDCChanges(const HudiMetadata & metadata)
    {
        CDCChangesPtr changes = std::make_shared<CDCChanges>(metadata);
        if (!changes->isValid())
        {
            throw Exception("Invalid CDC changes data", ErrorCodes::LOGICAL_ERROR);
        }
        return changes;
    }

    BootstrapDataPtr loadBootstrapData(const HudiMetadata & metadata)
    {
        BootstrapDataPtr data = std::make_shared<BootstrapData>(metadata);
        if (!data->isValid())
        {
            throw Exception("Invalid bootstrap data", ErrorCodes::LOGICAL_ERROR);
        }
        return data;
    }

    void processSnapshot(const SnapshotPtr & snapshot, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = snapshot->next())
        {
            input_stream->add(block);
        }
    }

    void processIncrementalChanges(const IncrementalChangesPtr & changes, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = changes->next())
        {
            input_stream->add(block);
        }
    }

    void applyCDCChanges(const CDCChangesPtr & changes, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = changes->next())
        {
            input_stream->add(block);
        }
    }

    void processBootstrapData(const BootstrapDataPtr & data, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = data->next())
        {
            input_stream->add(block);
        }
    }
};

}
