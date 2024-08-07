#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Common/Exception.h>
#include <DataStreams/BlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

class HudiMORQueries
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
            LOG_ERROR(&Logger::get("HudiMORQueries"), "Error executing MOR Snapshot Query: {}", e.what());
            throw;
        }
    }

    void executeReadOptimized(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto optimized_data = loadReadOptimizedData(metadata);
            processReadOptimizedData(optimized_data, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiMORQueries"), "Error executing MOR Read-Optimized Query: {}", e.what());
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
            LOG_ERROR(&Logger::get("HudiMORQueries"), "Error executing MOR Incremental Query: {}", e.what());
            throw;
        }
    }

    void executeMORCDC(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto mor_cdc_changes = loadMORCDCChanges(metadata);
            applyMORCDCChanges(mor_cdc_changes, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiMORQueries"), "Error executing MOR CDC Query: {}", e.what());
            throw;
        }
    }

    void executeBootstrapRO(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto bootstrap_ro_data = loadBootstrapROData(metadata);
            processBootstrapROData(bootstrap_ro_data, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiMORQueries"), "Error executing MOR Bootstrap RO Query: {}", e.what());
            throw;
        }
    }

    void executeBootstrapSnapshot(const BlockInputStreamPtr & input_stream, ContextPtr context)
    {
        try
        {
            auto metadata = HudiMetadata::load(context);
            auto bootstrap_snapshot_data = loadBootstrapSnapshotData(metadata);
            processBootstrapSnapshotData(bootstrap_snapshot_data, input_stream);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Logger::get("HudiMORQueries"), "Error executing MOR Bootstrap Snapshot Query: {}", e.what());
            throw;
        }
    }

private:
    SnapshotPtr loadSnapshot(const HudiMetadata & metadata)
    {
        SnapshotPtr snapshot = std::make_shared<Snapshot>(metadata);
        if (!snapshot->isValid())
        {
            throw Exception("Invalid MOR snapshot data", ErrorCodes::LOGICAL_ERROR);
        }
        return snapshot;
    }

    IncrementalChangesPtr loadIncrementalChanges(const HudiMetadata & metadata)
    {
        IncrementalChangesPtr changes = std::make_shared<IncrementalChanges>(metadata);
        if (!changes->isValid())
        {
            throw Exception("Invalid MOR incremental changes data", ErrorCodes::LOGICAL_ERROR);
        }
        return changes;
    }

    CDCChangesPtr loadMORCDCChanges(const HudiMetadata & metadata)
    {
        CDCChangesPtr changes = std::make_shared<CDCChanges>(metadata);
        if (!changes->isValid())
        {
            throw Exception("Invalid MOR CDC changes data", ErrorCodes::LOGICAL_ERROR);
        }
        return changes;
    }

    BootstrapDataPtr loadBootstrapROData(const HudiMetadata & metadata)
    {
        BootstrapDataPtr data = std::make_shared<BootstrapData>(metadata);
        if (!data->isValid())
        {
            throw Exception("Invalid bootstrap RO data", ErrorCodes::LOGICAL_ERROR);
        }
        return data;
    }

    BootstrapDataPtr loadBootstrapSnapshotData(const HudiMetadata & metadata)
    {
        BootstrapDataPtr data = std::make_shared<BootstrapData>(metadata);
        if (!data->isValid())
        {
            throw Exception("Invalid bootstrap snapshot data", ErrorCodes::LOGICAL_ERROR);
        }
        return data;
    }

    OptimizedDataPtr loadReadOptimizedData(const HudiMetadata & metadata)
    {
        OptimizedDataPtr data = std::make_shared<OptimizedData>(metadata);
        if (!data->isValid())
        {
            throw Exception("Invalid optimized data", ErrorCodes::LOGICAL_ERROR);
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

    void applyMORCDCChanges(const CDCChangesPtr & changes, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = changes->next())
        {
            input_stream->add(block);
        }
    }

    void processBootstrapROData(const BootstrapDataPtr & data, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = data->next())
        {
            input_stream->add(block);
        }
    }

    void processBootstrapSnapshotData(const BootstrapDataPtr & data, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = data->next())
        {
            input_stream->add(block);
        }
    }

    void processReadOptimizedData(const OptimizedDataPtr & data, const BlockInputStreamPtr & input_stream)
    {
        while (auto block = data->next())
        {
            input_stream->add(block);
        }
    }
};

}
