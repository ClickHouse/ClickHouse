#pragma once

#include "config.h"

#if USE_AVRO

#include <Columns/IColumn_fwd.h>
#include <Core/Block_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <map>

namespace DataLake
{
class ICatalog;
}

namespace DB
{

class FileNamesGenerator;

/// Theta sketches of the distinct values of the written columns, published per snapshot as the
/// `apache-datasketches-theta-v1` blobs of an Iceberg Puffin statistics file.
class IcebergColumnSketches
{
public:
    /// Sketches the columns of `header` whose type has an Iceberg single-value serialization.
    IcebergColumnSketches(const Block & header, const Poco::JSON::Array::Ptr & schema_fields);
    ~IcebergColumnSketches();

    /// `columns` start with the columns of the header, in its order.
    void update(const Columns & columns);

    /// The serialized compact sketch of each column by field id, or nothing if an update failed.
    std::optional<std::map<Int32, String>> finish();

private:
    /// Defined in the source file: the DataSketches headers clash with CRoaring's macros.
    class ColumnSketch;

    std::vector<ColumnSketch> sketches;
    [[maybe_unused]] bool failed = false;
    /// Declared after `sketches`, so its destructor waits for the tasks updating them first.
    ThreadPoolCallbackRunnerLocal<void> runner;
};

/// Whether a commit writes statistics: `iceberg_write_table_statistics_ndv` is on and the commit publishes the metadata
/// file it writes. A transactional catalog commits a list of changes instead, and that list carries no statistics.
bool shouldWriteIcebergTableStatisticsNdv(const ContextPtr & context, const std::shared_ptr<DataLake::ICatalog> & catalog);

/// Writes the Puffin statistics file of `snapshot` and registers it in `metadata`: `column_sketches`, unioned with the
/// parent snapshot's sketches if `union_with_parent`, or the parent's sketches without `column_sketches` (a commit that
/// writes no rows). Writes nothing if the parent is used and has rows but no sketches. Never throws. Returns the
/// storage path of the written file, to remove if the commit fails.
std::optional<String> writeIcebergTableStatisticsNdv(
    IcebergColumnSketches * column_sketches,
    bool union_with_parent,
    Poco::JSON::Object::Ptr metadata,
    const Poco::JSON::Object::Ptr & snapshot,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & resolver,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context);

}

#endif
