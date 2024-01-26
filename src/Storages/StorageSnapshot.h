#pragma once

#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/Streaming/Subscription_fwd.h>

namespace DB
{

class IStorage;

struct StorageSnapshotSettings {
    bool need_to_create_subscription = false;
};

/// Snapshot of storage that fixes set columns that can be read in query.
/// There are 3 sources of columns: regular columns from metadata,
/// dynamic columns from object Types, virtual columns.
struct StorageSnapshot
{
    const IStorage & storage;
    const StorageMetadataPtr metadata;
    const ColumnsDescription object_columns;
    const StorageSnapshotSettings additional_settings;

    /// Additional data, on which set of columns may depend.
    /// E.g. data parts in MergeTree, list of blocks in Memory, etc.
    struct Data
    {
        virtual ~Data() = default;
    };

    using DataPtr = std::unique_ptr<Data>;
    DataPtr data;

    /// Projection that is used in query.
    mutable const ProjectionDescription * projection = nullptr;

    /// Subscription for a streaming query
    /// if the storage supports atomic subscription with snapshot acquisition
    StreamSubscriptionPtr stream_subscription = nullptr;

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        StorageSnapshotSettings additional_settings_ = {},
        StreamSubscriptionPtr stream_subscription_ = nullptr)
        : storage(storage_)
        , metadata(std::move(metadata_))
        , additional_settings(std::move(additional_settings_))
        , stream_subscription(std::move(stream_subscription_))
    {
        init();
    }

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        ColumnsDescription object_columns_,
        StorageSnapshotSettings additional_settings_ = {},
        StreamSubscriptionPtr stream_subscription_ = nullptr)
        : storage(storage_)
        , metadata(std::move(metadata_))
        , object_columns(std::move(object_columns_))
        , additional_settings(std::move(additional_settings_))
        , stream_subscription(std::move(stream_subscription_))
    {
        init();
    }

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        ColumnsDescription object_columns_,
        DataPtr data_,
        StorageSnapshotSettings additional_settings_ = {},
        StreamSubscriptionPtr stream_subscription_ = nullptr)
        : storage(storage_)
        , metadata(std::move(metadata_))
        , object_columns(std::move(object_columns_))
        , additional_settings(std::move(additional_settings_))
        , data(std::move(data_))
        , stream_subscription(std::move(stream_subscription_))
    {
        init();
    }

    std::shared_ptr<StorageSnapshot> clone(DataPtr data_) const;

    /// Get all available columns with types according to options.
    NamesAndTypesList getColumns(const GetColumnsOptions & options) const;

    /// Get columns with types according to options only for requested names.
    NamesAndTypesList getColumnsByNames(const GetColumnsOptions & options, const Names & names) const;

    /// Get column with type according to options for requested name.
    std::optional<NameAndTypePair> tryGetColumn(const GetColumnsOptions & options, const String & column_name) const;
    NameAndTypePair getColumn(const GetColumnsOptions & options, const String & column_name) const;

    /// Block with ordinary + materialized + aliases + virtuals + subcolumns.
    Block getSampleBlockForColumns(const Names & column_names) const;

    ColumnsDescription getDescriptionForColumns(const Names & column_names) const;

    /// returns saved subscription or creates new one - not synchronized with getStorageSnapshot.
    StreamSubscriptionPtr getStreamSubscription() const;

    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names) const;

    DataTypePtr getConcreteType(const String & column_name) const;

    void addProjection(const ProjectionDescription * projection_) const { projection = projection_; }

    /// If we have a projection then we should use its metadata.
    StorageMetadataPtr getMetadataForQuery() const { return projection ? projection->metadata : metadata; }

private:
    void init();

    std::unordered_map<String, DataTypePtr> virtual_columns;

    /// System columns are not visible in the schema but might be persisted in the data.
    /// One example of such column is lightweight delete mask '_row_exists'.
    std::unordered_map<String, DataTypePtr> system_columns;
};

using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

}
