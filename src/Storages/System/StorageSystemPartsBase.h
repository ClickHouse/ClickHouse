#pragma once

#include <Formats/FormatSettings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Context;

struct StoragesInfo
{
    StoragePtr storage = nullptr;
    TableLockHolder table_lock;

    String database;
    String table;
    String engine;

    bool need_inactive_parts = false;
    MergeTreeData * data = nullptr;

    explicit operator bool() const { return storage != nullptr; }

    MergeTreeData::DataPartsVector getParts(MergeTreeData::DataPartStateVector & state, bool has_state_column) const;
    MergeTreeData::ProjectionPartsVector getProjectionParts(MergeTreeData::DataPartStateVector & state, bool has_state_column) const;
};

/** A helper class that enumerates the storages that match given query. */
class StoragesInfoStreamBase
{
public:
    StoragesInfoStreamBase(ContextPtr context)
        : query_id(context->getCurrentQueryId()), settings(context->getSettingsRef()), next_row(0), rows(0)
    {}

    StoragesInfoStreamBase(const StoragesInfoStreamBase&) = default;
    virtual ~StoragesInfoStreamBase() = default;

    StoragesInfo next()
    {
        while (next_row < rows)
        {
            StoragesInfo info;

            info.database = (*database_column)[next_row].get<String>();
            info.table = (*table_column)[next_row].get<String>();
            UUID storage_uuid = (*storage_uuid_column)[next_row].get<UUID>();

            auto is_same_table = [&storage_uuid, this] (size_t row) -> bool
            {
                return (*storage_uuid_column)[row].get<UUID>() == storage_uuid;
            };

            /// We may have two rows per table which differ in 'active' value.
            /// If rows with 'active = 0' were not filtered out, this means we
            /// must collect the inactive parts. Remember this fact in StoragesInfo.
            for (; next_row < rows && is_same_table(next_row); ++next_row)
            {
                const auto active = (*active_column)[next_row].get<UInt64>();
                if (active == 0)
                    info.need_inactive_parts = true;
            }

            info.storage = storages.at(storage_uuid);

            /// For table not to be dropped and set of columns to remain constant.
            if (!tryLockTable(info))
                continue;

            info.engine = info.storage->getName();

            info.data = dynamic_cast<MergeTreeData *>(info.storage.get());
            if (!info.data)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown engine {}", info.engine);

            return info;
        }

        return {};
    }
protected:
    virtual bool tryLockTable(StoragesInfo & info)
    {
        info.table_lock = info.storage->tryLockForShare(query_id, settings.lock_acquire_timeout);
        // nullptr means table was dropped while acquiring the lock
        return info.table_lock != nullptr;
    }
protected:
    String query_id;
    Settings settings;


    ColumnPtr database_column;
    ColumnPtr table_column;
    ColumnPtr active_column;
    ColumnPtr storage_uuid_column;

    size_t next_row;
    size_t rows;

    using StoragesMap = std::unordered_map<UUID, StoragePtr>;
    StoragesMap storages;
};


class StoragesInfoStream : public StoragesInfoStreamBase
{
public:
    StoragesInfoStream(const SelectQueryInfo & query_info, ContextPtr context);
};

/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemPartsBase : public IStorage
{
public:
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    bool isSystemStorage() const override { return true; }

private:
    static bool hasStateColumn(const Names & column_names, const StorageSnapshotPtr & storage_snapshot);

protected:
    const FormatSettings format_settings = {};

    StorageSystemPartsBase(const StorageID & table_id_, NamesAndTypesList && columns_);

    virtual std::unique_ptr<StoragesInfoStreamBase> getStoragesInfoStream(const SelectQueryInfo & query_info, ContextPtr context)
    {
        return std::make_unique<StoragesInfoStream>(query_info, context);
    }

    virtual void
    processNextStorage(ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) = 0;
};

}
