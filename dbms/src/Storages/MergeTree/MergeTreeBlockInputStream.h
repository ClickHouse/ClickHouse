#pragma once
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeThreadBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>

namespace DB
{


/// Used to read data from single part.
/// To read data from multiple parts, a Storage creates multiple such objects.
class MergeTreeBlockInputStream : public MergeTreeBaseBlockInputStream
{
public:
    MergeTreeBlockInputStream(
        MergeTreeData & storage,
        const MergeTreeData::DataPartPtr & owned_data_part,
        size_t max_block_size_rows,
        size_t preferred_block_size_bytes,
        Names column_names,
        const MarkRanges & mark_ranges,
        bool use_uncompressed_cache,
        ExpressionActionsPtr prewhere_actions,
        String prewhere_column,
        bool check_columns,
        size_t min_bytes_to_use_direct_io,
        size_t max_read_buffer_size,
        bool save_marks_in_cache,
        const Names & virt_column_names = {},
        size_t part_index_in_query = 0,
        bool quiet = false);

    ~MergeTreeBlockInputStream() override;

    String getName() const override { return "MergeTree"; }

    String getID() const override;

protected:

    Block readImpl() override;

    bool getNewTask() override;

private:

    Names ordered_names;
    NameSet column_name_set;
    NamesAndTypesList columns;
    NamesAndTypesList pre_columns;

    MergeTreeData::DataPartPtr data_part;    /// Кусок не будет удалён, пока им владеет этот объект.
    std::unique_ptr<Poco::ScopedReadRWLock> part_columns_lock; /// Не дадим изменить список столбцов куска, пока мы из него читаем.
    MarkRanges all_mark_ranges; /// В каких диапазонах засечек читать. В порядке возрастания номеров.
    size_t part_index_in_query = 0;

    bool check_columns;
    String path;
    bool is_first_task = true;
};

}
