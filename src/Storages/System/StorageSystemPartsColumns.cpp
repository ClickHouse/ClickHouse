#include <Storages/System/StorageSystemPartsColumns.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Common/assert_cast.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Databases/IDatabase.h>

namespace DB
{

namespace
{

/// Appends rows to an `Array` output column directly into its nested column, without building a `Field`.
/// A writer over nullptr stands for a column that is not selected.
template <typename NestedColumn>
class ArrayColumnWriter
{
public:
    explicit ArrayColumnWriter(IColumn * column)
        : array(column ? &assert_cast<ColumnArray &>(*column) : nullptr)
        , data(array ? &assert_cast<NestedColumn &>(array->getData()) : nullptr)
    {
    }

    explicit operator bool() const { return array != nullptr; }

    NestedColumn & getData() { return *data; }

    void insertString(std::string_view value) { data->insertData(value.data(), value.size()); }

    void finishRow()
    {
        if (array)
            array->getOffsets().push_back(data->size());
    }

private:
    ColumnArray * array;
    NestedColumn * data;
};

}

StorageSystemPartsColumns::StorageSystemPartsColumns(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    ColumnsDescription{
        {"partition",                                  std::make_shared<DataTypeString>(), "The partition name."},
        {"name",                                       std::make_shared<DataTypeString>(), "Name of the data part."},
        {"uuid",                                       std::make_shared<DataTypeUUID>(), "The parts UUID."},
        {"part_type",                                  std::make_shared<DataTypeString>(), "The data part storing format. "
            "Possible values: Wide — Each column is stored in a separate file in a filesystem, Compact — All columns are stored in one file in a filesystem."},
        {"active",                                     std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging."},
        {"marks",                                      std::make_shared<DataTypeUInt64>(), "The number of marks. To get the approximate number of rows in a data part, multiply marks by the index granularity (usually 8192) (this hint does not work for adaptive granularity)."},
        {"rows",                                       std::make_shared<DataTypeUInt64>(), "The number of rows."},
        {"bytes_on_disk",                              std::make_shared<DataTypeUInt64>(), "Total size of all the data part files in bytes."},
        {"data_compressed_bytes",                      std::make_shared<DataTypeUInt64>(), "Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"data_uncompressed_bytes",                    std::make_shared<DataTypeUInt64>(), "Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included."},
        {"marks_bytes",                                std::make_shared<DataTypeUInt64>(),"The size of the file with marks."},
        {"modification_time",                          std::make_shared<DataTypeDateTime>(), "The time the directory with the data part was modified. This usually corresponds to the time of data part creation."},
        {"remove_time",                                std::make_shared<DataTypeDateTime>(), "The time when the data part became inactive."},
        {"refcount",                                   std::make_shared<DataTypeUInt32>(), "The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges."},
        {"min_date",                                   std::make_shared<DataTypeDate>(), "The minimum value for the Date column if that is included in the partition key."},
        {"max_date",                                   std::make_shared<DataTypeDate>(), "The maximum value for the Date column if that is included in the partition key."},
        {"min_time",                                   std::make_shared<DataTypeDateTime>(), "The minimum value for the DateTime column if that is included in the partition key."},
        {"max_time",                                   std::make_shared<DataTypeDateTime>(), "The maximum value for the DateTime column if that is included in the partition key."},
        {"partition_id",                               std::make_shared<DataTypeString>(), "ID of the partition."},
        {"min_block_number",                           std::make_shared<DataTypeInt64>(),  "The minimum number of data parts that make up the current part after merging."},
        {"max_block_number",                           std::make_shared<DataTypeInt64>(),  "The maximum number of data parts that make up the current part after merging."},
        {"level",                                      std::make_shared<DataTypeUInt32>(), "Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts."},
        {"data_version",                               std::make_shared<DataTypeUInt64>(), "Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than data_version)."},
        {"primary_key_bytes_in_memory",                std::make_shared<DataTypeUInt64>(), "The amount of memory (in bytes) used by primary key values."},
        {"primary_key_bytes_in_memory_allocated",      std::make_shared<DataTypeUInt64>(), "The amount of memory (in bytes) reserved for primary key values."},

        {"database",                                   std::make_shared<DataTypeString>(), "Name of the database."},
        {"table",                                      std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine",                                     std::make_shared<DataTypeString>(), "Name of the table engine without parameters."},
        {"disk_name",                                  std::make_shared<DataTypeString>(), "Name of a disk that stores the data part."},
        {"path",                                       std::make_shared<DataTypeString>(), "Absolute path to the folder with data part files."},

        {"column",                                     std::make_shared<DataTypeString>(), "Name of the column."},
        {"type",                                       std::make_shared<DataTypeString>(), "Column type."},
        {"column_position",                            std::make_shared<DataTypeUInt64>(), "Ordinal position of a column in a table starting with 1."},
        {"default_kind",                               std::make_shared<DataTypeString>(), "Expression type (DEFAULT, MATERIALIZED, ALIAS) for the default value, or an empty string if it is not defined."},
        {"default_expression",                         std::make_shared<DataTypeString>(), "Expression for the default value, or an empty string if it is not defined."},
        {"column_bytes_on_disk",                       std::make_shared<DataTypeUInt64>(), "Total size of the column in bytes."},
        {"column_data_compressed_bytes",               std::make_shared<DataTypeUInt64>(), "Total size of the compressed data in the column, in bytes."},
        {"column_data_uncompressed_bytes",             std::make_shared<DataTypeUInt64>(), "Total size of the decompressed data in the column, in bytes."},
        {"column_marks_bytes",                         std::make_shared<DataTypeUInt64>(), "The size of the marks for column, in bytes."},
        {"column_modification_time",                   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "The last time the column was modified."},
        {"column_ttl_min",                             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "The minimum value of the calculated TTL expression of the column."},
        {"column_ttl_max",                             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "The maximum value of the calculated TTL expression of the column."},
        {"statistics",                                 std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The statistics of the column."},
        {"estimates.min",                              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Estimated minimum value of the column."},
        {"estimates.max",                              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Estimated maximum value of the column."},
        {"estimates.cardinality",                      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Estimated cardinality of the column."},
        {"estimates.null_count",                       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Estimated number of NULL values in the column."},
        {"estimates.default_count",                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Estimated number of rows equal to the column's storage default value (NULL for Nullable columns, 0 / '' / [] / ... for non-Nullable). NULL when no basic statistic is available."},
        {"serialization_kind",                         std::make_shared<DataTypeString>(), "Kind of serialization of a column"},
        {"substreams",                                 std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Names of substreams to which column is serialized"},
        {"filenames",                                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Names of files for each substream of a column respectively"},
        {"subcolumns.names",                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Names of subcolumns of a column"},
        {"subcolumns.types",                           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Types of subcolumns of a column"},
        {"subcolumns.serializations",                  std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Kinds of serialization of subcolumns of a column"},
        {"subcolumns.bytes_on_disk",                   std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Sizes in bytes for each subcolumn"},
        {"subcolumns.data_compressed_bytes",           std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Sizes of the compressed data for each subcolumn, in bytes"},
        {"subcolumns.data_uncompressed_bytes",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Sizes of the decompressed data for each subcolumn, in bytes"},
        {"subcolumns.marks_bytes",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Sizes of the marks for each subcolumn of a column, in bytes"},
    }
    )
{
}

struct StorageSystemPartsColumns::ColumnsProcessingState : StorageProcessingState
{
    struct ColumnInfo
    {
        String default_kind;
        String default_expression;
    };

    std::unordered_map<String, ColumnInfo> columns_info;
    QueryStatusPtr query_status;
};

StorageSystemPartsBase::StorageProcessingStatePtr StorageSystemPartsColumns::prepareStorage(
    ContextPtr context, const StoragesInfo & info, const std::vector<UInt8> & /*columns_mask*/)
{
    auto state = std::make_unique<ColumnsProcessingState>();
    state->query_status = context->getProcessListElement();

    auto metadata_snapshot = info.storage->getInMemoryMetadataPtr(context, false);
    size_t metadata_column_number = 0;
    for (const auto & column : metadata_snapshot->getColumns())
    {
        /// The prepass alone can take a long time on a table with many columns.
        /// A partially filled `columns_info` would report wrong defaults, so give up the whole storage instead.
        ++metadata_column_number;
        slowDownSystemPartsMetadataEnumeration(info.table, metadata_column_number);
        if (state->query_status && metadata_column_number % COLUMNS_CANCELLATION_CHECK_PERIOD == 0 && !state->query_status->checkTimeLimit())
            return nullptr;

        ColumnsProcessingState::ColumnInfo column_info;
        if (column.default_desc.expression)
        {
            column_info.default_kind = toString(column.default_desc.kind);
            column_info.default_expression = column.default_desc.expression->formatForLogging();
        }

        state->columns_info[column.name] = column_info;
    }

    return state;
}

void StorageSystemPartsColumns::processPart(
    ContextPtr /*context*/,
    MutableColumns & columns,
    const std::vector<UInt8> & columns_mask,
    const StoragesInfo & info,
    const MergeTreeData::DataPartPtr & part,
    MergeTreeData::DataPartState part_state,
    bool has_state_column,
    StorageProcessingState & storage_state)
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemPartsColumns::processPart");

    auto & state = assert_cast<ColumnsProcessingState &>(storage_state);
    const auto & query_status = state.query_status;
    const auto & columns_info = state.columns_info;

    auto columns_size = part->getTotalColumnsSize();

    /// Don't count the reference held by the enumeration in the returned refcount.
    auto use_count = part.use_count() - 1;

    auto min_max_date = part->getMinMaxDate();
    auto min_max_time = part->getMinMaxTime();

    auto index_size_in_bytes = part->getIndexSizeInBytes();
    auto index_size_in_allocated_bytes = part->getIndexSizeInAllocatedBytes();
    std::optional<Estimates> estimates;

    /// Lazy initialize statistics estimates if they are queried.
    auto find_estimate = [&](const auto & column_name)
    {
        if (!estimates.has_value())
            estimates = part->getEstimates();

        return estimates->find(column_name);
    };

    using State = MergeTreeDataPartState;

    /// The rendered text is identical for every column of a part; resolving the key per column
    /// re-enters the uncached patch branch of PartitionKeySamples::get once per column.
    String partition_str;
    if (columns_mask[0])
        partition_str = part->partition.serializeToString(state.partition_key_samples.get(*part));

    const auto data_settings = info.data->getSettings();

    size_t column_position = 0;
    for (const auto & column : part->getColumns())
    {
        ++column_position;
        slowDownSystemPartsColumnsEnumeration(info.table, column_position);
        /// The limit stays exceeded, so the source stops before the next part as well.
        if (query_status && column_position % COLUMNS_CANCELLATION_CHECK_PERIOD == 0 && !query_status->checkTimeLimit())
            return;

        size_t src_index = 0;
        size_t res_index = 0;

        /// Returns the next output column, or nullptr if it is not selected.
        auto next_column = [&]() -> IColumn *
        {
            return columns_mask[src_index++] ? columns[res_index++].get() : nullptr;
        };

        if (columns_mask[src_index++])
            columns[res_index++]->insert(partition_str);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->name);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->uuid);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getTypeName());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part_state == State::Active);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getMarksCount());

        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->rows_count);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getBytesOnDisk());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(columns_size.data_compressed);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(columns_size.data_uncompressed);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(columns_size.marks);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(UInt64(part->modification_time));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(UInt64(part->remove_time.load(std::memory_order_relaxed)));

        if (columns_mask[src_index++])
            columns[res_index++]->insert(UInt64(use_count));

        if (columns_mask[src_index++])
            columns[res_index++]->insert(min_max_date.first);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(min_max_date.second);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt32>(min_max_time.first));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(static_cast<UInt32>(min_max_time.second));

        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.getPartitionId());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.min_block);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.max_block);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->info.level);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(UInt64(part->info.getDataVersion()));
        if (columns_mask[src_index++])
            columns[res_index++]->insert(index_size_in_bytes);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(index_size_in_allocated_bytes);

        if (columns_mask[src_index++])
            columns[res_index++]->insert(info.database);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(info.table);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(info.engine);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(part->getDataPartStorage().getDiskName());
        if (columns_mask[src_index++])
        {
            /// The full path changes at clean up thread, so do not read it if parts can be deleted or renamed, avoid the race.
            if (part_state != State::Deleting && part_state != State::DeleteOnDestroy && part_state != State::Temporary && part_state != State::PreActive)
                columns[res_index++]->insert(part->getDataPartStorage().getFullPath());
            else
                columns[res_index++]->insertDefault();
        }

        if (columns_mask[src_index++])
            columns[res_index++]->insert(column.name);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(column.type->getName());
        if (columns_mask[src_index++])
            columns[res_index++]->insert(column_position);

        auto column_info_it = columns_info.find(column.name);
        if (column_info_it != columns_info.end())
        {
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_info_it->second.default_kind);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column_info_it->second.default_expression);
        }
        else
        {
            if (columns_mask[src_index++])
                columns[res_index++]->insertDefault();
            if (columns_mask[src_index++])
                columns[res_index++]->insertDefault();
        }

        ColumnSize column_size;
        if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2] || columns_mask[src_index + 3])
            column_size = part->getColumnSize(column.name);

        if (columns_mask[src_index++])
            columns[res_index++]->insert(column_size.data_compressed + column_size.marks);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(column_size.data_compressed);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(column_size.data_uncompressed);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(column_size.marks);
        if (columns_mask[src_index++])
        {
            if (auto column_modification_time = part->getColumnModificationTime(column.name))
                columns[res_index++]->insert(UInt64(column_modification_time.value()));
            else
                columns[res_index++]->insertDefault();
        }
        bool column_has_ttl = part->ttl_infos.columns_ttl.contains(column.name);
        if (columns_mask[src_index++])
        {
            if (column_has_ttl)
                columns[res_index++]->insert(static_cast<UInt32>(part->ttl_infos.columns_ttl[column.name].min));
            else
                columns[res_index++]->insertDefault();
        }
        if (columns_mask[src_index++])
        {
            if (column_has_ttl)
                columns[res_index++]->insert(static_cast<UInt32>(part->ttl_infos.columns_ttl[column.name].max));
            else
                columns[res_index++]->insertDefault();
        }

        if (ArrayColumnWriter<ColumnString> statistics(next_column()); statistics)
        {
            auto estimate_it = find_estimate(column.name);
            if (estimate_it != estimates->end())
            {
                for (const auto & type : estimate_it->second.types)
                    statistics.insertString(toString(type));
            }

            statistics.finishRow();
        }

        if (columns_mask[src_index++])
        {
            auto estimate_it = find_estimate(column.name);
            if (estimate_it != estimates->end() && estimate_it->second.estimated_min.has_value())
                columns[res_index++]->insert(applyVisitor(FieldVisitorToString(), estimate_it->second.estimated_min.value()));
            else
                columns[res_index++]->insertDefault();
        }

        if (columns_mask[src_index++])
        {
            auto estimate_it = find_estimate(column.name);
            if (estimate_it != estimates->end() && estimate_it->second.estimated_max.has_value())
                columns[res_index++]->insert(applyVisitor(FieldVisitorToString(), estimate_it->second.estimated_max.value()));
            else
                columns[res_index++]->insertDefault();
        }

        if (columns_mask[src_index++])
        {
            auto estimate_it = find_estimate(column.name);
            if (estimate_it != estimates->end() && estimate_it->second.estimated_cardinality.has_value())
                columns[res_index++]->insert(estimate_it->second.estimated_cardinality.value());
            else
                columns[res_index++]->insertDefault();
        }

        if (columns_mask[src_index++])
        {
            auto estimate_it = find_estimate(column.name);
            if (estimate_it != estimates->end() && estimate_it->second.estimated_null_count.has_value())
                columns[res_index++]->insert(estimate_it->second.estimated_null_count.value());
            else
                columns[res_index++]->insertDefault();
        }

        if (columns_mask[src_index++])
        {
            auto estimate_it = find_estimate(column.name);
            if (estimate_it != estimates->end() && estimate_it->second.estimated_default_count.has_value())
                columns[res_index++]->insert(estimate_it->second.estimated_default_count.value());
            else
                columns[res_index++]->insertDefault();
        }

        auto serialization = part->getSerialization(column.name);
        if (columns_mask[src_index++])
            columns[res_index++]->insert(ISerialization::kindStackToString(serialization->getKindStack()));

        ArrayColumnWriter<ColumnString> substreams(next_column());
        ArrayColumnWriter<ColumnString> filenames(next_column());

        if (substreams || filenames)
        {
            if (column.type->hasDynamicSubcolumns() && !part->getColumnsSubstreams().empty())
            {
                for (const auto & substream : part->getColumnsSubstreams().getColumnSubstreams(column_position - 1))
                {
                    if (substreams)
                        substreams.insertString(substream);
                    if (filenames)
                        filenames.insertString(IMergeTreeDataPart::getStreamNameOrHash(substream, ".bin", part->checksums).value_or(""));
                }
            }
            else
            {
                ISerialization::StreamFileNameSettings stream_file_name_settings(*data_settings);
                serialization->enumerateStreams([&](const auto & subpath)
                {
                    if (substreams)
                        substreams.insertString(ISerialization::getFileNameForStream(column.name, subpath, stream_file_name_settings));
                    if (filenames)
                        filenames.insertString(IMergeTreeDataPart::getStreamNameForColumn(column.name, subpath, ".bin", part->checksums, data_settings).value_or(""));
                });
            }

            substreams.finishRow();
            filenames.finishRow();
        }

        ArrayColumnWriter<ColumnString> subcolumn_names(next_column());
        ArrayColumnWriter<ColumnString> subcolumn_types(next_column());
        ArrayColumnWriter<ColumnString> subcolumn_serializations(next_column());
        ArrayColumnWriter<ColumnUInt64> subcolumn_bytes_on_disk(next_column());
        ArrayColumnWriter<ColumnUInt64> subcolumn_data_compressed_bytes(next_column());
        ArrayColumnWriter<ColumnUInt64> subcolumn_data_uncompressed_bytes(next_column());
        ArrayColumnWriter<ColumnUInt64> subcolumn_marks_bytes(next_column());

        const bool need_subcolumn_sizes = subcolumn_bytes_on_disk || subcolumn_data_compressed_bytes || subcolumn_data_uncompressed_bytes || subcolumn_marks_bytes;

        if (subcolumn_names || subcolumn_types || subcolumn_serializations || need_subcolumn_sizes)
        {
            IDataType::forEachSubcolumn([&](const auto & subpath, const auto & name, const auto & data)
            {
                /// We count only final subcolumns, which are represented by files on disk
                /// and skip intermediate subcolumns of types Tuple and Nested.
                if (isTuple(data.type) || isNested(data.type))
                    return;

                /// Skip ephemeral subcolumns that don't store any real data.
                if (ISerialization::isEphemeralSubcolumn(subpath, subpath.size()))
                    return;

                if (subcolumn_names)
                    subcolumn_names.insertString(name);
                if (subcolumn_types)
                    subcolumn_types.insertString(data.type->getName());
                if (subcolumn_serializations)
                    subcolumn_serializations.insertString(ISerialization::kindStackToString(data.serialization->getKindStack()));

                if (!need_subcolumn_sizes)
                    return;

                ColumnSize size;
                NameAndTypePair subcolumn(column.name, name, column.type, data.type);

                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(subcolumn, subpath, ".bin", part->checksums, data_settings);
                if (stream_name)
                {
                    auto bin_checksum = part->checksums.files.find(*stream_name + ".bin");
                    if (bin_checksum != part->checksums.files.end())
                    {
                        size.data_compressed += bin_checksum->second.file_size;
                        size.data_uncompressed += bin_checksum->second.uncompressed_size;
                    }

                    auto mrk_checksum = part->checksums.files.find(*stream_name + part->index_granularity_info.mark_type.getFileExtension());
                    if (mrk_checksum != part->checksums.files.end())
                        size.marks += mrk_checksum->second.file_size;
                }

                if (subcolumn_bytes_on_disk)
                    subcolumn_bytes_on_disk.getData().insertValue(size.data_compressed + size.marks);
                if (subcolumn_data_compressed_bytes)
                    subcolumn_data_compressed_bytes.getData().insertValue(size.data_compressed);
                if (subcolumn_data_uncompressed_bytes)
                    subcolumn_data_uncompressed_bytes.getData().insertValue(size.data_uncompressed);
                if (subcolumn_marks_bytes)
                    subcolumn_marks_bytes.getData().insertValue(size.marks);

            }, ISerialization::SubstreamData(serialization).withType(column.type));

            subcolumn_names.finishRow();
            subcolumn_types.finishRow();
            subcolumn_serializations.finishRow();
            subcolumn_bytes_on_disk.finishRow();
            subcolumn_data_compressed_bytes.finishRow();
            subcolumn_data_uncompressed_bytes.finishRow();
            subcolumn_marks_bytes.finishRow();
        }

        if (has_state_column)
            columns[res_index++]->insert(part->stateString());
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemPartsColumns) }
