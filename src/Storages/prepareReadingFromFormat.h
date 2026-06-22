#pragma once
#include <Core/Block.h>
#include <Storages/StorageSnapshot.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/Serialization.h>

namespace DB
{
    struct FormatSettings;

    struct PrewhereInfo;
    using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

    struct FilterDAGInfo;
    using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

    class ActionsDAG;

    struct ReadFromFormatInfo
    {
        /// Header that will return Source from storage.
        /// Concatenation of requested_columns and requested_virtual_columns.
        Block source_header;
        /// Header that will be passed to IInputFormat to read data from file.
        /// Superset of requested_columns.
        /// It can contain more columns than were requested if format doesn't support
        /// reading subset of columns.
        Block format_header;
        /// Columns that are read by `IInputFormat` only to evaluate `PREWHERE`/row-level filters.
        /// They are not exposed in `source_header`.
        Block format_filter_input_header;
        /// Description of columns for format_header. Used for inserting defaults.
        ColumnsDescription columns_description;
        /// Columns to request from IInputFormat.
        /// Includes columns to read from file and columns produced by the prewhere expression
        /// (the prewhere columns are not necessarily at the end of the list).
        /// Doesn't include columns that are only used as prewhere input.
        NamesAndTypesList requested_columns;
        /// The list of requested virtual columns.
        NamesAndTypesList requested_virtual_columns;
        /// Hints for the serialization of columns.
        /// For example can be retrieved from the destination table in INSERT SELECT query.
        SerializationInfoByName serialization_hints{{}};

        void serialize(IQueryPlanStep::Serialization & ctx) const;
        static ReadFromFormatInfo deserialize(IQueryPlanStep::Deserialization & ctx);

        /// The list of hive partition columns. It shall be read from the path regardless if it is present in the file
        NamesAndTypesList hive_partition_columns_to_read_from_file_path;
        PrewhereInfoPtr prewhere_info;
        FilterDAGInfoPtr row_level_filter;
    };

    /// Inputs reachable by storage-level pushdown consumers. Pass to splitFilterDagForAllowedInputs to drop IN-subqueries which can't be used.
    Block buildAllowedFilterInputs(
        const StorageSnapshotPtr & storage_snapshot,
        const Block & source_header,
        const PrewhereInfoPtr & prewhere_info,
        const FilterDAGInfoPtr & row_level_filter);

    /// Eagerly materialise IN-subquery sets that a format-level KeyCondition can consume.
    void prepareEagerKeyConditionSets(
        const std::shared_ptr<const ActionsDAG> & filter_actions_dag,
        const StorageSnapshotPtr & storage_snapshot,
        const Block & source_header,
        const PrewhereInfoPtr & prewhere_info,
        const FilterDAGInfoPtr & row_level_filter,
        const ContextPtr & context);

    struct PrepareReadingFromFormatHiveParams
    {
        /// Columns which exist inside data file.
        NamesAndTypesList file_columns;
        /// Columns which are read from path to data file.
        /// (Hive partition columns).
        std::unordered_map<std::string, DataTypePtr> hive_partition_columns_to_read_from_file_path_map;
    };

    /// Get all needed information for reading from data in some input format.
    ///
    /// `supports_tuple_elements` controls whether the format can read some subcolumns directly.
    /// If true, we'll request tuple elements such as `t.x`, and fixed `JSON`/`Dynamic` subcolumns
    /// when the type declares them. Unsupported subcolumns still fall back to the storage column
    /// and are extracted by `ExtractColumnsTransform` later in the pipeline.
    /// Note: currently `supports_tuple_elements` just means `ParquetV3BlockInputFormat`.
    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements = false,
        const PrepareReadingFromFormatHiveParams & hive_parameters = {});

    /// Returns columns_to_read from file.
    Names filterTupleColumnsToRead(NamesAndTypesList & requested_columns);

    /// Returns columns that file-like storages can use in `PREWHERE` for this exact format.
    /// Top-level columns come from metadata. Format-readable subcolumns are added only for
    /// formats whose reader can consume the corresponding subpath directly.
    NameSet getSupportedPrewhereColumnsForFormat(
        const StorageMetadataPtr & metadata_snapshot,
        const ContextPtr & context,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const NamesAndTypesList & exclude);

    ReadFromFormatInfo updateFormatPrewhereInfo(const ReadFromFormatInfo & info, const FilterDAGInfoPtr & row_level_filter, const PrewhereInfoPtr & prewhere_info);

    /// Returns the serialization hints from the insertion table (if it's set in the Context).
    SerializationInfoByName getSerializationHintsForFileLikeStorage(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

    /// Clamp the user-controlled max_streams_for_files_processing_in_cluster_functions setting to the
    /// same ceiling max_threads gets. The value flows into both num_streams and max_num_streams of the
    /// *Cluster read steps and drives pipes.reserve()/pipe.resize(); unbounded it can overflow the pipe
    /// vector (std::length_error) or exhaust memory.
    size_t clampClusterFunctionNumStreams(UInt64 num_streams);
}
