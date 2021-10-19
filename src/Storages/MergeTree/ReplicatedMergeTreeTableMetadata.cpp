#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_MISMATCH;
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return "";
    WriteBufferFromOwnString buf;
    formatAST(*ast, buf, false, true);
    return buf.str();
}

ReplicatedMergeTreeTableMetadata::ReplicatedMergeTreeTableMetadata(const MergeTreeData & data, const StorageMetadataPtr & metadata_snapshot)
{
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        auto minmax_idx_column_names = data.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey());
        date_column = minmax_idx_column_names[data.minmax_idx_date_column_pos];
    }

    const auto data_settings = data.getSettings();
    sampling_expression = formattedAST(metadata_snapshot->getSamplingKeyAST());
    index_granularity = data_settings->index_granularity;
    merging_params_mode = static_cast<int>(data.merging_params.mode);
    sign_column = data.merging_params.sign_column;

    /// This code may looks strange, but previously we had only one entity: PRIMARY KEY (or ORDER BY, it doesn't matter)
    /// Now we have two different entities ORDER BY and it's optional prefix -- PRIMARY KEY.
    /// In most cases user doesn't specify PRIMARY KEY and semantically it's equal to ORDER BY.
    /// So rules in zookeeper metadata is following:
    /// - When we have only ORDER BY, than store it in "primary key:" row of /metadata
    /// - When we have both, than store PRIMARY KEY in "primary key:" row and ORDER BY in "sorting key:" row of /metadata

    primary_key = formattedAST(metadata_snapshot->getPrimaryKey().expression_list_ast);
    if (metadata_snapshot->isPrimaryKeyDefined())
    {
        /// We don't use preparsed AST `sorting_key.expression_list_ast` because
        /// it contain version column for VersionedCollapsingMergeTree, which
        /// is not stored in ZooKeeper for compatibility reasons. So the best
        /// compatible way is just to convert definition_ast to list and
        /// serialize it. In all other places key.expression_list_ast should be
        /// used.
        sorting_key = formattedAST(extractKeyExpressionList(metadata_snapshot->getSortingKey().definition_ast));
    }

    data_format_version = data.format_version;

    if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        partition_key = formattedAST(metadata_snapshot->getPartitionKey().expression_list_ast);

    ttl_table = formattedAST(metadata_snapshot->getTableTTLs().definition_ast);

    skip_indices = metadata_snapshot->getSecondaryIndices().toString();

    projections = metadata_snapshot->getProjections().toString();

    if (data.canUseAdaptiveGranularity())
        index_granularity_bytes = data_settings->index_granularity_bytes;
    else
        index_granularity_bytes = 0;

    constraints = metadata_snapshot->getConstraints().toString();
}

void ReplicatedMergeTreeTableMetadata::write(WriteBuffer & out) const
{
    out << "metadata format version: 1\n"
        << "date column: " << date_column << "\n"
        << "sampling expression: " << sampling_expression << "\n"
        << "index granularity: " << index_granularity << "\n"
        << "mode: " << merging_params_mode << "\n"
        << "sign column: " << sign_column << "\n"
        << "primary key: " << primary_key << "\n";

    if (data_format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        out << "data format version: " << data_format_version.toUnderType() << "\n"
            << "partition key: " << partition_key << "\n";
    }

    if (!sorting_key.empty())
        out << "sorting key: " << sorting_key << "\n";

    if (!ttl_table.empty())
        out << "ttl: " << ttl_table << "\n";

    if (!skip_indices.empty())
        out << "indices: " << skip_indices << "\n";

    if (!projections.empty())
        out << "projections: " << projections << "\n";

    if (index_granularity_bytes != 0)
        out << "granularity bytes: " << index_granularity_bytes << "\n";

    if (!constraints.empty())
        out << "constraints: " << constraints << "\n";
}

String ReplicatedMergeTreeTableMetadata::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

void ReplicatedMergeTreeTableMetadata::read(ReadBuffer & in)
{
    in >> "metadata format version: 1\n";
    in >> "date column: " >> date_column >> "\n";
    in >> "sampling expression: " >> sampling_expression >> "\n";
    in >> "index granularity: " >> index_granularity >> "\n";
    in >> "mode: " >> merging_params_mode >> "\n";
    in >> "sign column: " >> sign_column >> "\n";
    in >> "primary key: " >> primary_key >> "\n";

    if (in.eof())
        data_format_version = 0;
    else if (checkString("data format version: ", in))
        in >> data_format_version.toUnderType() >> "\n";

    if (data_format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        in >> "partition key: " >> partition_key >> "\n";

    if (checkString("sorting key: ", in))
        in >> sorting_key >> "\n";

    if (checkString("ttl: ", in))
        in >> ttl_table >> "\n";

    if (checkString("indices: ", in))
        in >> skip_indices >> "\n";

    if (checkString("projections: ", in))
        in >> projections >> "\n";

    if (checkString("granularity bytes: ", in))
    {
        in >> index_granularity_bytes >> "\n";
        index_granularity_bytes_found_in_zk = true;
    }
    else
        index_granularity_bytes = 0;

    if (checkString("constraints: ", in))
        in >> constraints >> "\n";
}

ReplicatedMergeTreeTableMetadata ReplicatedMergeTreeTableMetadata::parse(const String & s)
{
    ReplicatedMergeTreeTableMetadata metadata;
    ReadBufferFromString buf(s);
    metadata.read(buf);
    return metadata;
}


void ReplicatedMergeTreeTableMetadata::checkImmutableFieldsEquals(const ReplicatedMergeTreeTableMetadata & from_zk) const
{
    if (data_format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        if (date_column != from_zk.date_column)
            throw Exception("Existing table metadata in ZooKeeper differs in date index column."
                " Stored in ZooKeeper: " + from_zk.date_column + ", local: " + date_column,
                ErrorCodes::METADATA_MISMATCH);
    }
    else if (!from_zk.date_column.empty())
    {
        throw Exception(
            "Existing table metadata in ZooKeeper differs in date index column."
            " Stored in ZooKeeper: " + from_zk.date_column + ", local is custom-partitioned.",
            ErrorCodes::METADATA_MISMATCH);
    }

    if (index_granularity != from_zk.index_granularity)
        throw Exception("Existing table metadata in ZooKeeper differs in index granularity."
            " Stored in ZooKeeper: " + DB::toString(from_zk.index_granularity) + ", local: " + DB::toString(index_granularity),
            ErrorCodes::METADATA_MISMATCH);

    if (merging_params_mode != from_zk.merging_params_mode)
        throw Exception("Existing table metadata in ZooKeeper differs in mode of merge operation."
            " Stored in ZooKeeper: " + DB::toString(from_zk.merging_params_mode) + ", local: "
            + DB::toString(merging_params_mode),
            ErrorCodes::METADATA_MISMATCH);

    if (sign_column != from_zk.sign_column)
        throw Exception("Existing table metadata in ZooKeeper differs in sign column."
            " Stored in ZooKeeper: " + from_zk.sign_column + ", local: " + sign_column,
            ErrorCodes::METADATA_MISMATCH);

    /// NOTE: You can make a less strict check of match expressions so that tables do not break from small changes
    ///    in formatAST code.
    if (primary_key != from_zk.primary_key)
        throw Exception("Existing table metadata in ZooKeeper differs in primary key."
            " Stored in ZooKeeper: " + from_zk.primary_key + ", local: " + primary_key,
            ErrorCodes::METADATA_MISMATCH);

    if (data_format_version != from_zk.data_format_version)
        throw Exception("Existing table metadata in ZooKeeper differs in data format version."
            " Stored in ZooKeeper: " + DB::toString(from_zk.data_format_version.toUnderType()) +
            ", local: " + DB::toString(data_format_version.toUnderType()),
            ErrorCodes::METADATA_MISMATCH);

    if (partition_key != from_zk.partition_key)
        throw Exception(
            "Existing table metadata in ZooKeeper differs in partition key expression."
            " Stored in ZooKeeper: " + from_zk.partition_key + ", local: " + partition_key,
            ErrorCodes::METADATA_MISMATCH);

}

void ReplicatedMergeTreeTableMetadata::checkEquals(const ReplicatedMergeTreeTableMetadata & from_zk, const ColumnsDescription & columns, ContextPtr context) const
{

    checkImmutableFieldsEquals(from_zk);

    if (sampling_expression != from_zk.sampling_expression)
        throw Exception("Existing table metadata in ZooKeeper differs in sample expression."
            " Stored in ZooKeeper: " + from_zk.sampling_expression + ", local: " + sampling_expression,
            ErrorCodes::METADATA_MISMATCH);

    if (sorting_key != from_zk.sorting_key)
    {
        throw Exception(
            "Existing table metadata in ZooKeeper differs in sorting key expression."
            " Stored in ZooKeeper: " + from_zk.sorting_key + ", local: " + sorting_key,
            ErrorCodes::METADATA_MISMATCH);
    }

    if (ttl_table != from_zk.ttl_table)
    {
        throw Exception(
                "Existing table metadata in ZooKeeper differs in TTL."
                " Stored in ZooKeeper: " + from_zk.ttl_table +
                ", local: " + ttl_table,
                ErrorCodes::METADATA_MISMATCH);
    }

    String parsed_zk_skip_indices = IndicesDescription::parse(from_zk.skip_indices, columns, context).toString();
    if (skip_indices != parsed_zk_skip_indices)
    {
        throw Exception(
                "Existing table metadata in ZooKeeper differs in skip indexes."
                " Stored in ZooKeeper: " + from_zk.skip_indices +
                ", parsed from ZooKeeper: " + parsed_zk_skip_indices +
                ", local: " + skip_indices,
                ErrorCodes::METADATA_MISMATCH);
    }

    String parsed_zk_projections = ProjectionsDescription::parse(from_zk.projections, columns, context).toString();
    if (projections != parsed_zk_projections)
    {
        throw Exception(
                "Existing table metadata in ZooKeeper differs in projections."
                " Stored in ZooKeeper: " + from_zk.projections +
                ", parsed from ZooKeeper: " + parsed_zk_projections +
                ", local: " + projections,
                ErrorCodes::METADATA_MISMATCH);
    }

    String parsed_zk_constraints = ConstraintsDescription::parse(from_zk.constraints).toString();
    if (constraints != parsed_zk_constraints)
    {
        throw Exception(
                "Existing table metadata in ZooKeeper differs in constraints."
                " Stored in ZooKeeper: " + from_zk.constraints +
                ", parsed from ZooKeeper: " + parsed_zk_constraints +
                ", local: " + constraints,
                       ErrorCodes::METADATA_MISMATCH);
    }

    if (from_zk.index_granularity_bytes_found_in_zk && index_granularity_bytes != from_zk.index_granularity_bytes)
        throw Exception("Existing table metadata in ZooKeeper differs in index granularity bytes."
            " Stored in ZooKeeper: " + DB::toString(from_zk.index_granularity_bytes) +
            ", local: " + DB::toString(index_granularity_bytes),
            ErrorCodes::METADATA_MISMATCH);
}

ReplicatedMergeTreeTableMetadata::Diff
ReplicatedMergeTreeTableMetadata::checkAndFindDiff(const ReplicatedMergeTreeTableMetadata & from_zk) const
{

    checkImmutableFieldsEquals(from_zk);

    Diff diff;

    if (sorting_key != from_zk.sorting_key)
    {
        diff.sorting_key_changed = true;
        diff.new_sorting_key = from_zk.sorting_key;
    }

    if (sampling_expression != from_zk.sampling_expression)
    {
        diff.sampling_expression_changed = true;
        diff.new_sampling_expression = from_zk.sampling_expression;
    }

    if (ttl_table != from_zk.ttl_table)
    {
        diff.ttl_table_changed = true;
        diff.new_ttl_table = from_zk.ttl_table;
    }

    if (skip_indices != from_zk.skip_indices)
    {
        diff.skip_indices_changed = true;
        diff.new_skip_indices = from_zk.skip_indices;
    }

    if (projections != from_zk.projections)
    {
        diff.projections_changed = true;
        diff.new_projections = from_zk.projections;
    }

    if (constraints != from_zk.constraints)
    {
        diff.constraints_changed = true;
        diff.new_constraints = from_zk.constraints;
    }

    return diff;
}

}
