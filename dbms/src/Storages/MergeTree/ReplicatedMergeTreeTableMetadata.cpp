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
    std::stringstream ss;
    formatAST(*ast, ss, false, true);
    return ss.str();
}

ReplicatedMergeTreeTableMetadata::ReplicatedMergeTreeTableMetadata(const MergeTreeData & data)
{
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        date_column = data.minmax_idx_columns[data.minmax_idx_date_column_pos];

    sampling_expression = formattedAST(data.sample_by_ast);
    index_granularity = data.index_granularity_info.fixed_index_granularity;
    merging_params_mode = static_cast<int>(data.merging_params.mode);
    sign_column = data.merging_params.sign_column;

    if (!data.primary_key_ast)
        primary_key = formattedAST(MergeTreeData::extractKeyExpressionList(data.order_by_ast));
    else
    {
        primary_key = formattedAST(MergeTreeData::extractKeyExpressionList(data.primary_key_ast));
        sorting_key = formattedAST(MergeTreeData::extractKeyExpressionList(data.order_by_ast));
    }

    data_format_version = data.format_version;

    if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        partition_key = formattedAST(MergeTreeData::extractKeyExpressionList(data.partition_by_ast));

    skip_indices = data.getIndices().toString();
    index_granularity_bytes = data.index_granularity_info.index_granularity_bytes;
    ttl_table = formattedAST(data.ttl_table_ast);
}

void ReplicatedMergeTreeTableMetadata::write(WriteBuffer & out) const
{
    out << "metadata format version: 1" << "\n"
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

    if (index_granularity_bytes != 0)
        out << "granularity bytes: " << index_granularity_bytes << "\n";
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
    else
        in >> "data format version: " >> data_format_version.toUnderType() >> "\n";

    if (data_format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        in >> "partition key: " >> partition_key >> "\n";

    if (checkString("sorting key: ", in))
        in >> sorting_key >> "\n";

    if (checkString("indices: ", in))
        in >> skip_indices >> "\n";

    if (checkString("granularity bytes: ", in))
        in >> index_granularity_bytes >> "\n";
    else
        index_granularity_bytes = 0;

    if (checkString("ttl: ", in))
        in >> ttl_table >> "\n";
}

ReplicatedMergeTreeTableMetadata ReplicatedMergeTreeTableMetadata::parse(const String & s)
{
    ReplicatedMergeTreeTableMetadata metadata;
    ReadBufferFromString buf(s);
    metadata.read(buf);
    return metadata;
}

ReplicatedMergeTreeTableMetadata::Diff
ReplicatedMergeTreeTableMetadata::checkAndFindDiff(const ReplicatedMergeTreeTableMetadata & from_zk, bool allow_alter) const
{
    Diff diff;

    if (data_format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        if (date_column != from_zk.date_column)
            throw Exception("Existing table metadata in ZooKeeper differs in date index column."
                " Stored in ZooKeeper: " + from_zk.date_column + ", local: " + date_column,
                ErrorCodes::METADATA_MISMATCH);
    }
    else if (!from_zk.date_column.empty())
        throw Exception(
            "Existing table metadata in ZooKeeper differs in date index column."
            " Stored in ZooKeeper: " + from_zk.date_column + ", local is custom-partitioned.",
            ErrorCodes::METADATA_MISMATCH);

    if (sampling_expression != from_zk.sampling_expression)
        throw Exception("Existing table metadata in ZooKeeper differs in sample expression."
            " Stored in ZooKeeper: " + from_zk.sampling_expression + ", local: " + sampling_expression,
            ErrorCodes::METADATA_MISMATCH);

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

    if (sorting_key != from_zk.sorting_key)
    {
        if (allow_alter)
        {
            diff.sorting_key_changed = true;
            diff.new_sorting_key = from_zk.sorting_key;
        }
        else
            throw Exception(
                "Existing table metadata in ZooKeeper differs in sorting key expression."
                " Stored in ZooKeeper: " + from_zk.sorting_key + ", local: " + sorting_key,
                ErrorCodes::METADATA_MISMATCH);
    }

    if (ttl_table != from_zk.ttl_table)
    {
        if (allow_alter)
        {
            diff.ttl_table_changed = true;
            diff.new_ttl_table = from_zk.ttl_table;
        }
        else
            throw Exception(
                    "Existing table metadata in ZooKeeper differs in ttl."
                    " Stored in ZooKeeper: " + from_zk.ttl_table +
                    ", local: " + ttl_table,
                    ErrorCodes::METADATA_MISMATCH);
    }

    if (skip_indices != from_zk.skip_indices)
    {
        if (allow_alter)
        {
            diff.skip_indices_changed = true;
            diff.new_skip_indices = from_zk.skip_indices;
        }
        else
            throw Exception(
                    "Existing table metadata in ZooKeeper differs in skip indexes."
                    " Stored in ZooKeeper: " + from_zk.skip_indices +
                    ", local: " + skip_indices,
                    ErrorCodes::METADATA_MISMATCH);
    }

    if (index_granularity_bytes != from_zk.index_granularity_bytes)
        throw Exception("Existing table metadata in ZooKeeper differs in index granularity bytes."
            " Stored in ZooKeeper: " + DB::toString(from_zk.index_granularity_bytes) +
            ", local: " + DB::toString(index_granularity_bytes),
            ErrorCodes::METADATA_MISMATCH);

    return diff;
}

}
