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

ReplicatedMergeTreeTableMetadata::ReplicatedMergeTreeTableMetadata(const MergeTreeData & data_)
    : data(data_)
    , sorting_and_primary_keys_independent(data.sorting_and_primary_keys_independent)
    , sorting_key_str(serializeAST(*data.sorting_key_ast))
{}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return "";
    std::stringstream ss;
    formatAST(*ast, ss, false, true);
    return ss.str();
}

void ReplicatedMergeTreeTableMetadata::write(WriteBuffer & out) const
{
    out << "metadata format version: 1" << "\n"
        << "date column: ";

    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        out << data.minmax_idx_columns[data.minmax_idx_date_column_pos] << "\n";
    else
        out << "\n";

    out << "sampling expression: " << formattedAST(data.sampling_expression) << "\n"
        << "index granularity: " << data.index_granularity << "\n"
        << "mode: " << static_cast<int>(data.merging_params.mode) << "\n"
        << "sign column: " << data.merging_params.sign_column << "\n"
        << "primary key: " << formattedAST(data.primary_key_ast) << "\n";

    if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        out << "data format version: " << data.format_version.toUnderType() << "\n";
        out << "partition key: " << formattedAST(data.partition_expr_ast) << "\n";
    }

    if (sorting_and_primary_keys_independent)
        out << "sorting key: " << sorting_key_str << "\n";
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

    in >> "date column: ";
    String read_date_column;
    in >> read_date_column >> "\n";

    in >> "sampling expression: ";
    String read_sample_expression;
    in >> read_sample_expression >> "\n";

    in >> "index granularity: ";
    size_t read_index_granularity = 0;
    in >> read_index_granularity >> "\n";

    in >> "mode: ";
    int read_mode = 0;
    in >> read_mode >> "\n";

    in >> "sign column: ";
    String read_sign_column;
    in >> read_sign_column >> "\n";

    in >> "primary key: ";
    String read_primary_key;
    in >> read_primary_key >> "\n";

    MergeTreeDataFormatVersion read_data_format_version;
    if (in.eof())
        read_data_format_version = 0;
    else
    {
        in >> "data format version: ";
        in >> read_data_format_version.toUnderType() >> "\n";
    }

    if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        in >> "partition key: ";
        String read_partition_key;
        in >> read_partition_key >> "\n";
    }

    if (checkString("sorting key: ", in))
    {
        String read_sorting_key;
        in >> read_sorting_key >> "\n";
        sorting_key_str = read_sorting_key;
    }

    assertEOF(in);
}

ReplicatedMergeTreeTableMetadata ReplicatedMergeTreeTableMetadata::parse(
    const MergeTreeData & data_, const String & s)
{
    ReplicatedMergeTreeTableMetadata metadata(data_);
    ReadBufferFromString buf(s);
    metadata.read(buf);
    return metadata;
}

void ReplicatedMergeTreeTableMetadata::check(ReadBuffer & in) const
{
    /// TODO Can be made less cumbersome.

    in >> "metadata format version: 1";

    in >> "\ndate column: ";
    String read_date_column;
    in >> read_date_column;

    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        const String & local_date_column = data.minmax_idx_columns[data.minmax_idx_date_column_pos];
        if (local_date_column != read_date_column)
            throw Exception("Existing table metadata in ZooKeeper differs in date index column."
                " Stored in ZooKeeper: " + read_date_column + ", local: " + local_date_column,
                ErrorCodes::METADATA_MISMATCH);
    }
    else if (!read_date_column.empty())
        throw Exception(
            "Existing table metadata in ZooKeeper differs in date index column."
            " Stored in ZooKeeper: " + read_date_column + ", local is custom-partitioned.",
            ErrorCodes::METADATA_MISMATCH);

    in >> "\nsampling expression: ";
    String read_sample_expression;
    String local_sample_expression = formattedAST(data.sampling_expression);
    in >> read_sample_expression;

    if (read_sample_expression != local_sample_expression)
        throw Exception("Existing table metadata in ZooKeeper differs in sample expression."
            " Stored in ZooKeeper: " + read_sample_expression + ", local: " + local_sample_expression,
            ErrorCodes::METADATA_MISMATCH);

    in >> "\nindex granularity: ";
    size_t read_index_granularity = 0;
    in >> read_index_granularity;

    if (read_index_granularity != data.index_granularity)
        throw Exception("Existing table metadata in ZooKeeper differs in index granularity."
            " Stored in ZooKeeper: " + DB::toString(read_index_granularity) + ", local: " + DB::toString(data.index_granularity),
            ErrorCodes::METADATA_MISMATCH);

    in >> "\nmode: ";
    int read_mode = 0;
    in >> read_mode;

    if (read_mode != static_cast<int>(data.merging_params.mode))
        throw Exception("Existing table metadata in ZooKeeper differs in mode of merge operation."
            " Stored in ZooKeeper: " + DB::toString(read_mode) + ", local: "
            + DB::toString(static_cast<int>(data.merging_params.mode)),
            ErrorCodes::METADATA_MISMATCH);

    in >> "\nsign column: ";
    String read_sign_column;
    in >> read_sign_column;

    if (read_sign_column != data.merging_params.sign_column)
        throw Exception("Existing table metadata in ZooKeeper differs in sign column."
            " Stored in ZooKeeper: " + read_sign_column + ", local: " + data.merging_params.sign_column,
            ErrorCodes::METADATA_MISMATCH);

    in >> "\nprimary key: ";
    String read_primary_key;
    String local_primary_key = formattedAST(data.primary_key_ast);
    in >> read_primary_key;

    /// NOTE: You can make a less strict check of match expressions so that tables do not break from small changes
    ///    in formatAST code.
    if (read_primary_key != local_primary_key)
        throw Exception("Existing table metadata in ZooKeeper differs in primary key."
            " Stored in ZooKeeper: " + read_primary_key + ", local: " + local_primary_key,
            ErrorCodes::METADATA_MISMATCH);

    in >> "\n";
    MergeTreeDataFormatVersion read_data_format_version;
    if (in.eof())
        read_data_format_version = 0;
    else
    {
        in >> "data format version: ";
        in >> read_data_format_version.toUnderType();
    }

    if (read_data_format_version != data.format_version)
        throw Exception("Existing table metadata in ZooKeeper differs in data format version."
            " Stored in ZooKeeper: " + DB::toString(read_data_format_version.toUnderType()) +
            ", local: " + DB::toString(data.format_version.toUnderType()),
            ErrorCodes::METADATA_MISMATCH);

    if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        in >> "\npartition key: ";
        String read_partition_key;
        String local_partition_key = formattedAST(data.partition_expr_ast);
        in >> read_partition_key;

        if (read_partition_key != local_partition_key)
            throw Exception(
                "Existing table metadata in ZooKeeper differs in partition key expression."
                " Stored in ZooKeeper: " + read_partition_key + ", local: " + local_partition_key,
                ErrorCodes::METADATA_MISMATCH);

        in >> "\n";
    }

    if (checkString("sorting key: ", in))
    {
        String read_sorting_key;
        in >> read_sorting_key >> "\n";
    }

    assertEOF(in);
}

void ReplicatedMergeTreeTableMetadata::check(const String & s) const
{
    ReadBufferFromString in(s);
    check(in);
}

}
