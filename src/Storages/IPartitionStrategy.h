#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/KeyDescription.h>
#include <Processors/Chunk.h>

namespace DB
{

/*
 * Class responsible for computing and generating a partition key for object storage.
 * As of now, there are two possible implementations: hive and wildcard.
 *
 * It also offers some helper APIs like `getFormatChunk` and `getFormatHeader`. Required mostly because of `hive` strategy
 * since the default behavior is not to write partition columns in the files and rely only on the file path.
 */
struct IPartitionStrategy
{
    struct PartitionExpressionActionsAndColumnName
    {
        ExpressionActionsPtr actions;
        std::string column_name;
    };

    IPartitionStrategy(KeyDescription partition_key_description_, const Block & sample_block_, ContextPtr context_);

    virtual ~IPartitionStrategy() = default;

    virtual ColumnPtr computePartitionKey(const Chunk & chunk) = 0;

    virtual std::string getPathForRead(const std::string & prefix) = 0;
    virtual std::string getPathForWrite(const std::string & prefix, const std::string & partition_key) = 0;

    virtual ColumnRawPtrs getFormatChunkColumns(const Chunk & chunk)
    {
        ColumnRawPtrs result_columns;

        for (const auto & column : chunk.getColumns())
        {
            result_columns.emplace_back(column.get());
        }

        return result_columns;
    }

    virtual Block getFormatHeader() { return sample_block; }

    NamesAndTypesList getPartitionColumns() const;
    const KeyDescription & getPartitionKeyDescription() const;

protected:
    const KeyDescription partition_key_description;
    const Block sample_block;
    ContextPtr context;
};

/*
 * Tries to create a partition strategy given a strategy name.
 * Performs validation on required arguments by each strategy. Example: Partition strategy hive can not be used without a PARTITION BY expression
 */
struct PartitionStrategyFactory
{
    enum class StrategyType
    {
        NONE, /// The default for data lakes. To keep it backwards compatible, it is promoted to `WILDCARD` in case it is plain object storage
        WILDCARD,
        HIVE
    };

    static std::shared_ptr<IPartitionStrategy> get(
        StrategyType strategy,
        ASTPtr partition_by,
        const NamesAndTypesList & partition_columns,
        ContextPtr context,
        const std::string & file_format,
        bool globbed_path,
        bool contains_partition_wildcard,
        bool partition_columns_in_data_file);
};

/*
 * Simply wraps the partition expression with a `toString` function call.
 * Path for reading is an identity function
 * Path for writing replaces the `{_partition_id}` wildcard with the partition key.
 */
struct WildcardPartitionStrategy : IPartitionStrategy
{
    WildcardPartitionStrategy(KeyDescription partition_key_description_, const Block & sample_block_, ContextPtr context_);

    ColumnPtr computePartitionKey(const Chunk & chunk) override;
    std::string getPathForRead(const std::string & prefix) override;
    std::string getPathForWrite(const std::string & prefix, const std::string & partition_key) override;

private:
    PartitionExpressionActionsAndColumnName actions_with_column_name;
};

/*
 * Builds partition keys in the hive format (e.g, key1=value1/key2=value2/)
 * Path for reading appends recursive reading + file extension (e.g **.parquet)
 * Path for writing appends partition key, snowflakeid as file name and file extension (e.g, table_root/key1=value1/key2=value2/1933642830979268608.parquet).
 */
struct HiveStylePartitionStrategy : IPartitionStrategy
{
    HiveStylePartitionStrategy(
        KeyDescription partition_key_description_,
        const Block & sample_block_,
        ContextPtr context_,
        const std::string & file_format_,
        bool partition_columns_in_data_file_);

    ColumnPtr computePartitionKey(const Chunk & chunk) override;
    std::string getPathForRead(const std::string & prefix) override;
    std::string getPathForWrite(const std::string & prefix, const std::string & partition_key) override;

    ColumnRawPtrs getFormatChunkColumns(const Chunk & chunk) override;
    Block getFormatHeader() override;

private:
    const std::string file_format;
    const bool partition_columns_in_data_file;
    std::unordered_set<std::string> partition_columns_name_set;
    PartitionExpressionActionsAndColumnName actions_with_column_name;
    Block block_without_partition_columns;
};

}
