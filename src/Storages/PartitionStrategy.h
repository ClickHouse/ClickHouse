#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

#include <Processors/Chunk.h>

namespace DB
{

struct PartitionStrategy
{
    struct PartitionExpressionActionsAndColumnName
    {
        ExpressionActionsPtr actions;
        std::string column_name;
    };

    PartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_);

    virtual ~PartitionStrategy() = default;

    virtual PartitionExpressionActionsAndColumnName getExpression() = 0;
    virtual std::string getPath(const std::string & prefix, const std::string & partition_key) = 0;

    virtual Chunk getChunkWithoutPartitionColumnsIfNeeded(const Chunk & chunk)
    {
        Chunk result;

        for (const auto & column : chunk.getColumns())
        {
            result.addColumn(column);
        }

        return result;
    }

    virtual Block getBlockWithoutPartitionColumnsIfNeeded()
    {
        return sample_block;
    }

protected:
    ASTPtr partition_by;
    Block sample_block;
    ContextPtr context;
};

struct PartitionStrategyProvider
{
    static std::shared_ptr<PartitionStrategy> get(
        ASTPtr partition_by,
        const Block & sample_block,
        ContextPtr context,
        const std::string & file_format,
        const std::string & partitioning_style = "",
        bool omit_partition_columns_from_file = true);
};

struct StringfiedPartitionStrategy : PartitionStrategy
{
    StringfiedPartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_);

    PartitionExpressionActionsAndColumnName getExpression() override;
    std::string getPath(const std::string & prefix, const std::string & partition_key) override;
};

struct HiveStylePartitionStrategy : PartitionStrategy
{
    HiveStylePartitionStrategy(
        ASTPtr partition_by_,
        const Block & sample_block_,
        ContextPtr context_,
        const std::string & file_format_,
        bool write_partition_columns_into_files_);

    PartitionExpressionActionsAndColumnName getExpression() override;
    std::string getPath(const std::string & prefix, const std::string & partition_key) override;
    Chunk getChunkWithoutPartitionColumnsIfNeeded(const Chunk & chunk) override;
    Block getBlockWithoutPartitionColumnsIfNeeded() override;

private:
    std::string file_format;
    bool write_partition_columns_into_files;
    Names partition_expression_required_columns;
    std::unordered_set<std::string> partition_expression_required_columns_set;
    PartitionExpressionActionsAndColumnName actions_with_column_name;
    Block block_without_partition_columns;
};

}
