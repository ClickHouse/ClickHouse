#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

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
        const std::string & partitioning_style = "");
};

struct StringfiedPartitionStrategy : public PartitionStrategy
{
    StringfiedPartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_);

    PartitionExpressionActionsAndColumnName getExpression() override;
    std::string getPath(const std::string & prefix, const std::string & partition_key) override;
};

struct HiveStylePartitionStrategy : public PartitionStrategy
{
    HiveStylePartitionStrategy(ASTPtr partition_by_, const Block & sample_block_, ContextPtr context_, const std::string & file_format_);

    PartitionExpressionActionsAndColumnName getExpression() override;
    std::string getPath(const std::string & prefix, const std::string & partition_key) override;

private:
    std::string file_format;
};

}
