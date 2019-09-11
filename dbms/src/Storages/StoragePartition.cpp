#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/StorageFactory.h>
#include <Storages/StoragePartition.h>

#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Common/HashTable/HashMap.h>
#include <Common/setThreadName.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_MANY_PARTS;
}

namespace
{
    void buildScatterSelector(const ColumnRawPtrs & columns, PODArray<size_t> & partition_num_to_first_row, IColumn::Selector & selector)
    {
        /// Use generic hashed variant since partitioning is unlikely to be a bottleneck.
        using Data = HashMap<UInt128, size_t, UInt128TrivialHash>;
        Data partitions_map;

        size_t num_rows = columns[0]->size();
        size_t partitions_count = 0;
        for (size_t i = 0; i < num_rows; ++i)
        {
            Data::key_type key = hash128(i, columns.size(), columns);
            typename Data::iterator it;
            bool inserted;
            partitions_map.emplace(key, it, inserted);

            if (inserted)
            {
                constexpr size_t max_parts = 1024;
                if (partitions_count >= max_parts)
                    throw Exception(
                        "Too many partitions for single INSERT block (more than " + toString(max_parts)
                            + "). The limit is controlled by 'max_partitions_per_insert_block' setting. Large number of partitions is a "
                              "common misconception. It will lead to severe negative performance impact, including slow server startup, "
                              "slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under "
                              "1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is "
                              "sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, "
                              "etc).",
                        ErrorCodes::TOO_MANY_PARTS);

                partition_num_to_first_row.push_back(i);
                it->getSecond() = partitions_count;

                ++partitions_count;

                /// Optimization for common case when there is only one partition - defer selector initialization.
                if (partitions_count == 2)
                {
                    selector = IColumn::Selector(num_rows);
                    std::fill(selector.begin(), selector.begin() + i, 0);
                }
            }

            if (partitions_count > 1)
                selector[i] = it->getSecond();
        }
    }
}

class PartitionBlockOutputStream : public IBlockOutputStream
{
public:
    explicit PartitionBlockOutputStream(StoragePartition & storage_, size_t max_threads_) : storage(storage_), max_threads(max_threads_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        if (!block || !block.rows())
            return;
        storage.check(block, true);
        block.checkNumberOfRows();
        auto part_blocks = [&]() {
            BlocksWithPartition result;
            if (!storage.partition_key_expr) /// Table is not partitioned.
            {
                result.emplace_back(Block(block), Row());
                return result;
            }
            Block block_copy = block;
            storage.partition_key_expr->execute(block_copy);

            ColumnRawPtrs partition_columns;
            partition_columns.reserve(storage.partition_key_sample.columns());
            for (const ColumnWithTypeAndName & element : storage.partition_key_sample)
                partition_columns.emplace_back(block_copy.getByName(element.name).column.get());

            PODArray<size_t> partition_num_to_first_row;
            IColumn::Selector selector;
            buildScatterSelector(partition_columns, partition_num_to_first_row, selector);

            size_t partitions_count = partition_num_to_first_row.size();
            result.reserve(partitions_count);

            auto get_partition = [&](size_t num) {
                Row partition(partition_columns.size());
                for (size_t i = 0; i < partition_columns.size(); ++i)
                    partition[i] = Field((*partition_columns[i])[partition_num_to_first_row[num]]);
                return partition;
            };

            if (partitions_count == 1)
            {
                /// A typical case is when there is one partition (you do not need to split anything).
                result.emplace_back(Block(block), get_partition(0));
                return result;
            }

            for (size_t i = 0; i < partitions_count; ++i)
                result.emplace_back(block.cloneEmpty(), get_partition(i));

            for (size_t col = 0; col < block.columns(); ++col)
            {
                MutableColumns scattered = block.getByPosition(col).column->scatter(partitions_count, selector);
                for (size_t i = 0; i < partitions_count; ++i)
                    result[i].block.getByPosition(col).column = std::move(scattered[i]);
            }

            return result;
        }();

        std::lock_guard lock(storage.mutex);
        for (auto & block_with_partition : part_blocks)
            partitions_data[std::move(block_with_partition.partition)].push_back(std::move(block_with_partition.block));
    }

    void writeSuffix() override
    {
        std::lock_guard lock(storage.mutex);
        if (partitions_data.size() < 2)
            return;

        auto sample = partitions_data.begin()->second.front().cloneEmpty();
        storage.sorting_key_expr->execute(sample);
        Names sort_columns = storage.sorting_key_columns;
        SortDescription sort_description;
        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(sample.getPositionByName(sort_columns[i]), 1, 1);

        storage.data.resize(partitions_data.size());

        auto build = [&](size_t seq, Blocks & data) {
            for (auto & block : data)
            {
                storage.sorting_key_expr->execute(block);
                sortBlock(block, sort_description);
            }
            storage.data[seq] = MergeSortingBlocksBlockInputStream(data, sort_description, -1).read();
        };

        ThreadPool pool(std::min(max_threads, partitions_data.size()));
        size_t i = 0;
        for (auto & kv : partitions_data)
        {
            auto seq = i++;
            auto thread_group = CurrentThread::getGroup();
            pool.schedule([&, seq, thread_group] {
                setThreadName("BuildingSeqBlock");
                build(seq, kv.second);
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
            });
        }
        // Wait for concurrent view processing
        pool.wait();
    }

private:
    std::map<Row, Blocks> partitions_data;
    StoragePartition & storage;
    size_t max_threads;
};


StoragePartition::StoragePartition(
    String database_name_,
    String table_name_,
    ColumnsDescription columns_,
    Context & context_,
    const ASTPtr & partition_by_ast_,
    const ASTPtr & order_by_ast_)
    : database_name(std::move(database_name_))
    , table_name(std::move(table_name_))
    , global_context(context_)
    , partition_by_ast(partition_by_ast_)
    , order_by_ast(order_by_ast_)
{
    setProperties(order_by_ast_, columns_);
    initPartitionKey();
}

static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name)
{
    for (const ExpressionAction & action : expr.getActions())
    {
        if (action.type == ExpressionAction::ARRAY_JOIN)
            throw Exception(key_name + " key cannot contain array joins", ErrorCodes::ILLEGAL_COLUMN);

        if (action.type == ExpressionAction::APPLY_FUNCTION)
        {
            IFunctionBase & func = *action.function_base;
            if (!func.isDeterministic())
                throw Exception(
                    key_name
                        + " key cannot contain non-deterministic functions, "
                          "but contains function "
                        + func.getName(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    for (const ColumnWithTypeAndName & element : sample_block)
    {
        const ColumnPtr & column = element.column;
        if (column && (isColumnConst(*column) || column->isDummy()))
            throw Exception{key_name + " key cannot contain constants", ErrorCodes::ILLEGAL_COLUMN};

        if (element.type->isNullable())
            throw Exception{key_name + " key cannot contain nullable columns", ErrorCodes::ILLEGAL_COLUMN};
    }
}

void StoragePartition::initPartitionKey()
{
    ASTPtr partition_key_expr_list = MergeTreeData::extractKeyExpressionList(partition_by_ast);

    if (partition_key_expr_list->children.empty())
        return;

    {
        auto syntax_result = SyntaxAnalyzer(global_context).analyze(partition_key_expr_list, getColumns().getAllPhysical());
        partition_key_expr = ExpressionAnalyzer(partition_key_expr_list, syntax_result, global_context).getActions(false);
    }

    for (const ASTPtr & ast : partition_key_expr_list->children)
    {
        String col_name = ast->getColumnName();
        partition_key_sample.insert(partition_key_expr->getSampleBlock().getByName(col_name));
    }

    checkKeyExpression(*partition_key_expr, partition_key_sample, "Partition");
}

void StoragePartition::setProperties(const ASTPtr & new_order_by_ast, const ColumnsDescription & new_columns)
{
    if (!new_order_by_ast)
        throw Exception("ORDER BY cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    ASTPtr new_sorting_key_expr_list = MergeTreeData::extractKeyExpressionList(new_order_by_ast);
    size_t sorting_key_size = new_sorting_key_expr_list->children.size();
    Names new_sorting_key_columns;

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        String sorting_key_column = new_sorting_key_expr_list->children[i]->getColumnName();
        new_sorting_key_columns.push_back(sorting_key_column);
    }

    auto all_columns = new_columns.getAllPhysical();

    auto new_sorting_key_syntax = SyntaxAnalyzer(global_context).analyze(new_sorting_key_expr_list, all_columns);
    auto new_sorting_key_expr = ExpressionAnalyzer(new_sorting_key_expr_list, new_sorting_key_syntax, global_context).getActions(false);
    auto new_sorting_key_sample
        = ExpressionAnalyzer(new_sorting_key_expr_list, new_sorting_key_syntax, global_context).getActions(true)->getSampleBlock();

    checkKeyExpression(*new_sorting_key_expr, new_sorting_key_sample, "Sorting");

    setColumns(std::move(new_columns));

    order_by_ast = new_order_by_ast;
    sorting_key_columns = std::move(new_sorting_key_columns);
    sorting_key_expr_ast = std::move(new_sorting_key_expr_list);
    sorting_key_expr = std::move(new_sorting_key_expr);
}

BlockInputStreams StoragePartition::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /* num_streams */)
{
    check(column_names);

    std::lock_guard lock(mutex);

    return {std::make_shared<BlocksListBlockInputStream>(BlocksList(data.begin(), data.end()))};
}


BlockOutputStreamPtr StoragePartition::write(const ASTPtr & /*query*/, const Context & context)
{
    return std::make_shared<PartitionBlockOutputStream>(*this, context.getSettingsRef().max_threads);
}


void StoragePartition::drop(TableStructureWriteLockHolder &)
{
    std::lock_guard lock(mutex);
    data.clear();
}

void StoragePartition::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    std::lock_guard lock(mutex);
    data.clear();
}


void registerStoragePartition(StorageFactory & factory)
{
    factory.registerStorage("Partition", [](const StorageFactory::Arguments & args) {
        ASTPtr partition_by_ast;
        ASTPtr order_by_ast;
        if (!args.storage_def->partition_by)
            throw Exception("You must provide a PARTITION BY expression in the table definition", ErrorCodes::BAD_ARGUMENTS);

        partition_by_ast = args.storage_def->partition_by->ptr();

        if (!args.storage_def->order_by)
            throw Exception(
                "You must provide an ORDER BY expression in the table definition. "
                "If you don't want this table to be sorted, use ORDER BY tuple()",
                ErrorCodes::BAD_ARGUMENTS);

        order_by_ast = args.storage_def->order_by->ptr();

        return StoragePartition::create(args.database_name, args.table_name, args.columns, args.context, partition_by_ast, order_by_ast);
    });
}

}
