#include <Interpreters/SubqueryForSet.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <DataStreams/LazyBlockInputStream.h>

namespace DB
{

void SubqueryForSet::makeSource(std::shared_ptr<InterpreterSelectWithUnionQuery> & interpreter,
                                const std::list<JoinedColumn> & columns_from_joined_table,
                                const NameSet & required_columns_from_joined_table)
{
    source = std::make_shared<LazyBlockInputStream>(interpreter->getSampleBlock(),
                                                    [interpreter]() mutable { return interpreter->execute().in; });

    for (const auto & column : columns_from_joined_table)
        if (required_columns_from_joined_table.count(column.name_and_type.name))
            joined_block_aliases.emplace_back(column.original_name, column.name_and_type.name);

    sample_block = source->getHeader();
    for (const auto & name_with_alias : joined_block_aliases)
    {
        if (sample_block.has(name_with_alias.first))
        {
            auto pos = sample_block.getPositionByName(name_with_alias.first);
            auto column = sample_block.getByPosition(pos);
            sample_block.erase(pos);
            column.name = name_with_alias.second;
            sample_block.insert(std::move(column));
        }
    }
}

void SubqueryForSet::renameColumns(Block & block)
{
    for (const auto & name_with_alias : joined_block_aliases)
    {
        if (block.has(name_with_alias.first))
        {
            auto pos = block.getPositionByName(name_with_alias.first);
            auto column = block.getByPosition(pos);
            block.erase(pos);
            column.name = name_with_alias.second;
            block.insert(std::move(column));
        }
    }
}

}
