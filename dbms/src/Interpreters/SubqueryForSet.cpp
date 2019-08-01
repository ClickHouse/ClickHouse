#include <Interpreters/SubqueryForSet.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <DataStreams/LazyBlockInputStream.h>

namespace DB
{

void SubqueryForSet::makeSource(std::shared_ptr<InterpreterSelectWithUnionQuery> & interpreter,
                                const Names & original_names, const Names & required_names)
{
    source = std::make_shared<LazyBlockInputStream>(interpreter->getSampleBlock(),
                                                    [interpreter]() mutable { return interpreter->execute().in; });

    for (size_t i = 0; i < original_names.size(); ++i)
        joined_block_aliases.emplace_back(original_names[i], required_names[i]);

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
