#include <Interpreters/SubqueryForSet.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <DataStreams/LazyBlockInputStream.h>

namespace DB
{

void SubqueryForSet::makeSource(std::shared_ptr<InterpreterSelectWithUnionQuery> & interpreter,
                                NamesWithAliases && joined_block_aliases_)
{
    joined_block_aliases = std::move(joined_block_aliases_);
    source = std::make_shared<LazyBlockInputStream>(interpreter->getSampleBlock(),
                                                    [interpreter]() mutable { return interpreter->execute().in; });

    sample_block = source->getHeader();
    renameColumns(sample_block);
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
