#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/Context.h>

namespace DB
{

/// A stream that pulls the blocks from `input` and executes mutation commands on these blocks
/// (see Storages/MutationCommands.h for a full list).
/// Example: if mutation commands contain ALTER DELETE, this stream will delete rows that satisfy the predicate specified in the command.
class ApplyingMutationsBlockInputStream : public IProfilingBlockInputStream
{
public:
    ApplyingMutationsBlockInputStream(
        const BlockInputStreamPtr & input, const std::vector<MutationCommand> & commands, const Context & context);

    String getName() const override { return "ApplyingMutations"; }

    Block getHeader() const override;
    Block getTotals() override;

private:
    Block readImpl() override;

    BlockInputStreamPtr impl;
};

}
