#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/Context.h>

namespace DB
{

class ApplyingMutationsBlockInputStream : public IProfilingBlockInputStream
{
public:
    ApplyingMutationsBlockInputStream(
        const BlockInputStreamPtr & input, const std::vector<MutationCommand> & commands, const Context & context);

    String getName() const override { return "ApplyMutations"; }

    Block getHeader() const override;
    Block getTotals() override;

private:
    Block readImpl() override;

    BlockInputStreamPtr impl;
};

}
