#pragma once

#include <Core/Names.h>
#include <Core/Block.h>


namespace DB
{

class Context;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

struct ArrayJoinAction
{
    NameSet columns;
    bool is_left = false;
    bool is_unaligned = false;

    /// For unaligned [LEFT] ARRAY JOIN
    FunctionOverloadResolverPtr function_length;
    FunctionOverloadResolverPtr function_greatest;
    FunctionOverloadResolverPtr function_arrayResize;

    /// For LEFT ARRAY JOIN.
    FunctionOverloadResolverPtr function_builder;

    ArrayJoinAction(const NameSet & array_joined_columns_, bool array_join_is_left, const Context & context);
    void prepare(Block & sample_block);
    void execute(Block & block, bool dry_run);
    void finalize(NameSet & needed_columns, NameSet & unmodified_columns, NameSet & final_columns);
};

}
