#pragma once

#include <Core/Names.h>
#include <Core/Block.h>


namespace DB
{

class Context;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class ArrayJoinAction
{
public:
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
    void prepare(ColumnsWithTypeAndName & sample) const;
    void execute(Block & block);
};

using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

}
