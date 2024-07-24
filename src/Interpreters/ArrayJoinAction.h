#pragma once

#include <Core/Names.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class DataTypeArray;
class ColumnArray;
std::shared_ptr<const DataTypeArray> getArrayJoinDataType(DataTypePtr type);
const ColumnArray * getArrayJoinColumnRawPtr(const ColumnPtr & column);

/// If input array join column has map type, convert it to array type.
/// Otherwise do nothing.
ColumnWithTypeAndName convertArrayJoinColumn(const ColumnWithTypeAndName & src_col);

class ArrayJoinAction
{
public:
    NameSet columns;
    bool is_left = false;
    bool is_unaligned = false;

    /// For unaligned [LEFT] ARRAY JOIN
    FunctionOverloadResolverPtr function_length;
    FunctionOverloadResolverPtr function_greatest;
    FunctionOverloadResolverPtr function_array_resize;

    /// For LEFT ARRAY JOIN.
    FunctionOverloadResolverPtr function_builder;

    ArrayJoinAction(const NameSet & array_joined_columns_, bool array_join_is_left, ContextPtr context);
    void prepare(ColumnsWithTypeAndName & sample) const;
    void execute(Block & block);
};

using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

}
