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


class ArrayJoinResultIterator;
using ArrayJoinResultIteratorPtr = std::unique_ptr<ArrayJoinResultIterator>;
class ArrayJoinAction
{
public:
    NameSet columns;
    bool is_left = false;
    bool is_unaligned = false;
    size_t max_block_size = DEFAULT_BLOCK_SIZE;

    /// For unaligned [LEFT] ARRAY JOIN
    FunctionOverloadResolverPtr function_length;
    FunctionOverloadResolverPtr function_array_resize;

    /// For LEFT ARRAY JOIN.
    FunctionOverloadResolverPtr function_builder;

    ArrayJoinAction(const Names & columns_, bool is_left_, bool is_unaligned_, size_t max_block_size_);
    static void prepare(const NameSet & columns, ColumnsWithTypeAndName & sample);
    static void prepare(const Names & columns, ColumnsWithTypeAndName & sample);

    ArrayJoinResultIteratorPtr execute(Block block);
};

using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinResultIterator
{
public:
    explicit ArrayJoinResultIterator(const ArrayJoinAction * array_join_, Block block_);
    ~ArrayJoinResultIterator() = default;

    Block next();
    bool hasNext() const;

private:
    const ArrayJoinAction * array_join;
    Block block;

    ColumnPtr any_array_map_ptr;
    const ColumnArray * any_array;
    /// If LEFT ARRAY JOIN, then we create columns in which empty arrays are replaced by arrays with one element - the default value.
    std::map<String, ColumnPtr> non_empty_array_columns;

    size_t total_rows;
    size_t current_row;
};
}
