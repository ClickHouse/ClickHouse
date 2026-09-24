#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>

#include <Core/Streaming/CursorTree.h>

namespace DB
{

MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree)
{
    MergeTreeCursor cursor;

    if (!cursor_tree)
        return cursor;

    for (const auto & [partition_id, node] : *cursor_tree)
    {
        const auto & partition_node = std::get<CursorTreeNodePtr>(node);
        cursor[partition_id] = PartitionCursor{
            .block_number = partition_node->getValue("block_number"),
            .block_offset = partition_node->getValue("block_offset", -1),
        };
    }

    return cursor;
}

Names extendWithAuxiliaryColumns(Names columns)
{
    for (const auto & aux_name : {String("_partition_id"), String(BlockNumberColumn::name), String(BlockOffsetColumn::name)})
        if (!std::ranges::contains(columns, aux_name))
            columns.push_back(aux_name);

    return columns;
}

FilterDAGInfo buildPartitionFilter(
    const String & partition_id,
    const PartitionCursor & last_emitted_position,
    const Int64 & safe_block_number,
    const Block & input_header,
    const ContextPtr & context)
{
    chassert(safe_block_number >= last_emitted_position.block_number);

    /// Build the cursor filter:
    ///   _partition_id = '<partition_id>'
    ///   AND _block_number <= <safe_block_number>
    ///   AND (_block_number > <last_bn> OR (_block_number = <last_bn> AND _block_offset > <last_bo>))
    ActionsDAG dag;

    /// All input-header columns are added as DAG inputs and outputs.
    const ActionsDAG::Node * partition_input = nullptr;
    const ActionsDAG::Node * bn_input = nullptr;
    const ActionsDAG::Node * bo_input = nullptr;
    for (const auto & col : input_header)
    {
        const auto * input = &dag.addInput(col.name, col.type);
        dag.getOutputs().push_back(input);

        if (col.name == "_partition_id")
            partition_input = input;
        else if (col.name == "_block_number")
            bn_input = input;
        else if (col.name == "_block_offset")
            bo_input = input;
    }

    chassert(partition_input && bn_input && bo_input);

    const auto string_type = std::make_shared<DataTypeString>();
    const auto int64_type = std::make_shared<DataTypeInt64>();
    const auto & partition_const = dag.addColumn(string_type->createColumnConst(1, partition_id), string_type, "_partition_id_const");
    const auto & safe_const = dag.addColumn(int64_type->createColumnConst(1, safe_block_number), int64_type, "_safe_block_number_const");
    const auto & last_bn_const = dag.addColumn(int64_type->createColumnConst(1, last_emitted_position.block_number), int64_type, "_last_bn_const");
    const auto & last_bo_const = dag.addColumn(int64_type->createColumnConst(1, last_emitted_position.block_offset), int64_type, "_last_bo_const");

    const auto construct_function = [&context](const String & name) { return FunctionFactory::instance().get(name, context); };
    const auto & eq_partition = dag.addFunction(construct_function("equals"), {partition_input, &partition_const}, "");
    const auto & le_safe      = dag.addFunction(construct_function("lessOrEquals"), {bn_input, &safe_const}, "");
    const auto & gt_bn        = dag.addFunction(construct_function("greater"), {bn_input, &last_bn_const}, "");
    const auto & eq_bn        = dag.addFunction(construct_function("equals"), {bn_input, &last_bn_const}, "");
    const auto & gt_bo        = dag.addFunction(construct_function("greater"), {bo_input, &last_bo_const}, "");
    const auto & eq_and_gt    = dag.addFunction(construct_function("and"), {&eq_bn, &gt_bo}, "");
    const auto & gt_or_eq     = dag.addFunction(construct_function("or"), {&gt_bn, &eq_and_gt}, "");
    const auto & filter       = dag.addFunction(construct_function("and"), {&eq_partition, &le_safe, &gt_or_eq}, "");

    /// Add filtering column to outputs.
    dag.getOutputs().push_back(&filter);

    return FilterDAGInfo{std::move(dag), filter.result_name, /*do_remove_column=*/true};
}

}
