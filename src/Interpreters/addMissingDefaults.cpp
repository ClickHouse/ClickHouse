#include <Interpreters/addMissingDefaults.h>

#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Core/Block.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

ActionsDAG addMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool null_as_default)
{
    ActionsDAG actions(header.getColumnsWithTypeAndName());
    auto & index = actions.getOutputs();

    /// For missing columns of nested structure, you need to create not a column of empty arrays, but a column of arrays of correct lengths.
    /// First, remember the offset columns for all arrays in the block.
    std::map<String, ActionsDAG::NodeRawConstPtrs> nested_groups;

    for (size_t i = 0, size = header.columns(); i < size; ++i)
    {
        const auto & elem = header.getByPosition(i);

        if (typeid_cast<const ColumnArray *>(&*elem.column))
        {
            String offsets_name = Nested::extractTableName(elem.name);

            auto & group = nested_groups[offsets_name];
            if (group.empty())
                group.push_back(nullptr);

            group.push_back(actions.getInputs()[i]);
        }
    }

    FunctionOverloadResolverPtr func_builder_replicate = FunctionFactory::instance().get("replicate", context);

    /// We take given columns from input block and missed columns without default value
    /// (default and materialized will be computed later).
    for (const auto & column : required_columns)
    {
        if (header.has(column.name))
            continue;

        if (columns.hasDefault(column.name))
            continue;

        String offsets_name = Nested::extractTableName(column.name);
        const auto * array_type = typeid_cast<const DataTypeArray *>(column.type.get());
        if (array_type && nested_groups.contains(offsets_name))
        {
            const auto & nested_type = array_type->getNestedType();
            ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(0);
            const auto & constant = actions.addColumn({nested_column, nested_type, column.name});

            auto & group = nested_groups[offsets_name];
            group[0] = &constant;
            index.push_back(&actions.addFunction(func_builder_replicate, group, constant.result_name));

            continue;
        }

        /** It is necessary to turn a constant column into a full column, since in part of blocks (from other parts),
        *  it can be full (or the interpreter may decide that it is constant everywhere).
        */
        auto new_column = column.type->createColumnConstWithDefaultValue(0);
        const auto * col = &actions.addColumn({new_column, column.type, column.name});
        index.push_back(&actions.materializeNode(*col));
    }

    /// Computes explicitly specified values by default and materialized columns.
    if (auto dag = evaluateMissingDefaults(actions.getResultColumns(), required_columns, columns, context, true, null_as_default))
        actions = ActionsDAG::merge(std::move(actions), std::move(*dag));

    /// Removes unused columns and reorders result.
    actions.removeUnusedActions(required_columns.getNames(), false);
    actions.addMaterializingOutputActions(/*materialize_sparse=*/ false);

    return actions;
}

}
