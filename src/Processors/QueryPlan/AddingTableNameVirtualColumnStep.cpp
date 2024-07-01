#include <Processors/QueryPlan/AddingTableNameVirtualColumnStep.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

static ActionsDAGPtr createAddingTableNameVirtualColumnDAG(const String & table_name)
{
    ColumnWithTypeAndName column;
    column.name = "_table";
    column.type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    column.column = column.type->createColumnConst(0, table_name);

    auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
    return adding_column_dag;
}

AddingTableNameVirtualColumnStep::AddingTableNameVirtualColumnStep(
    const DataStream & input_stream_,
    const String & table_name)
    : ExpressionStep(
        input_stream_,
        createAddingTableNameVirtualColumnDAG(table_name))
{
    setStepDescription("Add table name virtual column");
}

}
