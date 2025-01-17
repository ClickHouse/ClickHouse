#include <Processors/QueryPlan/AddingTableNameVirtualColumnStep.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

static ActionsDAG createAddingTableNameVirtualColumnDAG(const String & table_name)
{
    ColumnWithTypeAndName column;
    column.name = "_table";
    column.type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    column.column = column.type->createColumnConst(0, table_name);

    auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
    return adding_column_dag;
}

AddingTableNameVirtualColumnStep::AddingTableNameVirtualColumnStep(
    const Header & input_header_,
    const String & table_name_)
    : ExpressionStep(
        input_header_,
        createAddingTableNameVirtualColumnDAG(table_name_))
{
    setStepDescription("Add table name virtual column");
}

}
