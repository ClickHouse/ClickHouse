#pragma once
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{

/// Add table name virtual column
class AddingTableNameVirtualColumnStep : public ExpressionStep
{
public:
    explicit AddingTableNameVirtualColumnStep(
        const DataStream & input_stream_,
        const String & table_name);

    String getName() const override { return "AddingTableNameVirtualColumn"; }
};

}
