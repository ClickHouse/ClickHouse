#pragma once
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{

/// Add table name virtual column
class AddingTableNameVirtualColumnStep : public ExpressionStep
{
public:
    explicit AddingTableNameVirtualColumnStep(
        const Header & input_header_,
        const String & table_name_);

    String getName() const override { return "AddingTableNameVirtualColumn"; }
};

}
