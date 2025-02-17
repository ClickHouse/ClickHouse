#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ColumnNode.h>

namespace DB
{

using ColumnNodes = std::vector<ColumnNodePtr>;

class IQueryOrUnionNode : public IQueryTreeNode
{
public:
    bool isCorrelated() const
    {
        return !correlated_columns.empty();
    }

    const ColumnNodes & getCorrelatedColumns() const
    {
        return correlated_columns;
    }

    void addCorrelatedColumn(ColumnNodePtr correlated_column)
    {
        for (const auto & column : correlated_columns)
        {
            if (column->isEqual(*correlated_column))
                return;
        }
        correlated_columns.push_back(correlated_column);
    }

protected:
    using IQueryTreeNode::IQueryTreeNode;

private:
    ColumnNodes correlated_columns;
};

}
