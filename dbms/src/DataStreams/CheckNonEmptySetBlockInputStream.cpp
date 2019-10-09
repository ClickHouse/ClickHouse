#include <Interpreters/ExpressionActions.h>
#include <DataStreams/CheckNonEmptySetBlockInputStream.h>
#include <Interpreters/Set.h>
#include <Interpreters/Join.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>


namespace DB
{

CheckNonEmptySetBlockInputStream::CheckNonEmptySetBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_, const NameSet sets_)
        : expression(expression_), sets(sets_)
{
    children.push_back(input);
    cached_header = children.back()->getHeader();
}


String CheckNonEmptySetBlockInputStream::getName() const { return "CheckNonEmptySet"; }


Block CheckNonEmptySetBlockInputStream::getTotals()
{
    return children.back()->getTotals();
}


Block CheckNonEmptySetBlockInputStream::getHeader() const
{
    return cached_header.cloneEmpty();
}


Block CheckNonEmptySetBlockInputStream::readImpl()
{
    if (!initialized)
    {
        /// CheckNonEmptyBlockInputStream in the downstream with CreatingSetsBlockInputStream. So set has been created.
        cached_result = inOrInnerRightJoinWithEmpty();
        initialized = true;
    }

    Block res;

    if (isCancelledOrThrowIfKilled() || cached_result)
        return res;

    return children.back()->read();
}


bool CheckNonEmptySetBlockInputStream::inOrInnerRightJoinWithEmpty() const
{
    InOrInnerRightJoinWithEmpty checker;

    for (const auto & action : expression->getActions())
    {
        if (action.type == ExpressionAction::ARRAY_JOIN)
        {
            return false;
        }
        else if (action.type == ExpressionAction::JOIN)
        {
            if (const auto * join = dynamic_cast<Join *>(action.join.get()))
            {
                checker.hasJoin = true;
                checker.innerRightJoinWithEmpty &= join->getTotalRowCount() == 0 && isInnerOrRight(join->getKind());
            }
        }
        else if (action.type == ExpressionAction::ADD_COLUMN)
        {
            if (!sets.count(action.result_name))
                continue;
            checker.hasIn = true;
            ColumnPtr column_set_ptr = action.added_column;
            const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
            checker.inWithEmpty &= column_set && column_set->getData()->getTotalRowCount() == 0;
        }
    }
    /// Get the final result.
    return checker.result();
}

}
