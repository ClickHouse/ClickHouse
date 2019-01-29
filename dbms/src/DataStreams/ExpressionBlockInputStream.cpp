#include <Interpreters/ExpressionActions.h>
#include <DataStreams/ExpressionBlockInputStream.h>


namespace DB
{

ExpressionBlockInputStream::ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_)
    : expression(expression_)
{
    children.push_back(input);
}

String ExpressionBlockInputStream::getName() const { return "Expression"; }

Block ExpressionBlockInputStream::getTotals()
{
    totals = children.back()->getTotals();
    expression->executeOnTotals(totals);

    return totals;
}

Block ExpressionBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    expression->execute(res, true);
    return res;
}

Block ExpressionBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;
    expression->execute(res);
    return res;
}

}
