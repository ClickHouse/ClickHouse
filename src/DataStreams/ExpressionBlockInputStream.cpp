#include <Interpreters/ExpressionActions.h>
#include <DataStreams/ExpressionBlockInputStream.h>


namespace DB
{

ExpressionBlockInputStream::ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_)
    : expression(expression_)
{
    children.push_back(input);
    cached_header = children.back()->getHeader();
    expression->execute(cached_header, true);
}

String ExpressionBlockInputStream::getName() const { return "Expression"; }

Block ExpressionBlockInputStream::getTotals()
{
    totals = children.back()->getTotals();
    expression->execute(totals);

    return totals;
}

Block ExpressionBlockInputStream::getHeader() const
{
    return cached_header.cloneEmpty();
}

Block ExpressionBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (res)
        expression->execute(res);
    return res;
}

}
