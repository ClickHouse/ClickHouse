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
    expression->executeOnTotals(totals);

    return totals;
}

Block ExpressionBlockInputStream::getHeader() const
{
    return cached_header.cloneEmpty();
}

Block ExpressionBlockInputStream::readImpl()
{
    if (!initialized)
    {
        if (expression->resultIsAlwaysEmpty())
            return {};

        initialized = true;
    }

    Block res = children.back()->read();
    if (res)
        expression->execute(res);
    return res;
}

Block InflatingExpressionBlockInputStream::readImpl()
{
    if (!initialized)
    {
        if (expression->resultIsAlwaysEmpty())
            return {};

        initialized = true;
    }

    Block res;
    bool keep_going = not_processed && not_processed->empty(); /// There's data inside expression.

    if (!not_processed || keep_going)
    {
        not_processed.reset();

        res = children.back()->read();
        if (res || keep_going)
            expression->execute(res, not_processed, action_number);
    }
    else
    {
        res = std::move(not_processed->block);
        expression->execute(res, not_processed, action_number);
    }
    return res;
}

}
