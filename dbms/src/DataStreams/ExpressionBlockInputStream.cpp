#include <Interpreters/ExpressionActions.h>
#include <DataStreams/ExpressionBlockInputStream.h>


namespace DB
{

ExpressionBlockInputStream::ExpressionBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_)
    : expression(expression_)
{
    children.push_back(input_);
}

String ExpressionBlockInputStream::getName() const { return "Expression"; }

String ExpressionBlockInputStream::getID() const
{
    std::stringstream res;
    res << "Expression(" << children.back()->getID() << ", " << expression->getID() << ")";
    return res.str();
}

const Block & ExpressionBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        expression->executeOnTotals(totals);
    }

    return totals;
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
