#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <Parsers/formatAST.h>

namespace DB
{

void CheckConstraintsBlockOutputStream::write(const Block & block)
{
    for (size_t i = 0; i < expressions.size(); ++i)
    {
        auto constraint_expr = expressions[i];
        if (!checkConstraintOnBlock(block, constraint_expr))
            throw Exception{"Constraint " + constraints.constraints[i]->name + " is not satisfied at, constraint expression: " +
            serializeAST(*(constraints.constraints[i]->expr), true), ErrorCodes::LOGICAL_ERROR};
    }
    output->write(block);
}

void CheckConstraintsBlockOutputStream::flush()
{
    output->flush();
}

void CheckConstraintsBlockOutputStream::writePrefix()
{
    output->writePrefix();
}

void CheckConstraintsBlockOutputStream::writeSuffix()
{
    output->writeSuffix();
}

bool CheckConstraintsBlockOutputStream::checkConstraintOnBlock(const Block & block, const ExpressionActionsPtr & constraint)
{
    Block res = block;
    constraint->execute(res);
    assert(block.columns() == res.columns() - 1);
    ColumnWithTypeAndName res_column = res.safeGetByPosition(res.columns() - 1);
    size_t column_size = res_column.column->size();
    for (size_t i = 0; i < column_size; ++i)
        if (!res_column.column->getBool(i))
            return false;
    return true;
}

}
