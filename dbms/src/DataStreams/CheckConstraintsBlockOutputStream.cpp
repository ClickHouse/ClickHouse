#include <DataStreams/CheckConstraintsBlockOutputStream.h>


namespace DB
{

void CheckConstraintsBlockOutputStream::write(const Block & block)
{
    for (auto & constraint_expr: expressions)
        if (!checkConstraintOnBlock(block, constraint_expr))
            throw Exception("Some constraints are not satisfied", ErrorCodes::QUERY_WAS_CANCELLED);
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
