#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <Functions/FunctionHelpers.h>
#include <common/find_symbols.h>
#include <Parsers/formatAST.h>
#include <Columns/ColumnsCommon.h>

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

bool CheckConstraintsBlockOutputStream::checkImplMemory(const Block & block, const ExpressionActionsPtr & constraint)
{
    Block res = block;
    constraint->execute(res);
    assert(block.columns() == res.columns() - 1);
    ColumnWithTypeAndName res_column = res.safeGetByPosition(res.columns() - 1);
    auto res_column_uint8 = checkAndGetColumn<ColumnUInt8>(res_column.column.get());
    return memoryIsByte(res_column_uint8->getRawDataBegin<1>(), res_column_uint8->byteSize(), 0x1);
}

bool CheckConstraintsBlockOutputStream::checkImplBool(const Block & block, const ExpressionActionsPtr & constraint)
{
    Block res = block;
    constraint->execute(res);
    assert(block.columns() == res.columns() - 1);
    ColumnWithTypeAndName res_column = res.safeGetByPosition(res.columns() - 1);
    size_t column_size = res_column.column->size();
    // std::cerr << "Sizes of constraints: " << res_column.column->size() << ' ' << res_column.column->get << '\n';
    for (size_t i = 0; i < column_size; ++i)
        if (!res_column.column->getBool(i))
            return false;
    return true;
}

bool CheckConstraintsBlockOutputStream::checkConstraintOnBlock(const Block & block, const ExpressionActionsPtr & constraint)
{
    return checkImplMemory(block, constraint);
}

}
