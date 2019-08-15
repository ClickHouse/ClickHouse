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
        Block res = block;
        auto constraint_expr = expressions[i];
        auto res_column_uint8 = executeOnBlock(res, constraint_expr);
        if (!memoryIsByte(res_column_uint8->getRawDataBegin<1>(), res_column_uint8->byteSize(), 0x1))
        {
            auto indices_wrong = findAllWrong(res_column_uint8->getRawDataBegin<1>(), res_column_uint8->byteSize());
            std::string indices_str = "{";
            for (size_t j = 0; j < indices_wrong.size(); ++j)
            {
                indices_str += std::to_string(indices_wrong[j]);
                indices_str += (j != indices_wrong.size() - 1) ? ", " : "}";
            }

            throw Exception{"Violated constraint " + constraints.constraints[i]->name +
                            " in table " + table + " at indices " + indices_str + ", constraint expression: " +
                            serializeAST(*(constraints.constraints[i]->expr), true), ErrorCodes::VIOLATED_CONSTRAINT};
        }
    }
    output->write(block);
    rows_written += block.rows();
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

const ColumnUInt8 *CheckConstraintsBlockOutputStream::executeOnBlock(
        Block & block,
        const ExpressionActionsPtr & constraint)
{
    constraint->execute(block);
    ColumnWithTypeAndName res_column = block.safeGetByPosition(block.columns() - 1);
    return checkAndGetColumn<ColumnUInt8>(res_column.column.get());
}

std::vector<size_t> CheckConstraintsBlockOutputStream::findAllWrong(const void *data, size_t size)
{
    std::vector<size_t> res;

    if (size == 0)
        return res;

    auto ptr = reinterpret_cast<const uint8_t *>(data);

    for (size_t i = 0; i < size; ++i)
    {
        if (*(ptr + i) == 0x0)
        {
            res.push_back(i);
        }
    }

    return res;
}
}
