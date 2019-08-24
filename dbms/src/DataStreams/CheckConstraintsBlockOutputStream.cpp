#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <Parsers/formatAST.h>
#include <Columns/ColumnsCommon.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
}


CheckConstraintsBlockOutputStream::CheckConstraintsBlockOutputStream(
    const String & table_,
    const BlockOutputStreamPtr & output_,
    const Block & header_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : table(table_),
    output(output_),
    header(header_),
    constraints(constraints_),
    expressions(constraints_.getExpressions(context_, header.getNamesAndTypesList()))
{
}


void CheckConstraintsBlockOutputStream::write(const Block & block)
{
    if (block.rows() > 0)
    {
        std::cerr << "Checking " << expressions.size() << " constraints\n";
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            std::cerr << serializeAST(*(constraints.constraints[i]->expr), true) << "\n";

            Block block_to_calculate = block;
            auto constraint_expr = expressions[i];

            constraint_expr->execute(block_to_calculate);
            ColumnWithTypeAndName res_column = block_to_calculate.getByPosition(block_to_calculate.columns() - 1);
            const ColumnUInt8 & res_column_uint8 = assert_cast<const ColumnUInt8 &>(*res_column.column);

            const UInt8 * data = res_column_uint8.getData().data();
            size_t size = res_column_uint8.size();

            /// Is violated.
            if (!memoryIsByte(data, size, 1))
            {
                size_t row_idx = 0;
                for (; row_idx < size; ++row_idx)
                    if (data[row_idx] != 1)
                        break;

                throw Exception{"Violated constraint " + constraints.constraints[i]->name +
                                " in table " + table + " at row " + std::to_string(rows_written + row_idx + 1) + ", constraint expression: " +
                                serializeAST(*(constraints.constraints[i]->expr), true), ErrorCodes::VIOLATED_CONSTRAINT};
            }
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

}
