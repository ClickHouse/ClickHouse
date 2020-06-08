#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitors.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
}


CheckConstraintsBlockOutputStream::CheckConstraintsBlockOutputStream(
    const StorageID & table_id_,
    const BlockOutputStreamPtr & output_,
    const Block & header_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : table_id(table_id_),
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
        Block block_to_calculate = block;
        for (size_t i = 0; i < expressions.size(); ++i)
        {
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

                Names related_columns = constraint_expr->getRequiredColumns();

                std::stringstream exception_message;

                auto * constraint_ptr = constraints.constraints[i]->as<ASTConstraintDeclaration>();
                exception_message << "Constraint " << backQuote(constraint_ptr->name)
                    << " for table " << table_id.getNameForLogs()
                    << " is violated at row " << (rows_written + row_idx + 1)
                    << ". Expression: (" << serializeAST(*(constraint_ptr->expr), true) << ")"
                    << ". Column values";

                bool first = true;
                for (const auto & name : related_columns)
                {
                    const IColumn & column = *block.getByName(name).column;
                    assert(row_idx < column.size());

                    exception_message << (first ? ": " : ", ")
                        << backQuoteIfNeed(name) << " = " << applyVisitor(FieldVisitorToString(), column[row_idx]);

                    first = false;
                }

                throw Exception{exception_message.str(), ErrorCodes::VIOLATED_CONSTRAINT};
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
