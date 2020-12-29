#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitors.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
    extern const int LOGICAL_ERROR;
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

            auto * constraint_ptr = constraints.constraints[i]->as<ASTConstraintDeclaration>();

            ColumnWithTypeAndName res_column = block_to_calculate.getByPosition(block_to_calculate.columns() - 1);

            if (!isUInt8(res_column.type))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Constraint {} does not return a value of type UInt8",
                    backQuote(constraint_ptr->name));

            if (const ColumnConst * res_const = typeid_cast<const ColumnConst *>(res_column.column.get()))
            {
                UInt8 value = res_const->getValue<UInt64>();

                /// Is violated.
                if (!value)
                {
                    std::stringstream exception_message;

                    exception_message << "Constraint " << backQuote(constraint_ptr->name)
                        << " for table " << table_id.getNameForLogs()
                        << " is violated, because it is a constant expression returning 0."
                        << " It is most likely an error in table definition.";

                    throw Exception{exception_message.str(), ErrorCodes::VIOLATED_CONSTRAINT};
                }
            }
            else
            {
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
