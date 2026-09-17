#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Transforms/CheckConstraintsTransform.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/UTF8Helpers.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ConstraintsDescription.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/FailPoint.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
    extern const int UNSUPPORTED_METHOD;
}

namespace FailPoints
{
    extern const char check_constraints_transform_before_expression_pause[];
    extern const char check_constraints_transform_pause[];
    extern const char check_constraints_transform_between_constraints_pause[];
}


CheckConstraintsTransform::CheckConstraintsTransform(
    const StorageID & table_id_,
    SharedHeader header,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : ExceptionKeepingTransform(header, header)
    , table_id(table_id_)
    , constraints_to_check(constraints_.filterConstraints(ConstraintsDescription::ConstraintType::CHECK))
    , expressions(constraints_.getExpressions(context_, header->getNamesAndTypesList()))
    , context(std::move(context_))
{
}


void CheckConstraintsTransform::onCancel() noexcept
{
    ExceptionKeepingTransform::onCancel();

    /// A `CHECK` constraint can contain an arbitrarily long-running function, so cancellation has
    /// to reach the function that is being evaluated right now.
    for (const auto & expression : expressions)
        for (const auto & node : expression->getNodes())
            if (node.type == ActionsDAG::ActionType::FUNCTION && node.function)
                node.function->cancelExecution();
}


void CheckConstraintsTransform::onConsume(Chunk chunk)
{
    if (chunk.getNumRows() > 0)
    {
        FailPointInjection::pauseFailPoint(FailPoints::check_constraints_transform_before_expression_pause);

        /// The task could have been dispatched before the cancellation and picked up after it.
        /// There is nothing to check for a query that is not going to write anything.
        if (isCancelled())
        {
            cur_chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
            return;
        }

        if (rows_written == 0)
            for (const auto & expression : expressions)
                VirtualColumnUtils::buildSetsForDAG(expression->getActionsDAG(), context);

        Block block_to_calculate = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            /// A cancellation could have landed while the previous constraint was being validated.
            /// `ExpressionActions::execute` polls the cancellation flag only after its first action,
            /// so without this guard the next constraint would still evaluate one whole action, and
            /// that action can be an arbitrarily long-running function.
            if (isCancelled())
            {
                cur_chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
                return;
            }

            auto constraint_expr = expressions[i];
            constraint_expr->execute(block_to_calculate, false, false, &getCancellationFlag());

            FailPointInjection::pauseFailPoint(FailPoints::check_constraints_transform_pause);

            /// `execute` stops between actions when cancelled, so the result column of the
            /// constraint is not necessarily there any more.
            if (isCancelled())
            {
                cur_chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
                return;
            }

            auto * constraint_ptr = constraints_to_check[i]->as<ASTConstraintDeclaration>();

            ColumnWithTypeAndName res_column = block_to_calculate.getByName(constraint_ptr->expr->getColumnName());

            auto result_type = removeNullable(removeLowCardinality(res_column.type));

            if (!isUInt8(result_type))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Constraint {} does not return a value of type UInt8",
                    backQuote(constraint_ptr->name));

            auto result_column = res_column.column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();

            /// A constraint is checked row by row: the result is scanned for the first value that is not
            /// 1, and the block's own columns are then read at that index to report the offending row. So
            /// the result has to have exactly as many values as the block has rows. `arrayJoin` is the one
            /// thing that breaks this, and it is rejected when a constraint is declared - but a constraint
            /// stored before that check existed still loads, so the size is verified here rather than
            /// trusted, and a read past the end of a block column is reported instead of performed.
            if (result_column->size() != chunk.getNumRows())
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Constraint {} for table {} returned {} values for a block of {} rows. Expression: ({}). "
                    "An expression that changes the number of rows, such as `arrayJoin`, cannot be checked "
                    "as a constraint; drop the constraint to be able to insert into the table",
                    backQuote(constraint_ptr->name),
                    table_id.getNameForLogs(),
                    result_column->size(),
                    chunk.getNumRows(),
                    constraint_ptr->expr->formatForErrorMessage());

            if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(&*result_column))
            {
                const auto & nested_column = column_nullable->getNestedColumnPtr();

                /// Check if constraint value is nullable
                const auto & null_map = column_nullable->getNullMapColumn();
                const PaddedPODArray<UInt8> & null_map_data = null_map.getData();
                bool null_map_contains_null = !memoryIsZero(null_map_data.raw_data(), 0, null_map_data.size() * sizeof(UInt8));

                if (null_map_contains_null)
                    throw Exception(
                        ErrorCodes::VIOLATED_CONSTRAINT,
                        "Constraint {} for table {} is violated. Expression: ({})."\
                        "Constraint expression returns nullable column that contains null value",
                        backQuote(constraint_ptr->name),
                        table_id.getNameForLogs(),
                        constraint_ptr->expr->formatForErrorMessage());

                result_column = nested_column;
            }

            const ColumnUInt8 & res_column_uint8 = assert_cast<const ColumnUInt8 &>(*result_column);

            const UInt8 * res_data = res_column_uint8.getData().data();
            size_t size = res_column_uint8.size();

            /// Is violated.
            if (!memoryIsByte(res_data, 0, size, 1))
            {
                size_t row_idx = 0;
                for (; row_idx < size; ++row_idx)
                    if (res_data[row_idx] != 1)
                        break;

                Names related_columns = constraint_expr->getRequiredColumns();

                bool first = true;
                String column_values_msg;
                constexpr size_t approx_bytes_for_col = 32;
                column_values_msg.reserve(approx_bytes_for_col * related_columns.size());
                for (const auto & name : related_columns)
                {
                    const IColumn & column = *chunk.getColumns()[getInputPort().getHeader().getPositionByName(name)];
                    chassert(row_idx < column.size());

                    if (!first)
                        column_values_msg.append(", ");
                    column_values_msg.append(backQuoteIfNeed(name));
                    column_values_msg.append(" = ");

                    String value = applyVisitor(FieldVisitorToString(), column[row_idx]);
                    /// Limit the length, as we don't want too long exception messages.
                    static constexpr size_t max_value_length = 100;
                    size_t value_max_bytes = UTF8::computeBytesBeforeWidth(
                        reinterpret_cast<const UInt8 *>(value.data()), value.size(), 0, max_value_length);
                    if (value_max_bytes < value.size())
                    {
                        value.resize(value_max_bytes);
                        value.append("…");
                        /// Cosmetics.
                        if (value.starts_with("'"))
                            value.append("'");
                    }

                    column_values_msg.append(value);
                    first = false;
                }

                throw Exception(
                    ErrorCodes::VIOLATED_CONSTRAINT,
                    "Constraint {} for table {} is violated at row {}. Expression: ({}). Column values: {}",
                    backQuote(constraint_ptr->name),
                    table_id.getNameForLogs(),
                    rows_written + row_idx + 1,
                    constraint_ptr->expr->formatForErrorMessage(),
                    column_values_msg);
            }

            /// The window between two constraints of the same table: a `KILL QUERY` landing here
            /// must not let the next constraint expression start.
            if (i + 1 < expressions.size())
                FailPointInjection::pauseFailPoint(FailPoints::check_constraints_transform_between_constraints_pause);
        }
    }

    rows_written += chunk.getNumRows();
    cur_chunk = std::move(chunk);
}

}
