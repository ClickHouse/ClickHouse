#include <DataStreams/FillingBlockInputStream.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

FillingBlockInputStream::FillingBlockInputStream(
        const BlockInputStreamPtr & input, const SortDescription & sort_description_)
        : sort_description(sort_description_), filling_row(sort_description_), next_row(sort_description_)
{
    children.push_back(input);
    header = children.at(0)->getHeader();

    std::vector<bool> is_fill_column(header.columns());
    for (const auto & elem : sort_description)
        is_fill_column[header.getPositionByName(elem.column_name)] = true;

    auto try_convert_fields = [](FillColumnDescription & descr, const DataTypePtr & type)
    {
        auto max_type = Field::Types::Null;
        WhichDataType which(type);
        DataTypePtr to_type;
        if (isInteger(type) || which.isDateOrDateTime())
        {
            max_type = Field::Types::Int64;
            to_type = std::make_shared<DataTypeInt64>();
        }
        else if (which.isFloat())
        {
            max_type = Field::Types::Float64;
            to_type = std::make_shared<DataTypeFloat64>();
        }

        if (descr.fill_from.getType() > max_type || descr.fill_to.getType() > max_type
            || descr.fill_step.getType() > max_type)
            return false;
        descr.fill_from = convertFieldToType(descr.fill_from, *to_type);
        descr.fill_to = convertFieldToType(descr.fill_to, *to_type);
        descr.fill_step = convertFieldToType(descr.fill_step, *to_type);

        return true;
    };

    for (size_t i = 0; i < header.columns(); ++i)
    {
        if (is_fill_column[i])
        {
            size_t pos = fill_column_positions.size();
            auto & descr = filling_row.getFillDescription(pos);
            auto type = header.getByPosition(i).type;
            if (!try_convert_fields(descr, type))
                throw Exception("Incompatible types of WITH FILL expression values with column type "
                    + type->getName(), ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

            if (type->isValueRepresentedByUnsignedInteger() &&
                ((!descr.fill_from.isNull() && less(descr.fill_from, Field{0}, 1)) ||
                    (!descr.fill_to.isNull() && less(descr.fill_to, Field{0}, 1))))
            {
                throw Exception("WITH FILL bound values cannot be negative for unsigned type "
                    + type->getName(), ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
            }

            fill_column_positions.push_back(i);
        }
        else
            other_column_positions.push_back(i);
    }
}


Block FillingBlockInputStream::readImpl()
{
    Columns old_fill_columns;
    Columns old_other_columns;
    MutableColumns res_fill_columns;
    MutableColumns res_other_columns;

    auto init_columns_by_positions = [](const Block & block, Columns & columns,
        MutableColumns & mutable_columns, const Positions & positions)
    {
        for (size_t pos : positions)
        {
            auto column = block.getByPosition(pos).column;
            columns.push_back(column);
            mutable_columns.push_back(column->cloneEmpty()->assumeMutable());
        }
    };

    auto block = children.back()->read();
    if (!block)
    {
        init_columns_by_positions(header, old_fill_columns, res_fill_columns, fill_column_positions);
        init_columns_by_positions(header, old_other_columns, res_other_columns, other_column_positions);

        bool should_insert_first = next_row < filling_row;

        bool generated = false;
        for (size_t i = 0; i < filling_row.size(); ++i)
            next_row[i] = filling_row.getFillDescription(i).fill_to;

        if (should_insert_first && filling_row < next_row)
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        while (filling_row.next(next_row))
        {
            generated = true;
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);
        }

        if (generated)
            return createResultBlock(res_fill_columns, res_other_columns);

        return block;
    }

    size_t rows = block.rows();
    init_columns_by_positions(block, old_fill_columns, res_fill_columns, fill_column_positions);
    init_columns_by_positions(block, old_other_columns, res_other_columns, other_column_positions);

    if (first)
    {
        for (size_t i = 0; i < filling_row.size(); ++i)
        {
            auto current_value = (*old_fill_columns[i])[0];
            const auto & fill_from = filling_row.getFillDescription(i).fill_from;
            if (!fill_from.isNull() && !equals(current_value, fill_from))
            {
                filling_row.initFromDefaults(i);
                if (less(fill_from, current_value, filling_row.getDirection(i)))
                    insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);
                break;
            }
            filling_row[i] = current_value;
        }
        first = false;
    }

    for (size_t row_ind = 0; row_ind < rows; ++row_ind)
    {
        bool should_insert_first = next_row < filling_row;

        for (size_t i = 0; i < filling_row.size(); ++i)
        {
            auto current_value = (*old_fill_columns[i])[row_ind];
            const auto & fill_to = filling_row.getFillDescription(i).fill_to;

            if (fill_to.isNull() || less(current_value, fill_to, filling_row.getDirection(i)))
                next_row[i] = current_value;
            else
                next_row[i] = fill_to;
        }

        /// A case, when at previous step row was initialized from defaults 'fill_from' values
        ///  and probably we need to insert it to block.
        if (should_insert_first && filling_row < next_row)
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        /// Insert generated filling row to block, while it is less than current row in block.
        while (filling_row.next(next_row))
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        copyRowFromColumns(res_fill_columns, old_fill_columns, row_ind);
        copyRowFromColumns(res_other_columns, old_other_columns, row_ind);
    }

    return createResultBlock(res_fill_columns, res_other_columns);
}

Block FillingBlockInputStream::createResultBlock(MutableColumns & fill_columns, MutableColumns & other_columns) const
{
    MutableColumns result_columns(header.columns());
    for (size_t i = 0; i < fill_columns.size(); ++i)
        result_columns[fill_column_positions[i]] = std::move(fill_columns[i]);
    for (size_t i = 0; i < other_columns.size(); ++i)
        result_columns[other_column_positions[i]] = std::move(other_columns[i]);

    return header.cloneWithColumns(std::move(result_columns));
}

}
