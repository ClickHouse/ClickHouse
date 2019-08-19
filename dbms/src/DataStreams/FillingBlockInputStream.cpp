#include "FillingBlockInputStream.h"
#include <Common/FieldVisitors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

static bool less(const Field & lhs, const Field & rhs, int direction)
{
    if (direction == -1)
        return applyVisitor(FieldVisitorAccurateLess(), rhs, lhs);

    return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs);
}

static bool equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }

FillingRow::FillingRow(const SortDescription & description_) : description(description_)
{
    row.resize(description.size());
}

bool FillingRow::next(const FillingRow & to_row)
{
    size_t pos = 0;
    for (; pos < row.size(); ++pos)
        if (!row[pos].isNull() && !to_row[pos].isNull() && !equals(row[pos], to_row[pos]))
            break;

    if (pos == row.size() || less(to_row[pos], row[pos], getDirection(pos)))
        return false;

    for (size_t i = row.size() - 1; i > pos; --i)
    {
        if (getFillDescription(i).fill_to.isNull() || row[i].isNull())
            continue;

        auto next_value = row[i];
        applyVisitor(FieldVisitorSum(getFillDescription(i).fill_step), next_value);
        if (less(next_value, getFillDescription(i).fill_to, getDirection(i)))
        {
            initFromDefaults(i + 1);
            row[i] = next_value;
            return true;
        }
    }

    auto next_value = row[pos];
    applyVisitor(FieldVisitorSum(getFillDescription(pos).fill_step), next_value);

    if (equals(next_value, to_row[pos]))
    {
        bool is_less = false;
        for (size_t i = pos + 1; i < row.size(); ++i)
        {
            const auto & fill_from = getFillDescription(i).fill_from;
            if (!fill_from.isNull() && less(fill_from, to_row[i], getDirection(i)))
            {
                is_less = true;
                row[i] = fill_from;
            }
            else
                row[i] = to_row[i];
        }

        row[pos] = next_value;
        return is_less;
    }

    if (less(next_value, to_row[pos], getDirection(pos)))
    {
        initFromDefaults(pos + 1);
        row[pos] = next_value;
        return true;
    }

    return false;
}

void FillingRow::initFromColumns(const Columns & columns, size_t row_num, size_t from_pos, bool ignore_default_from)
{
    for (size_t i = from_pos; i < columns.size(); ++i)
    {
        columns[i]->get(row_num, row[i]);
        const auto & fill_from = getFillDescription(i).fill_from;
        if (!ignore_default_from && !fill_from.isNull() && less(fill_from, row[i], getDirection(i)))
            row[i] = fill_from;
    }
}

void FillingRow::initFromDefaults(size_t from_pos)
{
    for (size_t i = from_pos; i < row.size(); ++i)
        row[i] = getFillDescription(i).fill_from;
}


static void insertFromFillingRow(MutableColumns & filling_columns, MutableColumns & other_columns, const FillingRow & filling_row)
{
    for (size_t i = 0; i < filling_columns.size(); ++i)
    {
        if (filling_row[i].isNull())
            filling_columns[i]->insertDefault();
        else
            filling_columns[i]->insert(filling_row[i]);
    }

    for (size_t i = 0; i < other_columns.size(); ++i)
        other_columns[i]->insertDefault();

}

static void copyRowFromColumns(MutableColumns & dest, const Columns & source, size_t row_num)
{
    for (size_t i = 0; i < source.size(); ++i)
        dest[i]->insertFrom(*source[i], row_num);
}


FillingBlockInputStream::FillingBlockInputStream(
        const BlockInputStreamPtr & input, const SortDescription & fill_description_)
        : fill_description(fill_description_), filling_row(fill_description_), next_row(fill_description_)
{
    children.push_back(input);
    header = children.at(0)->getHeader();

    std::vector<bool> is_fill_column(header.columns());
    for (const auto & elem : fill_description)
    {
        size_t pos = header.getPositionByName(elem.column_name);
        fill_column_positions.push_back(pos);
        is_fill_column[pos] = true;
    }

    for (size_t i = 0; i < header.columns(); ++i)
    {
        if (is_fill_column[i])
        {
            if (!isNumber(header.getByPosition(i).type))
                throw Exception("WITH FILL can be used only with numeric types, but is set for column with type "
                    + header.getByPosition(i).type->getName(), ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
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

        bool generated = false;
        for (size_t i = 0; i < filling_row.size(); ++i)
            next_row[i] = filling_row.getFillDescription(i).fill_to;

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
        filling_row.initFromColumns(old_fill_columns, 0);
        for (size_t i = 0; i < filling_row.size(); ++i)
        {
            if (less(filling_row[i], (*old_fill_columns[i])[0], filling_row.getDirection(i)))
            {
                insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);
                break;
            }
        }

        first = false;
    }

    for (size_t row_ind = 0; row_ind < rows; ++row_ind)
    {
        next_row.initFromColumns(old_fill_columns, row_ind, 0, true);

        /// Comment
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
