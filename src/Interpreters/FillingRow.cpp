#include <Interpreters/FillingRow.h>
#include <Common/FieldVisitorsAccurateComparison.h>


namespace DB
{

bool less(const Field & lhs, const Field & rhs, int direction)
{
    if (direction == -1)
        return applyVisitor(FieldVisitorAccurateLess(), rhs, lhs);

    return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs);
}

bool equals(const Field & lhs, const Field & rhs)
{
    return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs);
}


FillingRow::FillingRow(const SortDescription & sort_description_, InterpolateDescriptionPtr interpolate_description_)
    : sort_description(sort_description_)
    , interpolate_description(interpolate_description_)
{
    std::unordered_set<std::string> col_set;
    for (const auto & col : sort_description)
        col_set.insert(col.column_name);
    if (interpolate_description)
        for (const auto & name : interpolate_description->columns_full_set)
            col_set.insert(name);
    row.resize(col_set.size());
}

bool FillingRow::operator<(const FillingRow & other) const
{
    for (size_t i = 0; i < sort_description.size(); ++i)
    {
        if (row[i].isNull() || other.row[i].isNull() || equals(row[i], other.row[i]))
            continue;
        return less(row[i], other.row[i], getDirection(i));
    }
    return false;
}

bool FillingRow::operator==(const FillingRow & other) const
{
    for (size_t i = 0; i < sort_description.size(); ++i)
        if (!equals(row[i], other.row[i]))
            return false;
    return true;
}

bool FillingRow::next(const FillingRow & to_row)
{
    size_t pos = 0;

    /// Find position we need to increment for generating next row.
    for (; pos < size(); ++pos)
        if (!row[pos].isNull() && !to_row.row[pos].isNull() && !equals(row[pos], to_row.row[pos]))
            break;

    if (pos == size() || less(to_row.row[pos], row[pos], getDirection(pos)))
        return false;

    /// If we have any 'fill_to' value at position greater than 'pos',
    ///  we need to generate rows up to 'fill_to' value.
    for (size_t i = size() - 1; i > pos; --i)
    {
        if (getFillDescription(i).fill_to.isNull() || row[i].isNull())
            continue;

        auto next_value = row[i];
        getFillDescription(i).step_func(next_value);
        if (less(next_value, getFillDescription(i).fill_to, getDirection(i)))
        {
            row[i] = next_value;
            initFromDefaults(i + 1);
            return true;
        }
    }

    auto next_value = row[pos];
    getFillDescription(pos).step_func(next_value);

    if (less(to_row.row[pos], next_value, getDirection(pos)))
        return false;

    row[pos] = next_value;
    if (equals(row[pos], to_row.row[pos]))
    {
        bool is_less = false;
        size_t i = pos + 1;
        for (; i < size(); ++i)
        {
            const auto & fill_from = getFillDescription(i).fill_from;
            if (!fill_from.isNull())
                row[i] = fill_from;
            else
                row[i] = to_row.row[i];
            is_less |= less(row[i], to_row.row[i], getDirection(i));
        }

        return is_less;
    }

    initFromDefaults(pos + 1);
    return true;
}

void FillingRow::initFromDefaults(size_t from_pos)
{
    for (size_t i = from_pos; i < sort_description.size(); ++i)
        row[i] = getFillDescription(i).fill_from;
}

void FillingRow::interpolate()
{
    if (!interpolate_description)
        return;

    Block block;
    if (interpolate_description->input_map.size())
    {
        for (const auto & [row_pos, name_type] : interpolate_description->input_map)
        {
            if (row[row_pos].isNull())
                return;
            block.insert({name_type.type->createColumnConst(1, row[row_pos]), name_type.type, name_type.name});
        }
    }
    else /// INTERPOLATE contains only constants
    {
        /// dirty hack - we need at least one column with one row to execute actions on block
        DataTypePtr dt = std::make_shared<DataTypeUInt64>();
        block.insert({dt->createColumnConst(1, dt->getDefault()), dt, "dummy"});
    }

    interpolate_description->actions->execute(block);

    for (auto [col_pos, row_pos] : interpolate_description->output_map)
        block.getByPosition(col_pos).column->get(0, row[row_pos]);

    for (const auto & [row_pos, col_type] : interpolate_description->reset_map)
        row[row_pos] = col_type->getDefault();
}


void insertFromFillingRow(MutableColumns & filling_columns, MutableColumns & other_columns, const FillingRow & filling_row)
{
    for (size_t i = 0; i < filling_columns.size(); ++i)
    {
        if (filling_row[i].isNull())
            filling_columns[i]->insertDefault();
        else
            filling_columns[i]->insert(filling_row[i]);
    }

    for (const auto & other_column : other_columns)
        other_column->insertDefault();
}

void copyRowFromColumns(MutableColumns & dest, const Columns & source, size_t row_num)
{
    for (size_t i = 0; i < source.size(); ++i)
        dest[i]->insertFrom(*source[i], row_num);
}

}
