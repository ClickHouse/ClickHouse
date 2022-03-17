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


FillingRow::FillingRow(const SortDescription & sort_description_, const InterpolateDescription & interpolate_description_)
    : sort{*this}
    , interpolate{*this}
    , sort_description(sort_description_)
    , interpolate_description(interpolate_description_)
{
    row.resize(sort_description.size() + interpolate_description.size());
}

bool FillingRow::operator<(const FillingRow & other) const
{
    for (size_t i = 0; i < sort.size(); ++i)
    {
        if (sort[i].isNull() || other.sort[i].isNull() || equals(sort[i], other.sort[i]))
            continue;
        return less(sort[i], other.sort[i], getDirection(i));
    }
    return false;
}

bool FillingRow::operator==(const FillingRow & other) const
{
    for (size_t i = 0; i < sort.size(); ++i)
        if (!equals(sort[i], other.sort[i]))
            return false;
    return true;
}

bool FillingRow::next(const FillingRow & to_row)
{
    size_t pos = 0;

    for(size_t i = 0; i < to_row.interpolate.size(); ++i) {
        std::cout << to_row.interpolate[i] <<" : ";
        interpolate[i] = to_row.interpolate[i];
    }

    /// Find position we need to increment for generating next row.
    for (; pos < sort.size(); ++pos)
        if (!sort[pos].isNull() && !to_row.sort[pos].isNull() && !equals(sort[pos], to_row.sort[pos]))
            break;

    if (pos == sort.size() || less(to_row.sort[pos], sort[pos], getDirection(pos)))
        return false;

    /// If we have any 'fill_to' value at position greater than 'pos',
    ///  we need to generate rows up to 'fill_to' value.
    for (size_t i = sort.size() - 1; i > pos; --i)
    {
        if (getFillDescription(i).fill_to.isNull() || sort[i].isNull())
            continue;

        auto next_value = sort[i];
        getFillDescription(i).step_func(next_value);
        if (less(next_value, getFillDescription(i).fill_to, getDirection(i)))
        {
            sort[i] = next_value;
            initFromDefaults(i + 1);
            return true;
        }
    }

    auto next_value = sort[pos];
    getFillDescription(pos).step_func(next_value);

    if (less(to_row.sort[pos], next_value, getDirection(pos)))
        return false;

    sort[pos] = next_value;
    if (equals(sort[pos], to_row.sort[pos]))
    {
        bool is_less = false;
        for (size_t i = pos + 1; i < sort.size(); ++i)
        {
            const auto & fill_from = getFillDescription(i).fill_from;
            if (!fill_from.isNull())
                sort[i] = fill_from;
            else
                sort[i] = to_row.sort[i];
            is_less |= less(sort[i], to_row.sort[i], getDirection(i));
        }

        return is_less;
    }

    initFromDefaults(pos + 1);
    return true;
}

void FillingRow::initFromDefaults(size_t from_pos)
{
    for (size_t i = from_pos; i < sort.size(); ++i)
        sort[i] = getFillDescription(i).fill_from;
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
