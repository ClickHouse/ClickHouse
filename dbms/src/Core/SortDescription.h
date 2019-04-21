#pragma once

#include <vector>
#include <memory>
#include <cstddef>
#include <string>


class Collator;

namespace DB
{

/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    std::string column_name; /// The name of the column.
    size_t column_number;    /// Column number (used if no name is given).
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
                             /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    std::shared_ptr<Collator> collator; /// Collator for locale-specific comparison of strings
    bool with_fill;          /// If true, all missed values in range [FROM, TO] will be filled
                             /// Range [FROM, TO] respects sorting direction
    double fill_from;             /// Value >= FROM
    double fill_to;               /// Value + STEP <= TO
    double fill_step;             /// Default = 1

    SortColumnDescription(
            size_t column_number_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr, bool with_fill_ = false,
            double fill_from_ = 0, double fill_to_ = 0, double fill_step_ = 0)
            : column_number(column_number_), direction(direction_), nulls_direction(nulls_direction_), collator(collator_)
            , with_fill(with_fill_), fill_from(fill_from_), fill_to(fill_to_), fill_step(fill_step_) {}

    SortColumnDescription(
            const std::string & column_name_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr, bool with_fill_ = false,
            double fill_from_ = 0, double fill_to_ = 0, double fill_step_ = 0)
            : column_name(column_name_), column_number(0), direction(direction_), nulls_direction(nulls_direction_)
            , collator(collator_), with_fill(with_fill_), fill_from(fill_from_), fill_to(fill_to_), fill_step(fill_step_) {}

    bool operator == (const SortColumnDescription & other) const
    {
        return column_name == other.column_name && column_number == other.column_number
            && direction == other.direction && nulls_direction == other.nulls_direction;
    }

    bool operator != (const SortColumnDescription & other) const
    {
        return !(*this == other);
    }
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

}
