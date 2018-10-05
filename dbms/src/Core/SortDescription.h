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

    SortColumnDescription(size_t column_number_, int direction_, int nulls_direction_, const std::shared_ptr<Collator> & collator_ = nullptr)
        : column_number(column_number_), direction(direction_), nulls_direction(nulls_direction_), collator(collator_) {}

    SortColumnDescription(const std::string & column_name_, int direction_, int nulls_direction_, const std::shared_ptr<Collator> & collator_ = nullptr)
        : column_name(column_name_), column_number(0), direction(direction_), nulls_direction(nulls_direction_), collator(collator_) {}
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

}

