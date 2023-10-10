#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/SortProperty.h>

namespace DB
{

class CalculateSortProp
{
public:
    CalculateSortProp(const ActionsDAGPtr & actions_, const SortProperty & input_sort_property_);

    SortProperty calcSortProp();

private:

    SortProperty calcOneSortProp(const SortDesc & input_sort_description);

    struct MonotonicityState
    {
        /// One of {-1, 0, 1} - direction of the match. 0 means - doesn't match.
        int direction = 0;
        /// If true then current key must be the last in the matched prefix of sort description.
        bool need_is_last_key = false;

        MonotonicityState() = default;

        MonotonicityState(int direction_, bool need_is_last_key_) : direction(direction_), need_is_last_key(need_is_last_key_) { }

        operator bool() const
        {
            return direction != 0;
        }
    };

    struct FindResult : public MonotonicityState
    {
        size_t column_index;

        FindResult() = default;

        FindResult(size_t column_index_, int direction_, bool need_is_last_key_)
            : MonotonicityState(direction_, need_is_last_key_), column_index(column_index_)
        {
        }
    };

    CalculateSortProp::FindResult tryFindInputSortColumn(const ActionsDAG::Node * node, MonotonicityState & last_state, std::unordered_map<String, size_t> & input_sort_column_index, const SortDesc & input_sort_description);

    const ActionsDAGPtr & actions;
    const SortProperty & input_sort_property;
};

}

