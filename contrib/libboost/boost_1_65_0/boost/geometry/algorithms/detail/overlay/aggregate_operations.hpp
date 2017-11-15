// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2016 Barend Gehrels, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_AGGREGATE_OPERATIONS_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_AGGREGATE_OPERATIONS_HPP

#include <set>

#include <boost/geometry/algorithms/detail/overlay/sort_by_side.hpp>

namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace overlay { namespace sort_by_side
{

struct ring_with_direction
{
    ring_identifier ring_id;
    direction_type direction;

    std::size_t turn_index;
    int operation_index;
    operation_type operation;
    signed_size_type region_id;
    bool isolated;

    inline bool operator<(ring_with_direction const& other) const
    {
        return this->ring_id != other.ring_id
                ? this->ring_id < other.ring_id
                : this->direction < other.direction;
    }

    ring_with_direction()
        : direction(dir_unknown)
        , turn_index(-1)
        , operation_index(0)
        , operation(operation_none)
        , region_id(-1)
        , isolated(false)
    {}
};

struct rank_with_rings
{
    std::size_t rank;
    std::set<ring_with_direction> rings;

    rank_with_rings()
        : rank(0)
    {
    }

    inline bool all_equal(direction_type dir_type) const
    {
        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            if (it->direction != dir_type)
            {
                return false;
            }
        }
        return true;
    }

    inline bool all_to() const
    {
        return all_equal(sort_by_side::dir_to);
    }

    inline bool all_from() const
    {
        return all_equal(sort_by_side::dir_from);
    }

    inline bool has_only(operation_type op) const
    {
        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            const ring_with_direction& rwd = *it;
            if (rwd.operation != op)
            {
                return false;
            }
        }
        return true;
    }

    //! Check if set has both op1 and op2, but no others
    inline bool has_only_both(operation_type op1, operation_type op2) const
    {
        bool has1 = false;
        bool has2 = false;
        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            const ring_with_direction& rwd = *it;

            if (rwd.operation == op1) { has1 = true; }
            else if (rwd.operation == op2) { has2 = true; }
            else { return false; }
        }
        return has1 && has2;
    }

    inline bool is_isolated() const
    {
        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            const ring_with_direction& rwd = *it;
            if (! rwd.isolated)
            {
                return false;
            }
        }
        return true;
    }

    inline bool has_unique_region_id() const
    {
        int region_id = -1;
        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            const ring_with_direction& rwd = *it;
            if (region_id == -1)
            {
                region_id = rwd.region_id;
            }
            else if (rwd.region_id != region_id)
            {
                return false;
            }
        }
        return true;
    }

    inline int region_id() const
    {
        int region_id = -1;
        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            const ring_with_direction& rwd = *it;
            if (region_id == -1)
            {
                region_id = rwd.region_id;
            }
            else if (rwd.region_id != region_id)
            {
                return -1;
            }
        }
        return region_id;
    }

    template <typename Turns>
    inline bool traversable(Turns const& turns) const
    {
        typedef typename boost::range_value<Turns>::type turn_type;
        typedef typename turn_type::turn_operation_type turn_operation_type;

        for (std::set<ring_with_direction>::const_iterator it = rings.begin();
             it != rings.end(); ++it)
        {
            const ring_with_direction& rwd = *it;
            turn_type const& turn = turns[rwd.turn_index];
            turn_operation_type const& op = turn.operations[rwd.operation_index];

            // TODO: this is still necessary, but makes it order-dependent
            // which should not be done.

            // This would obsolete the whole function and should be solved
            // in a different way
            if (op.visited.finalized() || op.visited.visited())
            {
                return false;
            }
        }
        return true;
    }

};

template <typename Sbs, typename Turns>
inline void aggregate_operations(Sbs const& sbs, std::vector<rank_with_rings>& aggregation,
                                 Turns const& turns,
                                 operation_type target_operation)
{
    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::turn_operation_type turn_operation_type;

    aggregation.clear();
    for (std::size_t i = 0; i < sbs.m_ranked_points.size(); i++)
    {
        typename Sbs::rp const& ranked_point = sbs.m_ranked_points[i];

        turn_type const& turn = turns[ranked_point.turn_index];

        turn_operation_type const& op = turn.operations[ranked_point.operation_index];

        if (! ((target_operation == operation_union && ranked_point.rank == 0)
               || op.operation == target_operation
               || op.operation == operation_continue
               || (op.operation == operation_blocked && ranked_point.direction == dir_from)))
        {
            // Always take rank 0 (because self-turns are blocked)
            // Don't consider union/blocked (aggregate is only used for intersections)
            // Blocked is allowed for from
            continue;
        }

        if (aggregation.empty() || aggregation.back().rank != ranked_point.rank)
        {
            rank_with_rings current;
            current.rank = ranked_point.rank;
            aggregation.push_back(current);
        }

        ring_with_direction rwd;
        segment_identifier const& sid = ranked_point.seg_id;

        rwd.ring_id = ring_identifier(sid.source_index, sid.multi_index, sid.ring_index);
        rwd.direction = ranked_point.direction;
        rwd.turn_index = ranked_point.turn_index;
        rwd.operation_index = ranked_point.operation_index;
        rwd.operation = op.operation;
        rwd.region_id = op.enriched.region_id;
        rwd.isolated = op.enriched.isolated;

        aggregation.back().rings.insert(rwd);
    }
}


}}} // namespace detail::overlay::sort_by_side
#endif //DOXYGEN_NO_DETAIL


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_AGGREGATE_OPERATIONS_HPP
