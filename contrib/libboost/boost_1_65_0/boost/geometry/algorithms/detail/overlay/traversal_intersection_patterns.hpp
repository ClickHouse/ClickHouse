// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2017 Barend Gehrels, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_TRAVERSAL_INTERSECTION_PATTERNS_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_TRAVERSAL_INTERSECTION_PATTERNS_HPP

#include <cstddef>
#include <vector>

#include <boost/geometry/algorithms/detail/overlay/aggregate_operations.hpp>
#include <boost/geometry/algorithms/detail/overlay/sort_by_side.hpp>

namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace overlay
{

inline bool check_pairs(std::vector<sort_by_side::rank_with_rings> const& aggregation,
                        signed_size_type incoming_region_id,
                        std::size_t first, std::size_t last)
{
    // Check if pairs 1,2 (and possibly 3,4 and 5,6 etc) satisfy

    for (std::size_t i = first; i <= last; i += 2)
    {
        sort_by_side::rank_with_rings const& curr = aggregation[i];
        sort_by_side::rank_with_rings const& next = aggregation[i + 1];
        int const curr_id = curr.region_id();
        int const next_id = next.region_id();

        bool const possible =
                curr.rings.size() == 2
                && curr.is_isolated()
                && curr.has_unique_region_id()
                && next.rings.size() == 2
                && next.is_isolated()
                && next.has_unique_region_id()
                && curr_id == next_id
                && curr_id != incoming_region_id;

        if (! possible)
        {
            return false;
        }
    }

    return true;
}

inline bool intersection_pattern_common_interior1(std::size_t& selected_rank,
           std::vector<sort_by_side::rank_with_rings> const& aggregation)
{
    // Pattern: coming from exterior ring, encountering an isolated
    // parallel interior ring, which should be skipped, and the first
    // left (normally intersection takes first right) should be taken.
    // Solves cases #case_133_multi
    // and #case_recursive_boxes_49

    std::size_t const n = aggregation.size();
    if (n < 4)
    {
        return false;
    }

    sort_by_side::rank_with_rings const& incoming = aggregation.front();
    sort_by_side::rank_with_rings const& outgoing = aggregation.back();

    bool const incoming_ok =
        incoming.all_from()
        && incoming.rings.size() == 1
        && incoming.has_only(operation_intersection);

    if (! incoming_ok)
    {
        return false;
    }

    bool const outgoing_ok =
        outgoing.all_to()
        && outgoing.rings.size() == 1
        && outgoing.has_only(operation_intersection)
        && outgoing.region_id() == incoming.region_id();

    if (! outgoing_ok)
    {
        return false;
    }

    if (check_pairs(aggregation, incoming.region_id(), 1, n - 2))
    {
        selected_rank = n - 1;
        return true;
    }
    return false;
}

inline bool intersection_pattern_common_interior2(std::size_t& selected_rank,
           std::vector<sort_by_side::rank_with_rings> const& aggregation)
{
    // Pattern: coming from two exterior rings, encountering two isolated
    // equal interior rings

    // See (for example, for ii) #case_recursive_boxes_53:

    // INCOMING:
    // Rank 0  {11[0] (s:0, m:0) i F rgn: 1 ISO}             {13[1] (s:1, m:0) i F rgn: 1 ISO}

    // PAIR:
    // Rank 1  {13[0] (s:0, r:1, m:0) i T rgn: 3 ISO ->16}   {11[1] (s:1, r:5, m:0) i T rgn: 3 ISO ->16}
    // Rank 2  {13[0] (s:0, r:1, m:0) i F rgn: 3 ISO}        {11[1] (s:1, r:5, m:0) i F rgn: 3 ISO}

    // LEAVING (in the same direction, take last one)
    // Rank 3  {11[0] (s:0, m:0) i T rgn: 1 ISO ->10}        {13[1] (s:1, m:0) i T rgn: 1 ISO ->10}


    std::size_t const n = aggregation.size();
    if (n < 4)
    {
        return false;
    }

    sort_by_side::rank_with_rings const& incoming = aggregation.front();
    sort_by_side::rank_with_rings const& outgoing = aggregation.back();

    bool const incoming_ok =
        incoming.all_from()
        && incoming.rings.size() == 2
        && incoming.has_unique_region_id();

    if (! incoming_ok)
    {
        return false;
    }

    bool const outgoing_ok =
        outgoing.all_to()
        && outgoing.rings.size() == 2
        && outgoing.has_unique_region_id()
        && outgoing.region_id() == incoming.region_id();

    if (! outgoing_ok)
    {
        return false;
    }

    bool const operation_ok =
            (incoming.has_only(operation_continue) && outgoing.has_only(operation_continue))
             || (incoming.has_only(operation_intersection) && outgoing.has_only(operation_intersection));

    if (! operation_ok)
    {
        return false;
    }

    // Check if pairs 1,2 (and possibly 3,4 and 5,6 etc) satisfy
    if (check_pairs(aggregation, incoming.region_id(), 1, n - 2))
    {
        selected_rank = n - 1;
        return true;
    }
    return false;
}

inline bool intersection_pattern_common_interior3(std::size_t& selected_rank,
           std::vector<sort_by_side::rank_with_rings> const& aggregation)
{
    // Pattern: approaches colocated turn (exterior+interior) from two
    // different directions, and both leaves in the same direction

    // See #case_136_multi:
    // INCOMING:
    //Rank 0  {10[0] (s:0, m:0) c F rgn: 1 ISO}

    // PAIR:
    //Rank 1  {14[0] (s:0, r:0, m:0) i T rgn: 2 ISO ->16} {11[1] (s:1, r:1, m:0) i T rgn: 2 ISO ->16}
    //Rank 2  {14[0] (s:0, r:0, m:0) i F rgn: 2 ISO}      {11[1] (s:1, r:1, m:0) i F rgn: 2 ISO}

    // LEAVING (select this one):
    //Rank 3  {10[0] (s:0, m:0) c T rgn: 1 ISO ->12}      {10[1] (s:1, m:0) c T rgn: 1 ISO ->12}

    // ADDITIONALLY: (other polygon coming in)
    //Rank 4  {10[1] (s:1, m:0) c F rgn: 1 ISO}

    std::size_t const n = aggregation.size();
    if (n < 4)
    {
        return false;
    }

    sort_by_side::rank_with_rings const& incoming = aggregation.front();
    sort_by_side::rank_with_rings const& outgoing = aggregation[n - 2];
    sort_by_side::rank_with_rings const& last = aggregation.back();

    bool const incoming_ok =
        incoming.all_from()
        && incoming.rings.size() == 1
        && incoming.has_only(operation_continue);

    if (! incoming_ok)
    {
        return false;
    }

    bool const outgoing_ok =
        outgoing.all_to()
        && outgoing.rings.size() == 2
        && outgoing.has_only(operation_continue)
        && outgoing.has_unique_region_id()
        && outgoing.region_id() == incoming.region_id()
        && last.all_from()
        && last.rings.size() == 1
        && last.region_id() == incoming.region_id()
        && last.all_from();

    if (! outgoing_ok)
    {
        return false;
    }

    // Check if pairs 1,2 (and possibly 3,4 and 5,6 etc) satisfy
    if (check_pairs(aggregation, incoming.region_id(), 1, n - 3))
    {
        selected_rank = n - 2;
        return true;
    }
    return false;
}


inline bool intersection_pattern_common_interior4(std::size_t& selected_rank,
           std::vector<sort_by_side::rank_with_rings> const& aggregation)
{
    // Pattern: approaches colocated turn (exterior+interior) from same
    // direction, but leaves in two different directions

    // See #case_137_multi:

    // INCOMING:
    //Rank 0  {11[0] (s:0, m:0) i F rgn: 1 ISO}            {10[1] (s:1, m:0) i F rgn: 1 ISO}

    // PAIR:
    //Rank 1  {13[0] (s:0, r:0, m:0) i T rgn: 2 ISO ->15}  {11[1] (s:1, r:1, m:0) i T rgn: 2 ISO ->15}
    //Rank 2  {13[0] (s:0, r:0, m:0) i F rgn: 2 ISO}       {11[1] (s:1, r:1, m:0) i F rgn: 2 ISO}

    // LEAVING (in two different directions, take last one)
    //Rank 3  {10[1] (s:1, m:0) i T rgn: 1 ISO ->0}
    //Rank 4  {11[0] (s:0, m:0) i T rgn: 1 ISO ->12}

    std::size_t const n = aggregation.size();
    if (n < 4)
    {
        return false;
    }

    sort_by_side::rank_with_rings const& incoming = aggregation.front();
    sort_by_side::rank_with_rings const& extra = aggregation[n - 2];
    sort_by_side::rank_with_rings const& outgoing = aggregation.back();

    bool const incoming_ok =
        incoming.all_from()
        && incoming.rings.size() == 2
        && incoming.has_unique_region_id()
        && incoming.has_only(operation_intersection);

    if (! incoming_ok)
    {
        return false;
    }

    bool const outgoing_ok =
        outgoing.all_to()
        && outgoing.rings.size() == 1
        && outgoing.has_only(operation_intersection)
        && outgoing.region_id() == incoming.region_id()
        && extra.all_to()
        && extra.rings.size() == 1
        && extra.has_only(operation_intersection)
        && extra.region_id() == incoming.region_id();

    if (! outgoing_ok)
    {
        return false;
    }

    // Check if pairs 1,2 (and possibly 3,4 and 5,6 etc) satisfy
    if (check_pairs(aggregation, incoming.region_id(), 1, n - 3))
    {
        selected_rank = n - 1;
        return true;
    }
    return false;
}

}} // namespace detail::overlay
#endif // DOXYGEN_NO_DETAIL

}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_TRAVERSAL_INTERSECTION_PATTERNS_HPP
