// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2015-2016 Barend Gehrels, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_TRAVERSAL_SWITCH_DETECTOR_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_TRAVERSAL_SWITCH_DETECTOR_HPP

#include <cstddef>

#include <boost/range.hpp>

#include <boost/geometry/algorithms/detail/ring_identifier.hpp>
#include <boost/geometry/algorithms/detail/overlay/copy_segments.hpp>
#include <boost/geometry/algorithms/detail/overlay/cluster_info.hpp>
#include <boost/geometry/algorithms/detail/overlay/is_self_turn.hpp>
#include <boost/geometry/algorithms/detail/overlay/turn_info.hpp>
#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/assert.hpp>

namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace overlay
{

// Generic function (is this used somewhere else too?)
inline ring_identifier ring_id_by_seg_id(segment_identifier const& seg_id)
{
    return ring_identifier(seg_id.source_index, seg_id.multi_index, seg_id.ring_index);
}

template
<
    bool Reverse1,
    bool Reverse2,
    overlay_type OverlayType,
    typename Geometry1,
    typename Geometry2,
    typename Turns,
    typename Clusters,
    typename RobustPolicy,
    typename Visitor
>
struct traversal_switch_detector
{
    enum isolation_type { isolation_unknown = -1, isolation_no = 0, isolation_yes = 1 };

    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::turn_operation_type turn_operation_type;

    // Per ring, first turns are collected (in turn_indices), and later
    // a region_id is assigned
    struct merged_ring_properties
    {
        signed_size_type region_id;
        std::set<signed_size_type> turn_indices;

        merged_ring_properties()
            : region_id(-1)
        {}
    };

    struct connection_properties
    {
        std::size_t count;
        std::set<signed_size_type> cluster_indices;
        connection_properties()
            : count(0)
        {}
    };

    typedef std::map<signed_size_type, connection_properties> connection_map;

    // Per region, a set of properties is maintained, including its connections
    // to other regions
    struct region_properties
    {
        signed_size_type region_id;
        isolation_type isolated;

        // Maps from connected region_id to their properties
        connection_map connected_region_counts;

        region_properties()
            : region_id(-1)
            , isolated(isolation_unknown)
        {}
    };

    // Keeps turn indices per ring
    typedef std::map<ring_identifier, merged_ring_properties > merge_map;
    typedef std::map<signed_size_type, region_properties> region_connection_map;

    typedef std::set<signed_size_type>::const_iterator set_iterator;

    inline traversal_switch_detector(Geometry1 const& geometry1, Geometry2 const& geometry2,
            Turns& turns, Clusters& clusters,
            RobustPolicy const& robust_policy, Visitor& visitor)
        : m_geometry1(geometry1)
        , m_geometry2(geometry2)
        , m_turns(turns)
        , m_clusters(clusters)
        , m_robust_policy(robust_policy)
        , m_visitor(visitor)
    {
    }

    isolation_type get_isolation(region_properties const& properties,
                                 signed_size_type parent_region_id,
                                 const std::set<signed_size_type>& visited)
    {
        if (properties.isolated != isolation_unknown)
        {
            return properties.isolated;
        }

        bool all_colocated = true;
        int unique_cluster_id = -1;
        for (typename connection_map::const_iterator it = properties.connected_region_counts.begin();
             all_colocated && it != properties.connected_region_counts.end(); ++it)
        {
            connection_properties const& cprop = it->second;
            if (cprop.cluster_indices.size() != 1)
            {
                // Either no cluster (non colocated point), or more clusters
                all_colocated = false;
            }
            int const cluster_id = *cprop.cluster_indices.begin();
            if (cluster_id == -1)
            {
                all_colocated = false;
            }
            else if (unique_cluster_id == -1)
            {
                unique_cluster_id = cluster_id;
            }
            else if (unique_cluster_id != cluster_id)
            {
                all_colocated = false;
            }
        }
        if (all_colocated)
        {
            return isolation_yes;
        }


        // It is isolated if there is only one connection, or if there are more connections but all
        // of them are isolated themselves, or if there are more connections
        // but they are all colocated
        std::size_t non_isolation_count = 0;
        bool child_not_isolated = false;
        for (typename connection_map::const_iterator it = properties.connected_region_counts.begin();
             it != properties.connected_region_counts.end(); ++it)
        {
            signed_size_type const region_id = it->first;
            connection_properties const& cprop = it->second;

            if (region_id == parent_region_id)
            {
                // Normal situation, skip its direct parent
                continue;
            }
            if (visited.count(region_id) > 0)
            {
                // Find one of its ancestors again, this is a ring. Not isolated.
                return isolation_no;
            }
            if (cprop.count > 1)
            {
                return isolation_no;
            }

            typename region_connection_map::iterator mit = m_connected_regions.find(region_id);
            if (mit == m_connected_regions.end())
            {
                // Should not occur
                continue;
            }

            std::set<signed_size_type> vis = visited;
            vis.insert(parent_region_id);

            region_properties& prop = mit->second;
            if (prop.isolated == isolation_unknown)
            {
                isolation_type const iso = get_isolation(prop, properties.region_id, vis);
                prop.isolated = iso;
                if (iso == isolation_no)
                {
                    child_not_isolated = true;
                }
            }
            if (prop.isolated == isolation_no)
            {
                non_isolation_count++;
            }
        }

        return child_not_isolated || non_isolation_count > 1 ? isolation_no : isolation_yes;
    }

    void get_isolated_regions()
    {
        for (typename region_connection_map::iterator it = m_connected_regions.begin();
             it != m_connected_regions.end(); ++it)
        {
            region_properties& properties = it->second;
            if (properties.isolated == isolation_unknown)
            {
                std::set<signed_size_type> visited;
                properties.isolated = get_isolation(properties, properties.region_id, visited);
            }
        }
    }

    void assign_isolation()
    {
        for (std::size_t turn_index = 0; turn_index < m_turns.size(); ++turn_index)
        {
            turn_type& turn = m_turns[turn_index];

            for (int op_index = 0; op_index < 2; op_index++)
            {
                turn_operation_type& op = turn.operations[op_index];
                typename region_connection_map::const_iterator mit = m_connected_regions.find(op.enriched.region_id);
                if (mit != m_connected_regions.end())
                {
                    region_properties const& prop = mit->second;
                    op.enriched.isolated = prop.isolated == isolation_yes;
                }
            }
        }
    }

    void assign_regions()
    {
        for (typename merge_map::const_iterator it
             = m_turns_per_ring.begin(); it != m_turns_per_ring.end(); ++it)
        {
            ring_identifier const& ring_id = it->first;
            merged_ring_properties const& properties = it->second;

            for (set_iterator sit = properties.turn_indices.begin();
                 sit != properties.turn_indices.end(); ++sit)
            {
                turn_type& turn = m_turns[*sit];

                for (int i = 0; i < 2; i++)
                {
                    turn_operation_type& op = turn.operations[i];
                    if (ring_id_by_seg_id(op.seg_id) == ring_id)
                    {
                        op.enriched.region_id = properties.region_id;
                    }
                }
                signed_size_type const& id0 = turn.operations[0].enriched.region_id;
                signed_size_type const& id1 = turn.operations[1].enriched.region_id;
                if (id0 != id1 && id0 != -1 && id1 != -1)
                {
                    // Force insertion
                    m_connected_regions[id0].region_id = id0;
                    m_connected_regions[id1].region_id = id1;

                    connection_properties& prop0 = m_connected_regions[id0].connected_region_counts[id1];
                    connection_properties& prop1 = m_connected_regions[id1].connected_region_counts[id0];

                    if (turn.cluster_id < 0)
                    {
                        // Turn is not colocated, add reference to connection
                        prop0.count++;
                        prop1.count++;
                    }
                    else
                    {
                        // Turn is colocated, only add region reference if it was not yet registered
                        if (prop0.cluster_indices.count(turn.cluster_id) == 0)
                        {
                            prop0.count++;
                        }
                        if (prop1.cluster_indices.count(turn.cluster_id) == 0)
                        {
                            prop1.count++;
                        }
                    }
                    // Insert cluster-id (also -1 is inserted - reinsertion of
                    // same cluster id is OK)
                    prop0.cluster_indices.insert(turn.cluster_id);
                    prop1.cluster_indices.insert(turn.cluster_id);
                }
            }
        }
    }

    inline bool connects_same_region(turn_type const& turn) const
    {
        if (turn.discarded)
        {
            // Discarded turns don't connect same region (otherwise discarded colocated uu turn
            // could make a connection)
            return false;
        }

        if (turn.cluster_id == -1)
        {
            // If it is a uu/ii-turn (non clustered), it is never same region
            return ! (turn.both(operation_union) || turn.both(operation_intersection));
        }

        if (operation_from_overlay<OverlayType>::value == operation_union)
        {
            // It is a cluster, check zones
            // (assigned by sort_by_side/handle colocations) of both operations
            return turn.operations[0].enriched.zone
                    == turn.operations[1].enriched.zone;
        }

        // If a cluster contains an ii/cc it is not same region (for intersection)
        typename Clusters::const_iterator it = m_clusters.find(turn.cluster_id);
        if (it == m_clusters.end())
        {
            // Should not occur
            return true;
        }

        cluster_info const& cinfo = it->second;
        for (set_iterator sit = cinfo.turn_indices.begin();
             sit != cinfo.turn_indices.end(); ++sit)
        {
            turn_type const& cluster_turn = m_turns[*sit];
            if (cluster_turn.both(operation_union)
                   || cluster_turn.both(operation_intersection))
            {
                return false;
            }
        }

        // It is the same region
        return false;
    }


    inline int get_region_id(turn_operation_type const& op) const
    {
        return op.enriched.region_id;
    }


    void create_region(signed_size_type& new_region_id, ring_identifier const& ring_id,
                merged_ring_properties& properties, int region_id = -1)
    {
        if (properties.region_id > 0)
        {
            // Already handled
            return;
        }

        // Assign new id if this is a new region
        if (region_id == -1)
        {
            region_id = new_region_id++;
        }

        // Assign this ring to specified region
        properties.region_id = region_id;

#if defined(BOOST_GEOMETRY_DEBUG_TRAVERSAL_SWITCH_DETECTOR)
        std::cout << " ADD " << ring_id << "  TO REGION " << region_id << std::endl;
#endif

        // Find connecting rings, recursively
        for (set_iterator sit = properties.turn_indices.begin();
             sit != properties.turn_indices.end(); ++sit)
        {
            signed_size_type const turn_index = *sit;
            turn_type const& turn = m_turns[turn_index];
            if (! connects_same_region(turn))
            {
                // This is a non clustered uu/ii-turn, or a cluster connecting different 'zones'
                continue;
            }

            // Union: This turn connects two rings (interior connected), create the region
            // Intersection: This turn connects two rings, set same regions for these two rings
            for (int op_index = 0; op_index < 2; op_index++)
            {
                turn_operation_type const& op = turn.operations[op_index];
                ring_identifier connected_ring_id = ring_id_by_seg_id(op.seg_id);
                if (connected_ring_id != ring_id)
                {
                    propagate_region(new_region_id, connected_ring_id, region_id);
                }
            }
        }
    }

    void propagate_region(signed_size_type& new_region_id,
            ring_identifier const& ring_id, int region_id)
    {
        typename merge_map::iterator it = m_turns_per_ring.find(ring_id);
        if (it != m_turns_per_ring.end())
        {
            create_region(new_region_id, ring_id, it->second, region_id);
        }
    }


    void iterate()
    {
#if defined(BOOST_GEOMETRY_DEBUG_TRAVERSAL_SWITCH_DETECTOR)
        std::cout << "SWITCH BEGIN ITERATION" << std::endl;
#endif

        // Collect turns per ring
        m_turns_per_ring.clear();
        m_connected_regions.clear();

        for (std::size_t turn_index = 0; turn_index < m_turns.size(); ++turn_index)
        {
            turn_type const& turn = m_turns[turn_index];

            if (turn.discarded
                    && operation_from_overlay<OverlayType>::value == operation_intersection)
            {
                // Discarded turn (union currently still needs it to determine regions)
                continue;
            }

            for (int op_index = 0; op_index < 2; op_index++)
            {
                turn_operation_type const& op = turn.operations[op_index];
                m_turns_per_ring[ring_id_by_seg_id(op.seg_id)].turn_indices.insert(turn_index);
            }
        }

        // All rings having turns are in the map. Now iterate them
        {
            signed_size_type new_region_id = 1;
            for (typename merge_map::iterator it
                 = m_turns_per_ring.begin(); it != m_turns_per_ring.end(); ++it)
            {
                create_region(new_region_id, it->first, it->second);
            }

            assign_regions();
            get_isolated_regions();
            assign_isolation();
        }

        // Now that all regions are filled, assign switch_source property
        // Iterate through all clusters
        for (typename Clusters::iterator it = m_clusters.begin(); it != m_clusters.end(); ++it)
        {
            cluster_info& cinfo = it->second;
            if (cinfo.open_count <= 1)
            {
                // Not a touching cluster
                continue;
            }

            // A touching cluster, gather regions
            std::set<int> regions;

            std::set<signed_size_type> const& ids = cinfo.turn_indices;

#if defined(BOOST_GEOMETRY_DEBUG_TRAVERSAL_SWITCH_DETECTOR)
                std::cout << "SWITCH EXAMINE CLUSTER " << it->first << std::endl;
#endif

            for (set_iterator sit = ids.begin(); sit != ids.end(); ++sit)
            {
                signed_size_type turn_index = *sit;
                turn_type const& turn = m_turns[turn_index];
                if (turn.colocated_ii && ! turn.colocated_uu)
                {
                    continue;
                }
                for (int oi = 0; oi < 2; oi++)
                {
                    int const region = get_region_id(turn.operations[oi]);
                    regions.insert(region);
                }
            }
            // Switch source if this cluster connects the same region
            cinfo.switch_source = regions.size() <= 1;
        }

        // Iterate through all uu/ii turns (non-clustered)
        for (std::size_t turn_index = 0; turn_index < m_turns.size(); ++turn_index)
        {
            turn_type& turn = m_turns[turn_index];

            if (turn.discarded
                    || turn.blocked()
                    || turn.cluster_id >= 0
                    || ! (turn.both(operation_union) || turn.both(operation_intersection)))
            {
                // Skip discarded, blocked, non-uu/ii and clustered turns
                continue;
            }

            if (OverlayType == overlay_buffer)
            {
                // For deflate, the region approach does not work because many
                // pieces are outside the real polygons
                // TODO: implement this in another way for buffer
                // (because now buffer might output invalid geometries)
                continue;
            }

            int const region0 = get_region_id(turn.operations[0]);
            int const region1 = get_region_id(turn.operations[1]);

            // Switch sources for same region
            turn.switch_source = region0 == region1;
        }


#if defined(BOOST_GEOMETRY_DEBUG_TRAVERSAL_SWITCH_DETECTOR)
        std::cout << "SWITCH END ITERATION" << std::endl;

        for (std::size_t turn_index = 0; turn_index < m_turns.size(); ++turn_index)
        {
            turn_type const& turn = m_turns[turn_index];

            if ((turn.both(operation_union) || turn.both(operation_intersection))
                 && turn.cluster_id < 0)
            {
                std::cout << "UU/II SWITCH RESULT "
                             << turn_index << " -> "
                          << turn.switch_source << std::endl;
            }
        }

        for (typename Clusters::const_iterator it = m_clusters.begin(); it != m_clusters.end(); ++it)
        {
            cluster_info const& cinfo = it->second;
            if (cinfo.open_count > 1)
            {
                std::cout << "CL SWITCH RESULT " << it->first
                             << " -> " << cinfo.switch_source << std::endl;
            }
            else
            {
                std::cout << "CL SWITCH RESULT " << it->first
                          << " is not registered as open" << std::endl;
            }
        }
#endif

    }

private:

    Geometry1 const& m_geometry1;
    Geometry2 const& m_geometry2;
    Turns& m_turns;
    Clusters& m_clusters;
    merge_map m_turns_per_ring;
    region_connection_map m_connected_regions;
    RobustPolicy const& m_robust_policy;
    Visitor& m_visitor;
};

}} // namespace detail::overlay
#endif // DOXYGEN_NO_DETAIL

}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_TRAVERSAL_SWITCH_DETECTOR_HPP
