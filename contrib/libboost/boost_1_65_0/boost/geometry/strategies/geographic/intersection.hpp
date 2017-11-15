// Boost.Geometry

// Copyright (c) 2016-2017, Oracle and/or its affiliates.
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_INTERSECTION_HPP
#define BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_INTERSECTION_HPP

#include <algorithm>

#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/radian_access.hpp>
#include <boost/geometry/core/srs.hpp>
#include <boost/geometry/core/tags.hpp>

#include <boost/geometry/algorithms/detail/assign_values.hpp>
#include <boost/geometry/algorithms/detail/assign_indexed_point.hpp>
#include <boost/geometry/algorithms/detail/equals/point_point.hpp>
#include <boost/geometry/algorithms/detail/recalculate.hpp>

#include <boost/geometry/formulas/andoyer_inverse.hpp>
#include <boost/geometry/formulas/sjoberg_intersection.hpp>
#include <boost/geometry/formulas/spherical.hpp>

#include <boost/geometry/geometries/concepts/point_concept.hpp>
#include <boost/geometry/geometries/concepts/segment_concept.hpp>

#include <boost/geometry/policies/robustness/segment_ratio.hpp>

#include <boost/geometry/strategies/geographic/area.hpp>
#include <boost/geometry/strategies/geographic/distance.hpp>
#include <boost/geometry/strategies/geographic/envelope_segment.hpp>
#include <boost/geometry/strategies/geographic/parameters.hpp>
#include <boost/geometry/strategies/geographic/side.hpp>
#include <boost/geometry/strategies/intersection.hpp>
#include <boost/geometry/strategies/intersection_result.hpp>
#include <boost/geometry/strategies/side_info.hpp>

#include <boost/geometry/util/math.hpp>
#include <boost/geometry/util/select_calculation_type.hpp>


namespace boost { namespace geometry
{

namespace strategy { namespace intersection
{

// CONSIDER: Improvement of the robustness/accuracy/repeatability by
// moving all segments to 0 longitude
// picking latitudes closer to 0
// etc.

template
<
    typename FormulaPolicy = strategy::andoyer,
    unsigned int Order = strategy::default_order<FormulaPolicy>::value,
    typename Spheroid = srs::spheroid<double>,
    typename CalculationType = void
>
struct geographic_segments
{
    typedef side::geographic
        <
            FormulaPolicy, Spheroid, CalculationType
        > side_strategy_type;

    inline side_strategy_type get_side_strategy() const
    {
        return side_strategy_type(m_spheroid);
    }

    template <typename Geometry1, typename Geometry2>
    struct point_in_geometry_strategy
    {
        typedef strategy::within::winding
            <
                typename point_type<Geometry1>::type,
                typename point_type<Geometry2>::type,
                side_strategy_type,
                CalculationType
            > type;
    };

    template <typename Geometry1, typename Geometry2>
    inline typename point_in_geometry_strategy<Geometry1, Geometry2>::type
        get_point_in_geometry_strategy() const
    {
        typedef typename point_in_geometry_strategy
            <
                Geometry1, Geometry2
            >::type strategy_type;
        return strategy_type(get_side_strategy());
    }

    template <typename Geometry>
    struct area_strategy
    {
        typedef area::geographic
            <
                typename point_type<Geometry>::type,
                FormulaPolicy,
                Order,
                Spheroid,
                CalculationType
            > type;
    };

    template <typename Geometry>
    inline typename area_strategy<Geometry>::type get_area_strategy() const
    {
        typedef typename area_strategy<Geometry>::type strategy_type;
        return strategy_type(m_spheroid);
    }

    template <typename Geometry>
    struct distance_strategy
    {
        typedef distance::geographic
            <
                FormulaPolicy,
                Spheroid,
                CalculationType
            > type;
    };

    template <typename Geometry>
    inline typename distance_strategy<Geometry>::type get_distance_strategy() const
    {
        typedef typename distance_strategy<Geometry>::type strategy_type;
        return strategy_type(m_spheroid);
    }

    typedef envelope::geographic_segment<FormulaPolicy, Spheroid, CalculationType>
        envelope_strategy_type;

    inline envelope_strategy_type get_envelope_strategy() const
    {
        return envelope_strategy_type(m_spheroid);
    }

    enum intersection_point_flag { ipi_inters = 0, ipi_at_a1, ipi_at_a2, ipi_at_b1, ipi_at_b2 };

    template <typename CoordinateType, typename SegmentRatio>
    struct segment_intersection_info
    {
        typedef typename select_most_precise
            <
                CoordinateType, double
            >::type promoted_type;

        promoted_type comparable_length_a() const
        {
            return robust_ra.denominator();
        }

        promoted_type comparable_length_b() const
        {
            return robust_rb.denominator();
        }

        template <typename Point, typename Segment1, typename Segment2>
        void assign_a(Point& point, Segment1 const& a, Segment2 const& b) const
        {
            assign(point, a, b);
        }
        template <typename Point, typename Segment1, typename Segment2>
        void assign_b(Point& point, Segment1 const& a, Segment2 const& b) const
        {
            assign(point, a, b);
        }

        template <typename Point, typename Segment1, typename Segment2>
        void assign(Point& point, Segment1 const& a, Segment2 const& b) const
        {
            if (ip_flag == ipi_inters)
            {
                // TODO: assign the rest of coordinates
                set_from_radian<0>(point, lon);
                set_from_radian<1>(point, lat);
            }
            else if (ip_flag == ipi_at_a1)
            {
                detail::assign_point_from_index<0>(a, point);
            }
            else if (ip_flag == ipi_at_a2)
            {
                detail::assign_point_from_index<1>(a, point);
            }
            else if (ip_flag == ipi_at_b1)
            {
                detail::assign_point_from_index<0>(b, point);
            }
            else // ip_flag == ipi_at_b2
            {
                detail::assign_point_from_index<1>(b, point);
            }
        }

        CoordinateType lon;
        CoordinateType lat;
        SegmentRatio robust_ra;
        SegmentRatio robust_rb;
        intersection_point_flag ip_flag;
    };

    explicit geographic_segments(Spheroid const& spheroid = Spheroid())
        : m_spheroid(spheroid)
    {}

    // Relate segments a and b
    template
    <
        typename Segment1,
        typename Segment2,
        typename Policy,
        typename RobustPolicy
    >
    inline typename Policy::return_type apply(Segment1 const& a, Segment2 const& b,
                                              Policy const& policy,
                                              RobustPolicy const& robust_policy) const
    {
        typedef typename point_type<Segment1>::type point1_t;
        typedef typename point_type<Segment2>::type point2_t;
        point1_t a1, a2;
        point2_t b1, b2;

        detail::assign_point_from_index<0>(a, a1);
        detail::assign_point_from_index<1>(a, a2);
        detail::assign_point_from_index<0>(b, b1);
        detail::assign_point_from_index<1>(b, b2);

        return apply(a, b, policy, robust_policy, a1, a2, b1, b2);
    }

    // Relate segments a and b
    template
    <
        typename Segment1,
        typename Segment2,
        typename Policy,
        typename RobustPolicy,
        typename Point1,
        typename Point2
    >
    inline typename Policy::return_type apply(Segment1 const& a, Segment2 const& b,
                                              Policy const&, RobustPolicy const&,
                                              Point1 a1, Point1 a2, Point2 b1, Point2 b2) const
    {
        bool is_a_reversed = get<1>(a1) > get<1>(a2);
        bool is_b_reversed = get<1>(b1) > get<1>(b2);
                           
        if (is_a_reversed)
        {
            std::swap(a1, a2);
        }

        if (is_b_reversed)
        {
            std::swap(b1, b2);
        }

        return apply<Policy>(a, b, a1, a2, b1, b2, is_a_reversed, is_b_reversed);
    }

private:
    // Relate segments a and b
    template
    <
        typename Policy,
        typename Segment1,
        typename Segment2,
        typename Point1,
        typename Point2
    >
    inline typename Policy::return_type apply(Segment1 const& a, Segment2 const& b,
                                              Point1 const& a1, Point1 const& a2,
                                              Point2 const& b1, Point2 const& b2,
                                              bool is_a_reversed, bool is_b_reversed) const
    {
        BOOST_CONCEPT_ASSERT( (concepts::ConstSegment<Segment1>) );
        BOOST_CONCEPT_ASSERT( (concepts::ConstSegment<Segment2>) );

        typedef typename select_calculation_type
            <Segment1, Segment2, CalculationType>::type calc_t;

        static const calc_t c0 = 0;

        // normalized spheroid
        srs::spheroid<calc_t> spheroid = normalized_spheroid<calc_t>(m_spheroid);

        // TODO: check only 2 first coordinates here?
        using geometry::detail::equals::equals_point_point;
        bool a_is_point = equals_point_point(a1, a2);
        bool b_is_point = equals_point_point(b1, b2);

        if(a_is_point && b_is_point)
        {
            return equals_point_point(a1, b2)
                ? Policy::degenerate(a, true)
                : Policy::disjoint()
                ;
        }
        
        calc_t const a1_lon = get_as_radian<0>(a1);
        calc_t const a1_lat = get_as_radian<1>(a1);
        calc_t const a2_lon = get_as_radian<0>(a2);
        calc_t const a2_lat = get_as_radian<1>(a2);
        calc_t const b1_lon = get_as_radian<0>(b1);
        calc_t const b1_lat = get_as_radian<1>(b1);
        calc_t const b2_lon = get_as_radian<0>(b2);
        calc_t const b2_lat = get_as_radian<1>(b2);

        side_info sides;

        // NOTE: potential optimization, don't calculate distance at this point
        // this would require to reimplement inverse strategy to allow
        // calculation of distance if needed, probably also storing intermediate
        // results somehow inside an object.
        typedef typename FormulaPolicy::template inverse<calc_t, true, true, false, false, false> inverse_dist_azi;
        typedef typename inverse_dist_azi::result_type inverse_result;

        // TODO: no need to call inverse formula if we know that the points are equal
        // distance can be set to 0 in this case and azimuth may be not calculated
        bool is_equal_a1_b1 = equals_point_point(a1, b1);
        bool is_equal_a2_b1 = equals_point_point(a2, b1);
        bool degen_neq_coords = false;

        inverse_result res_b1_b2, res_b1_a1, res_b1_a2;
        if (! b_is_point)
        {
            res_b1_b2 = inverse_dist_azi::apply(b1_lon, b1_lat, b2_lon, b2_lat, spheroid);
            if (math::equals(res_b1_b2.distance, c0))
            {
                b_is_point = true;
                degen_neq_coords = true;
            }
            else
            {
                res_b1_a1 = inverse_dist_azi::apply(b1_lon, b1_lat, a1_lon, a1_lat, spheroid);
                if (math::equals(res_b1_a1.distance, c0))
                {
                    is_equal_a1_b1 = true;
                }
                res_b1_a2 = inverse_dist_azi::apply(b1_lon, b1_lat, a2_lon, a2_lat, spheroid);
                if (math::equals(res_b1_a2.distance, c0))
                {
                    is_equal_a2_b1 = true;
                }
                sides.set<0>(is_equal_a1_b1 ? 0 : formula::azimuth_side_value(res_b1_a1.azimuth, res_b1_b2.azimuth),
                             is_equal_a2_b1 ? 0 : formula::azimuth_side_value(res_b1_a2.azimuth, res_b1_b2.azimuth));
                if (sides.same<0>())
                {
                    // Both points are at the same side of other segment, we can leave
                    return Policy::disjoint();
                }
            }
        }

        bool is_equal_a1_b2 = equals_point_point(a1, b2);

        inverse_result res_a1_a2, res_a1_b1, res_a1_b2;
        if (! a_is_point)
        {
            res_a1_a2 = inverse_dist_azi::apply(a1_lon, a1_lat, a2_lon, a2_lat, spheroid);
            if (math::equals(res_a1_a2.distance, c0))
            {
                a_is_point = true;
                degen_neq_coords = true;
            }
            else
            {
                res_a1_b1 = inverse_dist_azi::apply(a1_lon, a1_lat, b1_lon, b1_lat, spheroid);
                if (math::equals(res_a1_b1.distance, c0))
                {
                    is_equal_a1_b1 = true;
                }
                res_a1_b2 = inverse_dist_azi::apply(a1_lon, a1_lat, b2_lon, b2_lat, spheroid);
                if (math::equals(res_a1_b2.distance, c0))
                {
                    is_equal_a1_b2 = true;
                }
                sides.set<1>(is_equal_a1_b1 ? 0 : formula::azimuth_side_value(res_a1_b1.azimuth, res_a1_a2.azimuth),
                             is_equal_a1_b2 ? 0 : formula::azimuth_side_value(res_a1_b2.azimuth, res_a1_a2.azimuth));
                if (sides.same<1>())
                {
                    // Both points are at the same side of other segment, we can leave
                    return Policy::disjoint();
                }
            }
        }

        if(a_is_point && b_is_point)
        {
            return is_equal_a1_b2
                ? Policy::degenerate(a, true)
                : Policy::disjoint()
                ;
        }

        // NOTE: at this point the segments may still be disjoint
        // NOTE: at this point one of the segments may be degenerated

        bool collinear = sides.collinear();       

        if (! collinear)
        {
            // WARNING: the side strategy doesn't have the info about the other
            // segment so it may return results inconsistent with this intersection
            // strategy, as it checks both segments for consistency

            if (sides.get<0, 0>() == 0 && sides.get<0, 1>() == 0)
            {
                collinear = true;
                sides.set<1>(0, 0);
            }
            else if (sides.get<1, 0>() == 0 && sides.get<1, 1>() == 0)
            {
                collinear = true;
                sides.set<0>(0, 0);
            }
        }

        if (collinear)
        {
            if (a_is_point)
            {
                return collinear_one_degenerated<Policy, calc_t>(a, true, b1, b2, a1, a2, res_b1_b2, res_b1_a1, res_b1_a2, is_b_reversed, degen_neq_coords);
            }
            else if (b_is_point)
            {
                return collinear_one_degenerated<Policy, calc_t>(b, false, a1, a2, b1, b2, res_a1_a2, res_a1_b1, res_a1_b2, is_a_reversed, degen_neq_coords);
            }
            else
            {
                calc_t dist_a1_a2, dist_a1_b1, dist_a1_b2;
                calc_t dist_b1_b2, dist_b1_a1, dist_b1_a2;
                // use shorter segment
                if (res_a1_a2.distance <= res_b1_b2.distance)
                {
                    calculate_collinear_data(a1, a2, b1, b2, res_a1_a2, res_a1_b1, res_a1_b2, dist_a1_a2, dist_a1_b1);
                    calculate_collinear_data(a1, a2, b1, b2, res_a1_a2, res_a1_b2, res_a1_b1, dist_a1_a2, dist_a1_b2);
                    dist_b1_b2 = dist_a1_b2 - dist_a1_b1;
                    dist_b1_a1 = -dist_a1_b1;
                    dist_b1_a2 = dist_a1_a2 - dist_a1_b1;
                }
                else
                {
                    calculate_collinear_data(b1, b2, a1, a2, res_b1_b2, res_b1_a1, res_b1_a2, dist_b1_b2, dist_b1_a1);
                    calculate_collinear_data(b1, b2, a1, a2, res_b1_b2, res_b1_a2, res_b1_a1, dist_b1_b2, dist_b1_a2);
                    dist_a1_a2 = dist_b1_a2 - dist_b1_a1;
                    dist_a1_b1 = -dist_b1_a1;
                    dist_a1_b2 = dist_b1_b2 - dist_b1_a1;
                }

                // NOTE: this is probably not needed
                calc_t const c0 = 0;
                int a1_on_b = position_value(c0, dist_a1_b1, dist_a1_b2);
                int a2_on_b = position_value(dist_a1_a2, dist_a1_b1, dist_a1_b2);
                int b1_on_a = position_value(c0, dist_b1_a1, dist_b1_a2);
                int b2_on_a = position_value(dist_b1_b2, dist_b1_a1, dist_b1_a2);

                if ((a1_on_b < 1 && a2_on_b < 1) || (a1_on_b > 3 && a2_on_b > 3))
                {
                    return Policy::disjoint();
                }

                if (a1_on_b == 1)
                {
                    dist_b1_a1 = 0;
                    dist_a1_b1 = 0;
                }
                else if (a1_on_b == 3)
                {
                    dist_b1_a1 = dist_b1_b2;
                    dist_a1_b2 = 0;
                }

                if (a2_on_b == 1)
                {
                    dist_b1_a2 = 0;
                    dist_a1_b1 = dist_a1_a2;
                }
                else if (a2_on_b == 3)
                {
                    dist_b1_a2 = dist_b1_b2;
                    dist_a1_b2 = dist_a1_a2;
                }

                bool opposite = ! same_direction(res_a1_a2.azimuth, res_b1_b2.azimuth);

                // NOTE: If segment was reversed opposite, positions and segment ratios has to be altered
                if (is_a_reversed)
                {
                    // opposite
                    opposite = ! opposite;
                    // positions
                    std::swap(a1_on_b, a2_on_b);
                    b1_on_a = 4 - b1_on_a;
                    b2_on_a = 4 - b2_on_a;
                    // distances for ratios
                    std::swap(dist_b1_a1, dist_b1_a2);
                    dist_a1_b1 = dist_a1_a2 - dist_a1_b1;
                    dist_a1_b2 = dist_a1_a2 - dist_a1_b2;
                }
                if (is_b_reversed)
                {
                    // opposite
                    opposite = ! opposite;
                    // positions
                    a1_on_b = 4 - a1_on_b;
                    a2_on_b = 4 - a2_on_b;
                    std::swap(b1_on_a, b2_on_a);
                    // distances for ratios
                    dist_b1_a1 = dist_b1_b2 - dist_b1_a1;
                    dist_b1_a2 = dist_b1_b2 - dist_b1_a2;
                    std::swap(dist_a1_b1, dist_a1_b2);
                }

                segment_ratio<calc_t> ra_from(dist_b1_a1, dist_b1_b2);
                segment_ratio<calc_t> ra_to(dist_b1_a2, dist_b1_b2);
                segment_ratio<calc_t> rb_from(dist_a1_b1, dist_a1_a2);
                segment_ratio<calc_t> rb_to(dist_a1_b2, dist_a1_a2);

                return Policy::segments_collinear(a, b, opposite,
                    a1_on_b, a2_on_b, b1_on_a, b2_on_a,
                    ra_from, ra_to, rb_from, rb_to);
            }
        }
        else // crossing or touching
        {
            if (a_is_point || b_is_point)
            {
                return Policy::disjoint();
            }

            calc_t lon = 0, lat = 0;
            intersection_point_flag ip_flag;
            calc_t dist_a1_a2, dist_a1_i1, dist_b1_b2, dist_b1_i1;
            if (calculate_ip_data(a1, a2, b1, b2,
                                  a1_lon, a1_lat, a2_lon, a2_lat,
                                  b1_lon, b1_lat, b2_lon, b2_lat,
                                  res_a1_a2, res_a1_b1, res_a1_b2,
                                  res_b1_b2, res_b1_a1, res_b1_a2,
                                  sides, spheroid,
                                  lon, lat,
                                  dist_a1_a2, dist_a1_i1, dist_b1_b2, dist_b1_i1,
                                  ip_flag))
            {
                // NOTE: If segment was reversed sides and segment ratios has to be altered
                if (is_a_reversed)
                {
                    // sides
                    sides_reverse_segment<0>(sides);
                    // distance for ratio
                    dist_a1_i1 = dist_a1_a2 - dist_a1_i1;
                    // ip flag
                    ip_flag_reverse_segment(ip_flag, ipi_at_a1, ipi_at_a2);
                }
                if (is_b_reversed)
                {
                    // sides
                    sides_reverse_segment<1>(sides);
                    // distance for ratio
                    dist_b1_i1 = dist_b1_b2 - dist_b1_i1;
                    // ip flag
                    ip_flag_reverse_segment(ip_flag, ipi_at_b1, ipi_at_b2);
                }

                // intersects
                segment_intersection_info
                    <
                        calc_t,
                        segment_ratio<calc_t>
                    > sinfo;

                sinfo.lon = lon;
                sinfo.lat = lat;
                sinfo.robust_ra.assign(dist_a1_i1, dist_a1_a2);
                sinfo.robust_rb.assign(dist_b1_i1, dist_b1_b2);
                sinfo.ip_flag = ip_flag;

                return Policy::segments_crosses(sides, sinfo, a, b);
            }
            else
            {
                return Policy::disjoint();
            }
        }
    }

    template <typename Policy, typename CalcT, typename Segment, typename Point1, typename Point2, typename ResultInverse>
    static inline typename Policy::return_type
        collinear_one_degenerated(Segment const& segment, bool degenerated_a,
                                  Point1 const& a1, Point1 const& a2,
                                  Point2 const& b1, Point2 const& b2,
                                  ResultInverse const& res_a1_a2,
                                  ResultInverse const& res_a1_b1,
                                  ResultInverse const& res_a1_b2,
                                  bool is_other_reversed,
                                  bool degen_neq_coords)
    {
        CalcT dist_1_2, dist_1_o;
        if (! calculate_collinear_data(a1, a2, b1, b2, res_a1_a2, res_a1_b1, res_a1_b2, dist_1_2, dist_1_o, degen_neq_coords))
        {
            return Policy::disjoint();
        }

        // NOTE: If segment was reversed segment ratio has to be altered
        if (is_other_reversed)
        {
            // distance for ratio
            dist_1_o = dist_1_2 - dist_1_o;
        }
        
        return Policy::one_degenerate(segment, segment_ratio<CalcT>(dist_1_o, dist_1_2), degenerated_a);
    }

    // TODO: instead of checks below test bi against a1 and a2 here?
    //       in order to make this independent from is_near()
    template <typename Point1, typename Point2, typename ResultInverse, typename CalcT>
    static inline bool calculate_collinear_data(Point1 const& a1, Point1 const& a2, // in
                                                Point2 const& b1, Point2 const& b2, // in
                                                ResultInverse const& res_a1_a2,     // in
                                                ResultInverse const& res_a1_b1,     // in
                                                ResultInverse const& res_a1_b2,     // in
                                                CalcT& dist_a1_a2,                  // out
                                                CalcT& dist_a1_bi,                  // out
                                                bool degen_neq_coords = false)      // in
    {
        dist_a1_a2 = res_a1_a2.distance;

        dist_a1_bi = res_a1_b1.distance;
        if (! same_direction(res_a1_b1.azimuth, res_a1_a2.azimuth))
        {
            dist_a1_bi = -dist_a1_bi;
        }

        // if i1 is close to a1 and b1 or b2 is equal to a1
        if (is_endpoint_equal(dist_a1_bi, a1, b1, b2))
        {
            dist_a1_bi = 0;
            return true;
        }
        // or i1 is close to a2 and b1 or b2 is equal to a2
        else if (is_endpoint_equal(dist_a1_a2 - dist_a1_bi, a2, b1, b2))
        {
            dist_a1_bi = dist_a1_a2;
            return true;
        }

        // check the other endpoint of a very short segment near the pole
        if (degen_neq_coords)
        {
            static CalcT const c0 = 0;
            if (math::equals(res_a1_b2.distance, c0))
            {
                dist_a1_bi = 0;
                return true;
            }
            else if (math::equals(dist_a1_a2 - res_a1_b2.distance, c0))
            {
                dist_a1_bi = dist_a1_a2;
                return true;
            }
        }

        // or i1 is on b
        return segment_ratio<CalcT>(dist_a1_bi, dist_a1_a2).on_segment();
    }

    template <typename Point1, typename Point2, typename CalcT, typename ResultInverse, typename Spheroid_>
    static inline bool calculate_ip_data(Point1 const& a1, Point1 const& a2,       // in
                                         Point2 const& b1, Point2 const& b2,       // in
                                         CalcT const& a1_lon, CalcT const& a1_lat, // in
                                         CalcT const& a2_lon, CalcT const& a2_lat, // in
                                         CalcT const& b1_lon, CalcT const& b1_lat, // in
                                         CalcT const& b2_lon, CalcT const& b2_lat, // in
                                         ResultInverse const& res_a1_a2,           // in
                                         ResultInverse const& res_a1_b1,           // in
                                         ResultInverse const& res_a1_b2,           // in
                                         ResultInverse const& res_b1_b2,           // in
                                         ResultInverse const& res_b1_a1,           // in
                                         ResultInverse const& res_b1_a2,           // in
                                         side_info const& sides,                   // in
                                         Spheroid_ const& spheroid,                // in
                                         CalcT & lon, CalcT & lat,             // out
                                         CalcT& dist_a1_a2, CalcT& dist_a1_ip, // out
                                         CalcT& dist_b1_b2, CalcT& dist_b1_ip, // out
                                         intersection_point_flag& ip_flag)     // out
    {
        dist_a1_a2 = res_a1_a2.distance;
        dist_b1_b2 = res_b1_b2.distance;

        // assign the IP if some endpoints overlap
        using geometry::detail::equals::equals_point_point;
        if (equals_point_point(a1, b1))
        {
            lon = a1_lon;
            lat = a1_lat;
            dist_a1_ip = 0;
            dist_b1_ip = 0;
            ip_flag = ipi_at_a1;
            return true;
        }
        else if (equals_point_point(a1, b2))
        {
            lon = a1_lon;
            lat = a1_lat;
            dist_a1_ip = 0;
            dist_b1_ip = dist_b1_b2;
            ip_flag = ipi_at_a1;
            return true;
        }
        else if (equals_point_point(a2, b1))
        {
            lon = a2_lon;
            lat = a2_lat;
            dist_a1_ip = dist_a1_a2;
            dist_b1_ip = 0;
            ip_flag = ipi_at_a2;
            return true;
        }
        else if (equals_point_point(a2, b2))
        {
            lon = a2_lon;
            lat = a2_lat;
            dist_a1_ip = dist_a1_a2;
            dist_b1_ip = dist_b1_b2;
            ip_flag = ipi_at_a2;
            return true;
        }

        // at this point we know that the endpoints doesn't overlap
        // check cases when an endpoint lies on the other geodesic
        if (sides.template get<0, 0>() == 0) // a1 wrt b
        {
            if (res_b1_a1.distance <= res_b1_b2.distance
                && same_direction(res_b1_a1.azimuth, res_b1_b2.azimuth))
            {
                lon = a1_lon;
                lat = a1_lat;
                dist_a1_ip = 0;
                dist_b1_ip = res_b1_a1.distance;
                ip_flag = ipi_at_a1;
                return true;
            }
            else
            {
                return false;
            }
        }
        else if (sides.template get<0, 1>() == 0) // a2 wrt b
        {
            if (res_b1_a2.distance <= res_b1_b2.distance
                && same_direction(res_b1_a2.azimuth, res_b1_b2.azimuth))
            {
                lon = a2_lon;
                lat = a2_lat;
                dist_a1_ip = res_a1_a2.distance;
                dist_b1_ip = res_b1_a2.distance;
                ip_flag = ipi_at_a2;
                return true;
            }
            else
            {
                return false;
            }
        }
        else if (sides.template get<1, 0>() == 0) // b1 wrt a
        {
            if (res_a1_b1.distance <= res_a1_a2.distance
                && same_direction(res_a1_b1.azimuth, res_a1_a2.azimuth))
            {
                lon = b1_lon;
                lat = b1_lat;
                dist_a1_ip = res_a1_b1.distance;
                dist_b1_ip = 0;
                ip_flag = ipi_at_b1;
                return true;
            }
            else
            {
                return false;
            }
        }
        else if (sides.template get<1, 1>() == 0) // b2 wrt a
        {
            if (res_a1_b2.distance <= res_a1_a2.distance
                && same_direction(res_a1_b2.azimuth, res_a1_a2.azimuth))
            {
                lon = b2_lon;
                lat = b2_lat;
                dist_a1_ip = res_a1_b2.distance;
                dist_b1_ip = res_b1_b2.distance;
                ip_flag = ipi_at_b2;
                return true;
            }
            else
            {
                return false;
            }
        }

        // At this point neither the endpoints overlaps
        // nor any andpoint lies on the other geodesic
        // So the endpoints should lie on the opposite sides of both geodesics

        bool const ok = formula::sjoberg_intersection<CalcT, FormulaPolicy::template inverse, Order>
                        ::apply(a1_lon, a1_lat, a2_lon, a2_lat, res_a1_a2.azimuth,
                                b1_lon, b1_lat, b2_lon, b2_lat, res_b1_b2.azimuth,
                                lon, lat, spheroid);

        if (! ok)
        {
            return false;
        }

        typedef typename FormulaPolicy::template inverse<CalcT, true, true, false, false, false> inverse_dist_azi;
        typedef typename inverse_dist_azi::result_type inverse_result;

        inverse_result const res_a1_ip = inverse_dist_azi::apply(a1_lon, a1_lat, lon, lat, spheroid);
        dist_a1_ip = res_a1_ip.distance;
        if (! same_direction(res_a1_ip.azimuth, res_a1_a2.azimuth))
        {
            dist_a1_ip = -dist_a1_ip;
        }

        bool is_on_a = segment_ratio<CalcT>(dist_a1_ip, dist_a1_a2).on_segment();
        // NOTE: not fully consistent with equals_point_point() since radians are always used.
        bool is_on_a1 = math::equals(lon, a1_lon) && math::equals(lat, a1_lat);
        bool is_on_a2 = math::equals(lon, a2_lon) && math::equals(lat, a2_lat);

        if (! (is_on_a || is_on_a1 || is_on_a2))
        {
            return false;
        }

        inverse_result const res_b1_ip = inverse_dist_azi::apply(b1_lon, b1_lat, lon, lat, spheroid);
        dist_b1_ip = res_b1_ip.distance;
        if (! same_direction(res_b1_ip.azimuth, res_b1_b2.azimuth))
        {
            dist_b1_ip = -dist_b1_ip;
        }

        bool is_on_b = segment_ratio<CalcT>(dist_b1_ip, dist_b1_b2).on_segment();
        // NOTE: not fully consistent with equals_point_point() since radians are always used.
        bool is_on_b1 = math::equals(lon, b1_lon) && math::equals(lat, b1_lat);
        bool is_on_b2 = math::equals(lon, b2_lon) && math::equals(lat, b2_lat);

        if (! (is_on_b || is_on_b1 || is_on_b2))
        {
            return false;
        }
        
        ip_flag = ipi_inters;

        if (is_on_b1)
        {
            lon = b1_lon;
            lat = b1_lat;
            dist_b1_ip = 0;
            ip_flag = ipi_at_b1;
        }
        else if (is_on_b2)
        {
            lon = b2_lon;
            lat = b2_lat;
            dist_b1_ip = res_b1_b2.distance;
            ip_flag = ipi_at_b2;
        }

        if (is_on_a1)
        {
            lon = a1_lon;
            lat = a1_lat;
            dist_a1_ip = 0;
            ip_flag = ipi_at_a1;
        }
        else if (is_on_a2)
        {
            lon = a2_lon;
            lat = a2_lat;
            dist_a1_ip = res_a1_a2.distance;
            ip_flag = ipi_at_a2;
        }        

        return true;
    }

    template <typename CalcT, typename P1, typename P2>
    static inline bool is_endpoint_equal(CalcT const& dist,
                                         P1 const& ai, P2 const& b1, P2 const& b2)
    {
        static CalcT const c0 = 0;
        using geometry::detail::equals::equals_point_point;
        return is_near(dist) && (equals_point_point(ai, b1) || equals_point_point(ai, b2) || math::equals(dist, c0));
    }

    template <typename CalcT>
    static inline bool is_near(CalcT const& dist)
    {
        // NOTE: This strongly depends on the Inverse method
        CalcT const small_number = CalcT(boost::is_same<CalcT, float>::value ? 0.0001 : 0.00000001);
        return math::abs(dist) <= small_number;
    }

    template <typename ProjCoord1, typename ProjCoord2>
    static inline int position_value(ProjCoord1 const& ca1,
                                     ProjCoord2 const& cb1,
                                     ProjCoord2 const& cb2)
    {
        // S1x  0   1    2     3   4
        // S2       |---------->
        return math::equals(ca1, cb1) ? 1
             : math::equals(ca1, cb2) ? 3
             : cb1 < cb2 ?
                ( ca1 < cb1 ? 0
                : ca1 > cb2 ? 4
                : 2 )
              : ( ca1 > cb1 ? 0
                : ca1 < cb2 ? 4
                : 2 );
    }

    template <typename CalcT>
    static inline bool same_direction(CalcT const& azimuth1, CalcT const& azimuth2)
    {
        // distance between two angles normalized to (-180, 180]
        CalcT const angle_diff = math::longitude_distance_signed<radian>(azimuth1, azimuth2);
        return math::abs(angle_diff) <= math::half_pi<CalcT>();
    }

    template <int Which>
    static inline void sides_reverse_segment(side_info & sides)
    {
        // names assuming segment A is reversed (Which == 0)
        int a1_wrt_b = sides.template get<Which, 0>();
        int a2_wrt_b = sides.template get<Which, 1>();
        std::swap(a1_wrt_b, a2_wrt_b);
        sides.template set<Which>(a1_wrt_b, a2_wrt_b);
        int b1_wrt_a = sides.template get<1 - Which, 0>();
        int b2_wrt_a = sides.template get<1 - Which, 1>();
        sides.template set<1 - Which>(-b1_wrt_a, -b2_wrt_a);
    }

    static inline void ip_flag_reverse_segment(intersection_point_flag & ip_flag,
                                               intersection_point_flag const& ipi_at_p1,
                                               intersection_point_flag const& ipi_at_p2)
    {
        ip_flag = ip_flag == ipi_at_p1 ? ipi_at_p2 :
                  ip_flag == ipi_at_p2 ? ipi_at_p1 :
                  ip_flag;
    }

    template <typename CalcT, typename SpheroidT>
    static inline srs::spheroid<CalcT> normalized_spheroid(SpheroidT const& spheroid)
    {
        return srs::spheroid<CalcT>(CalcT(1),
                                    CalcT(get_radius<2>(spheroid)) // b/a
                                    / CalcT(get_radius<0>(spheroid)));
    }

private:
    Spheroid m_spheroid;
};


}} // namespace strategy::intersection

}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_INTERSECTION_HPP
