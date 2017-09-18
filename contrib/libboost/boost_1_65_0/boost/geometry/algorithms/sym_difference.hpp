// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2015, 2017.
// Modifications copyright (c) 2015-2017 Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_SYM_DIFFERENCE_HPP
#define BOOST_GEOMETRY_ALGORITHMS_SYM_DIFFERENCE_HPP


#include <algorithm>
#include <iterator>
#include <vector>

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>
#include <boost/variant/variant_fwd.hpp>

#include <boost/geometry/algorithms/intersection.hpp>
#include <boost/geometry/algorithms/union.hpp>
#include <boost/geometry/geometries/multi_polygon.hpp>
#include <boost/geometry/policies/robustness/get_rescale_policy.hpp>
#include <boost/geometry/strategies/default_strategy.hpp>
#include <boost/geometry/util/range.hpp>


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace sym_difference
{


template <typename GeometryOut>
struct compute_difference
{
    template
    <
        typename Geometry1,
        typename Geometry2,
        typename RobustPolicy,
        typename OutputIterator,
        typename Strategy
    >
    static inline OutputIterator apply(Geometry1 const& geometry1,
                                       Geometry2 const& geometry2,
                                       RobustPolicy const& robust_policy,
                                       OutputIterator out,
                                       Strategy const& strategy)
    {
        return geometry::dispatch::intersection_insert
            <
                Geometry1,
                Geometry2,
                GeometryOut,
                overlay_difference,
                geometry::detail::overlay::do_reverse
                    <
                        geometry::point_order<Geometry1>::value
                    >::value,
                geometry::detail::overlay::do_reverse
                    <
                        geometry::point_order<Geometry2>::value, true
                    >::value
            >::apply(geometry1, geometry2, robust_policy, out, strategy);
    }
};



template <typename GeometryOut, typename Geometry1, typename Geometry2>
struct sym_difference_generic
{
    template
    <
        typename RobustPolicy,
        typename OutputIterator,
        typename Strategy
    >
    static inline OutputIterator apply(Geometry1 const& geometry1,
                                       Geometry2 const& geometry2,
                                       RobustPolicy const& robust_policy,
                                       OutputIterator out,
                                       Strategy const& strategy)
    {
        out = compute_difference
            <
                GeometryOut
            >::apply(geometry1, geometry2, robust_policy, out, strategy);

        return compute_difference
            <
                GeometryOut
            >::apply(geometry2, geometry1, robust_policy, out, strategy);
    }
};


template <typename GeometryOut, typename Areal1, typename Areal2>
struct sym_difference_areal_areal
{
    template
    <
        typename RobustPolicy,
        typename OutputIterator,
        typename Strategy
    >
    static inline OutputIterator apply(Areal1 const& areal1,
                                       Areal2 const& areal2,
                                       RobustPolicy const& robust_policy,
                                       OutputIterator out,
                                       Strategy const& strategy)
    {
        typedef geometry::model::multi_polygon
            <
                GeometryOut
            > helper_geometry_type;

        helper_geometry_type diff12, diff21;

        std::back_insert_iterator<helper_geometry_type> oit12(diff12);
        std::back_insert_iterator<helper_geometry_type> oit21(diff21);

        compute_difference
            <
                GeometryOut
            >::apply(areal1, areal2, robust_policy, oit12, strategy);

        compute_difference
            <
                GeometryOut
            >::apply(areal2, areal1, robust_policy, oit21, strategy);

        return geometry::dispatch::union_insert
            <
                helper_geometry_type,
                helper_geometry_type,
                GeometryOut
            >::apply(diff12, diff21, robust_policy, out, strategy);
    }
};


}} // namespace detail::sym_difference
#endif // DOXYGEN_NO_DETAIL



#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{


template
<
    typename Geometry1,
    typename Geometry2,
    typename GeometryOut,
    typename TagIn1 = typename geometry::tag_cast
        <
            typename tag<Geometry1>::type, areal_tag
        >::type,
    typename TagIn2 = typename geometry::tag_cast
        <
            typename tag<Geometry2>::type, areal_tag
        >::type,
    typename TagOut = typename geometry::tag<GeometryOut>::type
>
struct sym_difference_insert
    : detail::sym_difference::sym_difference_generic
        <
            GeometryOut, Geometry1, Geometry2
        >
{};


template
<
    typename Areal1,
    typename Areal2,
    typename GeometryOut,
    typename TagOut
>
struct sym_difference_insert
    <
        Areal1, Areal2, GeometryOut,
        areal_tag, areal_tag, TagOut
    > : detail::sym_difference::sym_difference_areal_areal
        <
            GeometryOut, Areal1, Areal2
        >
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH



#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace sym_difference
{



/*!
\brief \brief_calc2{symmetric difference}  \brief_strategy
\ingroup sym_difference
\details \details_calc2{symmetric difference, spatial set theoretic symmetric difference (XOR)}
    \brief_strategy. \details_insert{sym_difference}
\tparam GeometryOut output geometry type, must be specified
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\tparam Strategy \tparam_strategy_overlay
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\param out \param_out{difference}
\param strategy \param_strategy{difference}
\return \return_out

\qbk{distinguish,with strategy}
*/
template
<
    typename GeometryOut,
    typename Geometry1,
    typename Geometry2,
    typename RobustPolicy,
    typename OutputIterator,
    typename Strategy
>
inline OutputIterator sym_difference_insert(Geometry1 const& geometry1,
            Geometry2 const& geometry2,
            RobustPolicy const& robust_policy,
            OutputIterator out,
            Strategy const& strategy)
{
    concepts::check<Geometry1 const>();
    concepts::check<Geometry2 const>();
    concepts::check<GeometryOut>();

    return dispatch::sym_difference_insert
        <
            Geometry1, Geometry2, GeometryOut
        >::apply(geometry1, geometry2, robust_policy, out, strategy);
}


/*!
\brief \brief_calc2{symmetric difference}
\ingroup sym_difference
\details \details_calc2{symmetric difference, spatial set theoretic symmetric difference (XOR)}
    \details_insert{sym_difference}
\tparam GeometryOut output geometry type, must be specified
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\param out \param_out{difference}
\return \return_out

*/
template
<
    typename GeometryOut,
    typename Geometry1,
    typename Geometry2,
    typename RobustPolicy,
    typename OutputIterator
>
inline OutputIterator sym_difference_insert(Geometry1 const& geometry1,
            Geometry2 const& geometry2,
            RobustPolicy const& robust_policy, OutputIterator out)
{
    concepts::check<Geometry1 const>();
    concepts::check<Geometry2 const>();
    concepts::check<GeometryOut>();

    typedef typename strategy::intersection::services::default_strategy
        <
            typename cs_tag<GeometryOut>::type
        >::type strategy_type;

    return sym_difference_insert<GeometryOut>(geometry1, geometry2, robust_policy, out, strategy_type());
}

}} // namespace detail::sym_difference
#endif // DOXYGEN_NO_DETAIL


namespace resolve_strategy {

struct sym_difference
{
    template
    <
        typename Geometry1,
        typename Geometry2,
        typename RobustPolicy,
        typename Collection,
        typename Strategy
    >
    static inline void apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             RobustPolicy const& robust_policy,
                             Collection & output_collection,
                             Strategy const& strategy)
    {
        typedef typename boost::range_value<Collection>::type geometry_out;

        detail::sym_difference::sym_difference_insert<geometry_out>(
            geometry1, geometry2, robust_policy,
            range::back_inserter(output_collection),
            strategy);
    }

    template
    <
        typename Geometry1,
        typename Geometry2,
        typename RobustPolicy,
        typename Collection
    >
    static inline void apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             RobustPolicy const& robust_policy,
                             Collection & output_collection,
                             default_strategy)
    {
        typedef typename boost::range_value<Collection>::type geometry_out;
        
        detail::sym_difference::sym_difference_insert<geometry_out>(
            geometry1, geometry2, robust_policy,
            range::back_inserter(output_collection));
    }
};

} // resolve_strategy


namespace resolve_variant
{
    
template <typename Geometry1, typename Geometry2>
struct sym_difference
{
    template <typename Collection, typename Strategy>
    static inline void apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Collection& output_collection,
                             Strategy const& strategy)
    {
        typedef typename geometry::rescale_overlay_policy_type
            <
                Geometry1,
                Geometry2
            >::type rescale_policy_type;

        rescale_policy_type robust_policy
                = geometry::get_rescale_policy<rescale_policy_type>(geometry1,
                                                                    geometry2);
        
        resolve_strategy::sym_difference::apply(geometry1, geometry2,
                                                robust_policy,
                                                output_collection,
                                                strategy);
    }
};


template <BOOST_VARIANT_ENUM_PARAMS(typename T), typename Geometry2>
struct sym_difference<variant<BOOST_VARIANT_ENUM_PARAMS(T)>, Geometry2>
{
    template <typename Collection, typename Strategy>
    struct visitor: static_visitor<>
    {
        Geometry2 const& m_geometry2;
        Collection& m_output_collection;
        Strategy const& m_strategy;
        
        visitor(Geometry2 const& geometry2,
                Collection& output_collection,
                Strategy const& strategy)
            : m_geometry2(geometry2)
            , m_output_collection(output_collection)
            , m_strategy(strategy)
        {}
        
        template <typename Geometry1>
        void operator()(Geometry1 const& geometry1) const
        {
            sym_difference
                <
                    Geometry1,
                    Geometry2
                >::apply(geometry1, m_geometry2, m_output_collection, m_strategy);
        }
    };
    
    template <typename Collection, typename Strategy>
    static inline void
    apply(variant<BOOST_VARIANT_ENUM_PARAMS(T)> const& geometry1,
          Geometry2 const& geometry2,
          Collection& output_collection,
          Strategy const& strategy)
    {
        boost::apply_visitor(visitor<Collection, Strategy>(geometry2,
                                                           output_collection,
                                                           strategy),
                             geometry1);
    }
};


template <typename Geometry1, BOOST_VARIANT_ENUM_PARAMS(typename T)>
struct sym_difference<Geometry1, variant<BOOST_VARIANT_ENUM_PARAMS(T)> >
{
    template <typename Collection, typename Strategy>
    struct visitor: static_visitor<>
    {
        Geometry1 const& m_geometry1;
        Collection& m_output_collection;
        Strategy const& m_strategy;
        
        visitor(Geometry1 const& geometry1,
                Collection& output_collection,
                Strategy const& strategy)
            : m_geometry1(geometry1)
            , m_output_collection(output_collection)
            , m_strategy(strategy)
        {}
        
        template <typename Geometry2>
        void operator()(Geometry2 const& geometry2) const
        {
            sym_difference
                <
                    Geometry1,
                    Geometry2
                >::apply(m_geometry1, geometry2, m_output_collection, m_strategy);
        }
    };
    
    template <typename Collection, typename Strategy>
    static inline void
    apply(Geometry1 const& geometry1,
          variant<BOOST_VARIANT_ENUM_PARAMS(T)> const& geometry2,
          Collection& output_collection,
          Strategy const& strategy)
    {
        boost::apply_visitor(visitor<Collection, Strategy>(geometry1,
                                                           output_collection,
                                                           strategy),
                             geometry2);
    }
};


template <BOOST_VARIANT_ENUM_PARAMS(typename T1), BOOST_VARIANT_ENUM_PARAMS(typename T2)>
struct sym_difference<variant<BOOST_VARIANT_ENUM_PARAMS(T1)>, variant<BOOST_VARIANT_ENUM_PARAMS(T2)> >
{
    template <typename Collection, typename Strategy>
    struct visitor: static_visitor<>
    {
        Collection& m_output_collection;
        Strategy const& m_strategy;
        
        visitor(Collection& output_collection, Strategy const& strategy)
            : m_output_collection(output_collection)
            , m_strategy(strategy)
        {}
        
        template <typename Geometry1, typename Geometry2>
        void operator()(Geometry1 const& geometry1,
                        Geometry2 const& geometry2) const
        {
            sym_difference
                <
                    Geometry1,
                    Geometry2
                >::apply(geometry1, geometry2, m_output_collection, m_strategy);
        }
    };
    
    template <typename Collection, typename Strategy>
    static inline void
    apply(variant<BOOST_VARIANT_ENUM_PARAMS(T1)> const& geometry1,
          variant<BOOST_VARIANT_ENUM_PARAMS(T2)> const& geometry2,
          Collection& output_collection,
          Strategy const& strategy)
    {
        boost::apply_visitor(visitor<Collection, Strategy>(output_collection,
                                                           strategy),
                             geometry1, geometry2);
    }
};
    
} // namespace resolve_variant


/*!
\brief \brief_calc2{symmetric difference}
\ingroup sym_difference
\details \details_calc2{symmetric difference, spatial set theoretic symmetric difference (XOR)}.
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\tparam Collection output collection, either a multi-geometry,
    or a std::vector<Geometry> / std::deque<Geometry> etc
\tparam Strategy \tparam_strategy{Sym_difference}
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\param output_collection the output collection
\param strategy \param_strategy{sym_difference}

\qbk{distinguish,with strategy}
\qbk{[include reference/algorithms/sym_difference.qbk]}
*/
template
<
    typename Geometry1,
    typename Geometry2,
    typename Collection,
    typename Strategy
>
inline void sym_difference(Geometry1 const& geometry1,
                           Geometry2 const& geometry2,
                           Collection& output_collection,
                           Strategy const& strategy)
{
    resolve_variant::sym_difference
        <
            Geometry1,
            Geometry2
        >::apply(geometry1, geometry2, output_collection, strategy);
}


/*!
\brief \brief_calc2{symmetric difference}
\ingroup sym_difference
\details \details_calc2{symmetric difference, spatial set theoretic symmetric difference (XOR)}.
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\tparam Collection output collection, either a multi-geometry,
    or a std::vector<Geometry> / std::deque<Geometry> etc
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\param output_collection the output collection

\qbk{[include reference/algorithms/sym_difference.qbk]}
*/
template
<
    typename Geometry1,
    typename Geometry2,
    typename Collection
>
inline void sym_difference(Geometry1 const& geometry1,
                           Geometry2 const& geometry2,
                           Collection& output_collection)
{
    resolve_variant::sym_difference
        <
            Geometry1,
            Geometry2
        >::apply(geometry1, geometry2, output_collection, default_strategy());    
}


}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_ALGORITHMS_SYM_DIFFERENCE_HPP
