// Boost.Geometry Index
//
// R-tree spatial query visitor implementation
//
// Copyright (c) 2011-2014 Adam Wulkiewicz, Lodz, Poland.
//
// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_INDEX_DETAIL_RTREE_VISITORS_SPATIAL_QUERY_HPP
#define BOOST_GEOMETRY_INDEX_DETAIL_RTREE_VISITORS_SPATIAL_QUERY_HPP

namespace boost { namespace geometry { namespace index {

namespace detail { namespace rtree { namespace visitors {

template <typename Value, typename Options, typename Translator, typename Box, typename Allocators, typename Predicates, typename OutIter>
struct spatial_query
    : public rtree::visitor<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag, true>::type
{
    typedef typename rtree::node<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag>::type node;
    typedef typename rtree::internal_node<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag>::type internal_node;
    typedef typename rtree::leaf<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag>::type leaf;

    typedef typename Allocators::size_type size_type;

    static const unsigned predicates_len = index::detail::predicates_length<Predicates>::value;

    inline spatial_query(Translator const& t, Predicates const& p, OutIter out_it)
        : tr(t), pred(p), out_iter(out_it), found_count(0)
    {}

    inline void operator()(internal_node const& n)
    {
        typedef typename rtree::elements_type<internal_node>::type elements_type;
        elements_type const& elements = rtree::elements(n);

        // traverse nodes meeting predicates
        for (typename elements_type::const_iterator it = elements.begin();
            it != elements.end(); ++it)
        {
            // if node meets predicates
            // 0 - dummy value
            if ( index::detail::predicates_check<index::detail::bounds_tag, 0, predicates_len>(pred, 0, it->first) )
                rtree::apply_visitor(*this, *it->second);
        }
    }

    inline void operator()(leaf const& n)
    {
        typedef typename rtree::elements_type<leaf>::type elements_type;
        elements_type const& elements = rtree::elements(n);

        // get all values meeting predicates
        for (typename elements_type::const_iterator it = elements.begin();
            it != elements.end(); ++it)
        {
            // if value meets predicates
            if ( index::detail::predicates_check<index::detail::value_tag, 0, predicates_len>(pred, *it, tr(*it)) )
            {
                *out_iter = *it;
                ++out_iter;

                ++found_count;
            }
        }
    }

    Translator const& tr;

    Predicates pred;

    OutIter out_iter;
    size_type found_count;
};

template <typename Value, typename Options, typename Translator, typename Box, typename Allocators, typename Predicates>
class spatial_query_incremental
    : public rtree::visitor<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag, true>::type
{
public:
    typedef typename rtree::node<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag>::type node;
    typedef typename rtree::internal_node<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag>::type internal_node;
    typedef typename rtree::leaf<Value, typename Options::parameters_type, Box, Allocators, typename Options::node_tag>::type leaf;

    typedef typename Allocators::size_type size_type;
    typedef typename Allocators::const_reference const_reference;
    typedef typename Allocators::node_pointer node_pointer;

    typedef typename rtree::elements_type<internal_node>::type::const_iterator internal_iterator;
    typedef typename rtree::elements_type<leaf>::type leaf_elements;
    typedef typename rtree::elements_type<leaf>::type::const_iterator leaf_iterator;

    static const unsigned predicates_len = index::detail::predicates_length<Predicates>::value;

    inline spatial_query_incremental()
        : m_translator(NULL)
//        , m_pred()
        , m_values(NULL)
        , m_current()
    {}

    inline spatial_query_incremental(Translator const& t, Predicates const& p)
        : m_translator(::boost::addressof(t))
        , m_pred(p)
        , m_values(NULL)
        , m_current()
    {}

    inline void operator()(internal_node const& n)
    {
        typedef typename rtree::elements_type<internal_node>::type elements_type;
        elements_type const& elements = rtree::elements(n);

        m_internal_stack.push_back(std::make_pair(elements.begin(), elements.end()));
    }

    inline void operator()(leaf const& n)
    {
        m_values = ::boost::addressof(rtree::elements(n));
        m_current = rtree::elements(n).begin();
    }

    const_reference dereference() const
    {
        BOOST_GEOMETRY_INDEX_ASSERT(m_values, "not dereferencable");
        return *m_current;
    }

    void initialize(node_pointer root)
    {
        rtree::apply_visitor(*this, *root);
        search_value();
    }

    void increment()
    {
        ++m_current;
        search_value();
    }

    void search_value()
    {
        for (;;)
        {
            // if leaf is choosen, move to the next value in leaf
            if ( m_values )
            {
                if ( m_current != m_values->end() )
                {
                    // return if next value is found
                    Value const& v = *m_current;
                    if ( index::detail::predicates_check<index::detail::value_tag, 0, predicates_len>(m_pred, v, (*m_translator)(v)) )
                        return;

                    ++m_current;
                }
                // no more values, clear current leaf
                else
                {
                    m_values = 0;
                }
            }
            // if leaf isn't choosen, move to the next leaf
            else
            {
                // return if there is no more nodes to traverse
                if ( m_internal_stack.empty() )
                    return;

                // no more children in current node, remove it from stack
                if ( m_internal_stack.back().first == m_internal_stack.back().second )
                {
                    m_internal_stack.pop_back();
                    continue;
                }

                internal_iterator it = m_internal_stack.back().first;
                ++m_internal_stack.back().first;

                // next node is found, push it to the stack
                if ( index::detail::predicates_check<index::detail::bounds_tag, 0, predicates_len>(m_pred, 0, it->first) )
                    rtree::apply_visitor(*this, *(it->second));
            }
        }
    }

    bool is_end() const
    {
        return 0 == m_values;
    }

    friend bool operator==(spatial_query_incremental const& l, spatial_query_incremental const& r)
    {
        return (l.m_values == r.m_values) && (0 == l.m_values || l.m_current == r.m_current );
    }

private:

    const Translator * m_translator;

    Predicates m_pred;

    std::vector< std::pair<internal_iterator, internal_iterator> > m_internal_stack;
    const leaf_elements * m_values;
    leaf_iterator m_current;
};

}}} // namespace detail::rtree::visitors

}}} // namespace boost::geometry::index

#endif // BOOST_GEOMETRY_INDEX_DETAIL_RTREE_VISITORS_SPATIAL_QUERY_HPP
