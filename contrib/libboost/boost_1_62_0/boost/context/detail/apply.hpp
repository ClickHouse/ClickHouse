
//          Copyright Oliver Kowalke 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_CONTEXT_DETAIL_APPLY_H
#define BOOST_CONTEXT_DETAIL_APPLY_H

#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include <boost/config.hpp>

#include <boost/context/detail/config.hpp>
#include <boost/context/detail/invoke.hpp>
#include <boost/context/detail/index_sequence.hpp>

#ifdef BOOST_HAS_ABI_HEADERS
# include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace context {
namespace detail {

template< typename Fn, typename Tpl, std::size_t ... I >
auto
apply_impl( Fn && fn, Tpl && tpl, index_sequence< I ... >) 
    -> decltype( invoke( std::forward< Fn >( fn), std::get< I >( std::forward< Tpl >( tpl) ) ... ) )
{
    return invoke( std::forward< Fn >( fn), std::get< I >( std::forward< Tpl >( tpl) ) ... );
}

template< typename Fn, typename Tpl >
auto
apply( Fn && fn, Tpl && tpl) 
    -> decltype( apply_impl( std::forward< Fn >( fn),
                 std::forward< Tpl >( tpl),
                 make_index_sequence< std::tuple_size< typename std::decay< Tpl >::type >::value >{}) )
{
    return apply_impl( std::forward< Fn >( fn),
                       std::forward< Tpl >( tpl),
                       make_index_sequence< std::tuple_size< typename std::decay< Tpl >::type >::value >{});
}

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#include BOOST_ABI_SUFFIX
#endif

#endif // BOOST_CONTEXT_DETAIL_APPLY_H
