// Copyright (c) 2001 Ronald Garcia, Indiana University (garcia@osl.iu.edu)
// Andrew Lumsdaine, Indiana University (lums@osl.iu.edu).
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_ARCHIVE_DETAIL_UTF8_CODECVT_FACET_HPP
#define BOOST_ARCHIVE_DETAIL_UTF8_CODECVT_FACET_HPP

#include <boost/config.hpp>

#ifdef BOOST_NO_STD_WSTREAMBUF
#error "wide char i/o not supported on this platform"
#endif

// std::codecvt_utf8 doesn't seem to work for any versions of msvc

#if defined(_MSC_VER) || defined(BOOST_NO_CXX11_HDR_CODECVT)
    // use boost's utf8 codecvt facet
    #include <boost/archive/detail/decl.hpp>
    #define BOOST_UTF8_BEGIN_NAMESPACE \
         namespace boost { namespace archive { namespace detail {
    #define BOOST_UTF8_DECL BOOST_ARCHIVE_DECL
    #define BOOST_UTF8_END_NAMESPACE }}}

    #include <boost/detail/utf8_codecvt_facet.hpp>

    #undef BOOST_UTF8_END_NAMESPACE
    #undef BOOST_UTF8_DECL
    #undef BOOST_UTF8_BEGIN_NAMESPACE
#else
    // use the standard vendor supplied facet
    #include <codecvt>
    namespace boost { namespace archive { namespace detail {
        typedef std::codecvt_utf8<wchar_t> utf8_codecvt_facet;
    } } }
#endif

#endif // BOOST_ARCHIVE_DETAIL_UTF8_CODECVT_FACET_HPP
