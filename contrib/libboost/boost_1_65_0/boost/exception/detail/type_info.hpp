//Copyright (c) 2006-2010 Emil Dotchevski and Reverge Studios, Inc.

//Distributed under the Boost Software License, Version 1.0. (See accompanying
//file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef UUID_C3E1741C754311DDB2834CCA55D89593
#define UUID_C3E1741C754311DDB2834CCA55D89593

#include <boost/config.hpp>
#include <boost/core/typeinfo.hpp>
#include <boost/core/demangle.hpp>
#include <boost/current_function.hpp>
#include <string>

#if (__GNUC__*100+__GNUC_MINOR__>301) && !defined(BOOST_EXCEPTION_ENABLE_WARNINGS)
#pragma GCC system_header
#endif
#if defined(_MSC_VER) && !defined(BOOST_EXCEPTION_ENABLE_WARNINGS)
#pragma warning(push,1)
#endif

namespace
boost
    {
    template <class T>
    inline
    std::string
    tag_type_name()
        {
#ifdef BOOST_NO_TYPEID
        return BOOST_CURRENT_FUNCTION;
#else
        return core::demangle(typeid(T*).name());
#endif
        }

    template <class T>
    inline
    std::string
    type_name()
        {
#ifdef BOOST_NO_TYPEID
        return BOOST_CURRENT_FUNCTION;
#else
        return core::demangle(typeid(T).name());
#endif
        }

    namespace
    exception_detail
        {
        struct
        type_info_
            {
            core::typeinfo const * type_;

            explicit
            type_info_( core::typeinfo const & type ):
                type_(&type)
                {
                }

            friend
            bool
            operator<( type_info_ const & a, type_info_ const & b )
                {
                return 0!=(a.type_->before(*b.type_));
                }
            };
        }
    }

#define BOOST_EXCEPTION_STATIC_TYPEID(T) ::boost::exception_detail::type_info_(BOOST_CORE_TYPEID(T))

#ifndef BOOST_NO_RTTI
#define BOOST_EXCEPTION_DYNAMIC_TYPEID(x) ::boost::exception_detail::type_info_(typeid(x))
#endif

#if defined(_MSC_VER) && !defined(BOOST_EXCEPTION_ENABLE_WARNINGS)
#pragma warning(pop)
#endif
#endif
