//Copyright (c) 2008-2016 Emil Dotchevski and Reverge Studios, Inc.

//Distributed under the Boost Software License, Version 1.0. (See accompanying
//file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef UUID_0595DEC2C72D11E6B6B77C2A07624D53
#define UUID_0595DEC2C72D11E6B6B77C2A07624D53

#include <boost/qvm/inline.hpp>
#include <boost/qvm/assert.hpp>

namespace
boost
    {
    namespace
    qvm
        {
        template <class>
        struct quat_traits;

        template <class QuatType,class ScalarType>
        struct
        quat_traits_defaults
            {
            typedef QuatType quat_type;
            typedef ScalarType scalar_type;

            template <int I>
            static
            BOOST_QVM_INLINE_CRITICAL
            scalar_type
            read_element( quat_type const & x )
                {
                return quat_traits<quat_type>::template write_element<I>(const_cast<quat_type &>(x));
                }
            };
        }
    }

#endif
