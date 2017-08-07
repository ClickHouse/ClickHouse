
//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/coroutine/exceptions.hpp"

namespace boost {
namespace coroutines {

class coroutine_error_category : public system::error_category
{
public:
    virtual const char* name() const BOOST_NOEXCEPT
    { return "coroutine"; }

    virtual std::string message( int ev) const
    {
        switch (BOOST_SCOPED_ENUM_NATIVE(coroutine_errc)(ev))
        {
        case coroutine_errc::no_data:
            return std::string("Operation not permitted because coroutine "
                          "has no valid result.");
        }
        return std::string("unspecified coroutine_errc value\n");
    }
};

BOOST_COROUTINES_DECL
system::error_category const& coroutine_category() BOOST_NOEXCEPT
{
    static coroutines::coroutine_error_category cat;
    return cat;
}

}}
