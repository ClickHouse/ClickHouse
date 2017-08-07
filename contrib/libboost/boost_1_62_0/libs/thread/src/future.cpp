// (C) Copyright 2012 Vicente J. Botet Escriba
// Use, modification and distribution are subject to the
// Boost Software License, Version 1.0. (See accompanying file
// LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <boost/thread/detail/config.hpp>
#ifndef BOOST_NO_EXCEPTIONS


#include <boost/thread/futures/future_error_code.hpp>
#include <string>

namespace boost
{

  namespace thread_detail
  {

    class  future_error_category :
      public boost::system::error_category
    {
    public:
        virtual const char* name() const BOOST_NOEXCEPT;
        virtual std::string message(int ev) const;
    };

    const char*
    future_error_category::name() const BOOST_NOEXCEPT
    {
        return "future";
    }

    std::string
    future_error_category::message(int ev) const
    {
        switch (BOOST_SCOPED_ENUM_NATIVE(future_errc)(ev))
        {
        case future_errc::broken_promise:
            return std::string("The associated promise has been destructed prior "
                          "to the associated state becoming ready.");
        case future_errc::future_already_retrieved:
            return std::string("The future has already been retrieved from "
                          "the promise or packaged_task.");
        case future_errc::promise_already_satisfied:
            return std::string("The state of the promise has already been set.");
        case future_errc::no_state:
            return std::string("Operation not permitted on an object without "
                          "an associated state.");
        }
        return std::string("unspecified future_errc value\n");
    }
    future_error_category future_error_category_var;
  }

  BOOST_THREAD_DECL
  const system::error_category&
  future_category() BOOST_NOEXCEPT
  {
      return thread_detail::future_error_category_var;
  }

}
#endif

