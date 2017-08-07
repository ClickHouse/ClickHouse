// (C) Copyright Michael Glassford 2004.
// (C) Copyright 2007 Anthony Williams
// Use, modification and distribution are subject to the
// Boost Software License, Version 1.0. (See accompanying file
// LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <boost/thread/detail/config.hpp>

#if defined(BOOST_HAS_WINTHREADS) && (defined(BOOST_THREAD_BUILD_LIB) || defined(BOOST_THREAD_TEST) || defined(UNDER_CE)) && (!defined(_MSC_VER) || defined(UNDER_CE))

namespace boost
{
    /*
    This file is a "null" implementation of tss cleanup; it's
    purpose is to to eliminate link errors in cases
    where it is known that tss cleanup is not needed.
    */

    void tss_cleanup_implemented(void)
    {
        /*
        This function's sole purpose is to cause a link error in cases where
        automatic tss cleanup is not implemented by Boost.Threads as a
        reminder that user code is responsible for calling the necessary
        functions at the appropriate times (and for implementing an a
        tss_cleanup_implemented() function to eliminate the linker's
        missing symbol error).

        If Boost.Threads later implements automatic tss cleanup in cases
        where it currently doesn't (which is the plan), the duplicate
        symbol error will warn the user that their custom solution is no
        longer needed and can be removed.
        */
    }

}

#endif //defined(BOOST_HAS_WINTHREADS) && defined(BOOST_THREAD_BUILD_LIB) && !defined(_MSC_VER)
