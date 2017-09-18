//  (C) Copyright Brian Kuhl 2016.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// Check this is a recent EDG based compiler, otherwise we don't support it here:


#ifndef __EDG_VERSION__
#     error "Unknown Diab compiler version - please run the configure tests and report the results"
#endif

#include "boost/config/compiler/common_edg.hpp"

#define BOOST_HAS_LONG_LONG
#define BOOST_NO_TWO_PHASE_NAME_LOOKUP
#define BOOST_NO_CXX11_HDR_INITIALIZER_LIST
#define BOOST_NO_CXX11_HDR_CODECVT
#define BOOST_COMPILER "Wind River Diab " BOOST_STRINGIZE(__VERSION_NUMBER__)
