/*
Copyright Ruslan Baratov 2017
Copyright Rene Rivera 2017
Distributed under the Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

#ifndef BOOST_PREDEF_PLAT_IOS_H
#define BOOST_PREDEF_PLAT_IOS_H

#include <boost/predef/os/ios.h> // BOOST_OS_IOS
#include <boost/predef/version_number.h> // BOOST_VERSION_NUMBER_NOT_AVAILABLE

/*`
[heading `BOOST_PLAT_IOS_DEVICE`]
[heading `BOOST_PLAT_IOS_SIMULATOR`]

[table
    [[__predef_symbol__] [__predef_version__]]

    [[`TARGET_IPHONE_SIMULATOR`] [__predef_detection__]]
    ]
 */

#define BOOST_PLAT_IOS_DEVICE BOOST_VERSION_NUMBER_NOT_AVAILABLE
#define BOOST_PLAT_IOS_SIMULATOR BOOST_VERSION_NUMBER_NOT_AVAILABLE

// https://opensource.apple.com/source/CarbonHeaders/CarbonHeaders-18.1/TargetConditionals.h
#if BOOST_OS_IOS
#    include <TargetConditionals.h>
#    if TARGET_IPHONE_SIMULATOR == 1
#        undef BOOST_PLAT_IOS_SIMULATOR
#        define BOOST_PLAT_IOS_SIMULATOR BOOST_VERSION_NUMBER_AVAILABLE
#    else
#        undef BOOST_PLAT_IOS_DEVICE
#        define BOOST_PLAT_IOS_DEVICE BOOST_VERSION_NUMBER_AVAILABLE
#    endif
#endif

#if BOOST_PLAT_IOS_SIMULATOR
#    define BOOST_PLAT_IOS_SIMULATOR_AVAILABLE
#    include <boost/predef/detail/platform_detected.h>
#endif

#if BOOST_PLAT_IOS_DEVICE
#    define BOOST_PLAT_IOS_DEVICE_AVAILABLE
#    include <boost/predef/detail/platform_detected.h>
#endif

#define BOOST_PLAT_IOS_SIMULATOR_NAME "iOS Simulator"
#define BOOST_PLAT_IOS_DEVICE_NAME "iOS Device"

#endif // BOOST_PREDEF_PLAT_IOS_H

#include <boost/predef/detail/test.h>
BOOST_PREDEF_DECLARE_TEST(BOOST_PLAT_IOS_SIMULATOR,BOOST_PLAT_IOS_SIMULATOR_NAME)
BOOST_PREDEF_DECLARE_TEST(BOOST_PLAT_IOS_DEVICE,BOOST_PLAT_IOS_DEVICE_NAME)
