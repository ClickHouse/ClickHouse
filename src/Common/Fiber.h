#pragma once
/// defines.h should be included before fiber.hpp
/// BOOST_USE_ASAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT should be correctly defined for sanitizers.
#include <base/defines.h>
#include <boost/context/fiber.hpp>

using Fiber = boost::context::fiber;
