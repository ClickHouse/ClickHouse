#pragma once
#include <common/defines.h>


#define BOOST_THREAD_PROVIDES_FUTURE
#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION

#include <boost/thread/future.hpp>

#ifdef BOOST_THREAD_PROVIDES_FUTURE
#endif



template <typename T>
using Future = boost::future<T>;

template <typename T>
using Promise = boost::promise<T>;
