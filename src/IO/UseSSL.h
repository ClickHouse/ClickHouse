#pragma once

#include <boost/noncopyable.hpp>
#include <atomic>

namespace DB
{
// http://stackoverflow.com/questions/18315472/https-request-in-c-using-poco
struct UseSSL : private boost::noncopyable
{
    std::atomic<uint8_t> ref_count{0};

    UseSSL();
    ~UseSSL();
};
}
