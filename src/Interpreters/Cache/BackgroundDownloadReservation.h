#pragma once

#include <boost/noncopyable.hpp>
#include <functional>


namespace DB
{

struct BackgroundDownloadReservation : private boost::noncopyable
{
    using ReleaseReservationFunc = std::function<void()>;

    BackgroundDownloadReservation(size_t size_, ReleaseReservationFunc && release_) : size(size_), release(std::move(release_)) {}

    ~BackgroundDownloadReservation() { release(); }

    const size_t size;

private:
    ReleaseReservationFunc release;
};

using BackgroundDownloadReservationPtr = std::unique_ptr<BackgroundDownloadReservation>;

}
