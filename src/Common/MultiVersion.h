#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <base/defines.h>


/** Allow to store and read-only usage of an object in several threads,
  *  and to atomically replace an object in another thread.
  * The replacement is atomic and reading threads can work with different versions of an object.
  *
  * Usage:
  *  MultiVersion<T> x;
  * - on data update:
  *  x.set(new value);
  * - on read-only usage:
  * {
  *     MultiVersion<T>::Version current_version = x.get();
  *     // use *current_version
  * }   // now we finish own current version; if the version is outdated and no one else is using it - it will be destroyed.
  *
  * All methods are thread-safe.
  *
  * Standard library does not have atomic_shared_ptr, and we do not use std::atomic* operations on shared_ptr,
  * because standard library implementation uses fixed table of mutexes, and it is better to avoid contention here.
  */
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;

    /// Default initialization - by nullptr.
    MultiVersion() = default;

    explicit MultiVersion(std::unique_ptr<const T> && value)
        : current_version(std::move(value))
    {
    }

    /// There is no copy constructor because only one MultiVersion should own the same object.
    MultiVersion(MultiVersion && src) { *this = std::move(src); } /// NOLINT

    MultiVersion & operator=(MultiVersion && src) /// NOLINT
    {
        if (this != &src)
        {
            Version version;

            {
                std::lock_guard<std::mutex> lock(src.mutex);
                src.current_version.swap(version);
            }

            std::lock_guard<std::mutex> lock(mutex);
            current_version = std::move(version);
        }

        return *this;
    }

    /// Obtain current version for read-only usage. Returns shared_ptr, that manages lifetime of version.
    Version get() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return current_version;
    }

    /// Update an object with new version.
    void set(std::unique_ptr<const T> && value)
    {
        Version version{std::move(value)};
        std::lock_guard<std::mutex> lock(mutex);
        current_version = std::move(version);
    }

private:
    mutable std::mutex mutex;
    Version current_version;
};
