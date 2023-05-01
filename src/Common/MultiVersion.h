#pragma once

#include <atomic>
#include <memory>
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

    /// Obtain current version for read-only usage. Returns shared_ptr, that manages lifetime of version.
    Version get() const
    {
        return std::atomic_load(&current_version);
    }

    /// TODO: replace atomic_load/store() on shared_ptr (which is deprecated as of C++20) by C++20 std::atomic<std::shared_ptr>.
    /// Clang 15 currently does not support it.

    /// Update an object with new version.
    void set(std::unique_ptr<const T> && value)
    {
        std::atomic_store(&current_version, Version{std::move(value)});
    }

private:
    Version current_version;
};
