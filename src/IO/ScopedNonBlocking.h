#pragma once

namespace Poco::Net
{
class SocketImpl;
}

namespace DB
{

/// Switches a blocking socket to non-blocking mode for the lifetime of the object and restores the
/// mode in the destructor, logging instead of throwing if that fails. A Silk fiber socket is left
/// alone: its TLS BIO is always non-blocking.
class ScopedNonBlocking
{
public:
    explicit ScopedNonBlocking(Poco::Net::SocketImpl & socket_impl_);
    ~ScopedNonBlocking();

    ScopedNonBlocking(const ScopedNonBlocking &) = delete;
    ScopedNonBlocking & operator=(const ScopedNonBlocking &) = delete;

private:
    Poco::Net::SocketImpl & socket_impl;
    /// Whether a regular (non-Silk) socket was blocking before.
    bool was_blocking = false;
};

}
