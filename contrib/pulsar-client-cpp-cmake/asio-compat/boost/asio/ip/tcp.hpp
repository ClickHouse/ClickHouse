//
// Based on boost/asio/ip/tcp.hpp
// Copyright (c) 2003-2025 Christopher M. Kohlhoff (chris at kohlhoff dot com)
// Distributed under the Boost Software License, Version 1.0.
//
// Compatibility shim for pulsar-client-cpp: Boost >= 1.87 removed the deprecated
// `resolver::query`/`resolver::iterator` typedefs and the `async_resolve(query, handler)`
// overload. This header replaces <boost/asio/ip/tcp.hpp> (the include guard matches the
// original header, so whichever is included first wins) and restores that API through
// a `resolver` subclass.
//

#ifndef BOOST_ASIO_IP_TCP_HPP
#define BOOST_ASIO_IP_TCP_HPP

#include <boost/asio/detail/config.hpp>
#include <boost/asio/basic_socket_acceptor.hpp>
#include <boost/asio/basic_socket_iostream.hpp>
#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio/detail/socket_option.hpp>
#include <boost/asio/detail/socket_types.hpp>
#include <boost/asio/ip/basic_endpoint.hpp>
#include <boost/asio/ip/basic_resolver.hpp>
#include <boost/asio/ip/basic_resolver_iterator.hpp>
#include <boost/asio/ip/basic_resolver_query.hpp>

#include <utility>

#include <boost/asio/detail/push_options.hpp>

namespace boost {
namespace asio {
namespace ip {

/// Encapsulates the flags needed for TCP.
class tcp
{
public:
  /// The type of a TCP endpoint.
  typedef basic_endpoint<tcp> endpoint;

  /// Construct to represent the IPv4 TCP protocol.
  static tcp v4() noexcept
  {
    return tcp(BOOST_ASIO_OS_DEF(AF_INET));
  }

  /// Construct to represent the IPv6 TCP protocol.
  static tcp v6() noexcept
  {
    return tcp(BOOST_ASIO_OS_DEF(AF_INET6));
  }

  /// Obtain an identifier for the type of the protocol.
  int type() const noexcept
  {
    return BOOST_ASIO_OS_DEF(SOCK_STREAM);
  }

  /// Obtain an identifier for the protocol.
  int protocol() const noexcept
  {
    return BOOST_ASIO_OS_DEF(IPPROTO_TCP);
  }

  /// Obtain an identifier for the protocol family.
  int family() const noexcept
  {
    return family_;
  }

  /// The TCP socket type.
  typedef basic_stream_socket<tcp> socket;

  /// The TCP acceptor type.
  typedef basic_socket_acceptor<tcp> acceptor;

  /// The TCP resolver type, with the deprecated pre-1.87 API restored.
  class resolver : public basic_resolver<tcp>
  {
  public:
    using basic_resolver<tcp>::basic_resolver;
    using basic_resolver<tcp>::async_resolve;

    typedef basic_resolver_query<tcp> query;
    typedef basic_resolver_iterator<tcp> iterator;

    template <typename Handler>
    void async_resolve(const query & q, Handler && handler)
    {
      basic_resolver<tcp>::async_resolve(
          q.host_name(), q.service_name(),
          [h = std::forward<Handler>(handler)](
              const boost::system::error_code & ec, results_type results) mutable
          { h(ec, ec ? iterator() : results.begin()); });
    }
  };

#if !defined(BOOST_ASIO_NO_IOSTREAM)
  /// The TCP iostream type.
  typedef basic_socket_iostream<tcp> iostream;
#endif // !defined(BOOST_ASIO_NO_IOSTREAM)

  /// Socket option for disabling the Nagle algorithm.
  typedef boost::asio::detail::socket_option::boolean<
    BOOST_ASIO_OS_DEF(IPPROTO_TCP), BOOST_ASIO_OS_DEF(TCP_NODELAY)> no_delay;

  /// Compare two protocols for equality.
  friend bool operator==(const tcp& p1, const tcp& p2)
  {
    return p1.family_ == p2.family_;
  }

  /// Compare two protocols for inequality.
  friend bool operator!=(const tcp& p1, const tcp& p2)
  {
    return p1.family_ != p2.family_;
  }

private:
  // Construct with a specific family.
  explicit tcp(int protocol_family) noexcept
    : family_(protocol_family)
  {
  }

  int family_;
};

} // namespace ip
} // namespace asio
} // namespace boost

#include <boost/asio/detail/pop_options.hpp>

#endif // BOOST_ASIO_IP_TCP_HPP
