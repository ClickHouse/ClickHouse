#pragma once

/** Compatibility shim for pulsar-client-cpp: Boost >= 1.87 removed the deprecated
  * `const_buffers_1`/`mutable_buffers_1` classes. In modern Asio a single buffer is
  * itself a valid buffer sequence, so plain aliases are sufficient.
  */

#include_next <boost/asio/buffer.hpp>

namespace boost::asio
{
typedef const_buffer const_buffers_1;
typedef mutable_buffer mutable_buffers_1;
}
