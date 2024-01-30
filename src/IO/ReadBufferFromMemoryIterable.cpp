#include <string>
#include <string_view>

#include <IO/ReadBufferFromMemoryIterable.h>

namespace DB
{

namespace ReadBufferFromMemoryIterableDetails
{

template <> const char * get_element_data(const std::string & element) { return element.data(); }
template <> size_t get_element_size(const std::string & element) { return element.size(); }

template <> const char * get_element_data(const std::string_view & element) { return element.data(); }
template <> size_t get_element_size(const std::string_view & element) { return element.size(); }

}

}
