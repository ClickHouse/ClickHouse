#include <base/int8_to_string.h>

namespace std
{
std::string to_string(Int8 v) /// NOLINT (cert-dcl58-cpp)
{
    return to_string(int8_t{v});
}
}
