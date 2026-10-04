#pragma once

#include <boost/container/vector.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace DB
{

class IAST;

void intrusive_ptr_add_ref(const IAST * p) noexcept;
void intrusive_ptr_release(const IAST * p) noexcept;

using ASTPtr = boost::intrusive_ptr<IAST>;

/// Boost requires the stored size type to be either strictly smaller than `size_t` or the very same type.
/// On 32-bit platforms `size_t` is already 32-bit, so there is nothing to shrink.
using ASTsStoredSize = std::conditional_t<(sizeof(uint32_t) < sizeof(size_t)), uint32_t, size_t>;

/// Boost vector with smaller stored size to save memory for AST children vectors.
using ASTs = boost::container::vector<
    ASTPtr,
    boost::container::new_allocator<ASTPtr>,
    boost::container::vector_options<boost::container::stored_size<ASTsStoredSize>>::type>;

template <typename T, typename ... Args>
constexpr boost::intrusive_ptr<T> make_intrusive(Args && ... args)
{
    return boost::intrusive_ptr<T>(new T(std::forward<Args>(args)...));
}

}
