#pragma once

#include <vector>

namespace DB
{
    class IAST;

    void intrusive_ptr_add_ref(const IAST * p);
    void intrusive_ptr_release(const IAST * p);
}


#include <boost/container/vector.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace DB
{

using ASTPtr = boost::intrusive_ptr<IAST>;
/// Boost vector with smaller stored size to save memory for AST children vectors.
using ASTs = boost::container::vector<
    ASTPtr,
    boost::container::new_allocator<ASTPtr>,
    boost::container::vector_options<boost::container::stored_size<uint32_t>>::type>;

template <typename T, typename ... Args>
constexpr boost::intrusive_ptr<T> make_intrusive(Args && ... args)
{
    return boost::intrusive_ptr<T>(new T(std::forward<Args>(args)...));
}

}
