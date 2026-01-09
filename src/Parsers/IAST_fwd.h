#pragma once

#include <vector>

namespace DB
{
    class IAST;

    void intrusive_ptr_add_ref(const IAST * p);
    void intrusive_ptr_release(const IAST * p);
}


#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace DB
{

using ASTPtr = boost::intrusive_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

template <typename T, typename ... Args>
constexpr boost::intrusive_ptr<T> make_intrusive(Args && ... args)
{
    return boost::intrusive_ptr<T>(new T(std::forward<Args>(args)...));
}

}
