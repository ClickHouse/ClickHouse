#pragma once

#include <Common/Exception.h>

#include <map>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Block;

/// Scalar results of subqueries
using Scalars = std::map<String, Block>;

class Context;

/// Most used types have shorter names
/// TODO: in the first part of refactoring all the context pointers are non-const.
using ContextPtr = std::shared_ptr<Context>;
using ContextConstPtr = std::shared_ptr<const Context>;
using ContextWeakPtr = std::weak_ptr<Context>;
using ContextWeakConstPtr = std::weak_ptr<const Context>;

template <class Shared = ContextPtr>
struct WithContextImpl
{
    using Weak = typename Shared::weak_type;
    using ConstShared = std::shared_ptr<const typename Shared::element_type>;
    using ConstWeak = typename ConstShared::weak_type;

    WithContextImpl() = default;
    explicit WithContextImpl(Weak context_) : context(context_) {}

    inline Shared getContext() const
    {
        auto ptr = context.lock();
        if (!ptr) throw Exception("Context has expired", ErrorCodes::LOGICAL_ERROR);
        return ptr;
    }

protected:
    Weak context;
};

using WithContext = WithContextImpl<>;
using WithConstContext = WithContextImpl<ContextConstPtr>;

}
