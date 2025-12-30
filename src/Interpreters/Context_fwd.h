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
using ContextPtr = std::shared_ptr<const Context>;
using ContextMutablePtr = std::shared_ptr<Context>;
using ContextWeakPtr = std::weak_ptr<const Context>;
using ContextWeakMutablePtr = std::weak_ptr<Context>;

template <class Shared = ContextPtr>
struct WithContextImpl
{
    using Weak = typename Shared::weak_type;
    using ConstShared = std::shared_ptr<const typename Shared::element_type>;
    using ConstWeak = typename ConstShared::weak_type;

    WithContextImpl() = default;
    explicit WithContextImpl(Weak context_) : context(context_) {}

    Shared getContext() const
    {
        auto ptr = context.lock();
        if (!ptr) throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");
        return ptr;
    }

protected:
    Weak context;
};

using WithContext = WithContextImpl<>;
using WithConstContext = WithContext; /// For compatibility. Use WithContext.
using WithMutableContext = WithContextImpl<ContextMutablePtr>;

}
