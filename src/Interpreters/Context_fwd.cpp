#include <Interpreters/Context_fwd.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Shared>
Shared WithContextImpl<Shared>::getContext() const
{
    auto ptr = context.lock();
    if (!ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");
    return ptr;
}

template struct WithContextImpl<ContextPtr>;
template struct WithContextImpl<ContextMutablePtr>;

}
