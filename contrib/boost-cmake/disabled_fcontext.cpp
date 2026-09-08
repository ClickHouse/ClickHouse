#include <boost/context/detail/fcontext.hpp>
#include <stdexcept>

/// The FilC ARM64 runtime does not implement fiber contexts.
extern "C" boost::context::detail::fcontext_t make_fcontext(
    void *, std::size_t, void (*)(boost::context::detail::transfer_t))
{
    throw std::runtime_error("Stackful coroutines are disabled in this experimental FilC build");
}

extern "C" boost::context::detail::transfer_t jump_fcontext(
    boost::context::detail::fcontext_t, void *)
{
    throw std::runtime_error("Stackful coroutines are disabled in this experimental FilC build");
}

extern "C" boost::context::detail::transfer_t ontop_fcontext(
    boost::context::detail::fcontext_t, void *,
    boost::context::detail::transfer_t (*)(boost::context::detail::transfer_t))
{
    throw std::runtime_error("Stackful coroutines are disabled in this experimental FilC build");
}
