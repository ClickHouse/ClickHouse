#pragma once

#include <utility>

namespace ext
{
    /// \brief Identity function for use with other algorithms as a pass-through.
    class identity
    {
        /** \brief Function pointer type template for converting identity to a function pointer.
         *    Presumably useless, provided for completeness. */
        template <typename T> using function_ptr_t = T &&(*)(T &&);

        /** \brief Implementation of identity as a non-instance member function for taking function pointer. */
        template <typename T> static T && invoke(T && t) { return std::forward<T>(t); }

    public:
        /** \brief Returns the value passed as a sole argument using perfect forwarding. */
        template <typename T> T && operator()(T && t) const { return std::forward<T>(t); }

        /** \brief Allows conversion of identity instance to a function pointer. */
        template <typename T> operator function_ptr_t<T>() const { return &invoke; };
    };
}
