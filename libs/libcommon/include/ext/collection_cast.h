#pragma once

#include <iterator>

namespace ext
{
    /** \brief Returns collection of specified container-type.
     *    Retains stored value_type, constructs resulting collection using iterator range. */
    template <template <typename...> class ResultCollection, typename Collection>
    auto collection_cast(const Collection & collection)
    {
        using value_type = typename Collection::value_type;

        return ResultCollection<value_type>(std::begin(collection), std::end(collection));
    };

    /** \brief Returns collection of specified type.
     *    Performs implicit conversion of between source and result value_type, if available and required. */
    template <typename ResultCollection, typename Collection>
    auto collection_cast(const Collection & collection)
    {
        return ResultCollection(std::begin(collection), std::end(collection));
    }
}
