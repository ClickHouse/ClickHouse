#pragma once

#include <Functions/JSONPath/Generator/IGenerator_fwd.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{

template <typename JSONParser>
class IGenerator
{
public:
    using TElement = typename JSONParser::Element;

    IGenerator() = default;

    virtual const char * getName() const = 0;

    /**
     * Used to yield next non-ignored element describes by JSONPath query.
     *
     * @param element to be extracted into
     * @return true if generator is not exhausted
     */
    virtual VisitorStatus getNextItem(TElement & element) = 0;

    virtual VisitorStatus getNextItemBatch(TElement & element, std::function<void(const TElement &)> & res_func) = 0;

    virtual ~IGenerator() = default;
};

}
