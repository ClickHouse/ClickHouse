#pragma once

#include <Functions/JSONPath/Generator/IGenerator_fwd.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{

template <typename JSONParser>
class IGenerator
{
public:
    IGenerator() = default;

    virtual const char * getName() const = 0;

    /**
     * Used to yield next non-ignored element describes by JSONPath query.
     *
     * @param element to be extracted into
     * @return true if generator is not exhausted
     */
    virtual VisitorStatus getNextItem(typename JSONParser::Element & element) = 0;

    virtual ~IGenerator() = default;
};

}
