#pragma once

#include <Functions/JSONPath/Generators/IGenerator_fwd.h>
#include <Functions/JSONPath/Generators/VisitorStatus.h>
#include <Parsers/IAST.h>

namespace DB
{

template <typename JSONParser>
class IGenerator
{
public:
    IGenerator() = default;

    virtual const char * getName() const = 0;

    /**
     * Used to yield next element in JSONPath query. Does so by recursively calling getNextItem
     * on its children Generators one by one.
     *
     * @param element to be extracted into
     * @return true if generator is not exhausted
     */
    virtual VisitorStatus getNextItem(typename JSONParser::Element & element) = 0;

    virtual ~IGenerator() = default;
};

} // namespace DB
