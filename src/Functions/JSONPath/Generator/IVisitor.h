#pragma once

#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class IVisitor
{
public:
    using TElement = typename JSONParser::Element;
    virtual const char * getName() const = 0;

    /**
     * Applies this visitor to document and mutates its state
     * @param element simdjson element
     */
    virtual VisitorStatus visit(TElement & element) = 0;

    /**
     * Applies this visitor to document and mutates its state, returning a batch of results
     * @param element simdjson element
     */
    virtual VisitorStatus visitBatch(TElement &element, std::function<void(const TElement &)> & res_func, bool can_reduce) = 0;

    /**
     * Applies this visitor to document, but does not mutate state
     * @param element simdjson element
     */
    virtual VisitorStatus apply(typename JSONParser::Element & element) const = 0;

    /**
     * Restores visitor's initial state for later use
     */
    virtual void reinitialize() = 0;

    virtual void updateState() = 0;

    bool isExhausted() { return is_exhausted; }

    void setExhausted(bool exhausted) { is_exhausted = exhausted; }

    virtual ~IVisitor() = default;

private:
    /**
     * This variable is for detecting whether a visitor's next visit will be able
     *  to yield a new item.
     */
    bool is_exhausted = false;
};

}
