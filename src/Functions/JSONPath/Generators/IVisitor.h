#pragma once

#include <Functions/JSONPath/Generators/VisitorStatus.h>

namespace DB {

template <typename JSONParser>
class IVisitor {
public:
    /**
     * Applies this visitor to document and mutates its state
     * @param element simdjson element
     */
    virtual VisitorStatus visit(typename JSONParser::Element & element) = 0;

    /**
     * Applies this visitor to document, but does not mutate state
     * @param element simdjson element
     */
    virtual VisitorStatus apply(typename JSONParser::Element & element) const = 0;

    /**
     * Restores visitor's initial state for later use
     */
    virtual void reinitialize() = 0;

    bool isExhausted() {
        return is_exhausted;
    }

    void setExhausted(bool exhausted) {
        is_exhausted = exhausted;
    }

    virtual ~IVisitor() = default;
private:
    bool is_exhausted = false;
};

} // namespace DB
