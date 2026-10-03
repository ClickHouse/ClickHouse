#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathStar.h>
#include <Functions/JSONPath/Generator/IVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class VisitorJSONPathStar : public IVisitor<JSONParser>
{
public:
    explicit VisitorJSONPathStar(ASTPtr)
    {
        current_index = 0;
    }

    const char * getName() const override { return "VisitorJSONPathStar"; }

    VisitorStatus apply(typename JSONParser::Element & element) const override
    {
        if (element.isArray())
        {
            typename JSONParser::Array array = element.getArray();
            element = array[current_index];
        }
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        /// Per RFC 9535 lax semantics, a non-array element is treated as a one-element array containing the element itself.
        const size_t array_size = element.isArray() ? element.getArray().size() : 1;

        VisitorStatus status = {};
        if (current_index < array_size)
        {
            apply(element);
            status = VisitorStatus::Ok;
        }
        else
        {
            status = VisitorStatus::Ignore;
            this->setExhausted(true);
        }

        return status;
    }

    void reinitialize() override
    {
        current_index = 0;
        this->setExhausted(false);
    }

    void updateState() override
    {
        current_index++;
    }

private:
    UInt32 current_index;
};

}
