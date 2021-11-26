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
    VisitorJSONPathStar(ASTPtr)
    {
        current_index = 0;
    }

    const char * getName() const override { return "VisitorJSONPathStar"; }

    VisitorStatus apply(typename JSONParser::Element & element) const override
    {
        typename JSONParser::Element result;
        typename JSONParser::Array array = element.getArray();
        element = array[current_index];
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        if (!element.isArray())
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        VisitorStatus status;
        if (current_index < element.getArray().size())
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
