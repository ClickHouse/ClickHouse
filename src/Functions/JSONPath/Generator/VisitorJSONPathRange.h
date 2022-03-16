#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/Generator/IVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class VisitorJSONPathRange : public IVisitor<JSONParser>
{
public:
    VisitorJSONPathRange(ASTPtr range_ptr_) : range_ptr(range_ptr_->as<ASTJSONPathRange>())
    {
        current_range = 0;
        current_index = range_ptr->ranges[current_range].first;
    }

    const char * getName() const override { return "VisitorJSONPathRange"; }

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
        }

        if (current_index + 1 == range_ptr->ranges[current_range].second
            && current_range + 1 == range_ptr->ranges.size())
        {
            this->setExhausted(true);
        }

        return status;
    }

    void reinitialize() override
    {
        current_range = 0;
        current_index = range_ptr->ranges[current_range].first;
        this->setExhausted(false);
    }

    void updateState() override
    {
        current_index++;
        if (current_index == range_ptr->ranges[current_range].second)
        {
            current_range++;
            current_index = range_ptr->ranges[current_range].first;
        }
    }

private:
    ASTJSONPathRange * range_ptr;
    size_t current_range;
    UInt32 current_index;
};

}
