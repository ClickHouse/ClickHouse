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
    explicit VisitorJSONPathRange(ASTPtr range_ptr_) : range_ptr(range_ptr_->as<ASTJSONPathRange>())
    {
        current_range = 0;
        current_index = range_ptr->ranges[current_range].first;
    }

    const char * getName() const override { return "VisitorJSONPathRange"; }

    VisitorStatus apply(typename JSONParser::Element & element) const override
    {
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

        const auto & range = range_ptr->ranges[current_range];
        VisitorStatus status = {};
        if (current_index < element.getArray().size())
        {
            apply(element);
            status = VisitorStatus::Ok;
        }
        else
        {
            /// The rest of the current range is past the end of the array too
            current_index = range.second;
            status = VisitorStatus::Ignore;
        }

        if (current_index == range.second && current_range + 1 == range_ptr->ranges.size())
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
        if (current_index == range_ptr->ranges[current_range].second)
        {
            current_range++;
            current_index = range_ptr->ranges[current_range].first;
        }
        else
        {
            current_index++;
        }
    }

private:
    ASTJSONPathRange * range_ptr;
    size_t current_range;
    UInt64 current_index;
};

}
