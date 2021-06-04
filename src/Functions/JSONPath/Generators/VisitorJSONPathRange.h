#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/Generators/IVisitor.h>
#include <Functions/JSONPath/Generators/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class VisitorJSONPathRange : public IVisitor<JSONParser>
{
public:
    VisitorJSONPathRange(ASTPtr range_ptr_) : range_ptr(range_ptr_)
    {
        const auto * range = range_ptr->as<ASTJSONPathRange>();
        current_range = 0;
        if (range->is_star)
        {
            current_index = 0;
        }
        else
        {
            current_index = range->ranges[current_range].first;
        }
    }

    const char * getName() const override { return "VisitorJSONPathRange"; }

    VisitorStatus apply(typename JSONParser::Element & element) const override
    {
        typename JSONParser::Element result;
        typename JSONParser::Array array = element.getArray();
        if (current_index >= array.size())
        {
            return VisitorStatus::Error;
        }
        result = array[current_index];
        element = result;
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        if (!element.isArray())
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        const auto * range = range_ptr->as<ASTJSONPathRange>();
        VisitorStatus status;
        if (current_index < element.getArray().size())
        {
            apply(element);
            status = VisitorStatus::Ok;
        }
        else if (!range->is_star)
        {
            status = VisitorStatus::Ignore;
        }
        else
        {
            status = VisitorStatus::Ignore;
            this->setExhausted(true);
        }

        if (!range->is_star)
        {
            if (current_index + 1 == range->ranges[current_range].second)
            {
                if (current_range + 1 == range->ranges.size())
                {
                    this->setExhausted(true);
                }
            }
        }

        return status;
    }

    void reinitialize() override
    {
        const auto * range = range_ptr->as<ASTJSONPathRange>();
        current_range = 0;
        if (range->is_star)
        {
            current_index = 0;
        }
        else
        {
            current_index = range->ranges[current_range].first;
        }
        this->setExhausted(false);
    }

    void updateState() override
    {
        const auto * range = range_ptr->as<ASTJSONPathRange>();
        current_index++;
        if (range->is_star)
        {
            return;
        }
        if (current_index == range->ranges[current_range].second)
        {
            current_range++;
            current_index = range->ranges[current_range].first;
        }
    }

private:
    ASTPtr range_ptr;
    size_t current_range;
    UInt32 current_index;
};

} // namespace
