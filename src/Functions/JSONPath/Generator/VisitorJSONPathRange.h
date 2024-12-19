#pragma once

#include <optional>
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
        element = (*array)[current_index];
        return VisitorStatus::Ok;
    }
    using TElement = JSONParser::Element;
    VisitorStatus visitBatch(TElement & element, std::function<void(const TElement &)> & res_func, bool can_reduce) override
    {
        if (!array && !element.isArray())
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        VisitorStatus status = VisitorStatus::Ok;
        if (!array)
        {
            current_element = element;
            array = current_element.getArray();
            if (!can_reduce)
                array_size = array.value().size();
        }

        if (can_reduce)
        {
            std::set<size_t> index_set{};
            for (auto range: range_ptr->ranges)
                for (size_t i = range.first ; i < range.second; ++i)
                    index_set.insert(i);

            size_t idx = 0;
            for (auto item: array.value())
                if (index_set.find(idx++) != index_set.end())
                    res_func(item);

            this->setExhausted(true);
            status = VisitorStatus::Ok;
        }
        else
        {
            if (current_index < array_size.value())
            {
                apply(element);
                status = VisitorStatus::Ok;
            }
            else
                status = VisitorStatus::Ignore;

            if (current_index + 1 == range_ptr->ranges[current_range].second
                && current_range + 1 == range_ptr->ranges.size())
            {
                this->setExhausted(true);
            }
        }

        return status;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        if (!array && !element.isArray())
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        VisitorStatus status;
        if (!array)
        {
            current_element = element;
            array = current_element.getArray();
            array_size = array.value().size();
        }
        if (current_index < array_size.value())
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
        array_size.reset();
        array.reset();
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
    std::optional<size_t> array_size{};
    std::optional<typename JSONParser::Array> array{};
    typename JSONParser::Element current_element{};
};

}
