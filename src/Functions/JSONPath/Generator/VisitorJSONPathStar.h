#pragma once

#include <optional>
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
        element = array.value()[current_index];
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        if (!array && !element.isArray())
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        if (!array_size)
        {
            array = element.getArray();
            array_size = array.value().size();
        }
        VisitorStatus status;
        if (current_index < array_size.value())
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
    using TElement = JSONParser::Element;
    VisitorStatus visitBatch(TElement & element, std::function<void(const TElement &)> & res_func, bool can_reduce) override
    {
        if (!array && !element.isArray())
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        if (!array)
        {
            array = element.getArray();
            array_size = array.value().size();
        }
        VisitorStatus status = VisitorStatus::Ok;

        if (can_reduce)
        {
            for (auto item: array.value())
                res_func(item);

            this->setExhausted(true);
        }
        else
        {
            if (current_index < array_size.value())
            {
                apply(element);
                status = VisitorStatus::Ok;
            }
            else
            {
                status = VisitorStatus::Ignore;
                this->setExhausted(true);
            }

        }

        return status;
    }

    void reinitialize() override
    {
        current_index = 0;
        this->setExhausted(false);
        array_size.reset();
        array.reset();
    }

    void updateState() override
    {
        current_index++;
    }

private:
    UInt32 current_index{};
    std::optional<typename JSONParser::Array> array{};
    std::optional<size_t> array_size{};
};

}
