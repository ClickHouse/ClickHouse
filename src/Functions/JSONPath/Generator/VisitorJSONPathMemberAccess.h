#pragma once

#include <optional>
#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generator/IVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class VisitorJSONPathMemberAccess : public IVisitor<JSONParser>
{
public:
    using TElement = JSONParser::Element;
    explicit VisitorJSONPathMemberAccess(ASTPtr member_access_ptr_)
        : member_access_ptr(member_access_ptr_->as<ASTJSONPathMemberAccess>()) { }

    const char * getName() const override { return "VisitorJSONPathMemberAccess"; }

    VisitorStatus apply(typename JSONParser::Element & element) const override
    {
        typename JSONParser::Element result;
        auto obj = element.getObject();
        if (!obj.find(std::string_view(member_access_ptr->member_name), result))
        {
            return VisitorStatus::Error;
        }
        element = std::move(result);
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        if (this->isExhausted())
        {
            return VisitorStatus::Exhausted;
        }
        this->setExhausted(true);
        if (!element.isObject())
        {
            return VisitorStatus::Error;
        }
        return apply(element);
    }

    VisitorStatus visitBatch(TElement & element, std::function<void(const TElement &)> & res_func, bool can_reduce) override
    {
        if (this->isExhausted())
            return VisitorStatus::Exhausted;
        this->setExhausted(true);
        if (!element.isObject())
            return VisitorStatus::Error;

        auto status = apply(element);
        if (status == VisitorStatus::Ok && can_reduce)
            res_func(element);

        return status;
    }

    void reinitialize() override { this->setExhausted(false); }

    void updateState() override { }

private:
    ASTJSONPathMemberAccess * member_access_ptr;
};

}
