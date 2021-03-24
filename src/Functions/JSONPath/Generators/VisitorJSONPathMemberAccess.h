#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generators/IVisitor.h>
#include <Functions/JSONPath/Generators/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class VisitorJSONPathMemberAccess : public IVisitor<JSONParser>
{
public:
    VisitorJSONPathMemberAccess(ASTPtr member_access_ptr_) : member_access_ptr(member_access_ptr_) { }

    VisitorStatus apply(typename JSONParser::Element & element) const override {
        const auto * member_access = member_access_ptr->as<ASTJSONPathMemberAccess>();
        typename JSONParser::Element result;
        bool result_ok = element.getObject().find(std::string_view(member_access->member_name), result);
        if (result_ok)
        {
            element = result;
            return VisitorStatus::Ok;
        }
        return VisitorStatus::Error;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        this->setExhausted(true);
        return apply(element);
    }

    void reinitialize() override {
        this->setExhausted(false);
    }

private:
    ASTPtr member_access_ptr;
};

} // namespace DB
