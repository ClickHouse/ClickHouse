#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathRoot.h>
#include <Functions/JSONPath/Generator/IVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
template <typename JSONParser>
class VisitorJSONPathRoot : public IVisitor<JSONParser>
{
public:
    VisitorJSONPathRoot(ASTPtr) { }

    const char * getName() const override { return "VisitorJSONPathRoot"; }

    VisitorStatus apply(typename JSONParser::Element & /*element*/) const override
    {
        /// No-op on document, since we are already passed document's root
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(typename JSONParser::Element & element) override
    {
        apply(element);
        this->setExhausted(true);
        return VisitorStatus::Ok;
    }

    void reinitialize() override { this->setExhausted(false); }

    void updateState() override { }
};

}
