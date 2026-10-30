#include <Processors/QueryPlan/Substrait/SubstraitCommon.h>

#if USE_SUBSTRAIT

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void SubstraitPlanExtensions::registerUrn(std::string_view urn) { urns.try_emplace(String(urn), static_cast<UInt32>(urns.size() + 1)); }

UInt32 SubstraitPlanExtensions::internFunction(std::string_view urn, std::string_view signature)
{
    registerUrn(urn);
    return functions.try_emplace({String(urn), String(signature)}, static_cast<UInt32>(functions.size() + 1)).first->second;
}

UInt32 SubstraitPlanExtensions::internType(std::string_view urn, std::string_view type_name)
{
    registerUrn(urn);
    return types.try_emplace({String(urn), String(type_name)}, static_cast<UInt32>(types.size() + 1)).first->second;
}

const String & SubstraitPlanExtensions::typeNameByAnchor(UInt32 anchor) const
{
    for (const auto & [key, type_anchor] : types)
        if (type_anchor == anchor)
            return key.second;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown user-defined type anchor {}", anchor);
}

void SubstraitPlanExtensions::declareIn(substrait::Plan & plan) const
{
    for (const auto & [urn, anchor] : urns)
    {
        auto * declaration = plan.add_extension_urns();
        declaration->set_extension_urn_anchor(anchor);
        declaration->set_urn(urn);
    }

    for (const auto & [key, anchor] : types)
    {
        auto * declaration = plan.add_extensions()->mutable_extension_type();
        declaration->set_extension_urn_reference(urns.at(key.first));
        declaration->set_type_anchor(anchor);
        declaration->set_name(key.second);
    }

    for (const auto & [key, anchor] : functions)
    {
        auto * declaration = plan.add_extensions()->mutable_extension_function();
        declaration->set_extension_urn_reference(urns.at(key.first));
        declaration->set_function_anchor(anchor);
        declaration->set_name(key.second);
    }
}

}

#endif
