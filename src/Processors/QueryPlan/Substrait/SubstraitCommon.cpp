#include <Processors/QueryPlan/Substrait/SubstraitCommon.h>

#if USE_SUBSTRAIT

#include <set>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

struct UsedAnchors
{
    std::set<UInt32> functions;
    std::set<UInt32> types;
};

/// Collects the anchors that `message` and the messages nested in it reference.
void collectUsedAnchors(const google::protobuf::Message & message, UsedAnchors & used)
{
    const auto * reflection = message.GetReflection();
    std::vector<const google::protobuf::FieldDescriptor *> fields;
    reflection->ListFields(message, &fields);
    for (const auto * field : fields)
    {
        if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            if (field->is_repeated())
                for (int i = 0; i < reflection->FieldSize(message, field); ++i)
                    collectUsedAnchors(reflection->GetRepeatedMessage(message, field, i), used);
            else
                collectUsedAnchors(reflection->GetMessage(message, field), used);
        }
        /// Function anchors live in `function_reference`, `comparison_function_reference` and `custom_function_reference`.
        else if (field->name().ends_with("function_reference"))
            used.functions.insert(reflection->GetUInt32(message, field));
        else if (field->name() == "type_reference")
            used.types.insert(reflection->GetUInt32(message, field));
    }
}

}

UInt32 SubstraitPlanExtensions::internFunction(std::string_view urn, std::string_view signature)
{
    return functions.try_emplace({String(urn), String(signature)}, static_cast<UInt32>(functions.size() + 1)).first->second;
}

UInt32 SubstraitPlanExtensions::internType(std::string_view urn, std::string_view type_name)
{
    return types.try_emplace({String(urn), String(type_name)}, static_cast<UInt32>(types.size() + 1)).first->second;
}

const String & SubstraitPlanExtensions::typeNameByAnchor(UInt32 anchor) const
{
    for (const auto & [key, type_anchor] : types)
        if (type_anchor == anchor)
            return key.second;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown user-defined type anchor {}", anchor);
}

void SubstraitPlanExtensions::declareUsedIn(substrait::Plan & plan) const
{
    UsedAnchors used;
    collectUsedAnchors(plan, used);

    /// A URN gets the next anchor the first time a declaration below refers to it.
    std::map<std::string_view, UInt32> urn_anchors;
    const auto urn_anchor
        = [&](std::string_view urn) { return urn_anchors.try_emplace(urn, static_cast<UInt32>(urn_anchors.size() + 1)).first->second; };

    for (const auto & [key, anchor] : types)
    {
        if (!used.types.contains(anchor))
            continue;
        auto * declaration = plan.add_extensions()->mutable_extension_type();
        declaration->set_extension_urn_reference(urn_anchor(key.first));
        declaration->set_type_anchor(anchor);
        declaration->set_name(key.second);
    }

    for (const auto & [key, anchor] : functions)
    {
        if (!used.functions.contains(anchor))
            continue;
        auto * declaration = plan.add_extensions()->mutable_extension_function();
        declaration->set_extension_urn_reference(urn_anchor(key.first));
        declaration->set_function_anchor(anchor);
        declaration->set_name(key.second);
    }

    for (const auto & [urn, anchor] : urn_anchors)
    {
        auto * declaration = plan.add_extension_urns();
        declaration->set_extension_urn_anchor(anchor);
        declaration->set_urn(urn);
    }
}

}

#endif
