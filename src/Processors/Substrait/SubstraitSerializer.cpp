#include <Processors/Substrait/SubstraitSerializer.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/Exception.h>

// Include generated Substrait protobuf headers
#include <substrait/plan.pb.h>
#include <substrait/algebra.pb.h>
#include <substrait/type.pb.h>
#include <substrait/extensions/extensions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

class SubstraitSerializer::Impl
{
public:
    explicit Impl() = default;

    void convertPlan(const QueryPlan & query_plan, substrait::Plan & substrait_plan)
    {
        if (!query_plan.isInitialized())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize uninitialized query plan to Substrait");

        // Set Substrait version
        auto * version = substrait_plan.mutable_version();
        version->set_major_number(0);
        version->set_minor_number(54);  // Latest Substrait version as of implementation
        version->set_patch_number(0);

        // TODO: Walk the query plan tree and convert each step
        // For now, just set up the basic structure
        
        // We'll add actual conversion logic in subsequent steps
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, 
            "Substrait serialization is not yet fully implemented. Currently supports: none");
    }

    std::string serializeToBinary(const substrait::Plan & plan)
    {
        std::string output;
        if (!plan.SerializeToString(&output))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to serialize Substrait plan to binary");
        return output;
    }

    std::string serializeToJSON(const substrait::Plan & plan)
    {
        std::string output;
        google::protobuf::json::PrintOptions json_options;
        json_options.add_whitespace = true;
        
        auto status = google::protobuf::json::MessageToJsonString(plan, &output, json_options);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to serialize Substrait plan to JSON: {}", 
                std::string(status.message()));
        
        return output;
    }
    
    std::string convertPlanToBinary(const QueryPlan & query_plan)
    {
        substrait::Plan substrait_plan;
        convertPlan(query_plan, substrait_plan);
        return serializeToBinary(substrait_plan);
    }
    
    std::string convertPlanToJSON(const QueryPlan & query_plan)
    {
        substrait::Plan substrait_plan;
        convertPlan(query_plan, substrait_plan);
        return serializeToJSON(substrait_plan);
    }
};

std::string SubstraitSerializer::serializePlanToBinary(const QueryPlan & query_plan)
{
    Impl impl;
    return impl.convertPlanToBinary(query_plan);
}

std::string SubstraitSerializer::serializePlanToJSON(const QueryPlan & query_plan)
{
    Impl impl;
    return impl.convertPlanToJSON(query_plan);
}

}
