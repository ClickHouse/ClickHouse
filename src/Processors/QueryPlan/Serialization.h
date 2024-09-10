#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct SerializedSetsRegistry;
struct DeserializedSetsRegistry;

struct IQueryPlanStep::Serialization
{
    WriteBuffer & out;
    SerializedSetsRegistry & registry;
};

struct SerializedSetsRegistry;

struct IQueryPlanStep::Deserialization
{
    ReadBuffer & in;
    DeserializedSetsRegistry & registry;

    const DataStreams & input_streams;
    const DataStream * output_stream;
    const QueryPlanSerializationSettings & settings;
};

}
