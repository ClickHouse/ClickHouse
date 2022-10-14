#include <Processors/Port.h>
#include <Processors/IProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void connect(OutputPort & output, InputPort & input)
{
    if (input.state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is already connected, (header: [{}])", input.header.dumpStructure());

    if (output.state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is already connected, (header: [{}])", output.header.dumpStructure());

    auto out_name = output.getProcessor().getName();
    auto in_name = input.getProcessor().getName();

    assertCompatibleHeader(output.getHeader(), input.getHeader(), fmt::format(" function connect between {} and {}", out_name, in_name));

    input.output_port = &output;
    output.input_port = &input;
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}

}
