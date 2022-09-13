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
    if (input.state || output.state)
        throw Exception("Port is already connected", ErrorCodes::LOGICAL_ERROR);

    auto out_name = output.getProcessor().getName();
    auto in_name = input.getProcessor().getName();

    assertCompatibleHeader(output.getHeader(), input.getHeader(), " function connect between " + out_name + " and " + in_name);

    input.output_port = &output;
    output.input_port = &input;
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}

}
