#include <Processors/Port.h>

namespace DB
{

void connect(OutputPort & output, InputPort & input)
{
    if (input.state || output.state)
        throw Exception("Port is already connected", ErrorCodes::LOGICAL_ERROR);

    input.output_port = &output;
    output.input_port = &input;
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}

}
