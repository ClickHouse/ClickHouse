#include <iostream>
#include <Processors/printPipeline.h>


namespace DB
{

void printPipeline(const std::list<ProcessorPtr> & processors)
{
    std::cout << "digraph\n{\n";

    /// Nodes // TODO quoting and escaping
    for (const auto & processor : processors)
        std::cout << "n" << processor.get() << "[label=" << processor->getName() << "];\n";

    /// Edges
    for (const auto & processor : processors)
    {
        for (const auto & port : processor->getOutputs())
        {
            const IProcessor & curr = *processor;
            const IProcessor & next = port.getInputPort().getProcessor();

            std::cout << "n" << &curr << " -> " << "n" << &next << ";\n";
        }
    }
    std::cout << "}\n";
}

}
