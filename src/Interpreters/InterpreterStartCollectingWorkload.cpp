#include <Interpreters/InterpreterStartCollectingWorkload.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

void registerInterpreterStartCollectingWorkload(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterStartCollectingWorkload>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterStartCollectingWorkload", create_fn);
}

}
