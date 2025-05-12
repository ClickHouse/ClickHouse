#include <Interpreters/InterpreterFinishCollectingWorkload.h>
#include <Interpreters/InterpreterFactory.h>

namespace DB
{

void registerInterpreterFinishCollectingWorkload(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterFinishCollectingWorkload>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterFinishCollectingWorkload", create_fn);
}

}
