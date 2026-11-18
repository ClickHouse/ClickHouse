#include <WindowFunctions/IAggregateWindowFunction.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionPrefix.h>
#include <WindowFunctions/Aggregate/AggregateWindowFunctionRecalculate.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

AggregateWindowFunctionPtr createAggregateWindowFunction(AggregateFunctionPtr function, const WindowDescription & window)
{
    if (window.frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        return std::make_shared<AggregateWindowFunctionPrefix>(std::move(function));

    return std::make_shared<AggregateWindowFunctionRecalculate>(std::move(function));
}

}
