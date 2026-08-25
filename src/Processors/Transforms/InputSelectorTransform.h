#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/// Selects one of two data inputs based on a signal input.
///
/// Has 3 input ports and 1 output port:
///   - Input 0 (signal): empty header. If it produces a chunk before finishing, input 1 ("true")
///     is selected; otherwise input 2 ("false") is selected.
///   - Input 1 (true branch): data port, used when signal fires.
///   - Input 2 (false branch): data port, used when signal does not fire.
///
/// The selected input is forwarded to the output; the other data input is closed.
class InputSelectorTransform : public IProcessor
{
public:
    explicit InputSelectorTransform(const Block & header);

    String getName() const override { return "InputSelectorTransform"; }
    Status prepare() override;

private:
    enum class State
    {
        WaitingForSignal,
        Forwarding,
    };

    State state = State::WaitingForSignal;

    InputPort & signal_port;
    InputPort & true_port;
    InputPort & false_port;
    InputPort * selected_port = nullptr;
};

}
