#pragma once

#include <WindowFunctions/IGeneralWindowFunction.h>

#include <new>

namespace DB
{

template <typename State>
class WindowFunctionState : public IGeneralWindowFunction
{
public:
    using IGeneralWindowFunction::IGeneralWindowFunction;

    size_t sizeOfState() const override;
    size_t alignOfState() const override;
    void createState(char * place) const override;
    void destroyState(char * place) const noexcept override;
    void resetPartition(char * place) const override;

protected:
    static State & stateAt(char * place);
};

template <typename State>
size_t WindowFunctionState<State>::sizeOfState() const
{
    return sizeof(State);
}

template <typename State>
size_t WindowFunctionState<State>::alignOfState() const
{
    return alignof(State);
}

template <typename State>
void WindowFunctionState<State>::createState(char * place) const
{
    new (place) State();
}

template <typename State>
void WindowFunctionState<State>::destroyState(char * place) const noexcept
{
    stateAt(place).~State();
}

template <typename State>
void WindowFunctionState<State>::resetPartition(char * place) const
{
    stateAt(place) = State();
}

template <typename State>
State & WindowFunctionState<State>::stateAt(char * place)
{
    return *reinterpret_cast<State *>(place);
}

}
