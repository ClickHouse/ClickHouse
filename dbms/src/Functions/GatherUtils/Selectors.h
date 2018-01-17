#pragma once

#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/ArraySourceVisitor.h>
#include <Functions/GatherUtils/ArraySinkVisitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
/// Base classes which selects template function implementation with concrete ArraySource or ArraySink
/// Derived classes should implement selectImpl for ArraySourceSelector and ArraySinkSelector,
///  selectSourceSink for ArraySinkSourceSelector and selectSourcePair for ArraySourcePairSelector

template <typename Base, typename Tuple, int index, typename ... Args>
void callSelectMemberFunctionWithTupleArgument(Tuple & tuple, Args && ... args)
{
    if constexpr (index == std::tuple_size<Tuple>::value)
        Base::selectImpl(args ...);
    else
        callSelectMemberFunctionWithTupleArgument<Base, Tuple, index + 1>(tuple, args ..., std::get<index>(tuple));
}

template <typename Base, typename ... Args>
struct ArraySourceSelectorVisitor : public ArraySourceVisitorImpl<ArraySourceSelectorVisitor<Base, Args ...>>
{
    explicit ArraySourceSelectorVisitor(Args && ... args) : packed_args(args ...) {}

    using Tuple = std::tuple<Args && ...>;

    template <typename Source>
    void visitImpl(Source & source)
    {
        callSelectMemberFunctionWithTupleArgument<Base, Tuple, 0>(packed_args, source);
    }

    Tuple packed_args;
};

template <typename Base>
struct ArraySourceSelector
{
    template <typename ... Args>
    static void select(IArraySource & source, Args && ... args)
    {
        ArraySourceSelectorVisitor<Base, Args ...> visitor(args ...);
        source.accept(visitor);
    }
};


template <typename Base, typename ... Args>
struct ArraySinkSelectorVisitor : public ArraySinkVisitorImpl<ArraySinkSelectorVisitor<Base, Args ...>>
{
    explicit ArraySinkSelectorVisitor(Args && ... args) : packed_args(args ...) {}

    using Tuple = std::tuple<Args && ...>;

    template <typename Sink>
    void visitImpl(Sink & sink)
    {
        callSelectMemberFunctionWithTupleArgument<Base, Tuple, 0>(packed_args, sink);
    }

    Tuple packed_args;
};

template <typename Base>
struct ArraySinkSelector
{
    template <typename ... Args>
    static void select(IArraySink & sink, Args && ... args)
    {
        ArraySinkSelectorVisitor<Base, Args ...> visitor(args ...);
        sink.accept(visitor);
    }
};

template <typename Base>
struct ArraySinkSourceSelector
{
    template <typename ... Args>
    static void select(IArraySource & source, IArraySink & sink, Args && ... args)
    {
        ArraySinkSelector<Base>::select(sink, source, args ...);
    }

    template <typename Sink, typename ... Args>
    static void selectImpl(Sink && sink, IArraySource & source, Args && ... args)
    {
        ArraySourceSelector<Base>::select(source, sink, args ...);
    }

    template <typename Source, typename Sink, typename ... Args>
    static void selectImpl(Source && source, Sink && sink, Args && ... args)
    {
        Base::selectSourceSink(source, sink, args ...);
    }
};

template <typename Base>
struct ArraySourcePairSelector
{
    template <typename ... Args>
    static void select(IArraySource & first, IArraySource & second, Args && ... args)
    {
        ArraySourceSelector<Base>::select(first, second, args ...);
    }

    template <typename FirstSource, typename ... Args>
    static void selectImpl(FirstSource && first, IArraySource & second, Args && ... args)
    {
        ArraySourceSelector<Base>::select(second, first, args ...);
    }

    template <typename SecondSource, typename FirstSource, typename ... Args>
    static void selectImpl(SecondSource && second, FirstSource && first, Args && ... args)
    {
        Base::selectSourcePair(first, second, args ...);
    }
};

}
