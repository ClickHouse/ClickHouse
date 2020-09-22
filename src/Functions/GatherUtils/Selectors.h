#pragma once

#include "Algorithms.h"
#include "ArraySourceVisitor.h"
#include "ArraySinkVisitor.h"
#include "ValueSourceVisitor.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace GatherUtils
{
#pragma GCC visibility push(hidden)

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

template <typename Base, typename Tuple, int index, typename ... Args>
void callSelectSource(bool is_const, bool is_nullable, Tuple & tuple, Args && ... args)
{
    if constexpr (index == std::tuple_size<Tuple>::value)
        Base::selectSource(is_const, is_nullable, args ...);
    else
        callSelectSource<Base, Tuple, index + 1>(is_const, is_nullable, tuple, args ..., std::get<index>(tuple));
}

template <typename Base, typename ... Args>
struct ArraySourceSelectorVisitor final : public ArraySourceVisitorImpl<ArraySourceSelectorVisitor<Base, Args ...>>
{
    explicit ArraySourceSelectorVisitor(IArraySource & source, Args && ... args) : packed_args(args ...), array_source(source) {}

    using Tuple = std::tuple<Args && ...>;

    template <typename Source>
    void visitImpl(Source & source)
    {
        callSelectSource<Base, Tuple, 0>(array_source.isConst(), array_source.isNullable(), packed_args, source);
    }

    Tuple packed_args;
    IArraySource & array_source;
};

template <typename Base>
struct ArraySourceSelector
{
    template <typename ... Args>
    static void select(IArraySource & source, Args && ... args)
    {
        ArraySourceSelectorVisitor<Base, Args ...> visitor(source, args ...);
        source.accept(visitor);
    }
};


template <typename Base, typename ... Args>
struct ArraySinkSelectorVisitor final : public ArraySinkVisitorImpl<ArraySinkSelectorVisitor<Base, Args ...>>
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
struct ArraySourcePairSelector
{
    template <typename ... Args>
    static void select(IArraySource & first, IArraySource & second, Args && ... args)
    {
        ArraySourceSelector<Base>::select(first, second, args ...);
    }

    template <typename FirstSource, typename ... Args>
    static void selectSource(bool is_const, bool is_nullable, FirstSource && first, IArraySource & second, Args && ... args)
    {
        ArraySourceSelector<Base>::select(second, is_const, is_nullable, first, args ...);
    }

    template <typename SecondSource, typename FirstSource, typename ... Args>
    static void selectSource(bool is_second_const, bool is_second_nullable, SecondSource && second,
                             bool is_first_const, bool is_first_nullable, FirstSource && first, Args && ... args)
    {
        Base::selectSourcePair(is_first_const, is_first_nullable, first,
                               is_second_const, is_second_nullable, second, args ...);
    }
};

template <typename Base>
struct ArrayAndValueSourceSelectorBySink : public ArraySinkSelector<ArrayAndValueSourceSelectorBySink<Base>>
{
    template <typename Sink, typename ... Args>
    static void selectImpl(Sink && sink, IArraySource & array_source, IValueSource & value_source, Args && ... args)
    {
        using SynkType = typename std::decay<Sink>::type;
        using ArraySource = typename SynkType::CompatibleArraySource;
        using ValueSource = typename SynkType::CompatibleValueSource;

        auto checkType = [] (auto source_ptr)
        {
            if (source_ptr == nullptr)
                throw Exception(demangle(typeid(Base).name()) + " expected "
                            + demangle(typeid(typename SynkType::CompatibleArraySource).name())
                            + " or " + demangle(typeid(ConstSource<typename SynkType::CompatibleArraySource>).name())
                            + " or " + demangle(typeid(typename SynkType::CompatibleValueSource).name()) +
                            + " or " + demangle(typeid(ConstSource<typename SynkType::CompatibleValueSource>).name())
                            + " but got " + demangle(typeid(*source_ptr).name()), ErrorCodes::LOGICAL_ERROR);
        };
        auto checkTypeAndCallConcat = [& sink, & checkType, & args ...] (auto array_source_ptr, auto value_source_ptr)
        {
            checkType(array_source_ptr);
            checkType(value_source_ptr);

            Base::selectArrayAndValueSourceBySink(*array_source_ptr, *value_source_ptr, sink, args ...);
        };

        if (array_source.isConst() && value_source.isConst())
            checkTypeAndCallConcat(typeid_cast<ConstSource<ArraySource> *>(&array_source),
                                   typeid_cast<ConstSource<ValueSource> *>(&value_source));
        else if (array_source.isConst())
            checkTypeAndCallConcat(typeid_cast<ConstSource<ArraySource> *>(&array_source),
                                   typeid_cast<ValueSource *>(&value_source));
        else if (value_source.isConst())
            checkTypeAndCallConcat(typeid_cast<ArraySource *>(&array_source),
                                   typeid_cast<ConstSource<ValueSource> *>(&value_source));
        else
            checkTypeAndCallConcat(typeid_cast<ArraySource *>(&array_source),
                                   typeid_cast<ValueSource *>(&value_source));
    }
};

#pragma GCC visibility pop
}

}
