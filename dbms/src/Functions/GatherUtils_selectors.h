#include "GatherUtils.h"

namespace DB
{
/// Base classes which selects template function implementation with concrete ArraySource or ArraySink
/// Derived classes should implement selectImpl for ArraySourceSelector and ArraySinkSelector
///  or selectSourceSink for ArraySinkSourceSelector
template <typename Base, typename ... Types>
struct ArraySourceSelector;

template <typename Base, typename Type, typename ... Types>
struct ArraySourceSelector<Base, Type, Types ...>
{
    template <typename ... Args>
    static void select(IArraySource & source, Args && ... args)
    {
        if (auto array = typeid_cast<NumericArraySource<Type> *>(&source))
            Base::selectImpl(*array, args ...);
        else if (auto nullable_array = typeid_cast<NullableArraySource<NumericArraySource<Type>> *>(&source))
            Base::selectImpl(*nullable_array, args ...);
        else if (auto const_array = typeid_cast<ConstSource<NumericArraySource<Type>> *>(&source))
            Base::selectImpl(*const_array, args ...);
        else if (auto const_nullable_array = typeid_cast<ConstSource<NullableArraySource<NumericArraySource<Type>>> *>(&source))
            Base::selectImpl(*const_nullable_array, args ...);
        else
            ArraySourceSelector<Base, Types ...>::select(source, args ...);
    }
};

template <typename Base>
struct ArraySourceSelector<Base>
{
    template <typename ... Args>
    static void select(IArraySource & source, Args && ... args)
    {
        if (auto array = typeid_cast<GenericArraySource *>(&source))
            Base::selectImpl(*array, args ...);
        else if (auto nullable_array = typeid_cast<NullableArraySource<GenericArraySource> *>(&source))
            Base::selectImpl(*nullable_array, args ...);
        else if (auto const_array = typeid_cast<ConstSource<GenericArraySource> *>(&source))
            Base::selectImpl(*const_array, args ...);
        else if (auto const_nullable_array = typeid_cast<ConstSource<NullableArraySource<GenericArraySource>> *>(&source))
            Base::selectImpl(*const_nullable_array, args ...);
        else
            throw Exception(std::string("Unknown ArraySource type: ") + demangle(typeid(source).name()), ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename Base>
using GetArraySourceSelector = typename ApplyTypeListForClass<ArraySourceSelector,
        typename PrependToTypeList<Base, TypeListNumbers>::Type>::Type;

template <typename Base, typename ... Types>
struct ArraySinkSelector;

template <typename Base, typename Type, typename ... Types>
struct ArraySinkSelector<Base, Type, Types ...>
{
    template <typename ... Args>
    static void select(IArraySink & sink, Args && ... args)
    {
        if (auto nullable_numeric_sink = typeid_cast<NullableArraySink<NumericArraySink<Type>> *>(&sink))
            Base::selectImpl(*nullable_numeric_sink, args ...);
        else if (auto numeric_sink = typeid_cast<NumericArraySink<Type> *>(&sink))
            Base::selectImpl(*numeric_sink, args ...);
        else
            ArraySinkSelector<Base, Types ...>::select(sink, args ...);
    }
};

template <typename Base>
struct ArraySinkSelector<Base>
{
    template <typename ... Args>
    static void select(IArraySink & sink, Args && ... args)
    {
        if (auto nullable_generic_sink = typeid_cast<NullableArraySink<GenericArraySink> *>(&sink))
            Base::selectImpl(*nullable_generic_sink, args ...);
        else if (auto generic_sink = typeid_cast<GenericArraySink *>(&sink))
            Base::selectImpl(*generic_sink, args ...);
        else
            throw Exception(std::string("Unknown ArraySink type: ") + demangle(typeid(sink).name()), ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename Base>
using GetArraySinkSelector = typename ApplyTypeListForClass<ArraySinkSelector,
        typename PrependToTypeList<Base, TypeListNumbers>::Type>::Type;

template <typename Base>
struct ArraySinkSourceSelector
{
    template <typename ... Args>
    static void select(IArraySource & source, IArraySink & sink, Args && ... args)
    {
        GetArraySinkSelector<Base>::select(sink, source, args ...);
    }

    template <typename Sink, typename ... Args>
    static void selectImpl(Sink && sink, IArraySource & source, Args && ... args)
    {
        GetArraySourceSelector<Base>::select(source, sink, args ...);
    }

    template <typename Source, typename Sink, typename ... Args>
    static void selectImpl(Source && source, Sink && sink, Args && ... args)
    {
        Base::selectSourceSink(source, sink, args ...);
    }
};

}
