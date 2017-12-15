#include "GatherUtils.h"
#include "GatherUtils_selectors.h"

namespace DB
{
/// Algorithms.

/// Appends slices from source to sink. Offsets for sink should be precalculated as start positions of result arrays.
/// Only for NumericArraySource, because can't insert values in the middle of arbitary column.
/// Used for array concat implementation.
template <typename Source, typename Sink>
static void append(Source && source, Sink && sink)
{
    sink.row_num = 0;
    while (!source.isEnd())
    {
        sink.current_offset = sink.offsets[sink.row_num];
        writeSlice(source.getWhole(), sink);
        sink.next();
        source.next();
    }
}

struct ArrayAppend : public GetArraySourceSelector<ArrayAppend>
{
    template <typename Source, typename Sink>
    static void selectImpl(Source && source, Sink && sink)
    {
        append(source, sink);
    }
};

template <typename Sink>
static void append(IArraySource & source, Sink && sink)
{
    ArrayAppend::select(source, sink);
}

/// Concat specialization for GenericArraySource. Because can't use append with arbitrary column type.

template <typename SourceType, typename SinkType>
struct ConcatGenericArrayWriteWholeImpl
{
    static void writeWhole(GenericArraySource * generic_source, SinkType && sink)
    {
        auto source = static_cast<SourceType *>(generic_source);
        writeSlice(source->getWhole(), sink);
        source->next();
    }
};

template <typename Sink>
static void NO_INLINE concatGenericArray(const std::vector<std::unique_ptr<IArraySource>> & sources, Sink && sink)
{
    std::vector<GenericArraySource *> generic_sources;
    std::vector<bool> is_nullable;
    std::vector<bool> is_const;

    generic_sources.reserve(sources.size());
    is_nullable.assign(sources.size(), false);
    is_const.assign(sources.size(), false);

    for (auto i : ext::range(0, sources.size()))
    {
        const auto & source = sources[i];
        if (auto generic_source = typeid_cast<GenericArraySource *>(source.get()))
            generic_sources.push_back(static_cast<GenericArraySource *>(generic_source));
        else if (auto const_generic_source = typeid_cast<ConstSource<GenericArraySource> *>(source.get()))
        {
            generic_sources.push_back(static_cast<GenericArraySource *>(const_generic_source));
            is_const[i] = true;
        }
        else if (auto nullable_source = typeid_cast<NullableArraySource<GenericArraySource> *>(source.get()))
        {
            generic_sources.push_back(static_cast<GenericArraySource *>(nullable_source));
            is_nullable[i] = true;
        }
        else if (auto const_nullable_source = typeid_cast<ConstSource<NullableArraySource<GenericArraySource>> *>(source.get()))
        {
            generic_sources.push_back(static_cast<GenericArraySource *>(const_nullable_source));
            is_nullable[i] = is_const[i] = true;
        }
        else
            throw Exception(
                std::string("GenericArraySource expected for GenericArraySink, got: ") + demangle(typeid(source).name()), ErrorCodes::LOGICAL_ERROR);
    }

    while (!sink.isEnd())
    {
        for (auto i : ext::range(0, sources.size()))
        {
            auto source = generic_sources[i];
            if (is_const[i])
            {
                if (is_nullable[i])
                    ConcatGenericArrayWriteWholeImpl<ConstSource<NullableArraySource<GenericArraySource>>, Sink>::writeWhole(source, sink);
                else
                    ConcatGenericArrayWriteWholeImpl<ConstSource<GenericArraySource>, Sink>::writeWhole(source, sink);
            }
            else
            {
                if (is_nullable[i])
                    ConcatGenericArrayWriteWholeImpl<NullableArraySource<GenericArraySource>, Sink>::writeWhole(source, sink);
                else
                    ConcatGenericArrayWriteWholeImpl<GenericArraySource, Sink>::writeWhole(source, sink);
            }
        }
        sink.next();
    }
}

/// Concat for array sources. Sources must be either all numeric either all generic.
template <typename Sink>
void NO_INLINE concat(const std::vector<std::unique_ptr<IArraySource>> & sources, Sink && sink)
{
    size_t elements_to_reserve = 0;
    bool is_first = true;

    /// Prepare offsets column. Offsets should point to starts of result arrays.
    for (const auto & source : sources)
    {
        elements_to_reserve += source->getSizeForReserve();
        const auto & offsets = source->getOffsets();

        if (is_first)
        {
            sink.offsets.resize(source->getColumnSize());
            memset(&sink.offsets[0], 0, sink.offsets.size() * sizeof(offsets[0]));
            is_first = false;
        }

        if (source->isConst())
        {
            for (size_t i : ext::range(1, offsets.size()))
            {
                sink.offsets[i] += offsets[0];
            }
        }
        else
        {
            for (size_t i : ext::range(1, offsets.size()))
            {
                sink.offsets[i] += offsets[i - 1] - (i > 1 ? offsets[i - 2] : 0);
            }
        }
    }

    for (auto i : ext::range(1, sink.offsets.size()))
    {
        sink.offsets[i] += sink.offsets[i - 1];
    }

    sink.reserve(elements_to_reserve);

    for (const auto & source : sources)
    {
        append(*source, sink);
    }
}

struct ArrayConcat : public GetArraySinkSelector<ArrayConcat>
{
    using Sources = std::vector<std::unique_ptr<IArraySource>>;

    template <typename Sink>
    static void selectImpl(Sink && sink, Sources & sources)
    {
        concat<Sink>(sources, sink);
    }

    static void selectImpl(GenericArraySink & sink, Sources & sources)
    {
        concatGenericArray(sources, sink);
    }

    static void selectImpl(NullableArraySink<GenericArraySink> & sink, Sources & sources)
    {
        concatGenericArray(sources, sink);
    }

    static void selectImpl(GenericArraySink && sink, Sources && sources)
    {
        concatGenericArray(sources, sink);
    }

    static void selectImpl(NullableArraySink<GenericArraySink> && sink, Sources & sources)
    {
        concatGenericArray(sources, sink);
    }
};

void concat(std::vector<std::unique_ptr<IArraySource>> & sources, IArraySink & sink)
{
    return ArrayConcat::select(sink, sources);
}
}
