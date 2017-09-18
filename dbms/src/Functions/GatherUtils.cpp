#include <Functions/GatherUtils.h>

namespace DB
{

/// Creates IArraySource from ColumnArray

template <typename ... Types>
struct ArraySourceCreator;

template <typename Type, typename ... Types>
struct ArraySourceCreator<Type, Types ...>
{
    static std::unique_ptr<IArraySource>
    create(const ColumnArray & col, const ColumnUInt8 * null_map, bool is_const, size_t total_rows)
    {
        if (typeid_cast<const ColumnVector<Type> *>(&col.getData()))
        {
            if (null_map)
            {
                if (is_const)
                    return std::make_unique<ConstSource<NullableArraySource<NumericArraySource<Type>>>>(col, *null_map, total_rows);
                return std::make_unique<NullableArraySource<NumericArraySource<Type>>>(col, *null_map);
            }
            if (is_const)
                return std::make_unique<ConstSource<NumericArraySource<Type>>>(col, total_rows);
            return std::make_unique<NumericArraySource<Type>>(col);
        }

        return ArraySourceCreator<Types...>::create(col, null_map, is_const, total_rows);
    }
};

template <>
struct ArraySourceCreator<>
{
    static std::unique_ptr<IArraySource>
    create(const ColumnArray & col, const ColumnUInt8 * null_map, bool is_const, size_t total_rows)
    {
        if (null_map)
        {
            if (is_const)
                return std::make_unique<ConstSource<NullableArraySource<GenericArraySource>>>(col, *null_map, total_rows);
            return std::make_unique<NullableArraySource<GenericArraySource>>(col, *null_map);
        }
        if (is_const)
            return std::make_unique<ConstSource<GenericArraySource>>(col, total_rows);
        return std::make_unique<GenericArraySource>(col);
    }
};

std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows)
{
    using Creator = typename ApplyTypeListForClass<ArraySourceCreator, TypeListNumbers>::Type;
    if (auto column_nullable = typeid_cast<const ColumnNullable *>(&col.getData()))
    {
        ColumnArray column(column_nullable->getNestedColumn(), col.getOffsetsColumn());
        return Creator::create(column, &column_nullable->getNullMapConcreteColumn(), is_const, total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}


/// Creates IArraySink from ColumnArray

template <typename ... Types>
struct ArraySinkCreator;

template <typename Type, typename ... Types>
struct ArraySinkCreator<Type, Types ...>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, ColumnUInt8 * null_map, size_t column_size)
    {
        if (typeid_cast<ColumnVector<Type> *>(&col.getData()))
        {
            if (null_map)
                return std::make_unique<NullableArraySink<NumericArraySink<Type>>>(col, *null_map, column_size);
            return std::make_unique<NumericArraySink<Type>>(col, column_size);
        }

        return ArraySinkCreator<Types ...>::create(col, null_map, column_size);
    }
};

template <>
struct ArraySinkCreator<>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, ColumnUInt8 * null_map, size_t column_size)
    {
        if (null_map)
            return std::make_unique<NullableArraySink<GenericArraySink>>(col, *null_map, column_size);
        return std::make_unique<GenericArraySink>(col, column_size);
    }
};

std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size)
{
    using Creator = ApplyTypeListForClass<ArraySinkCreator, TypeListNumbers>::Type;
    if (auto column_nullable = typeid_cast<ColumnNullable *>(&col.getData()))
    {
        ColumnArray column(column_nullable->getNestedColumn(), col.getOffsetsColumn());
        return Creator::create(column, &column_nullable->getNullMapConcreteColumn(), column_size);
    }
    return Creator::create(col, nullptr, column_size);
}


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
            throw Exception(std::string("Unknown ArraySource type: ") + typeid(source).name(), ErrorCodes::LOGICAL_ERROR);
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
            throw Exception(std::string("Unknown ArraySink type: ") + typeid(sink).name(), ErrorCodes::LOGICAL_ERROR);
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
                    std::string("GenericArraySource expected for GenericArraySink, got: ") + typeid(source).name(),
                    ErrorCodes::LOGICAL_ERROR);
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


/// Slice for array sources.

struct SliceFromLeftConstantOffsetUnboundedSelectArraySource
        : public ArraySinkSourceSelector<SliceFromLeftConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset)
    {
        sliceFromLeftConstantOffsetUnbounded(source, sink, offset);
    }
};

struct SliceFromLeftConstantOffsetBoundedSelectArraySource
        : public ArraySinkSourceSelector<SliceFromLeftConstantOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset, ssize_t & length)
    {
        sliceFromLeftConstantOffsetBounded(source, sink, offset, length);
    }
};

struct SliceFromRightConstantOffsetUnboundedSelectArraySource
        : public ArraySinkSourceSelector<SliceFromRightConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset)
    {
        sliceFromRightConstantOffsetUnbounded(source, sink, offset);
    }
};

struct SliceFromRightConstantOffsetBoundedSelectArraySource
        : public ArraySinkSourceSelector<SliceFromRightConstantOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset, ssize_t & length)
    {
        sliceFromRightConstantOffsetBounded(source, sink, offset, length);
    }
};

struct SliceDynamicOffsetUnboundedSelectArraySource
        : public ArraySinkSourceSelector<SliceDynamicOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, IColumn & offset_column)
    {
        sliceDynamicOffsetUnbounded(source, sink, offset_column);
    }
};

struct SliceDynamicOffsetBoundedSelectArraySource
        : public ArraySinkSourceSelector<SliceDynamicOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, IColumn & offset_column, IColumn & length_column)
    {
        sliceDynamicOffsetBounded(source, sink, offset_column, length_column);
    }
};

void sliceFromLeftConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset)
{
    SliceFromLeftConstantOffsetUnboundedSelectArraySource::select(src, sink, offset);
}

void sliceFromLeftConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length)
{
    SliceFromLeftConstantOffsetBoundedSelectArraySource::select(src, sink, offset, length);
}

void sliceFromRightConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset)
{
    SliceFromRightConstantOffsetUnboundedSelectArraySource::select(src, sink, offset);
}

void sliceFromRightConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length)
{
    SliceFromRightConstantOffsetBoundedSelectArraySource::select(src, sink, offset, length);
}

void sliceDynamicOffsetUnbounded(IArraySource & src, IArraySink & sink, IColumn & offset_column)
{
    SliceDynamicOffsetUnboundedSelectArraySource::select(src, sink, offset_column);
}

void sliceDynamicOffsetBounded(IArraySource & src, IArraySink & sink, IColumn & offset_column, IColumn & length_column)
{
    SliceDynamicOffsetBoundedSelectArraySource::select(src, sink, offset_column, length_column);
}

}
