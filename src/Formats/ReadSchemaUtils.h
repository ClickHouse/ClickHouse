#pragma once

#include <Storages/ColumnsDescription.h>
#include <Formats/FormatFactory.h>

namespace DB
{

class ReadBufferIterator
{
public:
    virtual std::unique_ptr<ReadBuffer> next() = 0;
    virtual bool isSingle() const = 0;
    virtual ~ReadBufferIterator() = default;
};

template <class ListIterator, class BufferCreator>
class ReadBufferListIterator : public ReadBufferIterator
{
public:
    ReadBufferListIterator(ListIterator begin_, ListIterator end_, BufferCreator buffer_creator_)
        : it(begin_), end(end_), buffer_creator(buffer_creator_), is_single(std::distance(begin_, end_) == 1)
    {
    }

    std::unique_ptr<ReadBuffer> next() override
    {
        if (it == end)
            return nullptr;

        auto res = buffer_creator(it);
        ++it;
        return res;
    }

    bool isSingle() const override { return is_single; }

private:
    ListIterator it;
    ListIterator end;
    BufferCreator buffer_creator;
    bool is_single;
};

template <class BufferCreator>
class ReadBufferSingleIterator : public ReadBufferIterator
{
public:
    ReadBufferSingleIterator(BufferCreator buffer_creator_) : buffer_creator(buffer_creator_)
    {
    }

    std::unique_ptr<ReadBuffer> next() override
    {
        if (done)
            return nullptr;
        done = true;
        return buffer_creator();
    }

    bool isSingle() const override { return true; }

private:
    BufferCreator buffer_creator;
    bool done = false;
};

/// Try to determine the schema of the data in specifying format.
/// For formats that have an external schema reader, it will
/// use it and won't create a read buffer.
/// For formats that have a schema reader from the data,
/// read buffer will be created by the provided iterator and
/// the schema will be extracted from the data. If schema reader
/// couldn't determine the schema we will try the next read buffer
/// from provided iterator if it makes sense. If format doesn't
/// have any schema reader or we couldn't determine the schema,
/// an exception will be thrown.
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    ReadBufferIterator & read_buffer_iterator,
    ContextPtr & context);

/// If ReadBuffer is created, it will be written to buf_out.
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    ReadBufferIterator & read_buffer_iterator,
    ContextPtr & context,
    std::unique_ptr<ReadBuffer> & buf_out);

/// Make type Nullable recursively:
/// - Type -> Nullable(type)
/// - Array(Type) -> Array(Nullable(Type))
/// - Tuple(Type1, ..., TypeN) -> Tuple(Nullable(Type1), ..., Nullable(TypeN))
/// - Map(KeyType, ValueType) -> Map(KeyType, Nullable(ValueType))
/// - LowCardinality(Type) -> LowCardinality(Nullable(Type))
/// If type is Nothing or one of the nested types is Nothing, return nullptr.
DataTypePtr makeNullableRecursivelyAndCheckForNothing(DataTypePtr type);

/// Call makeNullableRecursivelyAndCheckForNothing for all types
/// in the block and return names and types.
NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header);
}
