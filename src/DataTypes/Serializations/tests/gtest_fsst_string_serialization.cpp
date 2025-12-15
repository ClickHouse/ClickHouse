#include <cstddef>
#include <memory>
#include <stdexcept>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include "Columns/IColumn_fwd.h"
#include "DataTypes/SerializationStringFsst.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "DataTypes/Serializations/SerializationString.h"


using namespace DB;

bool Equals(const ColumnString & a, const ColumnString & b)
{
    return a.size() == b.size() && a.getOffsets() == b.getOffsets() && a.getChars() == b.getChars();
}

int main()
{
    MainThreadStatus::getInstance();

    constexpr size_t rows = 1'000'000;

    std::map<std::string, std::string> buffers;
    std::map<std::string, std::unique_ptr<WriteBufferFromString>> writers;
    std::map<std::string, std::unique_ptr<ReadBufferFromString>> readers;

    ISerialization::EnumerateStreamsSettings enum_settings;
    auto create_enumerate_callback = [&buffers](const ISerialization::SubstreamPath & path) { buffers[path.toString()]; };

    auto nested_serialization = std::make_shared<SerializationString>();
    auto serialization = std::make_shared<SerializationStringFsst>(nested_serialization);
    serialization->enumerateStreams(enum_settings, create_enumerate_callback, ISerialization::SubstreamData{});

    auto src_column = ColumnString::create();
    src_column->insertMany("foobar", rows);

    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state = std::make_shared<SerializeFsstState>();
        settings.getter = [&buffers, &writers](const ISerialization::SubstreamPath & path)
        {
            auto & w = writers[path.toString()];
            if (!w)
                w = std::make_unique<WriteBufferFromString>(buffers[path.toString()]);
            return w.get();
        };
        serialization->serializeBinaryBulkWithMultipleStreams(*src_column, 0, src_column->size(), settings, state);

        for (auto & [_, w] : writers)
        {
            w->finalize();
        }
    }
/*
    auto dst_column = ColumnString::create();
    ColumnPtr column_ptr = std::move(dst_column);
    {
        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.getter = [&buffers, &readers](const ISerialization::SubstreamPath & path)
        {
            auto & r = readers[path.toString()];
            if (!r)
                r = std::make_unique<ReadBufferFromString>(buffers[path.toString()]);
            return r.get();
        };
        serialization->deserializeBinaryBulkWithMultipleStreams(column_ptr, 0, rows, settings, state, nullptr);
    }

    if(!Equals(*src_column, *dst_column)) {
        throw std::runtime_error("fuck");
    }*/
}
