#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#    include "config_core.h"
#endif


#if USE_MSGPACK

#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>
#include <msgpack.hpp>
#include <stack>

namespace DB
{

class ReadBuffer;

class MsgPackVisitor : public msgpack::null_visitor
{
public:
    struct Info
    {
        IColumn & column;
        DataTypePtr type;
    };

    /// These functions are called when parser meets corresponding object in parsed data
    bool visit_positive_integer(UInt64 value);
    bool visit_negative_integer(Int64 value);
    bool visit_float32(Float32 value);
    bool visit_float64(Float64 value);
    bool visit_str(const char* value, size_t size);
    bool start_array(size_t size);
    bool end_array();

    /// This function will be called if error occurs in parsing
    [[noreturn]] void parse_error(size_t parsed_offset, size_t error_offset);

    /// Update info_stack
    void set_info(IColumn & column, DataTypePtr type);

    void insert_integer(UInt64 value);

    void reset();

private:
    /// Stack is needed to process nested arrays
    std::stack<Info> info_stack;
};

class MsgPackRowInputFormat : public IRowInputFormat
{
public:
    MsgPackRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "MagPackRowInputFormat"; }
    void resetParser() override;

private:
    bool readObject();

    PeekableReadBuffer buf;
    MsgPackVisitor visitor;
    msgpack::detail::parse_helper<MsgPackVisitor> parser;
    const DataTypes data_types;
};

}

#endif
