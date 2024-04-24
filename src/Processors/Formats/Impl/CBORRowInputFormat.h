#pragma once

#include "config.h"

#if USE_CBOR

#    include <Formats/CBORReader.h>
#    include <Formats/FormatFactory.h>
#    include <Processors/Formats/IRowInputFormat.h>

namespace DB
{

class ReadBuffer;

enum CBORTagTypes : uint64_t
{
    UNSIGNED_BIGNUM = 2, // byte string
    NEGATIVE_BIGNUM = 3, // byte string
    DECIMAL = 4, // array
    BIN_UUID = 37, // byte string
    IPV4 = 52, // 	byte string or array
    IPV6 = 54, // 	byte string or array
    DATE_IN_DAYS = 100, // 	Unsigned or negative integer
};

class WriteToDBListener final : public cbor::listener
{
private:
    struct Info
    {
        IColumn & column;
        DataTypePtr type;
        bool is_tuple_element;
        std::optional<size_t> array_size;
    };
    /// Stack is needed to process arrays and maps
    std::stack<Info> stack_info;
    std::optional<CBORTagTypes> current_tag = std::nullopt;
    //    bool in_array = false;
    //    bool in_map = false;

public:
    WriteToDBListener(MutableColumns & columns_, DataTypes & data_types_);
    virtual ~WriteToDBListener() = default;

    void on_integer(int value) override;
    void on_float32(float value) override;
    void on_double(double value) override;
    void on_bytes(unsigned char * data, int size) override;
    void on_string(std::string & str) override;
    void on_array(int size) override;
    void on_map(int size) override;
    void on_tag(unsigned int tag) override;
    void on_special(unsigned int code) override;
    void on_bool(bool) override;
    void on_null() override;
    void on_undefined() override;
    void on_error(const char * error) override;
    void on_extra_integer(unsigned long long value, int sign) override;
    void on_extra_tag(unsigned long long tag) override;
    void on_extra_special(unsigned long long tag) override;
};

class CBORRowInputFormat final : public IRowInputFormat
{
public:
    CBORRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    String getName() const override { return "CBORRowInputFormat"; }

private:
    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

    std::unique_ptr<CBORReader> reader;
    DataTypes data_types;
};

}

#endif
