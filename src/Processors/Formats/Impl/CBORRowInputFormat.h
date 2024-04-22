#pragma once

#include "config.h"

#if USE_CBOR

#    include <Formats/FormatFactory.h>
#    include <Formats/CBORReader.h>
#    include <Processors/Formats/IRowInputFormat.h>

namespace DB
{

class ReadBuffer;

enum class CBORTagTypes : uint64_t
{
    STANDARD_DATETIME = 0, // text string
    EPOCH_BASED_DATETIME = 1, // integer or float
    UNSIGNED_BIGNUM = 2, // byte string
    NEGATIVE_BIGNUM = 3, // byte string
    DECIMAL = 4, // array
    BIGFLOAT = 5, // byte string
    BIN_UUID = 37, // byte string
    IPV4 = 52, // 	byte string or array
    IPV6 = 54, // 	byte string or array
};

class WriteToDBListener final : public cbor::listener
{
private:
    struct Info
    {
        IColumn & column;
        DataTypePtr type;
    };
    std::stack<Info> stack_info;
    CBORTagTypes current_tag; // None
//    bool in_array = false;
//    bool in_map = false;

public:
    WriteToDBListener(MutableColumns & columns_, DataTypes & data_types_);
    virtual ~WriteToDBListener() = default;

    void set_info(IColumn & column, DataTypePtr type);
    bool info_empty() const;

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
