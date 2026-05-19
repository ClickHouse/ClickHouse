#pragma once

#include <DataTypes/IDataType.h>

#include <optional>

namespace DB
{

class DataTypeFactory;

enum class FixedEncodedTextKind : UInt8
{
    Base58,
    Base64,
};

struct FixedEncodedTextInfo
{
    FixedEncodedTextKind kind;
    size_t n;
};

String getFixedEncodedTextTypeName(FixedEncodedTextKind kind, size_t n);
std::optional<FixedEncodedTextInfo> getFixedEncodedTextInfo(const IDataType & type);
DataTypePtr createFixedEncodedTextType(FixedEncodedTextKind kind, size_t n);

void registerDataTypeFixedEncodedText(DataTypeFactory & factory);

}
