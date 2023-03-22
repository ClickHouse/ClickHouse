#pragma once

#include "config_formats.h"
#if USE_CAPNP

#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <Core/Block.h>
#include <capnp/schema-parser.h>
#include <capnp/dynamic.h>

namespace DB
{
// Wrapper for classes that could throw in destructor
// https://github.com/capnproto/capnproto/issues/553
template <typename T>
struct DestructorCatcher
{
    T impl;
    template <typename ... Arg>
    explicit DestructorCatcher(Arg && ... args) : impl(kj::fwd<Arg>(args)...) {}
    ~DestructorCatcher() noexcept try { } catch (...) { return; }
};

class CapnProtoSchemaParser : public DestructorCatcher<capnp::SchemaParser>
{
public:
    CapnProtoSchemaParser() = default;

    capnp::StructSchema getMessageSchema(const FormatSchemaInfo & schema_info);
};

bool compareEnumNames(const String & first, const String & second, FormatSettings::EnumComparingMode mode);

std::pair<capnp::DynamicStruct::Builder, capnp::StructSchema::Field> getStructBuilderAndFieldByColumnName(capnp::DynamicStruct::Builder struct_builder, const String & name);

capnp::DynamicValue::Reader getReaderByColumnName(const capnp::DynamicStruct::Reader & struct_reader, const String & name);

void checkCapnProtoSchemaStructure(const capnp::StructSchema & schema, const Block & header, FormatSettings::EnumComparingMode mode);

NamesAndTypesList capnProtoSchemaToCHSchema(const capnp::StructSchema & schema, bool skip_unsupported_fields);
}

#endif
