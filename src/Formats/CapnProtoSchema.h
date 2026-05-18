#pragma once

#include "config.h"
#if USE_CAPNP

#include <Formats/FormatSchemaInfo.h>
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

bool checkIfStructContainsUnnamedUnion(const capnp::StructSchema & struct_schema);
bool checkIfStructIsNamedUnion(const capnp::StructSchema & struct_schema);

/// Get full name of type for better exception messages.
String getCapnProtoFullTypeName(const capnp::Type & type);

NamesAndTypesList capnProtoSchemaToCHSchema(const capnp::StructSchema & schema, bool skip_unsupported_fields);

}

#endif
