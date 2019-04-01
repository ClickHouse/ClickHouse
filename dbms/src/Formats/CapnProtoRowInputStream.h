#pragma once
#include <Common/config.h>
#if USE_CAPNP

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>

#include <capnp/schema-parser.h>

namespace DB
{

class FormatSchemaInfo;
class ReadBuffer;

/** A stream for reading messages in Cap'n Proto format in given schema.
  * Like Protocol Buffers and Thrift (but unlike JSON or MessagePack),
  * Cap'n Proto messages are strongly-typed and not self-describing.
  * The schema in this case cannot be compiled in, so it uses a runtime schema parser.
  * See https://capnproto.org/cxx.html
  */
class CapnProtoRowInputStream : public IRowInputStream
{
public:
    struct NestedField
    {
        std::vector<std::string> tokens;
        size_t pos;
    };
    using NestedFieldList = std::vector<NestedField>;

    /** schema_dir  - base path for schema files
      * schema_file - location of the capnproto schema, e.g. "schema.capnp"
      * root_object - name to the root object, e.g. "Message"
      */
    CapnProtoRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSchemaInfo & info);

    bool read(MutableColumns & columns, RowReadExtension &) override;

private:
    kj::Array<capnp::word> readMessage();

    // Build a traversal plan from a sorted list of fields
    void createActions(const NestedFieldList & sortedFields, capnp::StructSchema reader);

    /* Action for state machine for traversing nested structures. */
    using BlockPositionList = std::vector<size_t>;
    struct Action
    {
        enum Type { POP, PUSH, READ };
        Type type;
        capnp::StructSchema::Field field = {};
        BlockPositionList columns = {};
    };

    // Wrapper for classes that could throw in destructor
    // https://github.com/capnproto/capnproto/issues/553
    template <typename T>
    struct DestructorCatcher
    {
        T impl;
        template <typename ... Arg>
        DestructorCatcher(Arg && ... args) : impl(kj::fwd<Arg>(args)...) {}
        ~DestructorCatcher() noexcept try { } catch (...) { return; }
    };
    using SchemaParser = DestructorCatcher<capnp::SchemaParser>;

    ReadBuffer & istr;
    Block header;
    std::shared_ptr<SchemaParser> parser;
    capnp::StructSchema root;
    std::vector<Action> actions;
};

}

#endif // USE_CAPNP
