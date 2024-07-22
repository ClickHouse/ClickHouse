#include "OpMsgMessage.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Loggers/Loggers.h>
#include <fmt/format.h>
#include "../BSON/Array.h"

namespace DB
{
namespace MongoDB
{

// Query and write
const std::string OpMsgMessage::CMD_INSERT{"insert"};
const std::string OpMsgMessage::CMD_DELETE{"delete"};
const std::string OpMsgMessage::CMD_UPDATE{"update"};
const std::string OpMsgMessage::CMD_FIND{"find"};
const std::string OpMsgMessage::CMD_FIND_AND_MODIFY{"findAndModify"};
const std::string OpMsgMessage::CMD_GET_MORE{"getMore"};

// Aggregation
const std::string OpMsgMessage::CMD_AGGREGATE{"aggregate"};
const std::string OpMsgMessage::CMD_COUNT{"count"};
const std::string OpMsgMessage::CMD_DISTINCT{"distinct"};
const std::string OpMsgMessage::CMD_MAP_REDUCE{"mapReduce"};

// Replication and administration
const std::string OpMsgMessage::CMD_HELLO{"hello"};
const std::string OpMsgMessage::CMD_REPL_SET_GET_STATUS{"replSetGetStatus"};
const std::string OpMsgMessage::CMD_REPL_SET_GET_CONFIG{"replSetGetConfig"};

const std::string OpMsgMessage::CMD_CREATE{"create"};
const std::string OpMsgMessage::CMD_CREATE_INDEXES{"createIndexes"};
const std::string OpMsgMessage::CMD_DROP{"drop"};
const std::string OpMsgMessage::CMD_DROP_DATABASE{"dropDatabase"};
const std::string OpMsgMessage::CMD_KILL_CURSORS{"killCursors"};
const std::string OpMsgMessage::CMD_LIST_DATABASES{"listDatabases"};
const std::string OpMsgMessage::CMD_LIST_INDEXES{"listIndexes"};

// Diagnostic
const std::string OpMsgMessage::CMD_BUILD_INFO{"buildInfo"};
const std::string OpMsgMessage::CMD_COLL_STATS{"collStats"};
const std::string OpMsgMessage::CMD_DB_STATS{"dbStats"};
const std::string OpMsgMessage::CMD_HOST_INFO{"hostInfo"};


static const std::string & commandIdentifier(const std::string & command);
/// Commands have different names for the payload that is sent in a separate section


static const std::string keyCursor{"cursor"};
static const std::string keyFirstBatch{"firstBatch"};
static const std::string keyNextBatch{"nextBatch"};

OpMsgMessage::OpMsgMessage(const MessageHeader & header_) : RequestMessage(header_)
{
}


OpMsgMessage::~OpMsgMessage() = default;

const std::string & OpMsgMessage::getDatabaseName() const
{
    return database_name;
}


const std::string & OpMsgMessage::getCollectionName() const
{
    return collection_name;
}


void OpMsgMessage::setCommandName(const std::string & command)
{
    command_name = command;
    body->clear();

    // IMPORTANT: Command name must be first
    if (collection_name.empty())
    {
        // Collection is not specified. It is assumed that this particular command does
        // not need it.
        body->add(command_name, Int32(1));
    }
    else
    {
        body->add(command_name, collection_name);
    }
    body->add("$db", database_name);
}


void OpMsgMessage::setCursor(Int64 cursorID, Int32 batchSize)
{
    command_name = OpMsgMessage::CMD_GET_MORE;
    body->clear();

    // IMPORTANT: Command name must be first
    body->add(command_name, cursorID);
    body->add("$db", database_name);
    body->add("collection", collection_name);
    if (batchSize > 0)
        body->add("batchSize", batchSize);
}


const std::string & OpMsgMessage::getCommandName() const
{
    return command_name;
}


void OpMsgMessage::setAcknowledgedRequest(bool ack)
{
    const auto & id = commandIdentifier(command_name);
    if (id.empty())
        return;

    acknowledged = ack;

    auto writeConcern = body->get<BSON::Document::Ptr>("writeConcern", nullptr);
    if (writeConcern)
        writeConcern->remove("w");

    if (ack)
    {
        flags = flags & (~MSG_MORE_TO_COME);
    }
    else
    {
        flags = flags | MSG_MORE_TO_COME;
        if (!writeConcern)
            body->addNewDocument("writeConcern").add("w", 0);
        else
            writeConcern->add("w", 0);
    }
}


bool OpMsgMessage::acknowledgedRequest() const
{
    return acknowledged;
}


UInt32 OpMsgMessage::getFlags() const
{
    return flags;
}


BSON::Document::Ptr OpMsgMessage::getBody()
{
    return body;
}


const BSON::Document::Ptr OpMsgMessage::getBody() const
{
    return body;
}


BSON::Document::Vector & OpMsgMessage::getDocuments()
{
    return documents;
}


const BSON::Document::Vector & OpMsgMessage::getDocuments() const
{
    return documents;
}


bool OpMsgMessage::responseOk() const
{
    Int64 ok{false};
    if (body->exists("ok"))
        ok = body->getInteger("ok");
    return (ok != 0);
}


void OpMsgMessage::clear()
{
    flags = MSG_FLAGS_DEFAULT;
    command_name.clear();
    body->clear();
    documents.clear();
}

void OpMsgMessage::send(WriteBuffer & writer)
{
    Int32 size = getLength();
    setContentLength(size);
    header.write(writer);
    writeContent(writer);
}


Int32 OpMsgMessage::getLength()
{
    Int32 length = sizeof(flags) + sizeof(PAYLOAD_TYPE_0) + body->getLength();
    if (!documents.empty())
    {
        length += sizeof(Int32) + commandIdentifier(command_name).length() + sizeof('\0');
        for (auto & doc : documents)
            length += doc->getLength();
    }
    return length;
}

void OpMsgMessage::writeContent(WriteBuffer & writer)
{
    writeIntBinary(flags, writer);
    // writer << _flags;

    writeChar(PAYLOAD_TYPE_0, writer);
    // writer << PAYLOAD_TYPE_0
    // _body.write(writer);
    BSON::BSONWriter(writer).write(body);

    if (!documents.empty())
    {
        // Serialise attached documents
        Int32 documents_length = 0;
        for (auto & doc : documents)
            documents_length += doc->getLength();
        //doc->write(wdoc);
        //wdoc.flush();

        const std::string & identifier = commandIdentifier(command_name);
        const Int32 size = Int32(sizeof(size) + identifier.size() + sizeof('\0') + documents_length);
        writeChar(PAYLOAD_TYPE_1, writer);
        writeIntBinary(size, writer);
        writeNullTerminatedString(identifier, writer);
        for (auto & doc : documents)
            BSON::BSONWriter(writer).write(doc);
        /*writer << PAYLOAD_TYPE_1;
        writer << size;
        writer.writeCString(identifier.c_str());*/
    }
}


void OpMsgMessage::read(ReadBuffer & reader)
{
    LoggerPtr log = getLogger("OpMsgMessage::read");
    Int32 remaining_size = header.getContentLength();
    UInt8 payload_type{0xFF};

    readIntBinary(flags, reader);
    // reader >> _flags;
    // reader >> payloadType;
    readIntBinary(payload_type, reader);
    assert(payload_type == PAYLOAD_TYPE_0);
    body = BSON::BSONReader(reader).read<BSON::Document::Ptr>();
    // body.read(reader);

    remaining_size -= sizeof(flags) + sizeof(payload_type) + body->getLength(); // read and write of the same size ?
    while (remaining_size > 0)
    {
        // NOTE: Not tested yet with database, because it returns everything in the body.
        // Does MongoDB ever return documents as Payload type 1?
        readIntBinary(payload_type, reader);
        remaining_size -= sizeof(payload_type);
        // reader >> payload_type;
        if (remaining_size == 0)
            break;
        assert(payload_type == PAYLOAD_TYPE_1);

        Int32 section_size{0};
        readIntBinary(section_size, reader);
        remaining_size -= section_size;
        // reader >> sectionSize;
        assert(section_size > 0);
        readNullTerminated(command_name, reader);
        //reader.readCString(identifier);

        // Loop to read documents from this section.
        while (section_size > 0)
        {
            BSON::Document::Ptr doc = BSON::BSONReader(reader).read<BSON::Document::Ptr>();
            documents.push_back(doc);
            section_size -= doc->getLength();
        }
    }

    // Extract documents from the cursor batch if they are there.
    BSON::Array::Ptr batch;
    auto curDoc = body->get<BSON::Document::Ptr>(keyCursor, nullptr);
    if (curDoc)
    {
        batch = curDoc->get<BSON::Array::Ptr>(keyFirstBatch, nullptr);
        if (!batch)
            batch = curDoc->get<BSON::Array::Ptr>(keyNextBatch, nullptr);
    }
    if (batch)
    {
        for (size_t i = 0; i < batch->size(); i++)
        {
            const auto & d = batch->get<BSON::Document::Ptr>(i, nullptr);
            if (d)
                documents.push_back(d);
        }
    }
}

const std::string & commandIdentifier(const std::string & command)
{
    // Names of identifiers for commands that send bulk documents in the request
    // The identifier is set in the section type 1.
    static std::map<std::string, std::string> identifiers {
        {{OpMsgMessage::CMD_INSERT, "documents"},
         {OpMsgMessage::CMD_DELETE, "deletes"},
         {OpMsgMessage::CMD_UPDATE, "updates"},

         // Not sure if create index can send document section
         {OpMsgMessage::CMD_CREATE_INDEXES, "indexes"}}};

    const auto i = identifiers.find(command);
    if (i != identifiers.end())
        return i->second;

    // This likely means that documents are incorrectly set for a command
    // that does not send list of documents in section type 1.
    static const std::string emptyIdentifier;
    return emptyIdentifier;
}


std::string OpMsgMessage::toString() const
{
    auto res = fmt::format(
        "flags_bits: {}\n"
        "body: {}\n"
        "database_name: {}\n"
        "collection_name: {}\n"
        "command_name: {}\n"
        "acknowledged: {}\n",
        static_cast<Int32>(flags),
        body->toString(),
        database_name,
        collection_name,
        command_name,
        static_cast<Int32>(acknowledged));
    res += "documents: ";
    for (size_t i = 0; i < documents.size(); i++)
        res += fmt::format("[i={}: {}]\n", i, documents[i]->toString());
    return res;
}

}
}
