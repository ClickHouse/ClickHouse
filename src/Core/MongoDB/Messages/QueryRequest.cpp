#include "QueryRequest.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Loggers/Loggers.h>
#include <base/types.h>
#include <fmt/format.h>
#include "../BSON/BSONReader.h"
#include "../BSON/BSONWriter.h"
#include "../BSON/Document.h"
#include "RequestMessage.h"


namespace DB
{
namespace MongoDB
{

QueryRequest::~QueryRequest()
{
}

//
// inlines
//
inline QueryRequest::Flags QueryRequest::getFlags() const
{
    return flags;
}


inline void QueryRequest::setFlags(QueryRequest::Flags flags_)
{
    flags = flags_;
}


inline std::string QueryRequest::getFullCollectionName() const
{
    return full_collection_name;
}


inline BSON::Document::Ptr QueryRequest::getSelector()
{
    return selector;
}


inline BSON::Document::Ptr QueryRequest::getFieldSelector()
{
    return return_field_selector;
}


inline Int32 QueryRequest::getNumberToSkip() const
{
    return number_to_skip;
}


inline void QueryRequest::setNumberToSkip(Int32 n)
{
    number_to_skip = n;
}


inline Int32 QueryRequest::getNumberToReturn() const
{
    return number_to_return;
}


inline void QueryRequest::setNumberToReturn(Int32 n)
{
    number_to_return = n;
}


void QueryRequest::read(ReadBuffer & reader)
{
    LoggerPtr log = getLogger("QueryRequest::read");
    Int32 message_length = header.getContentLength();
    LOG_DEBUG(log, "message_length: {}\n", message_length);
    Int32 flags_;
    readIntBinary(flags_, reader);
    LOG_DEBUG(log, "flags: {}\n", flags_);
    flags = static_cast<Flags>(flags_);
    readNullTerminated(full_collection_name, reader);
    LOG_DEBUG(log, "full_collection_name: {}\n", full_collection_name);
    readIntBinary(number_to_skip, reader);
    LOG_DEBUG(log, "number_to_skip: {}\n", number_to_skip);
    readIntBinary(number_to_return, reader);
    LOG_DEBUG(log, "number_to_return: {}\n", number_to_return);
    message_length -= sizeof(flags_) + sizeof(number_to_skip) + sizeof(number_to_return) + full_collection_name.length() + sizeof('\0');
    selector = new BSON::Document();
    return_field_selector = new BSON::Document();
    message_length -= selector->read(reader);
    LOG_DEBUG(log, "selector: {}\n", selector->toString());
    if (message_length > 0)
    {
        LOG_DEBUG(log, "return_field_selector: {}\n", return_field_selector->toString());
        return_field_selector->read(reader);
    }
}


std::string QueryRequest::toString() const
{
    return fmt::format(
        "flags: {}\n"
        "full_collection_name: {}\n"
        "number_to_skip: {}\n"
        "number_to_return: {}\n"
        "selector: {}\n"
        "return_field_selector: {}\n",
        static_cast<Int32>(flags),
        full_collection_name,
        number_to_skip,
        number_to_return,
        selector->toString(),
        return_field_selector->toString());
}

}
}
