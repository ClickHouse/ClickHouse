#include <Common/BSONParser/Document.h>

namespace DB
{
namespace MongoDB
{


class MessageHandler
{
public:
    static BSON::Document::Ptr handleIsMaster();
};

BSON::Document::Ptr MessageHandler::handleIsMaster()
{
    // TODO make all settings configable
    BSON::Document::Ptr response = new BSON::Document();
    Poco::Timestamp current_time;
    current_time.update();
    (*response)
        .add("ismaster", true)
        .add("maxBsonObjectSize", 2048)
        .add("maxMessageSizeBytes", 2048)
        .add("maxWriteBatchSize", 100000)
        .add("localTime", current_time)
        .add("minWireVersion", 0)
        .add("maxWireVersion", 21)
        .add("readOnly", false)
        .add("ok", 1.0);
    return response;
}


}
} // namespace DB::MongoDB
