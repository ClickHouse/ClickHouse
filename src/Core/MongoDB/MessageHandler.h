#include "Document.h"

namespace DB
{
namespace MongoDB
{

class MessageHandler
{
public:
    static BSON::Document::Ptr handleIsMaster();
    static BSON::Document::Ptr handleGetParameter(const std::vector<std::string> & element_names);
};

}
} // namespace DB::MongoDB
