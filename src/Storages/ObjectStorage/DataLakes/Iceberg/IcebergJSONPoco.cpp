#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergJSONPoco.h>

#include <Poco/JSON/Parser.h>

#include <sstream>

namespace DB::Iceberg
{

Poco::JSON::Object::Ptr toPocoJSON(const JSONObjectPtr & object)
{
    if (!object)
        return nullptr;
    Poco::JSON::Parser parser;
    return parser.parse(stringifyJSON(object)).extract<Poco::JSON::Object::Ptr>();
}

JSONObjectPtr fromPocoJSON(const Poco::JSON::Object::Ptr & object)
{
    if (!object)
        return nullptr;
    std::ostringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    object->stringify(out);
    return parseJSONObject(out.str());
}

}

#endif
