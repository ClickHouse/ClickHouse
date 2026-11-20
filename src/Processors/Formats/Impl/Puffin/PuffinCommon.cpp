#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>

#include <Poco/JSON/Array.h>

namespace DB
{

Poco::JSON::Object::Ptr PuffinBlob::toJSON(bool in_footer) const
{
    Poco::JSON::Array::Ptr fields_json = new Poco::JSON::Array;
    for (Int32 field_id : fields)
        fields_json->add(field_id);

    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    json->set("type", type);
    json->set("fields", fields_json);
    json->set("snapshot-id", snapshot_id);
    json->set("sequence-number", sequence_number);
    if (in_footer)
    {
        json->set("offset", offset);
        json->set("length", length);
        if (!compression_codec.empty())
            json->set("compression-codec", compression_codec);
    }
    if (!properties.empty())
    {
        Poco::JSON::Object::Ptr properties_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
        for (const auto & [key, value] : properties)
            properties_json->set(key, value);
        json->set("properties", properties_json);
    }
    return json;
}

}
