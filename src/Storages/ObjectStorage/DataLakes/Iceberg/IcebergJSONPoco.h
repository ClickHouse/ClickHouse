#pragma once

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergJSON.h>

#include <Poco/JSON/Object.h>

namespace DB::Iceberg
{

Poco::JSON::Object::Ptr toPocoJSON(const JSONObjectPtr & object);
JSONObjectPtr fromPocoJSON(const Poco::JSON::Object::Ptr & object);

}
