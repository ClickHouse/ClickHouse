#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>

namespace DB
{

class IObjectStorage;
struct ReadSettings;
struct WriteSettings;

/// Throws `LIMIT_EXCEEDED` if the object is larger than `max_size`.
String readObjectToString(const IObjectStorage & object_storage, const String & key, const ReadSettings & read_settings, size_t max_size);

/// Fails if `key` already exists.
void writeNewObject(IObjectStorage & object_storage, const String & key, const String & content, WriteSettings write_settings);

}
