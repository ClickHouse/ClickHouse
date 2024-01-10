#pragma once
#include <Core/Types.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/S3/URI.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

ObjectStorageKeysGeneratorPtr getKeyGenerator(
    String type,
    const S3::URI & uri,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix);

class S3ObjectStorage;
bool checkBatchRemove(S3ObjectStorage & storage, const std::string & key_with_trailing_slash);

}
