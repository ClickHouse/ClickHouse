#pragma once
#include "config.h"
#include <Core/Types.h>
#include <Common/ObjectStorageKeyGenerator.h>

#if USE_AWS_S3

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
namespace S3 { struct URI; }

ObjectStorageKeysGeneratorPtr getKeyGenerator(
    String type,
    const S3::URI & uri,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix);

class S3ObjectStorage;
bool checkBatchRemove(S3ObjectStorage & storage, const std::string & key_with_trailing_slash);

}

#endif
