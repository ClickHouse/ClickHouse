#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ObjectStorageKeyGenerator.h>


namespace DB
{

ObjectStorageKeysGeneratorPtr getBackupObjectKeyGenerator(const Poco::Util::AbstractConfiguration & config);
}
