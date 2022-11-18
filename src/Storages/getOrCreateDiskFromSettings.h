#pragma once

#include <Interpreters/Context_fwd.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/DOM/AutoPtr.h>

namespace DB
{


class SettingsChanges;
using DiskConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/**
 * Create a DiskPtr from key value list of disk settings.
 * add it to DiskSelector by a unique (but always the same for given configuration) disk name
 * and return this name.
 */
std::string getOrCreateDiskFromSettings(const SettingsChanges & configuration, ContextPtr context);

}
