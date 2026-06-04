#include <Storages/MergeTree/UniqueKey/UniqueKeyProbe.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyProbeSimple.h>

#include <Core/SettingsEnums.h>
#include <Common/Exception.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

UniqueKeyProbePtr makeUniqueKeyProbe(
    UniqueKeyProbeImplementation impl,
    ProbeTargetsSupplier supplier,
    Names unique_key_column_names,
    size_t max_encoded_size)
{
    switch (impl)
    {
        case UniqueKeyProbeImplementation::Auto:
        case UniqueKeyProbeImplementation::Sequential:
            return std::make_unique<UniqueKeyProbeSimple>(
                std::move(supplier), std::move(unique_key_column_names), max_encoded_size);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown UNIQUE KEY probe implementation: {}", static_cast<int>(impl));
}

}
