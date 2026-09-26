#pragma once

#include <optional>
#include <Core/Field.h>
#include <base/types.h>

namespace DB
{

class IReadBufferMetadataProvider
{
public:
    virtual ~IReadBufferMetadataProvider() = default;

    /// Return metadata identified by name, if supported by the buffer.
    virtual std::optional<Field> getMetadata(const String & name) const = 0;
};

}
