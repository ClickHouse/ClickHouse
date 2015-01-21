#pragma once

#include <DB/Core/Field.h>
#include <memory>

namespace DB
{

class IDictionary
{
public:
    using id_t = std::uint64_t;

    virtual StringRef getString(const id_t id, const std::string & attribute_name) const = 0;
	virtual UInt64 getUInt64(const id_t id, const std::string & attribute_name) const = 0;

    virtual ~IDictionary() = default;
};

using DictionaryPtr = std::unique_ptr<IDictionary>;

}
