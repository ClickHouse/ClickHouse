#pragma once

#include <DB/Core/Field.h>
#include <memory>
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{

class IDictionary
{
public:
    using id_t = std::uint64_t;

	virtual bool hasHierarchy() const = 0;

	/// do not call unless you ensure that hasHierarchy() returns true
	virtual id_t toParent(id_t id) const = 0;

	bool in(id_t child_id, const id_t ancestor_id) const
	{
		while (child_id != 0 && child_id != ancestor_id)
			child_id = toParent(child_id);

		return child_id != 0;
	}

	/// safe and slow functions, perform map lookup and type checks
	virtual UInt64 getUInt64(const std::string & attribute_name, id_t id) const = 0;
    virtual StringRef getString(const std::string & attribute_name, id_t id) const = 0;

	/// unsafe functions for maximum performance, you are on your own ensuring type-safety

	/// returns persistent attribute index for usage with following functions
	virtual std::size_t getAttributeIndex(const std::string & attribute_name) const = 0;

	/// type-checking functions
	virtual bool isUInt64(std::size_t attribute_idx) const = 0;
	virtual bool isString(std::size_t attribute_idx) const = 0;

	/// plain load from target container without any checks
	virtual UInt64 getUInt64Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual StringRef getStringUnsafe(std::size_t attribute_idx, id_t id) const = 0;

	/// entirely-loaded dictionaries should be immutable
	virtual bool isComplete() const = 0;
	virtual void reload() {}

    virtual ~IDictionary() = default;
};

using DictionaryPtr = std::unique_ptr<IDictionary>;

}
