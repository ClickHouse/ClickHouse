#pragma once

#include <DB/Core/Field.h>
#include <memory>
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{

class IDictionarySource;

class IDictionary;
using DictionaryPtr = std::unique_ptr<IDictionary>;

class DictionaryLifetime;

class IDictionary
{
public:
    using id_t = std::uint64_t;

	virtual std::string getName() const = 0;

	virtual std::string getTypeName() const = 0;

	virtual bool isCached() const = 0;
	virtual void reload() {}
	virtual DictionaryPtr clone() const = 0;

	virtual const IDictionarySource * const getSource() const = 0;

	virtual const DictionaryLifetime & getLifetime() const = 0;

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
	virtual UInt8 getUInt8(const std::string & attribute_name, id_t id) const = 0;
	virtual UInt16 getUInt16(const std::string & attribute_name, id_t id) const = 0;
	virtual UInt32 getUInt32(const std::string & attribute_name, id_t id) const = 0;
	virtual UInt64 getUInt64(const std::string & attribute_name, id_t id) const = 0;
	virtual Int8 getInt8(const std::string & attribute_name, id_t id) const = 0;
	virtual Int16 getInt16(const std::string & attribute_name, id_t id) const = 0;
	virtual Int32 getInt32(const std::string & attribute_name, id_t id) const = 0;
	virtual Int64 getInt64(const std::string & attribute_name, id_t id) const = 0;
	virtual Float32 getFloat32(const std::string & attribute_name, id_t id) const = 0;
	virtual Float64 getFloat64(const std::string & attribute_name, id_t id) const = 0;
	virtual StringRef getString(const std::string & attribute_name, id_t id) const = 0;

	/// unsafe functions for maximum performance, you are on your own ensuring type-safety

	/// returns persistent attribute index for usage with following functions
	virtual std::size_t getAttributeIndex(const std::string & attribute_name) const = 0;

	/// type-checking functions
	virtual bool isUInt8(std::size_t attribute_idx) const = 0;
	virtual bool isUInt16(std::size_t attribute_idx) const = 0;
	virtual bool isUInt32(std::size_t attribute_idx) const = 0;
	virtual bool isUInt64(std::size_t attribute_idx) const = 0;
	virtual bool isInt8(std::size_t attribute_idx) const = 0;
	virtual bool isInt16(std::size_t attribute_idx) const = 0;
	virtual bool isInt32(std::size_t attribute_idx) const = 0;
	virtual bool isInt64(std::size_t attribute_idx) const = 0;
	virtual bool isFloat32(std::size_t attribute_idx) const = 0;
	virtual bool isFloat64(std::size_t attribute_idx) const = 0;
	virtual bool isString(std::size_t attribute_idx) const = 0;

	/// plain load from target container without any checks
	virtual UInt8 getUInt8Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual UInt16 getUInt16Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual UInt32 getUInt32Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual UInt64 getUInt64Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual Int8 getInt8Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual Int16 getInt16Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual Int32 getInt32Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual Int64 getInt64Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual Float32 getFloat32Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual Float64 getFloat64Unsafe(std::size_t attribute_idx, id_t id) const = 0;
	virtual StringRef getStringUnsafe(std::size_t attribute_idx, id_t id) const = 0;

    virtual ~IDictionary() = default;
};

}
