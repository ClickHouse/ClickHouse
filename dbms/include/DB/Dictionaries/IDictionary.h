#pragma once

#include <DB/Core/Field.h>
#include <DB/Core/StringRef.h>
#include <Poco/Util/XMLConfiguration.h>
#include <DB/Common/PODArray.h>
#include <memory>
#include <chrono>

namespace DB
{

class IDictionarySource;

class IDictionary;
using DictionaryPtr = std::unique_ptr<IDictionary>;

class DictionaryLifetime;
class DictionaryStructure;
class ColumnString;

class IDictionary
{
public:
	using id_t = std::uint64_t;

	virtual std::string getName() const = 0;

	virtual std::string getTypeName() const = 0;

	virtual std::size_t getBytesAllocated() const = 0;

	virtual double getHitRate() const = 0;

	virtual std::size_t getElementCount() const = 0;

	virtual double getLoadFactor() const = 0;

	virtual bool isCached() const = 0;
	virtual DictionaryPtr clone() const = 0;

	virtual const IDictionarySource * getSource() const = 0;

	virtual const DictionaryLifetime & getLifetime() const = 0;

	virtual const DictionaryStructure & getStructure() const = 0;

	virtual std::chrono::time_point<std::chrono::system_clock> getCreationTime() const = 0;

	virtual bool hasHierarchy() const = 0;

	/// do not call unless you ensure that hasHierarchy() returns true
	virtual id_t toParent(id_t id) const = 0;
	virtual void toParent(const PODArray<id_t> & ids, PODArray<id_t> & out) const = 0;

	bool in(id_t child_id, const id_t ancestor_id) const
	{
		while (child_id != 0 && child_id != ancestor_id)
			child_id = toParent(child_id);

		return child_id != 0;
	}

	/// functions for individual access
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
	virtual String getString(const std::string & attribute_name, id_t id) const = 0;

	/// functions for multiple access
	virtual void getUInt8(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<UInt8> & out) const = 0;
	virtual void getUInt16(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<UInt16> & out) const = 0;
	virtual void getUInt32(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<UInt32> & out) const = 0;
	virtual void getUInt64(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<UInt64> & out) const = 0;
	virtual void getInt8(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<Int8> & out) const = 0;
	virtual void getInt16(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<Int16> & out) const = 0;
	virtual void getInt32(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<Int32> & out) const = 0;
	virtual void getInt64(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<Int64> & out) const = 0;
	virtual void getFloat32(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<Float32> & out) const = 0;
	virtual void getFloat64(const std::string & attr_name, const PODArray<id_t> & ids, PODArray<Float64> & out) const = 0;
	virtual void getString(const std::string & attr_name, const PODArray<id_t> & ids, ColumnString * out) const = 0;

	virtual ~IDictionary() = default;
};

}
