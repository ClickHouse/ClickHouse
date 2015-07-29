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

class IDictionaryBase;
using DictionaryPtr = std::unique_ptr<IDictionaryBase>;

class DictionaryLifetime;
class DictionaryStructure;
class ColumnString;

struct IDictionaryBase
{
	using id_t = std::uint64_t;

	virtual std::exception_ptr getCreationException() const = 0;

	virtual std::string getName() const = 0;

	virtual std::string getTypeName() const = 0;

	virtual std::size_t getBytesAllocated() const = 0;

	virtual std::size_t getQueryCount() const = 0;

	virtual double getHitRate() const = 0;

	virtual std::size_t getElementCount() const = 0;

	virtual double getLoadFactor() const = 0;

	virtual bool isCached() const = 0;
	virtual DictionaryPtr clone() const = 0;

	virtual const IDictionarySource * getSource() const = 0;

	virtual const DictionaryLifetime & getLifetime() const = 0;

	virtual const DictionaryStructure & getStructure() const = 0;

	virtual std::chrono::time_point<std::chrono::system_clock> getCreationTime() const = 0;

	virtual bool isInjective(const std::string & attribute_name) const = 0;

	virtual ~IDictionaryBase() = default;
};

struct IDictionary : IDictionaryBase
{
	virtual bool hasHierarchy() const = 0;

	/// do not call unless you ensure that hasHierarchy() returns true
	id_t toParent(id_t id) const
	{
		const PODArray<UInt64> ids(1, id);
		PODArray<UInt64> out(1);

		toParent(ids, out);

		return out.front();
	}

	virtual void toParent(const PODArray<id_t> & ids, PODArray<id_t> & out) const = 0;

	bool in(id_t child_id, const id_t ancestor_id) const
	{
		while (child_id != 0 && child_id != ancestor_id)
			child_id = toParent(child_id);

		return child_id != 0;
	}

	/// return mapped values for a collection of identifiers
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
};

}
