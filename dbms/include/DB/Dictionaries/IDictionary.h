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

struct IDictionaryBase;
using DictionaryPtr = std::unique_ptr<IDictionaryBase>;

struct DictionaryLifetime;
struct DictionaryStructure;
class ColumnString;

struct IDictionaryBase
{
	using Key = UInt64;

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

	virtual void toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const = 0;

	virtual void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const = 0;

	/// do not call unless you ensure that hasHierarchy() returns true
	Key toParent(Key id) const
	{
		const PaddedPODArray<UInt64> ids(1, id);
		PaddedPODArray<UInt64> out(1);

		toParent(ids, out);

		return out.front();
	}

	bool in(Key child_id, const Key ancestor_id) const
	{
		while (child_id != 0 && child_id != ancestor_id)
			child_id = toParent(child_id);

		return child_id != 0;
	}
};

}
