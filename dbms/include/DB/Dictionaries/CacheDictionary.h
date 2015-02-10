#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>

namespace DB
{

class CacheDictionary final : public IDictionary
{
public:
	CacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
		const std::size_t size)
		: name{name}, dict_struct(dict_struct),
		  source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
		  size{size}
	{
		if (!this->source_ptr->supportsSelectiveLoad())
			throw Exception{
				"Source cannot be used with CacheDictionary",
				ErrorCodes::UNSUPPORTED_METHOD
			};
	}

	CacheDictionary(const CacheDictionary & other)
		: CacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
	{}

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "CacheDictionary"; }

	bool isCached() const override { return true; }

	DictionaryPtr clone() const override { return ext::make_unique<CacheDictionary>(*this); }

	const IDictionarySource * getSource() const override { return source_ptr.get(); }

	const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

	bool hasHierarchy() const override { return false; }

	id_t toParent(const id_t id) const override { return 0; }

#define DECLARE_SAFE_GETTER(TYPE, NAME, LC_TYPE) \
	TYPE get##NAME(const std::string & attribute_name, const id_t id) const override\
    {\
        return {};\
	}
	DECLARE_SAFE_GETTER(UInt8, UInt8, uint8)
	DECLARE_SAFE_GETTER(UInt16, UInt16, uint16)
	DECLARE_SAFE_GETTER(UInt32, UInt32, uint32)
	DECLARE_SAFE_GETTER(UInt64, UInt64, uint64)
	DECLARE_SAFE_GETTER(Int8, Int8, int8)
	DECLARE_SAFE_GETTER(Int16, Int16, int16)
	DECLARE_SAFE_GETTER(Int32, Int32, int32)
	DECLARE_SAFE_GETTER(Int64, Int64, int64)
	DECLARE_SAFE_GETTER(Float32, Float32, float32)
	DECLARE_SAFE_GETTER(Float64, Float64, float64)
	DECLARE_SAFE_GETTER(StringRef, String, string)
#undef DECLARE_SAFE_GETTER

	std::size_t getAttributeIndex(const std::string & attribute_name) const override
	{
		return {};
	}

#define DECLARE_TYPE_CHECKER(NAME, LC_NAME)\
	bool is##NAME(const std::size_t attribute_idx) const override\
	{\
		return true;\
	}
	DECLARE_TYPE_CHECKER(UInt8, uint8)
	DECLARE_TYPE_CHECKER(UInt16, uint16)
	DECLARE_TYPE_CHECKER(UInt32, uint32)
	DECLARE_TYPE_CHECKER(UInt64, uint64)
	DECLARE_TYPE_CHECKER(Int8, int8)
	DECLARE_TYPE_CHECKER(Int16, int16)
	DECLARE_TYPE_CHECKER(Int32, int32)
	DECLARE_TYPE_CHECKER(Int64, int64)
	DECLARE_TYPE_CHECKER(Float32, float32)
	DECLARE_TYPE_CHECKER(Float64, float64)
	DECLARE_TYPE_CHECKER(String, string)
#undef DECLARE_TYPE_CHECKER

#define DECLARE_UNSAFE_GETTER(TYPE, NAME, LC_NAME)\
	TYPE get##NAME##Unsafe(const std::size_t attribute_idx, const id_t id) const override\
	{\
		return {};\
	}
	DECLARE_UNSAFE_GETTER(UInt8, UInt8, uint8)
	DECLARE_UNSAFE_GETTER(UInt16, UInt16, uint16)
	DECLARE_UNSAFE_GETTER(UInt32, UInt32, uint32)
	DECLARE_UNSAFE_GETTER(UInt64, UInt64, uint64)
	DECLARE_UNSAFE_GETTER(Int8, Int8, int8)
	DECLARE_UNSAFE_GETTER(Int16, Int16, int16)
	DECLARE_UNSAFE_GETTER(Int32, Int32, int32)
	DECLARE_UNSAFE_GETTER(Int64, Int64, int64)
	DECLARE_UNSAFE_GETTER(Float32, Float32, float32)
	DECLARE_UNSAFE_GETTER(Float64, Float64, float64)
	DECLARE_UNSAFE_GETTER(StringRef, String, string)
#undef DECLARE_UNSAFE_GETTER

private:
	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;
	const std::size_t size;

	union item
	{
		UInt8 uint8_value;
		UInt16 uint16_value;
		UInt32 uint32_value;
		UInt64 uint64_value;
		Int8 int8_value;
		Int16 int16_value;
		Int32 int32_value;
		Int64 int64_value;
		Float32 float32_value;
		Float64 float64_value;
		StringRef string_value;
	};

	struct cell
	{
		id_t id;
		std::vector<item> attrs;
	};

	std::vector<cell> cells;
};

}
