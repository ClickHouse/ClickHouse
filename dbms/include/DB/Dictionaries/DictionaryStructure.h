#pragma once

#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <ext/range.hpp>
#include <vector>
#include <string>
#include <map>
#include <experimental/optional>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_TYPE;
	extern const int ARGUMENT_OUT_OF_BOUND;
	extern const int TYPE_MISMATCH;
}


enum class AttributeUnderlyingType
{
	UInt8,
	UInt16,
	UInt32,
	UInt64,
	Int8,
	Int16,
	Int32,
	Int64,
	Float32,
	Float64,
	String
};

inline AttributeUnderlyingType getAttributeUnderlyingType(const std::string & type)
{
	static const std::unordered_map<std::string, AttributeUnderlyingType> dictionary{
		{ "UInt8", AttributeUnderlyingType::UInt8 },
		{ "UInt16", AttributeUnderlyingType::UInt16 },
		{ "UInt32", AttributeUnderlyingType::UInt32 },
		{ "UInt64", AttributeUnderlyingType::UInt64 },
		{ "Int8", AttributeUnderlyingType::Int8 },
		{ "Int16", AttributeUnderlyingType::Int16 },
		{ "Int32", AttributeUnderlyingType::Int32 },
		{ "Int64", AttributeUnderlyingType::Int64 },
		{ "Float32", AttributeUnderlyingType::Float32 },
		{ "Float64", AttributeUnderlyingType::Float64 },
		{ "String", AttributeUnderlyingType::String },
		{ "Date", AttributeUnderlyingType::UInt16 },
		{ "DateTime", AttributeUnderlyingType::UInt32 },
	};

	const auto it = dictionary.find(type);
	if (it != std::end(dictionary))
		return it->second;

	throw Exception{
		"Unknown type " + type,
		ErrorCodes::UNKNOWN_TYPE
	};
}

inline std::string toString(const AttributeUnderlyingType type)
{
	switch (type)
	{
		case AttributeUnderlyingType::UInt8: return "UInt8";
		case AttributeUnderlyingType::UInt16: return "UInt16";
		case AttributeUnderlyingType::UInt32: return "UInt32";
		case AttributeUnderlyingType::UInt64: return "UInt64";
		case AttributeUnderlyingType::Int8: return "Int8";
		case AttributeUnderlyingType::Int16: return "Int16";
		case AttributeUnderlyingType::Int32: return "Int32";
		case AttributeUnderlyingType::Int64: return "Int64";
		case AttributeUnderlyingType::Float32: return "Float32";
		case AttributeUnderlyingType::Float64: return "Float64";
		case AttributeUnderlyingType::String: return "String";
	}

	throw Exception{
		"Unknown attribute_type " + toString(static_cast<int>(type)),
		ErrorCodes::ARGUMENT_OUT_OF_BOUND
	};
}

/// Min and max lifetimes for a dictionary or it's entry
struct DictionaryLifetime final
{
	std::uint64_t min_sec;
	std::uint64_t max_sec;

	DictionaryLifetime(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
	{
		const auto & lifetime_min_key = config_prefix + ".min";
		const auto has_min = config.has(lifetime_min_key);

		this->min_sec = has_min ? config.getInt(lifetime_min_key) : config.getInt(config_prefix);
		this->max_sec = has_min ? config.getInt(config_prefix + ".max") : this->min_sec;
	}
};

/** Holds the description of a single dictionary attribute:
*	- name, used for lookup into dictionary and source;
*	- type, used in conjunction with DataTypeFactory and getAttributeUnderlyingTypeByname;
*	- null_value, used as a default value for non-existent entries in the dictionary,
*		decimal representation for numeric attributes;
*	- hierarchical, whether this attribute defines a hierarchy;
*	- injective, whether the mapping to parent is injective (can be used for optimization of GROUP BY?)
*/
struct DictionaryAttribute final
{
	const std::string name;
	const AttributeUnderlyingType underlying_type;
	const DataTypePtr type;
	const std::string expression;
	const Field null_value;
	const bool hierarchical;
	const bool injective;
};

struct DictionarySpecialAttribute final
{
	const std::string name;
	const std::string expression;

	DictionarySpecialAttribute(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
		: name{config.getString(config_prefix + ".name", "")},
		  expression{config.getString(config_prefix + ".expression", "")}
	{
		if (name.empty() && !expression.empty())
			throw Exception{
				"Element " + config_prefix + ".name is empty",
				ErrorCodes::BAD_ARGUMENTS
			};
	}
};

/// Name of identifier plus list of attributes
struct DictionaryStructure final
{
	std::experimental::optional<DictionarySpecialAttribute> id;
	std::experimental::optional<std::vector<DictionaryAttribute>> key;
	std::vector<DictionaryAttribute> attributes;
	std::experimental::optional<DictionarySpecialAttribute> range_min;
	std::experimental::optional<DictionarySpecialAttribute> range_max;
	bool has_expressions = false;

	DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
	{
		const auto has_id = config.has(config_prefix + ".id");
		const auto has_key = config.has(config_prefix + ".key");

		if (has_key && has_id)
			throw Exception{"Only one of 'id' and 'key' should be specified", ErrorCodes::BAD_ARGUMENTS};

		if (has_id)
			id.emplace(config, config_prefix + ".id");
		else if (has_key)
		{
			key.emplace(getAttributes(config, config_prefix + ".key", false, false));
			if (key->empty())
				throw Exception{"Empty 'key' supplied", ErrorCodes::BAD_ARGUMENTS};
		}
		else
			throw Exception{"Dictionary structure should specify either 'id' or 'key'", ErrorCodes::BAD_ARGUMENTS};

		if (id)
		{
			if (id->name.empty())
				throw Exception{"'id' cannot be empty", ErrorCodes::BAD_ARGUMENTS};

			if (config.has(config_prefix + ".range_min"))
				range_min.emplace(config, config_prefix + ".range_min");

			if (config.has(config_prefix + ".range_max"))
				range_max.emplace(config, config_prefix + ".range_max");

			if (!id->expression.empty() ||
				(range_min && !range_min->expression.empty()) ||
				(range_max && !range_max->expression.empty()))
				has_expressions = true;
		}

		attributes = getAttributes(config, config_prefix);
		if (attributes.empty())
			throw Exception{"Dictionary has no attributes defined", ErrorCodes::BAD_ARGUMENTS};
	}

	void validateKeyTypes(const DataTypes & key_types) const
	{
		if (key_types.size() != key.value().size())
			throw Exception{
				"Key structure does not match, expected " + getKeyDescription(),
				ErrorCodes::TYPE_MISMATCH
			};

		for (const auto i : ext::range(0, key_types.size()))
		{
			const auto & expected_type = (*key)[i].type->getName();
			const auto & actual_type = key_types[i]->getName();

			if (expected_type != actual_type)
				throw Exception{
					"Key type at position " + std::to_string(i) + " does not match, expected " + expected_type +
						", found " + actual_type,
					ErrorCodes::TYPE_MISMATCH
				};
		}
	}

	std::string getKeyDescription() const
	{
		if (id)
			return "UInt64";

		std::ostringstream out;

		out << '(';

		auto first = true;
		for (const auto & key_i : *key)
		{
			if (!first)
				out << ", ";

			first = false;

			out << key_i.type->getName();
		}

		out << ')';

		return out.str();
	}

	bool isKeySizeFixed() const
	{
		if (!key)
			return true;

		for (const auto key_i : * key)
			if (key_i.underlying_type == AttributeUnderlyingType::String)
				return false;

		return true;
	}

	std::size_t getKeySize() const
	{
		return std::accumulate(std::begin(*key), std::end(*key), std::size_t{},
			[] (const auto running_size, const auto & key_i) {return running_size + key_i.type->getSizeOfField(); });
	}

private:
	std::vector<DictionaryAttribute> getAttributes(
		const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		const bool hierarchy_allowed = true, const bool allow_null_values = true)
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_prefix, keys);
		auto has_hierarchy = false;

		std::vector<DictionaryAttribute> attributes;

		for (const auto & key : keys)
		{
			if (0 != strncmp(key.data(), "attribute", strlen("attribute")))
				continue;

			const auto prefix = config_prefix + '.' + key + '.';

			const auto name = config.getString(prefix + "name");
			const auto type_string = config.getString(prefix + "type");
			const auto type = DataTypeFactory::instance().get(type_string);
			const auto underlying_type = getAttributeUnderlyingType(type_string);

			const auto expression = config.getString(prefix + "expression", "");
			if (!expression.empty())
				has_expressions = true;

			Field null_value;
			if (allow_null_values)
			{
				const auto null_value_string = config.getString(prefix + "null_value");
				try
				{
					ReadBufferFromString null_value_buffer{null_value_string};
					ColumnPtr column_with_null_value = type->createColumn();
					type->deserializeTextEscaped(*column_with_null_value, null_value_buffer);
					null_value = (*column_with_null_value)[0];
				}
				catch (const std::exception & e)
				{
					throw Exception{
						std::string{"Error parsing null_value: "} + e.what(),
						ErrorCodes::BAD_ARGUMENTS
					};
				}
			}

			const auto hierarchical = config.getBool(prefix + "hierarchical", false);
			const auto injective = config.getBool(prefix + "injective", false);
			if (name.empty())
				throw Exception{
					"Properties 'name' and 'type' of an attribute cannot be empty",
					ErrorCodes::BAD_ARGUMENTS
				};

			if (has_hierarchy && !hierarchy_allowed)
				throw Exception{
					"Hierarchy not allowed in '" + prefix,
					ErrorCodes::BAD_ARGUMENTS
				};

			if (has_hierarchy && hierarchical)
				throw Exception{
					"Only one hierarchical attribute supported",
					ErrorCodes::BAD_ARGUMENTS
				};

			has_hierarchy = has_hierarchy || hierarchical;

			attributes.emplace_back(DictionaryAttribute{
				name, underlying_type, type, expression, null_value, hierarchical, injective
			});
		}

		return attributes;
	}
};

}
