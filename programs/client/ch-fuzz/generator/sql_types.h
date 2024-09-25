#pragma once

#include "random_generator.h"
#include "sql_grammar.pb.h"

#include <cstdint>
#include <string>
#include <optional>
#include <memory>
#include <vector>
#include <tuple>

namespace chfuzz {

const constexpr uint32_t allow_nullable = (1 << 0),
						 allow_dynamic = (1 << 1),
						 allow_nested = (1 << 2),
						 allow_variant = (1 << 3),
						 allow_json = (1 << 4),
						 allow_enum = (1 << 5);

class SQLType {
public:
	virtual const std::string TypeName(const bool escape) = 0;

	virtual ~SQLType() = default;
};

class BoolType : public SQLType {
public:
	const std::string TypeName(const bool escape) override {
		(void) escape;
		return "Bool";
	}
	~BoolType() override = default;
};

class IntType : public SQLType {
public:
	const uint32_t size;
	const bool is_unsigned;
	IntType(const uint32_t s, const bool isu) : size(s), is_unsigned(isu) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		(void) escape;
		ret += is_unsigned ? "U" : "";
		ret += "Int";
		ret += std::to_string(size);
		return ret;
	}
	~IntType() override = default;
};

class FloatType : public SQLType {
public:
	const uint32_t size;
	FloatType(const uint32_t s) : size(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		(void) escape;
		ret += "Float";
		ret += std::to_string(size);
		return ret;
	}
	~FloatType() override = default;
};

class DateType : public SQLType {
public:
	const bool has_time, extended;
	DateType(const bool ht, const bool ex) : has_time(ht), extended(ex) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		(void) escape;
		ret += "Date";
		ret += has_time ? "Time" : "";
		if (extended) {
			ret += has_time ? "64" : "32";
		}
		return ret;
	}
	~DateType() override = default;
};

class DecimalType : public SQLType {
public:
	const std::optional<uint32_t> precision, scale;
	DecimalType(const std::optional<uint32_t> p, const std::optional<uint32_t> s) : precision(p), scale(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		(void) escape;
		ret += "Decimal";
		if (precision.has_value()) {
			ret += "(";
			ret += std::to_string(precision.value());
			if (scale.has_value()) {
				ret += ",";
				ret += std::to_string(scale.value());
			}
			ret += ")";
		}
		return ret;
	}
	~DecimalType() override = default;
};

class StringType : public SQLType {
public:
	std::optional<uint32_t> precision;
	StringType(const std::optional<uint32_t> p) : precision(p) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		(void) escape;
		if (precision.has_value()) {
			ret += "FixedString(";
			ret += std::to_string(precision.value());
			ret += ")";
		} else {
			ret += "String";
		}
		return ret;
	}
	~StringType() override = default;
};

class EnumType : public SQLType {
public:
	const uint32_t size;
	std::vector<int32_t> values;
	EnumType(const uint32_t s, const std::vector<int32_t> v) : size(s), values(v) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Enum";
		ret += std::to_string(size);
		ret += "(";
		for (size_t i = 0 ; i < values.size(); i++) {
			const std::string next = std::to_string(values[i]);

			if (i != 0) {
				ret += ", ";
			}
			if (escape) {
				ret += "\\";
			}
			ret += "'";
			ret += next;
			if (escape) {
				ret += "\\";
			}
			ret += "' = ";
			ret += next;
		}
		ret += ")";
		return ret;
	}
	~EnumType() override = default;
};

class DynamicType : public SQLType {
public:
	std::optional<uint32_t> ntypes;
	DynamicType(const std::optional<uint32_t> n) : ntypes(n) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		(void) escape;
		ret += "Dynamic";
		if (ntypes.has_value()) {
			ret += "(max_types=";
			ret += std::to_string(ntypes.value());
			ret += ")";
		}
		return ret;
	}
	~DynamicType() override = default;
};

class LowCardinality : public SQLType {
public:
	SQLType* subtype;
	LowCardinality(SQLType* s) : subtype(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "LowCardinality(";
		ret += subtype->TypeName(escape);
		ret += ")";
		return ret;
	}
	~LowCardinality() override { delete subtype; }
};


class MapType : public SQLType {
public:
	SQLType* key, *value;
	MapType(SQLType* k, SQLType* v) : key(k), value(v) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Map(";
		ret += key->TypeName(escape);
		ret += ",";
		ret += value->TypeName(escape);
		ret += ")";
		return ret;
	}
	~MapType() override { delete key; delete value; }
};

class ArrayType : public SQLType {
public:
	SQLType* subtype;
	ArrayType(SQLType* s) : subtype(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Array(";
		ret += subtype->TypeName(escape);
		ret += ")";
		return ret;
	}
	~ArrayType() override { delete subtype; }
};

class SubType {
public:
	uint32_t cname;
	SQLType* subtype;

	SubType(uint32_t n, SQLType* s) : cname(n), subtype(s) {}
};

class TupleType : public SQLType {
public:
	std::vector<SubType> subtypes;
	TupleType(std::vector<SubType> s) : subtypes(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Tuple(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += std::to_string(subtypes[i].cname);
			ret += " ";
			ret += subtypes[i].subtype->TypeName(escape);
		}
		ret += ")";
		return ret;
	}
	~TupleType() override {
		for (auto &entry : subtypes) {
			delete entry.subtype;
		}
	}
};

class VariantType : public SQLType {
public:
	std::vector<SQLType*> subtypes;
	VariantType(std::vector<SQLType*> s) : subtypes(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Variant(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += subtypes[i]->TypeName(escape);
		}
		ret += ")";
		return ret;
	}
	~VariantType() override {
		for (auto &entry : subtypes) {
			delete entry;
		}
	}
};

class NestedSubType {
public:
	uint32_t cname;
	SQLType* subtype;
	ArrayType *array_subtype;

	NestedSubType(uint32_t n, SQLType* s) : cname(n), subtype(s) {
		array_subtype = new ArrayType(subtype);
	}
};

class NestedType : public SQLType {
public:
	std::vector<NestedSubType> subtypes;
	NestedType(std::vector<NestedSubType> s) : subtypes(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Nested(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += std::to_string(subtypes[i].cname);
			ret += " ";
			ret += subtypes[i].subtype->TypeName(escape);
		}
		ret += ")";
		return ret;
	}
	~NestedType() override {
		for (auto &entry : subtypes) {
			entry.array_subtype->subtype = nullptr;
			delete entry.array_subtype;
			delete entry.subtype;
		}
	}
};

class JSONType : public SQLType {
public:
	const std::string desc;
	JSONType(const std::string &s) : desc(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "JSON";
		if (escape) {
			for (const auto &c : desc) {
				if (c == '\'') {
					ret += '\\';
				}
				ret += c;
			}
		} else {
			ret += desc;
		}
		return ret;
	}
	~JSONType() override = default;
};

class Nullable : public SQLType {
public:
	SQLType* subtype;
	Nullable(SQLType* s) : subtype(s) {}

	const std::string TypeName(const bool escape) override {
		std::string ret;

		ret += "Nullable(";
		ret += subtype->TypeName(escape);
		ret += ")";
		return ret;
	}
	~Nullable() override { delete subtype; }
};

std::tuple<SQLType*, sql_query_grammar::Integers> RandomIntType(RandomGenerator &rg);
std::tuple<SQLType*, sql_query_grammar::FloatingPoints> RandomFloatType(RandomGenerator &rg);
std::tuple<SQLType*, sql_query_grammar::Dates> RandomDateType(RandomGenerator &rg);

}
