#pragma once

#include "random_generator.h"
#include "buzz-house/ast/sql_grammar.pb.h"

#include <optional>
#include <memory>

namespace buzzhouse {

const constexpr uint32_t allow_nullable = (1 << 0),
						 allow_dynamic = (1 << 1),
						 allow_nested = (1 << 2),
						 allow_variant = (1 << 3),
						 allow_json = (1 << 4),
						 allow_enum = (1 << 5);

class SQLType {
public:
	virtual const std::string TypeName(const bool escape) const = 0;

	virtual ~SQLType() = default;
};

const SQLType* TypeDeepCopy(const SQLType *tp);

class BoolType : public SQLType {
public:
	const std::string TypeName(const bool escape) const override {
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

	const std::string TypeName(const bool escape) const override {
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

	const std::string TypeName(const bool escape) const override {
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

	const std::string TypeName(const bool escape) const override {
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
	const std::optional<const uint32_t> precision, scale;
	DecimalType(const std::optional<const uint32_t> p, const std::optional<const uint32_t> s) : precision(p), scale(s) {}

	const std::string TypeName(const bool escape) const override {
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
	const std::optional<const uint32_t> precision;
	StringType(const std::optional<const uint32_t> p) : precision(p) {}

	const std::string TypeName(const bool escape) const override {
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

class UUIDType : public SQLType {
public:
	const std::string TypeName(const bool escape) const override {
		(void) escape;
		return "UUID";
	}
	~UUIDType() override = default;
};

class EnumValue {
public:
	const std::string val;
	const int32_t number;

	EnumValue(const std::string v, const int32_t n) : val(v), number(n) {}
};

class EnumType : public SQLType {
public:
	const uint32_t size;
	const std::vector<const EnumValue> values;
	EnumType(const uint32_t s, const std::vector<const EnumValue> v) : size(s), values(v) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "Enum";
		ret += std::to_string(size);
		ret += "(";
		for (size_t i = 0 ; i < values.size(); i++) {
			const EnumValue &v = values[i];

			if (i != 0) {
				ret += ", ";
			}
			for (const auto &c : v.val) {
				if (escape && c == '\'') {
					ret += "\\";
				}
				ret += v.val;
			}
			ret += " = ";
			ret += std::to_string(v.number);
		}
		ret += ")";
		return ret;
	}
	~EnumType() override = default;
};

class DynamicType : public SQLType {
public:
	const std::optional<const uint32_t> ntypes;
	DynamicType(const std::optional<const uint32_t> n) : ntypes(n) {}

	const std::string TypeName(const bool escape) const override {
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

class JSONType : public SQLType {
public:
	const std::string desc;
	JSONType(const std::string &s) : desc(s) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "JSON";
		for (const auto &c : desc) {
			if (escape && c == '\'') {
				ret += '\\';
			}
			ret += c;
		}
		return ret;
	}
	~JSONType() override = default;
};

class Nullable : public SQLType {
public:
	const SQLType* subtype;
	Nullable(const SQLType* s) : subtype(s) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "Nullable(";
		ret += subtype->TypeName(escape);
		ret += ")";
		return ret;
	}
	~Nullable() override { delete subtype; }
};

class LowCardinality : public SQLType {
public:
	const SQLType* subtype;
	LowCardinality(const SQLType* s) : subtype(s) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "LowCardinality(";
		ret += subtype->TypeName(escape);
		ret += ")";
		return ret;
	}
	~LowCardinality() override { delete subtype; }
};

class ArrayType : public SQLType {
public:
	const SQLType* subtype;
	ArrayType(const SQLType* s) : subtype(s) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "Array(";
		ret += subtype->TypeName(escape);
		ret += ")";
		return ret;
	}
	~ArrayType() override { delete subtype; }
};

class MapType : public SQLType {
public:
	const SQLType* key, *value;
	MapType(const SQLType* k, const SQLType* v) : key(k), value(v) {}

	const std::string TypeName(const bool escape) const override {
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

class SubType {
public:
	const uint32_t cname;
	const SQLType* subtype;

	SubType(const uint32_t n, const SQLType* s) : cname(n), subtype(s) {}
};

class TupleType : public SQLType {
public:
	const std::vector<const SubType> subtypes;
	TupleType(const std::vector<const SubType> s) : subtypes(s) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "Tuple(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += "c";
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
	const std::vector<const SQLType*> subtypes;
	VariantType(const std::vector<const SQLType*> s) : subtypes(s) {}

	const std::string TypeName(const bool escape) const override {
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
	const uint32_t cname;
	const SQLType* subtype;
	const ArrayType *array_subtype;

	NestedSubType(const uint32_t n, const SQLType* s) :
		cname(n), subtype(s), array_subtype(new ArrayType(TypeDeepCopy(s))) {}
};

class NestedType : public SQLType {
public:
	const std::vector<const NestedSubType> subtypes;
	NestedType(const std::vector<const NestedSubType> s) : subtypes(s) {}

	const std::string TypeName(const bool escape) const override {
		std::string ret;

		ret += "Nested(";
		for (uint32_t i = 0 ; i < subtypes.size(); i++) {
			if (i != 0) {
				ret += ",";
			}
			ret += "c";
			ret += std::to_string(subtypes[i].cname);
			ret += " ";
			ret += subtypes[i].subtype->TypeName(escape);
		}
		ret += ")";
		return ret;
	}
	~NestedType() override {
		for (auto &entry : subtypes) {
			delete entry.array_subtype;
			delete entry.subtype;
		}
	}
};

template<typename T>
bool HasType(const SQLType *tp) {
	const Nullable *nl;
	const LowCardinality *lc;
	const ArrayType *at;

	if (dynamic_cast<const T*>(tp)) {
		return true;
	} else if ((nl = dynamic_cast<const Nullable*>(tp))) {
		return HasType<T>(nl->subtype);
 	} else if ((lc = dynamic_cast<const LowCardinality*>(tp))) {
		return HasType<T>(lc->subtype);
	} else if ((at = dynamic_cast<const ArrayType*>(tp))) {
		return HasType<T>(at->subtype);
	}
	return false;
}

std::tuple<const SQLType*, sql_query_grammar::Integers> RandomIntType(RandomGenerator &rg);
std::tuple<const SQLType*, sql_query_grammar::FloatingPoints> RandomFloatType(RandomGenerator &rg);
std::tuple<const SQLType*, sql_query_grammar::Dates> RandomDateType(RandomGenerator &rg, const bool low_card);

}
