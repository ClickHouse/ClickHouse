#pragma once

#include <algorithm>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>
#include <Core/UatraitsFast/uatraits-fast.h>
#include <common/StringRef.h>


bool operator>=(const StringRef & lhs, const StringRef & rhs)
{
    return lhs == rhs || lhs > rhs;
}

bool operator<=(const StringRef & lhs, const StringRef & rhs)
{
    return lhs == rhs || lhs < rhs;
}

template <typename Traits>
class BasicCondition
{
public:
	virtual bool match(Traits & traits) const = 0;
	virtual ~BasicCondition() = default;
};

template <typename Traits, template<typename> class Comparator, typename ValueType>
class Condition : public BasicCondition<Traits> {
public:
    Condition(std::string const & parameter, ValueType const & value)
		: condition_value(value)
	{
		if constexpr (std::is_same_v<ValueType, UATraits::Version>)
		{
			bool field_found = false;
			for (std::size_t i = 0; i < Traits::VersionFields::VersionFieldsCount; ++i)
				if (std::string_view(Traits::versionFieldNames()[i]) == parameter)
				{
					field_index = i;
					field_found = true;
				}
			if (!field_found)
				throw TWithBackTrace<yexception>() << "Couldn'd find property name: " << parameter << " for condition";
		}
		else if constexpr (std::is_same_v<ValueType, StringRef>)
		{
			/// Сохраним данные для StringRef.
			data = condition_value.toString();

			condition_value = ValueType{data};

			bool field_found = false;
			for (std::size_t i = 0; i < Traits::StringRefFields::StringRefFieldsCount; ++i)
				if (std::string_view(Traits::stringRefFieldNames()[i]) == parameter)
				{
					field_index = i;
					field_found = true;
				}
			if (!field_found)
				throw TWithBackTrace<yexception>() << "Couldn'd find property name: " << parameter << " for condition";
		}
	}

	bool match(Traits & traits) const override;

private:
	std::size_t field_index = 0;
	std::string data;
	ValueType condition_value;
};

template <typename Traits, template<typename> class Comparator, typename ValueType>
bool Condition<Traits, Comparator, ValueType>::match(Traits & traits) const
{
	if constexpr (std::is_same_v<ValueType, UATraits::Version>)
	{
		return Comparator<ValueType>()(traits.version_fields[field_index], condition_value);
	}
	if constexpr (std::is_same_v<ValueType, StringRef>)
	{
		return Comparator<ValueType>()(traits.string_ref_fields[field_index], condition_value);
	}
	return false;
}

template <typename Traits>
class BaseGroupCondition : public BasicCondition<Traits>
{
public:
	using ConditionPointer = std::shared_ptr<BasicCondition<Traits>>;

	virtual bool match(Traits & traits) const = 0;
	virtual void addCondition(ConditionPointer const & condition);

protected:
	std::vector<ConditionPointer> conditions;
};

template <typename Traits>
inline void BaseGroupCondition<Traits>::addCondition(ConditionPointer const & condition)
{
	conditions.push_back(condition);
}

class AndType {};
class OrType {};

template <typename Traits, class Type>
class GroupCondition : public BaseGroupCondition<Traits>
{
};

template <typename Traits>
class GroupCondition<Traits, AndType> : public BaseGroupCondition<Traits>
{
public:
	bool match(Traits & traits) const override
	{
		for (const auto & condition : BaseGroupCondition<Traits>::conditions)
			if (!condition->match(traits))
				return false;

		return true;
	}
};

template <typename Traits>
class GroupCondition<Traits, OrType> : public BaseGroupCondition<Traits>
{
public:
	bool match(Traits & traits) const
	{
		for (const auto & condition : BaseGroupCondition<Traits>::conditions)
			if (condition->match(traits))
				return true;

		return false;
	}
};

template <typename Traits>
class Rule {
public:
	using ConditionPointer = std::shared_ptr<BasicCondition<Traits>>;

	Rule(std::string const & name, std::string const & value, ConditionPointer const & condition);
	virtual ~Rule() = default;

	void trigger(Traits & traits) const;

private:
	std::string name;
	std::string value;
	std::size_t field_index = 0;
	ConditionPointer condition;
};

template <typename Traits>
Rule<Traits>::Rule(std::string const & name, std::string const & value, ConditionPointer const & condition) :
	name(name), value(value), condition(condition)
{
	bool field_found = false;
	for (std::size_t i = 0; i < Traits::BoolFields::BoolFieldsCount; ++i)
		if (std::string_view(Traits::boolFieldNames()[i]) == name)
		{
			field_index = i;
			field_found = true;
		}
	if (!field_found)
		throw TWithBackTrace<yexception>() << "Couldn't find field property for rule: " << name;
}

template <typename Traits>
void Rule<Traits>::trigger(Traits & traits) const
{
	if (condition->match(traits))
		traits.bool_fields[field_index] = true;
}

template <typename Traits>
class RootRule
{
public:
    using RulePointer = std::shared_ptr<Rule<Traits>>;
	void addRule(RulePointer const & rule);
	void trigger(Traits & traits) const;

private:
	std::vector<RulePointer> rules;
};

template <typename Traits>
void RootRule<Traits>::addRule(RulePointer const & rule)
{
	rules.push_back(rule);
}

template <typename Traits> void
RootRule<Traits>::trigger(Traits & traits) const {
	for (const auto & rule : rules)
		rule->trigger(traits);
}
