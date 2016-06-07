#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Parsers/CommonParsers.h>
#include <ext/range.hpp>
#include <boost/range/iterator_range_core.hpp>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTLiteral.h>
#include <bitset>
#include <stack>


namespace DB
{

namespace ErrorCodes
{
	extern const int TOO_SLOW;
	extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
	extern const int TOO_MUCH_ARGUMENTS_FOR_FUNCTION;
	extern const int SYNTAX_ERROR;
}

/// helper type for comparing `std::pair`s using solely the .first member
template <template <typename> class Comparator>
struct ComparePairFirst final
{
	template <typename T1, typename T2>
	bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
	{
		return Comparator<T1>{}(lhs.first, rhs.first);
	}
};

struct AggregateFunctionSequenceMatchData final
{
	static constexpr auto max_events = 32;

	using Timestamp = std::uint32_t;
	using Events = std::bitset<max_events>;
	using TimestampEvents = std::pair<Timestamp, Events>;
	using Comparator = ComparePairFirst<std::less>;

	bool sorted = true;
	std::vector<TimestampEvents> eventsList;

	void add(const Timestamp timestamp, const Events & events)
	{
		/// store information exclusively for rows with at least one event
		if (events.any())
		{
			eventsList.emplace_back(timestamp, events);
			sorted = false;
		}
	}

	void merge(const AggregateFunctionSequenceMatchData & other)
	{
		const auto size = eventsList.size();

		eventsList.insert(std::end(eventsList), std::begin(other.eventsList), std::end(other.eventsList));

		/// either sort whole container or do so partially merging ranges afterwards
		if (!sorted && !other.sorted)
			std::sort(std::begin(eventsList), std::end(eventsList), Comparator{});
		else
		{
			const auto begin = std::begin(eventsList);
			const auto middle = std::next(begin, size);
			const auto end = std::end(eventsList);

			if (!sorted)
				std::sort(begin, middle, Comparator{});

			if (!other.sorted)
				std::sort(middle, end, Comparator{});

			std::inplace_merge(begin, middle, end, Comparator{});
		}

		sorted = true;
	}

	void sort()
	{
		if (!sorted)
		{
			std::sort(std::begin(eventsList), std::end(eventsList), Comparator{});
			sorted = true;
		}
	}

	void serialize(WriteBuffer & buf) const
	{
		writeBinary(sorted, buf);
		writeBinary(eventsList.size(), buf);

		for (const auto & events : eventsList)
		{
			writeBinary(events.first, buf);
			writeBinary(events.second.to_ulong(), buf);
		}
	}

	void deserialize(ReadBuffer & buf)
	{
		readBinary(sorted, buf);

		std::size_t size;
		readBinary(size, buf);

		decltype(eventsList) eventsList;
		eventsList.reserve(size);

		for (std::size_t i = 0; i < size; ++i)
		{
			std::uint32_t timestamp;
			readBinary(timestamp, buf);

			unsigned long events;
			readBinary(events, buf);

			eventsList.emplace_back(timestamp, Events{events});
		}

		this->eventsList = std::move(eventsList);
	}
};


/// Max number of iterations to match the pattern against a sequence, exception thrown when exceeded
constexpr auto sequence_match_max_iterations = 1000000;

class AggregateFunctionSequenceMatch : public IAggregateFunctionHelper<AggregateFunctionSequenceMatchData>
{
public:
	static bool sufficientArgs(const std::size_t arg_count) { return arg_count >= 3; }

	String getName() const override { return "sequenceMatch"; }

	DataTypePtr getReturnType() const override { return new DataTypeUInt8; }

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception{
				"Aggregate function " + getName() + " requires exactly one parameter.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		pattern = params.front().safeGet<std::string>();
	}

	void setArguments(const DataTypes & arguments) override
	{
		arg_count = arguments.size();

		if (!sufficientArgs(arg_count))
			throw Exception{
				"Aggregate function " + getName() + " requires at least 3 arguments.",
				ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION
			};

		if (arg_count - 1 > Data::max_events)
			throw Exception{
				"Aggregate function " + getName() + " supports up to " +
					std::to_string(Data::max_events) + " event arguments.",
				ErrorCodes::TOO_MUCH_ARGUMENTS_FOR_FUNCTION
			};

		const auto time_arg = arguments.front().get();
		if (!typeid_cast<const DataTypeDateTime *>(time_arg))
			throw Exception{
				"Illegal type " + time_arg->getName() + " of first argument of aggregate function " +
					getName() + ", must be DateTime",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};

		for (const auto i : ext::range(1, arg_count))
		{
			const auto cond_arg = arguments[i].get();
			if (!typeid_cast<const DataTypeUInt8 *>(cond_arg))
				throw Exception{
					"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) +
						" of aggregate function " + getName() + ", must be UInt8",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
				};
		}

		parsePattern();
	}

	void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num) const override
	{
		const auto timestamp = static_cast<const ColumnUInt32 *>(columns[0])->getData()[row_num];

		Data::Events events;
		for (const auto i : ext::range(1, arg_count))
		{
			const auto event = static_cast<const ColumnUInt8 *>(columns[i])->getData()[row_num];
			events.set(i - 1, event);
		}

		data(place).add(timestamp, events);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		data(place).merge(data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		data(place).serialize(buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		data(place).deserialize(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		const_cast<Data &>(data(place)).sort();

		const auto & data_ref = data(place);

		const auto events_begin = std::begin(data_ref.eventsList);
		const auto events_end = std::end(data_ref.eventsList);
		auto events_it = events_begin;

		static_cast<ColumnUInt8 &>(to).getData().push_back(match(events_it, events_end));
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num)
	{
		return static_cast<const AggregateFunctionSequenceMatch &>(*that).add(place, columns, row_num);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }

private:
	enum class PatternActionType
	{
		SpecificEvent,
		AnyEvent,
		KleeneStar,
		TimeLessOrEqual,
		TimeLess,
		TimeGreaterOrEqual,
		TimeGreater
	};

	struct PatternAction final
	{
		PatternActionType type;
		std::uint32_t extra;

		PatternAction() = default;
		PatternAction(const PatternActionType type, const std::uint32_t extra = 0) : type{type}, extra{extra} {}
	};

	using PatternActions = std::vector<PatternAction>;


	void parsePattern()
	{
		PatternActions actions{
			{ PatternActionType::KleeneStar }
		};

		ParserString special_open_p("(?");
		ParserString special_close_p(")");
		ParserString t_p("t");
		ParserString less_or_equal_p("<=");
		ParserString less_p("<");
		ParserString greater_or_equal_p(">=");
		ParserString greater_p(">");
		ParserString dot_closure_p(".*");
		ParserString dot_p(".");
		ParserNumber number_p;

		auto pos = pattern.data();
		const auto begin = pos;
		const auto end = pos + pattern.size();

		ASTPtr node;
		decltype(pos) max_parsed_pos{};
		Expected expected;

		const auto throw_exception = [&] (const std::string & msg) {
			throw Exception{
				msg + " '" + std::string(pos, end) + "' at position " + std::to_string(pos - begin),
				ErrorCodes::SYNTAX_ERROR
			};
		};

		while (pos < end)
		{
			if (special_open_p.ignore(pos, end))
			{
				if (t_p.ignore(pos, end))
				{
					PatternActionType type;

					if (less_or_equal_p.ignore(pos, end))
						type = PatternActionType::TimeLessOrEqual;
					else if (less_p.ignore(pos, end))
						type = PatternActionType::TimeLess;
					else if (greater_or_equal_p.ignore(pos, end))
						type = PatternActionType::TimeGreaterOrEqual;
					else if (greater_p.ignore(pos, end))
						type = PatternActionType::TimeGreater;
					else
						throw_exception("Unknown time condition");

					if (!number_p.parse(pos, end, node, max_parsed_pos, expected))
						throw_exception("Could not parse number");

					if (actions.back().type != PatternActionType::SpecificEvent &&
						actions.back().type != PatternActionType::AnyEvent &&
						actions.back().type != PatternActionType::KleeneStar)
						throw Exception{
							"Temporal condition should be preceeded by an event condition",
							ErrorCodes::BAD_ARGUMENTS
						};

					actions.emplace_back(type, typeid_cast<const ASTLiteral &>(*node).value.safeGet<UInt64>());
				}
				else if (number_p.parse(pos, end, node, max_parsed_pos, expected))
				{
					const auto event_number = typeid_cast<const ASTLiteral &>(*node).value.safeGet<UInt64>();
					if (event_number > arg_count - 1)
						throw Exception{
							"Event number " + std::to_string(event_number) + " is out of range",
							ErrorCodes::BAD_ARGUMENTS
						};

					actions.emplace_back(PatternActionType::SpecificEvent, event_number - 1);
				}
				else
					throw_exception("Unexpected special sequence");

				if (!special_close_p.ignore(pos, end))
					throw_exception("Expected closing parenthesis, found");

			}
			else if (dot_closure_p.ignore(pos, end))
				actions.emplace_back(PatternActionType::KleeneStar);
			else if (dot_p.ignore(pos, end))
				actions.emplace_back(PatternActionType::AnyEvent);
			else
				throw_exception("Could not parse pattern, unexpected starting symbol");
		}

		this->actions = std::move(actions);
	}

protected:
	template <typename T>
	bool match(T & events_it, const T events_end) const
	{
		const auto action_begin = std::begin(actions);
		const auto action_end = std::end(actions);
		auto action_it = action_begin;

		const auto events_begin = events_it;
		auto base_it = events_it;

		/// an iterator to action plus an iterator to row in events list plus timestamp at the start of sequence
		using backtrack_info = std::tuple<decltype(action_it), T, T>;
		std::stack<backtrack_info> back_stack;

		/// backtrack if possible
		const auto do_backtrack = [&] {
			while (!back_stack.empty())
			{
				auto & top = back_stack.top();

				action_it = std::get<0>(top);
				events_it = std::next(std::get<1>(top));
				base_it = std::get<2>(top);

				back_stack.pop();

				if (events_it != events_end)
					return true;
			}

			return false;
		};

		std::size_t i = 0;
		while (action_it != action_end && events_it != events_end)
		{
			if (action_it->type == PatternActionType::SpecificEvent)
			{
				if (events_it->second.test(action_it->extra))
				{
					/// move to the next action and events
					base_it = events_it;
					++action_it, ++events_it;
				}
				else if (!do_backtrack())
					/// backtracking failed, bail out
					break;
			}
			else if (action_it->type == PatternActionType::AnyEvent)
			{
				base_it = events_it;
				++action_it, ++events_it;
			}
			else if (action_it->type == PatternActionType::KleeneStar)
			{
				back_stack.emplace(action_it, events_it, base_it);
				base_it = events_it;
				++action_it;
			}
			else if (action_it->type == PatternActionType::TimeLessOrEqual)
			{
				if (events_it->first - base_it->first <= action_it->extra)
				{
					/// condition satisfied, move onto next action
					back_stack.emplace(action_it, events_it, base_it);
					base_it = events_it;
					++action_it;
				}
				else if (!do_backtrack())
					break;
			}
			else if (action_it->type == PatternActionType::TimeLess)
			{
				if (events_it->first - base_it->first < action_it->extra)
				{
					back_stack.emplace(action_it, events_it, base_it);
					base_it = events_it;
					++action_it;
				}
				else if (!do_backtrack())
					break;
			}
			else if (action_it->type == PatternActionType::TimeGreaterOrEqual)
			{
				if (events_it->first - base_it->first >= action_it->extra)
				{
					back_stack.emplace(action_it, events_it, base_it);
					base_it = events_it;
					++action_it;
				}
				else if (++events_it == events_end && !do_backtrack())
					break;
			}
			else if (action_it->type == PatternActionType::TimeGreater)
			{
				if (events_it->first - base_it->first > action_it->extra)
				{
					back_stack.emplace(action_it, events_it, base_it);
					base_it = events_it;
					++action_it;
				}
				else if (++events_it == events_end && !do_backtrack())
					break;
			}
			else
				throw Exception{
					"Unknown PatternActionType",
					ErrorCodes::LOGICAL_ERROR
				};

			if (++i > sequence_match_max_iterations)
				throw Exception{
					"Pattern application proves too difficult, exceeding max iterations (" + toString(sequence_match_max_iterations) + ")",
					ErrorCodes::TOO_SLOW
				};
		}

		/// if there are some actions remaining
		if (action_it != action_end)
		{
			/// match multiple empty strings at end
			while (action_it->type == PatternActionType::KleeneStar ||
				   action_it->type == PatternActionType::TimeLessOrEqual ||
				   action_it->type == PatternActionType::TimeLess ||
				   (action_it->type == PatternActionType::TimeGreaterOrEqual && action_it->extra == 0))
				++action_it;
		}

		if (events_it == events_begin)
			++events_it;

		return action_it == action_end;
	}

private:
	std::string pattern;
	std::size_t arg_count;
	PatternActions actions;
};

class AggregateFunctionSequenceCount final : public AggregateFunctionSequenceMatch
{
public:
	String getName() const override { return "sequenceCount"; }

	DataTypePtr getReturnType() const override { return new DataTypeUInt64; }

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		const_cast<Data &>(data(place)).sort();
		static_cast<ColumnUInt64 &>(to).getData().push_back(count(place));
	}

private:
	UInt64 count(const ConstAggregateDataPtr & place) const
	{
		const auto & data_ref = data(place);

		const auto events_begin = std::begin(data_ref.eventsList);
		const auto events_end = std::end(data_ref.eventsList);
		auto events_it = events_begin;

		std::size_t count = 0;
		while (events_it != events_end && match(events_it, events_end))
			++count;

		return count;
	}
};

}
