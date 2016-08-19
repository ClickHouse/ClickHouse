#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>
#include <ext/range.hpp>
#include <math.h>

#define DEGREES_IN_RADIANS (M_PI / 180.0)

namespace DB
{

namespace ErrorCodes
{
	extern const int ARGUMENT_OUT_OF_BOUND;
}

const Float64 EARTH_RADIUS_IN_METERS = 6372797.560856;

static inline Float64 degToRad(Float64 angle) { return angle * DEGREES_IN_RADIANS; }
static inline Float64 radToDeg(Float64 angle) { return angle / DEGREES_IN_RADIANS; }
  
/**
 *  The function calculates distance in meters between two points on Earth specified by longitude and latitude in degrees.
 *  The function uses great circle distance formula https://en.wikipedia.org/wiki/Great-circle_distance. 
 *  Throws exception when one or several input values are not within reasonable bounds. 
 *  Latitude must be in [-90, 90], longitude must be [-180, 180]
 * 
 */
class FunctionGreatCircleDistance : public IFunction
{
public:
	
	static constexpr auto name = "greatCircleDistance";
	static FunctionPtr create(const Context &) { return std::make_shared<FunctionGreatCircleDistance>(); }
	
private:

	enum class instr_type : uint8_t
	{
		get_float_64,
		get_const_float_64
	};

	using instr_t = std::pair<instr_type, const IColumn *>;
	using instrs_t = std::array<instr_t, 4>;

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 4)
			throw Exception(
				"Number of arguments for function " + getName() + "doesn't match: passed "
					+ toString(arguments.size()) + ", should be 4",
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		for (const auto arg_idx : ext::range(0, arguments.size()))
		{
			const auto arg = arguments[arg_idx].get();
			if (!typeid_cast<const DataTypeFloat64 *>(arg))
				throw Exception(
					"Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}

		return std::make_shared<DataTypeFloat64>();
	}

	instrs_t getInstructions(const Block & block, const ColumnNumbers & arguments, bool & out_const)
	{
		instrs_t result;
		out_const = true;

		for (const auto arg_idx : ext::range(0, arguments.size()))
		{
			const auto column = block.getByPosition(arguments[arg_idx]).column.get();

			if (const auto col = typeid_cast<const ColumnVector<Float64> *>(column))
			{
				out_const = false;
				result[arg_idx] = instr_t{instr_type::get_float_64, col};
			}
			else if (const auto col = typeid_cast<const ColumnConst<Float64> *>(column))
			{
				result[arg_idx] = instr_t{instr_type::get_const_float_64, col};
			}
			else
				throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		return result;
	}

	/// https://en.wikipedia.org/wiki/Great-circle_distance 
	Float64 greatCircleDistance(Float64 lon1Deg, Float64 lat1Deg, Float64 lon2Deg, Float64 lat2Deg)
	{
		if (lon1Deg < -180 || lon1Deg > 180 ||
			lon2Deg < -180 || lon2Deg > 180 ||
			lat1Deg < -90 || lat1Deg > 90 ||
			lat2Deg < -90 || lat2Deg > 90)
		{
			throw Exception("Arguments values out of bounds for function " + getName(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}

		Float64 lon1Rad = degToRad(lon1Deg);
		Float64 lat1Rad = degToRad(lat1Deg);
		Float64 lon2Rad = degToRad(lon2Deg);
		Float64 lat2Rad = degToRad(lat2Deg);
		Float64 u = sin((lat2Rad - lat1Rad) / 2);
		Float64 v = sin((lon2Rad - lon1Rad) / 2);
		return 2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(u * u + cos(lat1Rad) * cos(lat2Rad) * v * v));
	}


	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto size = block.rowsInFirstColumn();

		bool result_is_const{};
		auto instrs = getInstructions(block, arguments, result_is_const);

		if (result_is_const)
		{
			const auto & colLon1 = static_cast<const ColumnConst<Float64> *>(block.getByPosition(arguments[0]).column.get())->getData();
			const auto & colLat1 = static_cast<const ColumnConst<Float64> *>(block.getByPosition(arguments[1]).column.get())->getData();
			const auto & colLon2 = static_cast<const ColumnConst<Float64> *>(block.getByPosition(arguments[2]).column.get())->getData();
			const auto & colLat2 = static_cast<const ColumnConst<Float64> *>(block.getByPosition(arguments[3]).column.get())->getData();

			Float64 res = greatCircleDistance(colLon1, colLat1, colLon2, colLat2);
			block.getByPosition(result).column = std::make_shared<ColumnConst<Float64>>(size, res);
		}
		else 
		{
			const auto dst = std::make_shared<ColumnVector<Float64>>();
			block.getByPosition(result).column = dst;
			auto & dst_data = dst->getData();
			dst_data.resize(size);
			Float64 vals[instrs.size()];
			for (const auto row : ext::range(0, size))
			{
				for (const auto idx : ext::range(0, instrs.size()))
				{
					if (instr_type::get_float_64 == instrs[idx].first)
						vals[idx] = static_cast<const ColumnVector<Float64> *>(instrs[idx].second)->getData()[row];
					else if (instr_type::get_const_float_64 == instrs[idx].first)
						vals[idx] = static_cast<const ColumnConst<Float64> *>(instrs[idx].second)->getData();
					else 
						throw std::logic_error{"unknown instr_type"};
				}
				dst_data[row] = greatCircleDistance(vals[0], vals[1], vals[2], vals[3]);
			}
		}
	}
};
}

#undef DEGREES_IN_RADIANS
