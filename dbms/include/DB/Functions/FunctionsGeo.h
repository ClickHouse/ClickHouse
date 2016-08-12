#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>
#include <ext/range.hpp>
#include <math.h>

#define D_R (M_PI / 180.0)

namespace DB{

const Float64 EARTH_RADIUS_IN_METERS = 6372797.560856;

static inline Float64 deg_rad(Float64 ang) { return ang * D_R; }
static inline Float64 rad_deg(Float64 ang) { return ang / D_R; }
  
class FunctionGeoDistance : public IFunction
{
public:
	
	static constexpr auto name = "distance";
	static FunctionPtr create(const Context &) { return std::make_shared<FunctionGeoDistance>(); }
	
private:

	enum class instr_type : uint8_t
	{
		get_float_64,
		get_const_float_64
	};
  
	using instr_t = std::pair<instr_type, const IColumn *>;
	using instrs_t = std::vector<instr_t>;
  
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
					"Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}

		return std::make_shared<DataTypeFloat64>();
	}
	
	instrs_t getInstructions(const Block & block, const ColumnNumbers & arguments, bool & out_const)
	{
		instrs_t result{};
		result.reserve(arguments.size());

		out_const = true;

		for (const auto arg_pos : arguments)
		{
			const auto column = block.getByPosition(arg_pos).column.get();

			if (const auto col = typeid_cast<const ColumnVector<Float64> *>(column))
			{
				out_const = false;

				result.emplace_back(instr_type::get_float_64, col);
			}
			else if (const auto col = typeid_cast<const ColumnConst<Float64> *>(column))
			{
				out_const = out_const && true;

				result.emplace_back(instr_type::get_const_float_64, col);
			}
			else
				throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		return result;
	}
	
	
	Float64 geohashGetDistance(Float64 lon1d, Float64 lat1d, Float64 lon2d, Float64 lat2d) {
		Float64 lat1r, lon1r, lat2r, lon2r, u, v;
		lat1r = deg_rad(lat1d);
		lon1r = deg_rad(lon1d);
		lat2r = deg_rad(lat2d);
		lon2r = deg_rad(lon2d);
		u = sin((lat2r - lat1r) / 2);
		v = sin((lon2r - lon1r) / 2);
		return 2.0 * EARTH_RADIUS_IN_METERS *
			asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
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

			Float64 res = geohashGetDistance(colLon1, colLat1, colLon2, colLat2);
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
						vals[idx] = static_cast<const ColumnVector<Float64> *>(block.getByPosition(arguments[idx]).column.get())->getData()[row];
					else if (instr_type::get_const_float_64 == instrs[idx].first)
						vals[idx] = static_cast<const ColumnConst<Float64> *>(block.getByPosition(arguments[idx]).column.get())->getData();
					else 
						throw std::logic_error{"unknown instr_type"};
				}
				dst_data[row] = geohashGetDistance(vals[0], vals[1], vals[2], vals[3]);
			}
		}
	}
};
}
