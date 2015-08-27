#pragma once

#include <array>

namespace DB
{

struct UniqCombinedBiasData
{
	using InterpolatedData = std::array<double, 178>;

	static double getThreshold();
	static const InterpolatedData & getRawEstimates();
	static const InterpolatedData & getBiases();
};

}