#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCramersV.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include "registerAggregateFunctions.h"
#include <memory>

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{
namespace
{


struct ContingencyData : public AggregateFunctionCramersVData
{
    Float64 get_result() const
    {
        if (cur_size < 2){
            throw Exception("Aggregate function contingency coefficient requires at least 2 values in columns", ErrorCodes::BAD_ARGUMENTS);
        }
        Float64 phi = 0.0;
        for (const auto & cell : pairs) {
            UInt128 hash_pair = cell.getKey();
            UInt64 count_of_pair_tmp = cell.getMapped();
            Float64 count_of_pair = Float64(count_of_pair_tmp);
            UInt64 hash1 = (hash_pair << 64 >> 64);
            UInt64 hash2 = (hash_pair >> 64);

            UInt64 count1_tmp = n_i.find(hash1)->getMapped();
            UInt64 count2_tmp = n_j.find(hash2)->getMapped();
            Float64 count1 = static_cast<Float64>(count1_tmp);
            Float64 count2 = Float64(count2_tmp);

            phi += ((count_of_pair * count_of_pair / (count1 * count2) * cur_size)
                    - 2 * count_of_pair + (count1 * count2 / cur_size));
        }
        phi /= cur_size;
        return sqrt(phi / (phi + cur_size));
    }
};


AggregateFunctionPtr createAggregateFunctionContingencyCoefficient(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    return std::make_shared<AggregateFunctionCramersV<ContingencyData>>(argument_types);
}

}

void registerAggregateFunctionContingencyCoefficient(AggregateFunctionFactory & factory)
{
    factory.registerFunction("ContingencyCoefficient", createAggregateFunctionContingencyCoefficient);
}

}
