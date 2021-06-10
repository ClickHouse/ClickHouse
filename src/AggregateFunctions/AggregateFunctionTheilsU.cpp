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


    struct TheilsUData : public AggregateFunctionCramersVData
{
    Float64 get_result() const
    {
        if (cur_size < 2){
            throw Exception("Aggregate function theil's u requires at least 2 values in columns", ErrorCodes::BAD_ARGUMENTS);
        }
        Float64 h_x = 0.0;
        for (const auto & cell : n_i) {
            UInt64 count_x_tmp = cell.getMapped();
            Float64 count_x = Float64(count_x_tmp);
            h_x += (count_x / cur_size) * (log(count_x / cur_size));
        }


        Float64 dep = 0.0;
        for (const auto & cell : pairs) {
            UInt128 hash_pair = cell.getKey();
            UInt64 count_of_pair_tmp = cell.getMapped();
            Float64 count_of_pair = Float64(count_of_pair_tmp);

            UInt64 hash2 = (hash_pair >> 64);

            UInt64 count2_tmp = n_j.find(hash2)->getMapped();
            Float64 count2 = Float64 (count2_tmp);

            dep += (count_of_pair / cur_size) * log(count_of_pair / count2);
        }

        dep -= h_x;
        dep /= h_x;
        return dep;
    }
};


AggregateFunctionPtr createAggregateFunctionTheilsU(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    return std::make_shared<AggregateFunctionCramersV<TheilsUData>>(argument_types);
}

}

void registerAggregateFunctionTheilsU(AggregateFunctionFactory & factory)
{
    factory.registerFunction("TheilsU", createAggregateFunctionTheilsU);
}

}
