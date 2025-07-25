#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/PODArray_fwd.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace DB
{

struct Settings;

namespace
{

struct KolmogorovSmirnov : public StatisticalSample<Float64, Float64>
{
    enum class Alternative : uint8_t
    {
        TwoSided,
        Less,
        Greater
    };

    std::pair<Float64, Float64> getResult(Alternative alternative, String method)
    {
        ::sort(x.begin(), x.end());
        ::sort(y.begin(), y.end());

        Float64 max_s = std::numeric_limits<Float64>::min();
        Float64 min_s = std::numeric_limits<Float64>::max();
        Float64 now_s = 0;
        UInt64 pos_x = 0;
        UInt64 pos_y = 0;
        UInt64 pos_tmp;
        UInt64 n1 = x.size();
        UInt64 n2 = y.size();

        const Float64 n1_d = 1. / n1;
        const Float64 n2_d = 1. / n2;
        const Float64 tol = 1e-7;

        // reference: https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
        while (pos_x < x.size() && pos_y < y.size())
        {
            if (likely(fabs(x[pos_x] - y[pos_y]) >= tol))
            {
                if (x[pos_x] < y[pos_y])
                {
                    now_s += n1_d;
                    ++pos_x;
                }
                else
                {
                    now_s -= n2_d;
                    ++pos_y;
                }
            }
            else
            {
                pos_tmp = pos_x + 1;
                while (pos_tmp < x.size() && unlikely(fabs(x[pos_tmp] - x[pos_x]) <= tol))
                    pos_tmp++;
                now_s += n1_d * (pos_tmp - pos_x);
                pos_x = pos_tmp;
                pos_tmp = pos_y + 1;
                while (pos_tmp < y.size() && unlikely(fabs(y[pos_tmp] - y[pos_y]) <= tol))
                    pos_tmp++;
                now_s -= n2_d * (pos_tmp - pos_y);
                pos_y = pos_tmp;
            }
            max_s = std::max(max_s, now_s);
            min_s = std::min(min_s, now_s);
        }
        now_s += n1_d * (x.size() - pos_x) - n2_d * (y.size() - pos_y);
        min_s = std::min(min_s, now_s);
        max_s = std::max(max_s, now_s);

        Float64 d = 0;
        if (alternative == Alternative::TwoSided)
            d = std::max(std::abs(max_s), std::abs(min_s));
        else if (alternative == Alternative::Less)
            d = -min_s;
        else if (alternative == Alternative::Greater)
            d = max_s;

        UInt64 g = std::__gcd(n1, n2);
        UInt64 nx_g = n1 / g;
        UInt64 ny_g = n2 / g;

        if (method == "auto")
            method = std::max(n1, n2) <= 10000 ? "exact" : "asymptotic";
        else if (method == "exact" && nx_g >= std::numeric_limits<Int32>::max() / ny_g)
            method = "asymptotic";

        Float64 p_value = std::numeric_limits<Float64>::infinity();

        if (method == "exact")
        {
            /* reference:
             * Gunar Schröer and Dietrich Trenkler
             * Exact and Randomization Distributions of Kolmogorov-Smirnov, Tests for Two or Three Samples
             *
             * and
             *
             * Thomas Viehmann
             * Numerically more stable computation of the p-values for the two-sample Kolmogorov-Smirnov test
             */
            if (n2 > n1)
                std::swap(n1, n2);

            const Float64 f_n1 = static_cast<Float64>(n1);
            const Float64 f_n2 = static_cast<Float64>(n2);
            const Float64 k_d = (0.5 + floor(d * f_n2 * f_n1 - tol)) / (f_n2 * f_n1);
            PaddedPODArray<Float64> c(n1 + 1);

            auto check = alternative == Alternative::TwoSided ?
                         [](const Float64 & q, const Float64 & r, const Float64 & s) { return fabs(r - s) >= q; }
                       : [](const Float64 & q, const Float64 & r, const Float64 & s) { return r - s >= q; };

            c[0] = 0;
            for (UInt64 j = 1; j <= n1; j++)
                if (check(k_d, 0., j / f_n1))
                    c[j] = 1.;
                else
                    c[j] = c[j - 1];

            for (UInt64 i = 1; i <= n2; i++)
            {
                if (check(k_d, i / f_n2, 0.))
                    c[0] = 1.;
                for (UInt64 j = 1; j <= n1; j++)
                    if (check(k_d, i / f_n2, j / f_n1))
                        c[j] = 1.;
                    else
                    {
                        Float64 v = i / static_cast<Float64>(i + j);
                        Float64 w = j / static_cast<Float64>(i + j);
                        c[j] = v * c[j] + w * c[j - 1];
                    }
            }
            p_value = c[n1];
        }
        else if (method == "asymp" || method == "asymptotic")
        {
            Float64 n = std::min(n1, n2);
            Float64 m = std::max(n1, n2);
            Float64 p = sqrt((n * m) / (n + m)) * d;

            if (alternative == Alternative::TwoSided)
            {
                /* reference:
                 * J.DURBIN
                 * Distribution theory for tests based on the sample distribution function
                 */
                Float64 new_val, old_val, s, w, z;
                UInt64 k_max = static_cast<UInt64>(sqrt(2 - log(tol)));

                if (p < 1)
                {
                    z = - (M_PI_2 * M_PI_4) / (p * p);
                    w = log(p);
                    s = 0;
                    for (UInt64 k = 1; k < k_max; k += 2)
                        s += exp(k * k * z - w);
                    p = s / 0.398942280401432677939946059934;
                }
                else
                {
                    z = -2 * p * p;
                    s = -1;
                    UInt64 k = 1;
                    old_val = 0;
                    new_val = 1;
                    while (fabs(old_val - new_val) > tol)
                    {
                        old_val = new_val;
                        new_val += 2 * s * exp(z * k * k);
                        s *= -1;
                        k++;
                    }
                    p = new_val;
                }
                p_value = 1 - p;
            }
            else
            {
                /* reference:
                 * J. L. HODGES, Jr
                 * The significance probability of the Smirnov two-sample test
                 */

                // Use Hodges' suggested approximation Eqn 5.3
                // Requires m to be the larger of (n1, n2)
                Float64 expt = -2 * p * p - 2 * p * (m + 2 * n) / sqrt(m * n * (m + n)) / 3.0;
                p_value = exp(expt);
            }
        }
        return {d, p_value};
    }

};

class AggregateFunctionKolmogorovSmirnov final:
    public IAggregateFunctionDataHelper<KolmogorovSmirnov, AggregateFunctionKolmogorovSmirnov>
{
private:
    using Alternative = typename KolmogorovSmirnov::Alternative;
    Alternative alternative = Alternative::TwoSided;
    String method = "auto";

public:
    explicit AggregateFunctionKolmogorovSmirnov(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<KolmogorovSmirnov, AggregateFunctionKolmogorovSmirnov> ({arguments}, {}, createResultType())
    {
        if (params.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} require two parameter or less", getName());

        if (params.empty())
            return;

        if (params[0].getType() != Field::Types::String)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require first parameter to be a String", getName());

        const auto & param = params[0].safeGet<String>();
        if (param == "two-sided")
            alternative = Alternative::TwoSided;
        else if (param == "less")
            alternative = Alternative::Less;
        else if (param == "greater")
            alternative = Alternative::Greater;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown parameter in aggregate function {}. "
                    "It must be one of: 'two-sided', 'less', 'greater'", getName());

        if (params.size() != 2)
            return;

        if (params[1].getType() != Field::Types::String)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require second parameter to be a String", getName());

        method = params[1].safeGet<String>();
        if (method != "auto" && method != "exact" && method != "asymp" && method != "asymptotic")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown method in aggregate function {}. "
                    "It must be one of: 'auto', 'exact', 'asymp' (or 'asymptotic')", getName());
    }

    String getName() const override
    {
        return "kolmogorovSmirnovTest";
    }

    bool allocatesMemoryInArena() const override { return true; }

    static DataTypePtr createResultType()
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>(),
        };

        Strings names
        {
            "d_statistic",
            "p_value"
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 value = columns[0]->getFloat64(row_num);
        UInt8 is_second = columns[1]->getUInt(row_num);
        if (is_second)
            data(place).addY(value, arena);
        else
            data(place).addX(value, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if (!data(place).size_x || !data(place).size_y)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} require both samples to be non empty", getName());

        auto [d_statistic, p_value] = data(place).getResult(alternative, method);

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(d_statistic);
        column_value.getData().push_back(p_value);
    }

};


AggregateFunctionPtr createAggregateFunctionKolmogorovSmirnovTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionKolmogorovSmirnov>(argument_types, parameters);
}


}

void registerAggregateFunctionKolmogorovSmirnovTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("kolmogorovSmirnovTest", createAggregateFunctionKolmogorovSmirnovTest, AggregateFunctionFactory::Case::Insensitive);
}

}
