#pragma once

#include <Functions/TargetSpecific.h>
#include <Functions/IFunctionImpl.h>

#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>

#include <random>

/// This file contains Adaptors which help to combine several implementations of the function.
/// Adaptors check that implementation can be executed on the current platform and choose
/// that one which works faster according to previous runs. 

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUITABLE_FUNCTION_IMPLEMENTATION;
}

// TODO(dakovalkov): This is copied and pasted struct from LZ4_decompress_faster.h with little changes.
struct PerformanceStatistics
{
    struct Element
    {
        double count = 0;
        double sum = 0;

        double adjustedCount() const
        {
            return count - NUM_INVOCATIONS_TO_THROW_OFF;
        }

        double mean() const
        {
            return sum / adjustedCount();
        }

        /// For better convergence, we don't use proper estimate of stddev.
        /// We want to eventually separate between two algorithms even in case
        ///  when there is no statistical significant difference between them.
        double sigma() const
        {
            return mean() / sqrt(adjustedCount());
        }

        void update(double seconds, double bytes)
        {
            ++count;

            if (count > NUM_INVOCATIONS_TO_THROW_OFF)
                sum += seconds / bytes;
        }

        double sample(pcg64 & stat_rng) const
        {
            /// If there is a variant with not enough statistics, always choose it.
            /// And in that case prefer variant with less number of invocations.

            if (adjustedCount() < 2)
                return adjustedCount() - 1;
            else
                return std::normal_distribution<>(mean(), sigma())(stat_rng);
        }
    };

    /// Cold invocations may be affected by additional memory latencies. Don't take first invocations into account.
    static constexpr double NUM_INVOCATIONS_TO_THROW_OFF = 2;

    /// How to select method to run.
    /// -1 - automatically, based on statistics (default);
    /// -2 - choose methods in round robin fashion (for performance testing).
    /// >= 0 - always choose specified method (for performance testing);
    ssize_t choose_method = -1;

    std::vector<Element> data;

    /// It's Ok that generator is not seeded.
    pcg64 rng;

    /// To select from different algorithms we use a kind of "bandits" algorithm.
    /// Sample random values from estimated normal distributions and choose the minimal.
    size_t select()
    {
        if (choose_method < 0)
        {
            std::vector<double> samples(data.size());
            for (size_t i = 0; i < data.size(); ++i)
                samples[i] = choose_method == -1
                    ? data[i].sample(rng)
                    : data[i].adjustedCount();

            return std::min_element(samples.begin(), samples.end()) - samples.begin();
        }
        else
            return choose_method;
    }

    size_t size() const
    {
        return data.size();
    }

    bool empty() const
    {
        return size() == 0;
    }

    void emplace_back()
    {
        data.emplace_back();
    }

    PerformanceStatistics() {}
    PerformanceStatistics(ssize_t choose_method_) : choose_method(choose_method_) {}
};

/* Class which is used to store implementations for the function and selecting the best one to run
 * based on processor architecture and statistics from previous runs.
 * 
 * FunctionInterface is typically IFunction or IExecutableFunctionImpl, but practically it can be
 * any interface that contains "execute" method (IFunction is an exception and is supported as well).
 * 
 * Example of usage:
 * 
 * class MyDefaulImpl : public IFunction {...};
 * class MySecondImpl : public IFunction {...};
 * class MyAVX2Impl : public IFunction {...};
 * 
 * /// All methods but execute/executeImpl are usually not bottleneck, so just use them from
 * /// default implementation.
 * class MyFunction : public MyDefaultImpl
 * {
 *     MyFunction(const Context & context) : selector(context) {
 *         /// Register all implementations in constructor.
 *         /// There could be as many implementation for every target as you want.
 *         selector.registerImplementation<TargetArch::Default, MyDefaultImpl>();
 *         selector.registerImplementation<TargetArch::Default, MySecondImpl>();
 *         selector.registreImplementation<TargetArch::AVX2, MyAVX2Impl>();
 *     }
 *
 *     void executeImpl(...) override {
 *         selector.selectAndExecute(...);
 *     }
 *
 *     static FunctionPtr create(const Context & context) {
 *         return std::make_shared<MyFunction>(context);
 *     }
 * private:
 *     ImplementationSelector<IFunction> selector;
 * };
 */
template <typename FunctionInterface>
class ImplementationSelector
{
public:
    using ImplementationPtr = std::shared_ptr<FunctionInterface>;

    ImplementationSelector(const Context & context_) : context(context_) {}

    /* Select the best implementation based on previous runs.
     * If FunctionInterface is IFunction, then "executeImpl" method of the implementation will be called
     * and "execute" otherwise.
     */
    void selectAndExecute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        if (implementations.empty())
            throw Exception("There are no available implementations for function " "TODO(dakovalkov): add name",
                            ErrorCodes::NO_SUITABLE_FUNCTION_IMPLEMENTATION);

        auto id = statistics.select();
        Stopwatch watch;

        if constexpr (std::is_same_v<FunctionInterface, IFunction>)
            implementations[id]->executeImpl(block, arguments, result, input_rows_count);
        else
            implementations[id]->execute(block, arguments, result, input_rows_count);
        
        watch.stop();

        // TODO(dakovalkov): Calculate something more informative.
        size_t rows_summary = 0;
        for (auto i : arguments)
        {
            rows_summary += block.getByPosition(i).column->size();
        }
        rows_summary += block.getByPosition(result).column->size();

        if (rows_summary >= 1000)
        {
            statistics.data[id].update(watch.elapsedSeconds(), rows_summary);
        }
    }

    /* Register new implementation for function.
     *
     * Arch - required instruction set for running the implementation. It's guarantied that no one method would
     * be called (even the constructor and static methods) if the processor doesn't support this instruction set.
     * 
     * FunctionImpl - implementation, should be inherited from template argument FunctionInterface.
     * 
     * All function arguments will be forwarded to the implementation constructor.
     */
    template <TargetArch Arch, typename FunctionImpl, typename ...Args>
    void registerImplementation(Args&&... args)
    {
        if (IsArchSupported(Arch))
        {
            // TODO(dakovalkov): make this option better.
            const auto & choose_impl = context.getSettingsRef().function_implementation.value;
            if (choose_impl.empty() || choose_impl == FunctionImpl::getImplementationTag())
            {
                implementations.emplace_back(std::make_shared<FunctionImpl>(std::forward<Args>(args)...));
                statistics.emplace_back();
            }
        }
    }

private:
    const Context & context;
    std::vector<ImplementationPtr> implementations;
    PerformanceStatistics statistics;
};

}
