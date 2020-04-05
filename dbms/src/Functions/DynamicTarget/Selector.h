#pragma once

#include "Target.h"

#include <Functions/IFunctionImpl.h>

#include <random>

namespace DB::DynamicTarget
{

// TODO(dakovalkov): This is copied and pasted struct from LZ4_decompress_faster.h

/** When decompressing uniform sequence of blocks (for example, blocks from one file),
  *  you can pass single PerformanceStatistics object to subsequent invocations of 'decompress' method.
  * It will accumulate statistics and use it as a feedback to choose best specialization of algorithm at runtime.
  * One PerformanceStatistics object cannot be used concurrently from different threads.
  */
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

    size_t size() {
        return data.size();
    }

    void emplace_back() {
        data.emplace_back();
    }

    PerformanceStatistics() {}
    PerformanceStatistics(ssize_t choose_method_) : choose_method(choose_method_) {}
};

// template <typename... Params>
// class PerformanceExecutor
// {
// public:
//     using Executor = std::function<void(Params...)>;
//     // Should register all executors before execute
//     void registerExecutor(Executor executor)
//     {
//         executors.emplace_back(std::move(executor));
//     }

//     // The performance of the execution is time / weight.
//     // Weight is usualy the 
//     void execute(int weight, Params... params)
//     {
//         if (executors_.empty()) {
//             throw "There are no realizations for current Arch";
//         }
//         int impl = 0;
//         // TODO: choose implementation.
//         executors_[impl](params...);
//     }

// private:
//     std::vector<Executor> executors;
//     PerformanceStatistics statistics;
// };

template <typename DefaultFunction>
class FunctionDynamicAdaptor : public DefaultFunction
{
public:
    template <typename ...Params>
    FunctionDynamicAdaptor(const Context & context_, Params ...params)
        : DefaultFunction(params...)
        , context(context_)
    {
        statistics.emplace_back();
    }

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        int id = statistics.select();
        // TODO(dakovalkov): measure time and change statistics.
        if (id == 0) {
            DefaultFunction::executeImpl(block, arguments, result, input_rows_count);
        } else {
            impls[id - 1]->executeImpl(block, arguments, result, input_rows_count);
        }
    }

protected:
    /*
     * Register implementation of the function.
     */  
    template<typename Function>
    void registerImplementation(TargetArch arch = TargetArch::Default) {
        if (arch == TargetArch::Default || IsArchSupported(arch)) {
            impls.emplace_back(Function::create(context));
            statistics.emplace_back();
        }
    }

private:
    const Context & context;
    std::vector<FunctionPtr> impls; // Alternative implementations.
    PerformanceStatistics statistics;
};

// TODO(dakovalkov): May be it's better to delete this macros and write every function explicitly for better readability.
#define DECLARE_STANDART_TARGET_ADAPTOR(Function) \
class Function : public FunctionDynamicAdaptor<TargetSpecific::Default::Function> \
{ \
public: \
    Function(const Context & context) : FunctionDynamicAdaptor(context) \
    { \
        registerImplementation<TargetSpecific::SSE4::Function>(TargetArch::SSE4); \
        registerImplementation<TargetSpecific::AVX::Function>(TargetArch::AVX); \
        registerImplementation<TargetSpecific::AVX2::Function>(TargetArch::AVX2); \
        registerImplementation<TargetSpecific::AVX512::Function>(TargetArch::AVX512); \
    } \
    static FunctionPtr create(const Context & context) \
    { \
        return std::make_shared<Function>(context); \
    } \
}

} // namespace DB::DynamicTarget
