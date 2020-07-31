#pragma once

#include <Functions/TargetSpecific.h>
#include <Functions/IFunctionImpl.h>

#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>

#include <mutex>
#include <random>

/* This file contains helper class ImplementationSelector. It makes easier to combine
 * several implementations of IFunction/IExecutableFunctionImpl.
 */

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUITABLE_FUNCTION_IMPLEMENTATION;
}

namespace detail
{
    class PerformanceStatistics
    {
    public:
        size_t select(bool considarable)
        {
            /// We don't need to choose/measure anything if there's only one variant.
            if (size() == 1)
                return 0;

            std::lock_guard guard(lock);

            size_t best = 0;
            double best_sample = data[0].sample(rng);

            for (size_t i = 1; i < data.size(); ++i)
            {
                double sample = data[i].sample(rng);
                if (sample < best_sample)
                {
                    best_sample = sample;
                    best = i;
                }
            }

            if (considarable)
                data[best].run();

            return best;
        }

        void complete(size_t id, double seconds, double bytes)
        {
            if (size() == 1)
                return;

            std::lock_guard guard(lock);
            data[id].complete(seconds, bytes);
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

    private:
        struct Element
        {
            int completed_count = 0;
            int running_count = 0;
            double sum = 0;

            int adjustedCount() const
            {
                return completed_count - NUM_INVOCATIONS_TO_THROW_OFF;
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

            void run()
            {
                ++running_count;
            }

            void complete(double seconds, double bytes)
            {
                --running_count;
                ++completed_count;

                if (adjustedCount() > 0)
                    sum += seconds / bytes;
            }

            double sample(pcg64 & stat_rng) const
            {
                /// If there is a variant with not enough statistics, always choose it.
                /// And in that case prefer variant with less number of invocations.

                if (adjustedCount() < 2)
                    return adjustedCount() - 1 + running_count;
                return std::normal_distribution<>(mean(), sigma())(stat_rng);
            }
        };

        std::vector<Element> data;
        std::mutex lock;
        /// It's Ok that generator is not seeded.
        pcg64 rng;
        /// Cold invocations may be affected by additional memory latencies. Don't take first invocations into account.
        static constexpr int NUM_INVOCATIONS_TO_THROW_OFF = 2;
    };

    template <typename T, class = decltype(T::getImplementationTag())>
    std::true_type hasImplementationTagTest(const T&);
    std::false_type hasImplementationTagTest(...);

    template <typename T>
    constexpr bool has_implementation_tag = decltype(hasImplementationTagTest(std::declval<T>()))::value;

    /* Implementation tag is used to run specific implementation (for debug/testing purposes).
     * It can be specified via static method ::getImplementationTag() in Function (optional).
     */
    template <typename T>
    String getImplementationTag(TargetArch arch)
    {
        if constexpr (has_implementation_tag<T>)
            return toString(arch) + "_" + T::getImplementationTag();
        else
            return toString(arch);
    }
}

/* Class which is used to store implementations for the function and to select the best one to run
 * based on processor architecture and statistics from previous runs.
 *
 * FunctionInterface is typically IFunction or IExecutableFunctionImpl, but practically it can be
 * any interface that contains "execute" method (IFunction is an exception and is supported as well).
 *
 * Example of usage:
 *
 * class MyDefaulImpl : public IFunction {...};
 * DECLARE_AVX2_SPECIFIC_CODE(
 * class MyAVX2Impl : public IFunction {...};
 * )
 *
 * /// All methods but execute/executeImpl are usually not bottleneck, so just use them from
 * /// default implementation.
 * class MyFunction : public MyDefaultImpl
 * {
 *     MyFunction(const Context & context) : selector(context) {
 *         /// Register all implementations in constructor.
 *         /// There could be as many implementation for every target as you want.
 *         selector.registerImplementation<TargetArch::Default, MyDefaultImpl>();
 *     #if USE_MULTITARGET_CODE
 *         selector.registerImplementation<TargetArch::AVX2, TargetSpecific::AVX2::MyAVX2Impl>();
 *     #endif
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
    void selectAndExecute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
    {
        if (implementations.empty())
            throw Exception("There are no available implementations for function " "TODO(dakovalkov): add name",
                            ErrorCodes::NO_SUITABLE_FUNCTION_IMPLEMENTATION);

        /// Statistics shouldn't rely on small blocks.
        bool considerable = (input_rows_count > 1000);

        size_t id = statistics.select(considerable);
        Stopwatch watch;

        if constexpr (std::is_same_v<FunctionInterface, IFunction>)
            implementations[id]->executeImpl(block, arguments, result, input_rows_count);
        else
            implementations[id]->execute(block, arguments, result, input_rows_count);

        watch.stop();

        if (considerable)
        {
            // TODO(dakovalkov): Calculate something more informative than rows count.
            statistics.complete(id, watch.elapsedSeconds(), input_rows_count);
        }
    }

    /* Register new implementation for function.
     *
     * Arch - required instruction set for running the implementation. It's guarantied that no method would
     * be called (even the constructor and static methods) if the processor doesn't support this instruction set.
     *
     * FunctionImpl - implementation, should be inherited from template argument FunctionInterface.
     *
     * All function arguments will be forwarded to the implementation constructor.
     */
    template <TargetArch Arch, typename FunctionImpl, typename ...Args>
    void registerImplementation(Args &&... args)
    {
        if (isArchSupported(Arch))
        {
            // TODO(dakovalkov): make this option better.
            const auto & choose_impl = context.getSettingsRef().function_implementation.value;
            if (choose_impl.empty() || choose_impl == detail::getImplementationTag<FunctionImpl>(Arch))
            {
                implementations.emplace_back(std::make_shared<FunctionImpl>(std::forward<Args>(args)...));
                statistics.emplace_back();
            }
        }
    }

private:
    const Context & context;
    std::vector<ImplementationPtr> implementations;
    mutable detail::PerformanceStatistics statistics; /// It is protected by internal mutex.
};

}
