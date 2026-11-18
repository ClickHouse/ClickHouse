#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/Sinks/EmptySink.h>

using namespace DB;
using namespace DB::Runtime::V1;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

/// Works once, then becomes asynchronous, and the executor fails to schedule it.
class ThrowingScheduleSource final : public IProcessor
{
public:
    ThrowingScheduleSource(SharedHeader header, const bool & exception_callback_called_)
        : IProcessor({}, OutputPorts(1, header))
        , exception_callback_called(exception_callback_called_)
    {
    }

    String getName() const override { return "ThrowingScheduleSource"; }

    Status prepare() override
    {
        if (outputs.front().isFinished())
            return Status::Finished;
        return worked ? Status::Async : Status::Ready;
    }

    void work() override { worked = true; }

    std::tuple<int, uint32_t, Int64> scheduleForEvent() override
    {
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Cannot schedule the processor");
    }

    bool cancelled = false;
    bool exception_callback_called_on_cancel = false;

protected:
    void onCancel() noexcept override
    {
        cancelled = true;
        exception_callback_called_on_cancel = exception_callback_called;
    }

private:
    const bool & exception_callback_called;
    bool worked = false;
};

}

/// A processor can react to its cancellation by making other processors throw (the in-memory exchanges of a
/// distributed plan), so the exception that stops the execution must be recorded before the processors are cancelled.
TEST(PipelineExecutor, PushTasksFailureIsRecordedBeforeCancellingProcessors)
{
    bool exception_callback_called = false;

    SharedHeader header = std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
    auto source = std::make_shared<ThrowingScheduleSource>(header, exception_callback_called);
    auto sink = std::make_shared<EmptySink>(header);
    connect(source->getOutputs().front(), sink->getPort());
    auto processors = std::make_shared<Processors>(Processors{source, sink});

    PipelineExecutor executor(processors, /*elem=*/ nullptr);
    executor.setExceptionCallback([&] { exception_callback_called = true; });

    try
    {
        executor.execute(1, /*concurrency_control=*/ false);
        FAIL() << "The execution did not fail";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    EXPECT_TRUE(exception_callback_called);
    EXPECT_TRUE(source->cancelled);
    EXPECT_TRUE(source->exception_callback_called_on_cancel);
}

#endif
