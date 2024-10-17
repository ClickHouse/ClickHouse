#pragma once

#include <Processors/IProcessor.h>
#include <queue>


namespace DB
{

/** Has arbitrary non zero number of inputs and arbitrary non zero number of outputs.
  * All of them have the same structure.
  *
  * Pulls data from arbitrary input (whenever it is ready) and shuffle the input data into
  * several buckets according to the value of special column(e.g. ID column), then 
  * pushes the shuffled data to output.
  * Doesn't do any heavy calculations.
  * Doesn't preserve an order of data.
  *
  * Examples:
  * - shuffle data by ID column into 4 buckets, push data to 4 outputs.
  */
class ShuffleProcessor final : public IProcessor
{
public:
    ShuffleProcessor(const Block & header, size_t shuffle_optimize_buckets, size_t shuffle_optimize_max)
        : IProcessor(InputPorts(1, header), OutputPorts(shuffle_optimize_buckets, header))
        , num_unfinished_outputs{shuffle_optimize_buckets}
    {
        if (shuffle_optimize_buckets < 2)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Number of shuffle buckets must be at least 2");
        }

        if (shuffle_optimize_max < shuffle_optimize_buckets - 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Max key value must be at least number of shuffle buckets - 1");

        }

        thresholds.resize(shuffle_optimize_buckets-1);
        for (size_t i = 1; i < shuffle_optimize_buckets; ++i)
        {
            thresholds[i-1] = shuffle_optimize_max * i / shuffle_optimize_buckets + 1;
        }
    }

    String getName() const override { return "Shuffle"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    enum class OutputStatus
    {
        NotActive, // 未准备好
        NeedData, // 需要数据输出，处于可输出数据的状态
        PortFull, // 输出端口有数据未被下游处理
        Finished, // 结束，端口关闭
    };

    enum class InputStatus
    {
        NotActive, // 未准备好
        HasData, // 有输入数据待处理
        Finished, // 结束，端口关闭
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    bool initialized{false};
    size_t num_unfinished_outputs;
    InputPortWithStatus input_port;
    std::vector<OutputPortWithStatus> output_ports;
    std::vector<size_t> thresholds;
};

}
