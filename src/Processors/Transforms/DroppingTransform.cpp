#include <Processors/Transforms/DroppingTransform.h>

#include <Processors/Port.h>

namespace DB
{

static InputPorts createInputPorts(
    SharedHeader header, size_t num_streams, SharedHeader totals_header, SharedHeader extremes_header)
{
    InputPorts res;
    for (size_t i = 0; i < num_streams; ++i)
        res.emplace_back(header);
    if (totals_header)
        res.emplace_back(totals_header);
    if (extremes_header)
        res.emplace_back(extremes_header);
    return res;
}

DroppingTransform::DroppingTransform(
    SharedHeader header, size_t num_streams_, SharedHeader totals_header, SharedHeader extremes_header)
    : IProcessor(
        createInputPorts(header, num_streams_, totals_header, extremes_header), OutputPorts(num_streams_, header))
    , num_streams(num_streams_)
{
    data_inputs.reserve(num_streams);
    data_outputs.reserve(num_streams);

    auto input_it = inputs.begin();
    for (size_t i = 0; i < num_streams; ++i, ++input_it)
        data_inputs.push_back(&*input_it);

    if (totals_header)
    {
        totals_input = &*input_it;
        ++input_it;
    }

    if (extremes_header)
    {
        extremes_input = &*input_it;
        ++input_it;
    }

    for (auto & output : outputs)
        data_outputs.push_back(&output);
}

IProcessor::Status DroppingTransform::prepare()
{
    bool all_outputs_done = true;
    bool need_data = false;

    /// Data streams: 1:1 forward.
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto & output = *data_outputs[i];
        auto & input = *data_inputs[i];

        if (output.isFinished())
        {
            input.close();
            continue;
        }

        if (input.isFinished())
        {
            output.finish();
            continue;
        }

        /// Only count a pair as still-active after we know it is neither output-finished
        /// nor (input-finished -> just finished here). Otherwise, when all inputs finish in
        /// this same prepare() call, we would return PortFull and never be rescheduled to
        /// report Finished, stalling the pipeline ("Pipeline stuck").
        all_outputs_done = false;

        if (!output.canPush())
            continue; /// PortFull for this stream; will be revisited.

        input.setNeeded();
        if (input.hasData())
            output.push(input.pull());
        else
            need_data = true;
    }

    /// Never request the dropped streams: a `TotalsHavingTransform` above a discarded totals port would
    /// evaluate `HAVING` on the totals row. Close them only once the data streams are done, because
    /// closing a `DelayedPortsProcessor` output finishes its pair and releases the gate.
    if (all_outputs_done)
    {
        for (InputPort * aux : {totals_input, extremes_input})
        {
            if (aux)
                aux->close();
        }
        return Status::Finished;
    }

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}

}
