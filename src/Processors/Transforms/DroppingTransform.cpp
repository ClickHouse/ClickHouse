#include <Processors/Transforms/DroppingTransform.h>

#include <Processors/Port.h>

namespace DB
{

static InputPorts createInputPorts(SharedHeader header, size_t num_streams, bool has_totals, bool has_extremes)
{
    InputPorts res;
    for (size_t i = 0; i < num_streams; ++i)
        res.emplace_back(header);
    if (has_totals)
        res.emplace_back(header);
    if (has_extremes)
        res.emplace_back(header);
    return res;
}

DroppingTransform::DroppingTransform(SharedHeader header, size_t num_streams_, bool has_totals, bool has_extremes)
    : IProcessor(createInputPorts(header, num_streams_, has_totals, has_extremes), OutputPorts(num_streams_, header))
    , num_streams(num_streams_)
{
    data_inputs.reserve(num_streams);
    data_outputs.reserve(num_streams);

    auto input_it = inputs.begin();
    for (size_t i = 0; i < num_streams; ++i, ++input_it)
        data_inputs.push_back(&*input_it);

    if (has_totals)
    {
        totals_input = &*input_it;
        ++input_it;
    }

    if (has_extremes)
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

    /// Totals / extremes: drain and discard.
    for (InputPort * aux : {totals_input, extremes_input})
    {
        if (!aux)
            continue;
        if (aux->isFinished())
            continue;
        aux->setNeeded();
        if (aux->hasData())
            aux->pull(); /// discard
        else
            need_data = true;
    }

    if (all_outputs_done)
    {
        /// Close any still-open aux inputs so upstream can stop.
        if (totals_input)
            totals_input->close();
        if (extremes_input)
            extremes_input->close();
        return Status::Finished;
    }

    if (need_data)
        return Status::NeedData;

    return Status::PortFull;
}

}
