#include <Processors/QueryPlan/ConvertingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

ConvertingStep::ConvertingStep(const DataStream & input_stream_, Block result_header_, bool ignore_constant_values_)
    : ITransformingStep(input_stream_, result_header_, getTraits())
    , result_header(std::move(result_header_))
    , ignore_constant_values(ignore_constant_values_)
{
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void ConvertingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ConvertingTransform>(header, result_header, ConvertingTransform::MatchColumnsMode::Name, ignore_constant_values);
    });
}

void ConvertingStep::describeActions(FormatSettings & settings) const
{
    const auto & header = input_streams[0].header;
    auto conversion = ConvertingTransform(header, result_header, ConvertingTransform::MatchColumnsMode::Name, ignore_constant_values)
            .getConversion();

    auto dump_description = [&](const ColumnWithTypeAndName & elem, bool is_const)
    {
        settings.out << elem.name << ' ' << elem.type->getName() << (is_const ? " Const" : "") << '\n';
    };

    String prefix(settings.offset, ' ');

    for (size_t i = 0; i < conversion.size(); ++i)
    {
        const auto & from = header.getByPosition(conversion[i]);
        const auto & to = result_header.getByPosition(i);

        bool from_const = from.column && isColumnConst(*from.column);
        bool to_const = to.column && isColumnConst(*to.column);

        settings.out << prefix;

        if (from.name == to.name && from.type->equals(*to.type) && from_const == to_const)
            dump_description(from, from_const);
        else
        {
            dump_description(to, to_const);
            settings.out << " ← ";
            dump_description(from, from_const);
        }

        settings.out << '\n';
    }
}

}
